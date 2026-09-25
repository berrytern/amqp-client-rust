use amqp_client_rust::errors::{AppError, AppErrorType};
use std::collections::BTreeMap;
use tokio::sync::oneshot;

/// Helper replicating the exact ACK/NACK draining algorithm from ConnectionManager::run
fn handle_publish_ack(
    pending: &mut BTreeMap<u64, oneshot::Sender<Result<(), AppError>>>,
    tag: u64,
    multiple: bool,
) {
    if multiple {
        while let Some(entry) = pending.first_entry() {
            if entry.key() > &tag {
                break;
            }
            let confirm = entry.remove();
            let _ = confirm.send(Ok(()));
        }
    } else if let Some(confirm) = pending.remove(&tag) {
        let _ = confirm.send(Ok(()));
    }
}

fn handle_publish_nack(
    pending: &mut BTreeMap<u64, oneshot::Sender<Result<(), AppError>>>,
    tag: u64,
    multiple: bool,
) {
    if multiple {
        while let Some(entry) = pending.first_entry() {
            if entry.key() > &tag {
                break;
            }
            let confirm = entry.remove();
            let _ = confirm.send(Err(AppError {
                message: None,
                description: None,
                error_type: AppErrorType::NackError,
            }));
        }
    } else if let Some(confirm) = pending.remove(&tag) {
        let _ = confirm.send(Err(AppError {
            message: None,
            description: None,
            error_type: AppErrorType::NackError,
        }));
    }
}

#[tokio::test]
async fn test_single_ack_resolves_only_target_tag() {
    let mut pending = BTreeMap::new();
    let (tx1, mut rx1) = oneshot::channel();
    let (tx2, mut rx2) = oneshot::channel();
    let (tx3, mut rx3) = oneshot::channel();

    pending.insert(1, tx1);
    pending.insert(2, tx2);
    pending.insert(3, tx3);

    // Single ACK for tag 2
    handle_publish_ack(&mut pending, 2, false);

    assert_eq!(pending.len(), 2, "Pending map should contain remaining tags 1 and 3");
    assert!(pending.contains_key(&1));
    assert!(!pending.contains_key(&2));
    assert!(pending.contains_key(&3));

    // rx2 must be resolved with Ok(())
    let res2 = rx2.try_recv();
    assert!(matches!(res2, Ok(Ok(()))));

    // rx1 and rx3 must NOT be resolved yet
    assert!(rx1.try_recv().is_err());
    assert!(rx3.try_recv().is_err());
}

#[tokio::test]
async fn test_multiple_ack_resolves_all_preceding_tags() {
    let mut pending = BTreeMap::new();
    let (tx1, mut rx1) = oneshot::channel();
    let (tx2, mut rx2) = oneshot::channel();
    let (tx3, mut rx3) = oneshot::channel();
    let (tx4, mut rx4) = oneshot::channel();

    pending.insert(1, tx1);
    pending.insert(2, tx2);
    pending.insert(3, tx3);
    pending.insert(4, tx4);

    // Multiple ACK up to tag 3
    handle_publish_ack(&mut pending, 3, true);

    // Only tag 4 should remain
    assert_eq!(pending.len(), 1);
    assert!(pending.contains_key(&4));

    // rx1, rx2, rx3 must all be Ok(())
    assert!(matches!(rx1.try_recv(), Ok(Ok(()))));
    assert!(matches!(rx2.try_recv(), Ok(Ok(()))));
    assert!(matches!(rx3.try_recv(), Ok(Ok(()))));

    // rx4 must NOT be resolved
    assert!(rx4.try_recv().is_err());
}

#[tokio::test]
async fn test_single_nack_resolves_with_nack_error() {
    let mut pending = BTreeMap::new();
    let (tx1, mut rx1) = oneshot::channel();
    let (tx2, mut rx2) = oneshot::channel();

    pending.insert(1, tx1);
    pending.insert(2, tx2);

    // Single NACK for tag 1
    handle_publish_nack(&mut pending, 1, false);

    assert_eq!(pending.len(), 1);
    assert!(pending.contains_key(&2));

    let res1 = rx1.try_recv();
    assert!(matches!(res1, Ok(Err(e)) if e.error_type == AppErrorType::NackError));
    assert!(rx2.try_recv().is_err());
}

#[tokio::test]
async fn test_multiple_nack_resolves_all_preceding_with_nack_error() {
    let mut pending = BTreeMap::new();
    let (tx1, mut rx1) = oneshot::channel();
    let (tx2, mut rx2) = oneshot::channel();
    let (tx3, mut rx3) = oneshot::channel();

    pending.insert(1, tx1);
    pending.insert(2, tx2);
    pending.insert(3, tx3);

    // Multiple NACK for tag 2
    handle_publish_nack(&mut pending, 2, true);

    assert_eq!(pending.len(), 1);
    assert!(pending.contains_key(&3));

    let res1 = rx1.try_recv();
    let res2 = rx2.try_recv();
    assert!(matches!(res1, Ok(Err(e)) if e.error_type == AppErrorType::NackError));
    assert!(matches!(res2, Ok(Err(e)) if e.error_type == AppErrorType::NackError));
    assert!(rx3.try_recv().is_err());
}

// -----------------------------------------------------------------
// Integration Test: Rapid Sequential Publishes with Confirms Enabled
// -----------------------------------------------------------------

mod base;
use base::{cleanup_test_resources, create_test_config};
use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    api::utils::PublishOptions,
    domain::config::QoSConfig,
};
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn test_publisher_confirms_rapid_stream() {
    let config = create_test_config();
    let qos_config = QoSConfig {
        pub_confirm: true,
        ..Default::default()
    };

    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), qos_config);
    let exchange_name = format!("test_confirms_ex_{}", Uuid::new_v4());
    let routing_key = format!("test_confirms_key_{}", Uuid::new_v4());

    // Subscribe to declare exchange and queue
    eventbus.subscribe(
        &exchange_name,
        &routing_key,
        |_msg| Box::pin(async { Ok(()) }),
        None,
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe to declare exchange");

    // Publish 50 messages rapidly in sequence, each awaiting broker confirm
    for i in 0..50 {
        let payload = format!("Confirmed message #{}", i).into_bytes();
        let pub_res = eventbus.publish(
            &exchange_name,
            &routing_key,
            payload,
            &PublishOptions::default()
                .with_content_type("text/plain")
                .with_command_timeout(Duration::from_secs(5)),
        ).await;

        assert!(pub_res.is_ok(), "Publish #{} with confirm failed: {:?}", i, pub_res.err());
    }

    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[&exchange_name]).await;
}
