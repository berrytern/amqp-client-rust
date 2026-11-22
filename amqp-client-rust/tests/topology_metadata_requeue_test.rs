mod base;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use amqp_client_rust::{
    api::{
        eventbus::AsyncEventbusRabbitMQ,
        utils::{ExchangeOptions, Message, PublishOptions, QueueOptions, RpcClientOptions, Topology},
    },
    domain::config::QoSConfig,
    errors::RequeueError,
};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

use crate::base::{cleanup_test_resources, create_test_config};

#[tokio::test]
async fn test_declare_topology_fail_fast_and_passive() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());

    let ex_name = format!("topo_ex_{}", Uuid::new_v4());
    let q_name = format!("topo_q_{}", Uuid::new_v4());
    let non_existent_ex = format!("missing_ex_{}", Uuid::new_v4());

    // 1. Passive declare on a non-existent exchange must fail fast
    let passive_res = eventbus
        .declare_exchange(
            &non_existent_ex,
            ExchangeOptions::topic().no_create(true),
            Some(Duration::from_secs(5)),
        )
        .await;
    assert!(passive_res.is_err(), "Passive declare on missing exchange should fail fast");

    // 2. Declare full topology (direct exchange + queue with typed TTL & max_length + binding)
    let topology = Topology::new()
        .exchange(&ex_name, ExchangeOptions::direct().durable(true))
        .queue(
            &q_name,
            QueueOptions::new()
                .durable(true)
                .message_ttl(60_000)
                .max_length(1_000),
        )
        .bind(&q_name, &ex_name, "orders.created");

    let topo_res = eventbus
        .declare_topology(topology, Some(Duration::from_secs(5)))
        .await;
    assert!(topo_res.is_ok(), "declare_topology failed: {:?}", topo_res.err());

    // 3. Passive declare on the now-existing exchange must succeed
    let passive_ok = eventbus
        .declare_exchange(
            &ex_name,
            ExchangeOptions::direct().no_create(true),
            Some(Duration::from_secs(5)),
        )
        .await;
    assert!(passive_ok.is_ok(), "Passive declare on existing exchange failed: {:?}", passive_ok.err());

    // 4. Declaring the same exchange with a conflicting type (fanout vs direct) must fail fast
    let conflict_res = eventbus
        .declare_exchange(
            &ex_name,
            ExchangeOptions::fanout().durable(true),
            Some(Duration::from_secs(5)),
        )
        .await;
    assert!(conflict_res.is_err(), "Conflicting exchange type should fail fast");

    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[&ex_name]).await;
}

#[tokio::test]
async fn test_lazy_exchange_cache_opt_in_publish_and_rpc() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());

    // Brand new exchange that does NOT exist yet on the broker
    let lazy_ex = format!("lazy_pub_ex_{}", Uuid::new_v4());
    let pub_opts = PublishOptions::new()
        .with_content_type("text/plain")
        .with_command_timeout(Duration::from_secs(5))
        .with_exchange(ExchangeOptions::topic().durable(false));

    // 1st publish declares the exchange lazily and caches it; 2nd & 3rd hit the local cache
    for i in 0..3 {
        let res = eventbus
            .publish(&lazy_ex, "lazy.key", format!("msg-{}", i).into_bytes(), &pub_opts)
            .await;
        assert!(res.is_ok(), "Lazy publish #{} failed: {:?}", i, res.err());
    }

    // RPC client with opt-in exchange declaration
    let rpc_key = format!("lazy_rpc_key_{}", Uuid::new_v4());
    eventbus
        .provide_resource(
            &rpc_key,
            |req: Message| {
                Box::pin(async move {
                    let corr = req.correlation_id().map(|s| s.to_string());
                    assert!(corr.is_some(), "RPC server should receive correlation_id in metadata");
                    assert_eq!(req.routing_key(), Some(req.routing_key().unwrap()));
                    Ok(Message::new(b"ok".to_vec(), Some("text/plain".to_string())))
                })
            },
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("provide_resource");

    let rpc_opts = RpcClientOptions::new()
        .with_content_type("text/plain")
        .with_response_timeout_millis(5000)
        .with_command_timeout(Duration::from_secs(5))
        .with_exchange(ExchangeOptions::topic().durable(true));

    let resp = eventbus
        .rpc_client(&config.options.rpc_exchange_name, &rpc_key, b"ping".to_vec(), &rpc_opts)
        .await;
    assert_eq!(resp.expect("rpc call"), b"ok".to_vec());

    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[&lazy_ex]).await;
}

#[tokio::test]
async fn test_message_metadata_wildcard_delivery() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());

    let exchange_name = format!("meta_ex_{}", Uuid::new_v4());
    let notify = Arc::new(Notify::new());
    let notify_clone = Arc::clone(&notify);

    let captured_rk = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let captured_ex = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let captured_redelivered = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let captured_tag = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let rk_clone = Arc::clone(&captured_rk);
    let ex_clone = Arc::clone(&captured_ex);
    let redel_clone = Arc::clone(&captured_redelivered);
    let tag_clone = Arc::clone(&captured_tag);

    eventbus
        .subscribe(
            &exchange_name,
            "orders.#",
            move |msg: Message| {
                let notify = Arc::clone(&notify_clone);
                let rk_slot = Arc::clone(&rk_clone);
                let ex_slot = Arc::clone(&ex_clone);
                let redel_slot = Arc::clone(&redel_clone);
                let tag_slot = Arc::clone(&tag_clone);
                Box::pin(async move {
                    *rk_slot.lock().await = msg.routing_key().map(|s| s.to_string());
                    *ex_slot.lock().await = msg.exchange().map(|s| s.to_string());
                    redel_slot.store(msg.redelivered(), Ordering::SeqCst);
                    tag_slot.store(msg.delivery_tag().unwrap_or(0), Ordering::SeqCst);
                    notify.notify_one();
                    Ok(())
                })
            },
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("subscribe");

    let pub_opts = PublishOptions::new()
        .with_content_type("text/plain")
        .with_command_timeout(Duration::from_secs(5));

    eventbus
        .publish(&exchange_name, "orders.brazil.created", b"payload".to_vec(), &pub_opts)
        .await
        .expect("publish");

    timeout(Duration::from_secs(5), notify.notified())
        .await
        .expect("timed out waiting for message");

    assert_eq!(captured_rk.lock().await.as_deref(), Some("orders.brazil.created"));
    assert_eq!(captured_ex.lock().await.as_deref(), Some(exchange_name.as_str()));
    assert!(!captured_redelivered.load(Ordering::SeqCst));
    assert!(captured_tag.load(Ordering::SeqCst) > 0);

    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[&exchange_name]).await;
}

#[tokio::test]
async fn test_declarative_requeue_error_redelivers_message() {
    let config = create_test_config();
    let qos = QoSConfig::default().with_sub_auto_ack(false);
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), qos);

    let exchange_name = format!("requeue_ex_{}", Uuid::new_v4());
    let attempts = Arc::new(AtomicUsize::new(0));
    let saw_redelivered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let notify = Arc::new(Notify::new());

    let attempts_clone = Arc::clone(&attempts);
    let saw_redel_clone = Arc::clone(&saw_redelivered);
    let notify_clone = Arc::clone(&notify);

    eventbus
        .subscribe(
            &exchange_name,
            "jobs.process",
            move |msg: Message| {
                let attempts = Arc::clone(&attempts_clone);
                let saw_redel = Arc::clone(&saw_redel_clone);
                let notify = Arc::clone(&notify_clone);
                Box::pin(async move {
                    let count = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if count == 1 {
                        assert!(!msg.redelivered(), "First delivery must not be marked redelivered");
                        return Err(RequeueError::new("transient db failure").into());
                    }
                    saw_redel.store(msg.redelivered(), Ordering::SeqCst);
                    notify.notify_one();
                    Ok(())
                })
            },
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("subscribe");

    let pub_opts = PublishOptions::new()
        .with_content_type("text/plain")
        .with_command_timeout(Duration::from_secs(5));

    eventbus
        .publish(&exchange_name, "jobs.process", b"retry-me".to_vec(), &pub_opts)
        .await
        .expect("publish");

    timeout(Duration::from_secs(5), notify.notified())
        .await
        .expect("timed out waiting for requeued message redelivery");

    assert_eq!(attempts.load(Ordering::SeqCst), 2, "Handler should have been invoked twice");
    assert!(
        saw_redelivered.load(Ordering::SeqCst),
        "Second delivery must have redelivered == true"
    );

    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[&exchange_name]).await;
}
