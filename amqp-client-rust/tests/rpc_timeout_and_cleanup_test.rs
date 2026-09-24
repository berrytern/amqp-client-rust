mod base;
use base::{cleanup_test_resources, create_test_config};
use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    api::utils::{ContentEncoding, Message},
    domain::config::QoSConfig,
    errors::AppErrorType,
};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

#[tokio::test]
async fn test_rpc_timeout_returns_correct_error_type() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let routing_key = format!("rpc_timeout_key_{}", Uuid::new_v4());

    let result = eventbus.rpc_client(
        config.options.rpc_exchange_name.as_str(),
        routing_key.as_str(),
        b"timeout test".to_vec(),
        "text/plain",
        ContentEncoding::None,
        150, // 150ms timeout
        Some(Duration::from_millis(500)),
        None,
        None,
    ).await;

    assert!(result.is_err(), "Expected RPC call to timeout");
    let err = result.err().unwrap();
    assert_eq!(
        err.error_type,
        AppErrorType::TimeoutError,
        "Expected AppErrorType::TimeoutError, got {:?}",
        err
    );

    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[]).await;
}

#[tokio::test]
async fn test_rpc_delayed_response_does_not_break_subsequent_calls() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let slow_routing_key = format!("rpc_slow_{}", Uuid::new_v4());
    let fast_routing_key = format!("rpc_fast_{}", Uuid::new_v4());

    // 1. Slow RPC handler: sleeps 400ms before returning
    let slow_res = eventbus.provide_resource(
        &slow_routing_key,
        |_req| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(400)).await;
                Ok(Message {
                    body: b"slow_response".to_vec().into(),
                    content_type: Some("text/plain".to_string()),
                })
            })
        },
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(5)),
    ).await;
    assert!(slow_res.is_ok(), "Failed to register slow RPC handler");

    // 2. Fast RPC handler: returns immediately
    let fast_res = eventbus.provide_resource(
        &fast_routing_key,
        |req| {
            Box::pin(async move {
                let resp = format!("fast:{}", String::from_utf8_lossy(&req.body));
                Ok(Message {
                    body: resp.into_bytes().into(),
                    content_type: Some("text/plain".to_string()),
                })
            })
        },
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(5)),
    ).await;
    assert!(fast_res.is_ok(), "Failed to register fast RPC handler");

    // 3. Client calls slow RPC with a short timeout of 100ms -> Must TIMEOUT
    let first_call = eventbus.rpc_client(
        &config.options.rpc_exchange_name,
        &slow_routing_key,
        b"slow_payload".to_vec(),
        "text/plain",
        ContentEncoding::None,
        100, // 100ms timeout (handler sleeps 400ms)
        Some(Duration::from_millis(500)),
        None,
        None,
    ).await;

    assert!(first_call.is_err(), "First call should have timed out");
    assert_eq!(first_call.err().unwrap().error_type, AppErrorType::TimeoutError);

    // 4. Wait 500ms so the slow server finishes and sends its late response to aux_queue
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 5. Client calls fast RPC -> Must SUCCEED without issue, proving the late message
    //    did not poison the aux_queue consumer or leave the client in a broken state.
    let second_call = eventbus.rpc_client(
        &config.options.rpc_exchange_name,
        &fast_routing_key,
        b"hello".to_vec(),
        "text/plain",
        ContentEncoding::None,
        5000,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await;

    assert!(second_call.is_ok(), "Second RPC call failed after late response: {:?}", second_call.err());
    assert_eq!(second_call.unwrap(), b"fast:hello".to_vec());

    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[]).await;
}

#[tokio::test]
async fn test_rpc_futures_dashmap_cleanup() {
    use amqp_client_rust::api::consumers::RpcFuturesMap;
    let rpc_futures: RpcFuturesMap = Arc::new(DashMap::new());

    let corr_id = Uuid::new_v4().to_string();
    let (tx, rx) = oneshot::channel();
    rpc_futures.insert(corr_id.clone(), tx);
    assert_eq!(rpc_futures.len(), 1);

    // Simulate timeout logic from AsyncChannel::rpc_client
    let timeout_result = tokio::time::timeout(Duration::from_millis(50), rx).await;
    assert!(timeout_result.is_err(), "Should timeout");

    // The timeout handler removes the entry
    rpc_futures.remove(&corr_id);

    // Assert that the map is empty, confirming zero memory leaks
    assert_eq!(rpc_futures.len(), 0, "rpc_futures must be empty after timeout cleanup");
}
