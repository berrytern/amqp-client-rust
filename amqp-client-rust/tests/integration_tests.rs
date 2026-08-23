use amqp_client_rust::api::utils::{ContentEncoding};
use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    domain::config::QoSConfig
};
use tokio::{self, sync::Mutex};
use uuid::Uuid;
use std::{sync::Arc};
use std::time::Duration;
mod base;
use base::{create_test_config, cleanup_test_resources};

// Helper function to create a test configuration


#[tokio::test]
async fn test_publish_and_subscribe() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let exchange_name = "test_exchange";
    let routing_key = format!("test_routing_key_{}", Uuid::new_v4());
    let test_message = "Hello, RabbitMQ!".as_bytes();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let tx = Arc::new(Mutex::new(tx));
    
    // Subscribe to messages
    eventbus.subscribe(
        exchange_name,
        routing_key.as_str(),
        move |message| {
            let tx = Arc::clone(&tx);
            Box::pin(async move {
                let _ = tx.lock().await.send(message).await;
                Ok(())
            })
        },
        None, Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe");

    // Publish a message
    eventbus.publish(
        exchange_name,
        routing_key.as_str(),
        test_message,
        Some("text/plain"),
        ContentEncoding::None,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await.expect("Failed to publish message");
    // Wait for the message to be received
    let received_message = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("Timed out waiting for message")
        .expect("Failed to receive message");

    assert_eq!(received_message.body, test_message.into(), "Received message does not match sent message");
    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[exchange_name]).await;
}

#[tokio::test]
async fn test_rpc_client_and_server() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let routing_key = format!("test_rpc_routing_key_{}", Uuid::new_v4());
    let test_message = "RPC request".as_bytes().to_vec();

    // Set up RPC server
    let _ = eventbus.provide_resource(
        routing_key.as_str(),
        |request| {
            Box::pin(async move {
                println!("Get request: {:?}", request);
                let response = format!("Processed: {}", String::from_utf8_lossy(&request.body));
                println!("Send request: {:?}, {}", response.as_bytes().to_vec(), response);
                Ok(amqp_client_rust::api::utils::Message { body: response.as_bytes().into(), content_type: Some("text/plain".to_string()) })
            })
        },
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(10)),
    ).await;

    // Make RPC client call
    let rpc_result = eventbus.rpc_client(
        config.options.rpc_exchange_name.as_str(),
        routing_key.as_str(),
        test_message,
        "text/plain",
        ContentEncoding::None,
        5000, // 5 seconds timeout
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await;

    // Wait for the RPC response
    assert!(rpc_result.is_ok(), "RPC call failed: {:?}", rpc_result.err());
    let rpc_result = rpc_result.unwrap();
    let expected_response = "Processed: RPC request".as_bytes().to_vec();
    assert_eq!(rpc_result, expected_response, "RPC response does not match expected result");
    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[]).await;
}

#[cfg(feature = "zstd")]
#[tokio::test]
async fn test_rpc_client_and_server_zstd() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let routing_key = format!("test_rpc_zstd_key_{}", Uuid::new_v4());
    let test_message = "ZSTD compressed RPC payload data. ".repeat(100).into_bytes();

    let _ = eventbus.provide_resource(
        routing_key.as_str(),
        |request| {
            Box::pin(async move {
                let response = format!("Processed: {}", String::from_utf8_lossy(&request.body));
                Ok(amqp_client_rust::api::utils::Message {
                    body: response.into_bytes().into(),
                    content_type: Some("application/json".to_string()),
                })
            })
        },
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(10)),
    ).await;

    let rpc_result = eventbus.rpc_client(
        config.options.rpc_exchange_name.as_str(),
        routing_key.as_str(),
        test_message.clone(),
        "application/json",
        ContentEncoding::Zstd,
        5000,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await;

    assert!(rpc_result.is_ok(), "RPC ZSTD call failed: {:?}", rpc_result.err());
    let rpc_result = rpc_result.unwrap();
    let expected = format!("Processed: {}", String::from_utf8_lossy(&test_message)).into_bytes();
    assert_eq!(rpc_result, expected, "RPC ZSTD response mismatch");
    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[]).await;
}

#[cfg(feature = "lz4_flex")]
#[tokio::test]
async fn test_rpc_client_and_server_lz4() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let routing_key = format!("test_rpc_lz4_key_{}", Uuid::new_v4());
    let test_message = "LZ4 compressed RPC payload data. ".repeat(100).into_bytes();

    let _ = eventbus.provide_resource(
        routing_key.as_str(),
        |request| {
            Box::pin(async move {
                let response = format!("Processed: {}", String::from_utf8_lossy(&request.body));
                Ok(amqp_client_rust::api::utils::Message {
                    body: response.into_bytes().into(),
                    content_type: Some("text/plain".to_string()),
                })
            })
        },
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(10)),
    ).await;

    let rpc_result = eventbus.rpc_client(
        config.options.rpc_exchange_name.as_str(),
        routing_key.as_str(),
        test_message.clone(),
        "text/plain",
        ContentEncoding::Lz4,
        5000,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await;

    assert!(rpc_result.is_ok(), "RPC LZ4 call failed: {:?}", rpc_result.err());
    let rpc_result = rpc_result.unwrap();
    let expected = format!("Processed: {}", String::from_utf8_lossy(&test_message)).into_bytes();
    assert_eq!(rpc_result, expected, "RPC LZ4 response mismatch");
    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[]).await;
}

#[cfg(feature = "flate2")]
#[tokio::test]
async fn test_rpc_client_and_server_zlib() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let routing_key = format!("test_rpc_zlib_key_{}", Uuid::new_v4());
    let test_message = "ZLIB compressed RPC payload data. ".repeat(100).into_bytes();

    let _ = eventbus.provide_resource(
        routing_key.as_str(),
        |request| {
            Box::pin(async move {
                let response = format!("Processed: {}", String::from_utf8_lossy(&request.body));
                Ok(amqp_client_rust::api::utils::Message {
                    body: response.into_bytes().into(),
                    content_type: Some("text/plain".to_string()),
                })
            })
        },
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(10)),
    ).await;

    let rpc_result = eventbus.rpc_client(
        config.options.rpc_exchange_name.as_str(),
        routing_key.as_str(),
        test_message.clone(),
        "text/plain",
        ContentEncoding::Zlib,
        5000,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await;

    assert!(rpc_result.is_ok(), "RPC ZLIB call failed: {:?}", rpc_result.err());
    let rpc_result = rpc_result.unwrap();
    let expected = format!("Processed: {}", String::from_utf8_lossy(&test_message)).into_bytes();
    assert_eq!(rpc_result, expected, "RPC ZLIB response mismatch");
    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[]).await;
}

#[tokio::test]
async fn test_rpc_client_timeout() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let routing_key = format!("test_rpc_timeout_key_{}", Uuid::new_v4());

    // RPC client call with 100ms timeout to a key where no server is listening
    let rpc_result = eventbus.rpc_client(
        config.options.rpc_exchange_name.as_str(),
        routing_key.as_str(),
        b"timeout test payload".to_vec(),
        "text/plain",
        ContentEncoding::None,
        100, // 100ms timeout
        Some(Duration::from_secs(1)),
        None,
        None,
    ).await;

    assert!(rpc_result.is_err(), "Expected RPC call to timeout, but got Ok");
    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[]).await;
}

#[tokio::test]
async fn test_publish_and_subscribe_with_confirms() {
    let config = create_test_config();
    let mut qos_config = QoSConfig::default();
    qos_config.pub_confirm = true;
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), qos_config);
    let exchange_name = "test_confirms_exchange";
    let routing_key = format!("test_confirms_key_{}", Uuid::new_v4());
    let test_message = "Message with publisher confirms".as_bytes();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let tx = Arc::new(Mutex::new(tx));

    eventbus.subscribe(
        exchange_name,
        routing_key.as_str(),
        move |message| {
            let tx = Arc::clone(&tx);
            Box::pin(async move {
                let _ = tx.lock().await.send(message).await;
                Ok(())
            })
        },
        None,
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe");

    eventbus.publish(
        exchange_name,
        routing_key.as_str(),
        test_message,
        Some("text/plain"),
        ContentEncoding::None,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await.expect("Failed to publish with confirms");

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(received.is_ok(), "Timed out waiting for confirmed message");
    let msg = received.unwrap().expect("Channel closed");
    assert_eq!(&*msg.body, test_message);
    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[exchange_name]).await;
}

#[cfg(feature = "zstd")]
#[tokio::test]
async fn test_publish_and_subscribe_zstd() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let exchange_name = "test_zstd_exchange";
    let routing_key = format!("test_zstd_key_{}", Uuid::new_v4());
    let test_message = "ZSTD compressed pubsub payload data. ".repeat(100).into_bytes();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let tx = Arc::new(Mutex::new(tx));

    eventbus.subscribe(
        exchange_name,
        routing_key.as_str(),
        move |message| {
            let tx = Arc::clone(&tx);
            Box::pin(async move {
                let _ = tx.lock().await.send(message).await;
                Ok(())
            })
        },
        None,
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe");

    eventbus.publish(
        exchange_name,
        routing_key.as_str(),
        test_message.clone(),
        Some("application/json"),
        ContentEncoding::Zstd,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await.expect("Failed to publish zstd");

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(received.is_ok(), "Timed out waiting for zstd message");
    let msg = received.unwrap().expect("Channel closed");
    assert_eq!(&*msg.body, &test_message[..]);
    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[exchange_name]).await;
}

#[tokio::test]
async fn test_topic_wildcard_routing() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let exchange_name = "test_wildcard_exchange";
    let base_key = format!("wildcard_{}", Uuid::new_v4());
    let orders_pattern = format!("{}.orders.*", base_key);
    let users_pattern = format!("{}.users.*", base_key);
    let orders_created_key = format!("{}.orders.created", base_key);

    let (tx_orders, mut rx_orders) = tokio::sync::mpsc::channel(1);
    let tx_orders = Arc::new(Mutex::new(tx_orders));

    let (tx_users, mut rx_users) = tokio::sync::mpsc::channel(1);
    let tx_users = Arc::new(Mutex::new(tx_users));

    eventbus.subscribe(
        exchange_name,
        orders_pattern.as_str(),
        move |message| {
            let tx = Arc::clone(&tx_orders);
            Box::pin(async move {
                let _ = tx.lock().await.send(message).await;
                Ok(())
            })
        },
        None,
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe orders");

    eventbus.subscribe(
        exchange_name,
        users_pattern.as_str(),
        move |message| {
            let tx = Arc::clone(&tx_users);
            Box::pin(async move {
                let _ = tx.lock().await.send(message).await;
                Ok(())
            })
        },
        None,
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe users");

    // Publish to orders.created
    let message_payload = b"Order #123 created";
    eventbus.publish(
        exchange_name,
        orders_created_key.as_str(),
        message_payload,
        Some("text/plain"),
        ContentEncoding::None,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await.expect("Failed to publish");

    let received_orders = tokio::time::timeout(Duration::from_secs(5), rx_orders.recv()).await;
    assert!(received_orders.is_ok(), "Orders subscriber should have received message");
    assert_eq!(&*received_orders.unwrap().unwrap().body, message_payload);

    // Users subscriber should NOT receive this message
    let received_users = tokio::time::timeout(Duration::from_millis(500), rx_users.recv()).await;
    assert!(received_users.is_err(), "Users subscriber should not have received orders message");

    assert!(eventbus.dispose().await.is_ok());
    cleanup_test_resources(&config, &[exchange_name]).await;
}

#[tokio::test]
async fn test_dispose_lifecycle() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    
    // Dispose all connections
    assert!(eventbus.dispose().await.is_ok());

    // Subsequent publish should fail gracefully
    let pub_result = eventbus.publish(
        "test_exchange",
        "any_key",
        b"data",
        None,
        ContentEncoding::None,
        Some(Duration::from_millis(500)),
        None,
        None,
    ).await;
    assert!(pub_result.is_err(), "Publish after dispose should return an error");
    cleanup_test_resources(&config, &["test_exchange"]).await;
}