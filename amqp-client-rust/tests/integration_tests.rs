use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    domain::config::QoSConfig
}; // Replace with your actual crate name
use tokio::{self, sync::Mutex};
use uuid::Uuid;
use std::{sync::Arc};
use std::time::Duration;
mod base;
use base::create_test_config;

// Helper function to create a test configuration


#[tokio::test]
async fn test_publish_and_subscribe() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config, QoSConfig::default());
    let exchange_name = "test_exchange";
    let routing_key = format!("test_routing_key_{}", Uuid::new_v4());
    let test_message = "Hello, RabbitMQ!".as_bytes();

    // Create a channel to signal when the message is received
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
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to publish message");
    // Wait for the message to be received
    let received_message = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("Timed out waiting for message")
        .expect("Failed to receive message");

    assert_eq!(received_message, test_message, "Received message does not match sent message");
    assert!(eventbus.dispose().await.is_ok());
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
                let response = format!("Processed: {}", String::from_utf8_lossy(&request));
                println!("Send request: {:?}, {}", response.as_bytes().to_vec(), response);
                Ok(response.as_bytes().to_vec())
            })
        },
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(10)),
    ).await;
    // Create a channel to receive the RPC response
    //let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    // Make RPC client call
    let rpc_result = eventbus.rpc_client(
        config.options.rpc_exchange_name.as_str(),
        routing_key.as_str(),
        test_message,
        /*move |result| {
            let tx = tx.clone();
            Box::pin(async move {
                let _ = tx.send(result).await;
                Ok(())
            })
        },*/
        "text/plain",
        5000, // 5 seconds timeout
        Some(Duration::from_secs(5)),
        None,
    ).await;

    // Wait for the RPC response
    assert!(rpc_result.is_ok(), "RPC call failed: {:?}", rpc_result.err());
    let rpc_result = rpc_result.unwrap();
    // println!("Received result: {:?}", String::from_utf8(rpc_result.clone()));
    let expected_response = "Processed: RPC request".as_bytes().to_vec();
    assert_eq!(rpc_result, expected_response, "RPC response does not match expected result");
    assert!(eventbus.dispose().await.is_ok());
}