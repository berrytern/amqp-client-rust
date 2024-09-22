use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    domain::config::{Config, ConfigOptions}
}; // Replace with your actual crate name
use tokio::{self, sync::Mutex};
use std::sync::Arc;
use std::time::Duration;

// Helper function to create a test configuration
fn create_test_config() -> Config {
    Config {
        host: "localhost".to_string(),
        port: 5672,
        username: "guest".to_string(),
        password: "guest".to_string(),
        options: ConfigOptions {
            queue_name: "test_queue".to_string(),
            rpc_queue_name: "test_rpc_queue".to_string(),
            rpc_exchange_name: "test_rpc_exchange".to_string(),
        },
    }
}

#[tokio::test]
async fn test_publish_and_subscribe() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config).await;
    let exchange_name = "test_exchange";
    let routing_key = "test_routing_key";
    let test_message = "Hello, RabbitMQ!".as_bytes().to_vec();

    // Create a channel to signal when the message is received
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let tx = Arc::new(Mutex::new(tx));
    
    // Subscribe to messages
    eventbus.subscribe(
        exchange_name,
        move |message| {
            let tx = Arc::clone(&tx);
            Box::pin(async move {
                let _ = tx.lock().await.send(message).await;
                Ok(())
            })
        },
        routing_key,
        "text/plain",
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe");

    // Publish a message
    eventbus.publish(
        exchange_name,
        routing_key,
        test_message.clone(),
        Some("text/plain"),
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to publish message");
    // Wait for the message to be received
    let received_message = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("Timed out waiting for message")
        .expect("Failed to receive message");

    assert_eq!(received_message, test_message, "Received message does not match sent message");
}

#[tokio::test]
async fn test_rpc_client_and_server() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config).await;
    let exchange_name = "test_rpc_exchange";
    let routing_key = "test_rpc_routing_key";
    let test_message = "RPC request".as_bytes().to_vec();

    // Set up RPC server
    eventbus.rpc_server(
        |request| {
            Box::pin(async move {
                println!("Get request: {:?}", request);
                let response = format!("Processed: {}", String::from_utf8_lossy(&request));
                println!("Send request: {:?}", response.as_bytes().to_vec());
                Ok(response.as_bytes().to_vec())
            })
        },
        routing_key,
        "text/plain",
        Some(Duration::from_secs(5)),
    ).await;
    // Create a channel to receive the RPC response
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    // Make RPC client call
    eventbus.rpc_client(
        exchange_name,
        routing_key,
        test_message.clone(),
        move |result| {
            let tx = tx.clone();
            Box::pin(async move {
                let _ = tx.send(result).await;
                Ok(())
            })
        },
        "text/plain",
        5000, // 5 seconds timeout
        Some(Duration::from_secs(5)),
        None,
    ).await.expect("Failed to make RPC call");

    // Wait for the RPC response
    let rpc_result = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("Timed out waiting for RPC response")
        .expect("Failed to receive RPC response")
        .expect("RPC call failed");
    // println!("Received result: {:?}", String::from_utf8(rpc_result.clone()));
    let expected_response = "Processed: RPC request".as_bytes().to_vec();
    assert_eq!(rpc_result, expected_response, "RPC response does not match expected result");
}