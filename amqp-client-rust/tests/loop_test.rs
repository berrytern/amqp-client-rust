use std::{error::Error as StdError, sync::{Arc, atomic::AtomicU32}, time::{Duration}};
use amqp_client_rust::{
    api::{eventbus::AsyncEventbusRabbitMQ, utils::ContentEncoding},
    domain::{
        config::QoSConfig, integration_event::IntegrationEvent
    }
};
mod base;
use base::{create_test_config, Rand};
use uuid::Uuid;


#[tokio::test]
async fn test_loop() {
    let mut rng = Rand::new(0);
    let config = create_test_config();

    let qos_config = QoSConfig::default();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), qos_config);
    let routing_key = format!("test_routing_key_{}", Uuid::new_v4());
    let example_event = IntegrationEvent::new(routing_key.as_str(), config.options.rpc_exchange_name.as_str());

    async fn rpc_handler(body: Vec<u8>) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        Ok(body)
    }
    assert!(eventbus
        .provide_resource(&routing_key, rpc_handler, None, Duration::from_secs(5).into())
        .await
        .is_ok());
    println!("RPC server started");
    println!("Starting RPC client loop...");
    let success_count = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    let message_count = 300_000;
    for _ in 0..message_count {
        //let (tx, rx) = tokio::sync::mpsc::channel(1);
        let eventbus = eventbus.clone();
        let exchange_name = example_event.event_type().clone();
        let routing_key = example_event.routing_key.clone();
        let rand = rng.rand();
        let success_count = success_count.clone();
        tasks.push(tokio::spawn(async move {
            match eventbus
            .rpc_client(
                &exchange_name,
                &routing_key,
                rand.to_string().as_bytes(),
                "application/json",
                ContentEncoding::None,
                160_000,
                Some(Duration::from_secs(60)),
                None,
                None,
            )
            .await {
                Ok(result)=> {
                    assert_eq!(result, rand.to_string().as_bytes().to_vec());
                    success_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                },
                Err(e)=>assert!(false, "RPC call failed: {:?}", e),
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    let value= success_count.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(value, message_count as u32, "Not all RPC calls succeeded");
    let _ = eventbus.dispose().await;
}
