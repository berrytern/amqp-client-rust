use std::error::Error as StdError;
use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    domain::{
        config::{Config, ConfigOptions},
        integration_event::IntegrationEvent,
    }
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_url(
        "amqp://guest:guest@localhost:5672",
        ConfigOptions {
            queue_name: "example_queue".to_string(),
            rpc_queue_name: "rpc_queue".to_string(),
            rpc_exchange_name: "rpc_exchange".to_string(),
        }
    )?;

    let eventbus = AsyncEventbusRabbitMQ::new(config).await;
    let example_event = IntegrationEvent::new("example.run", "example_exchange");

    let content = String::from(
        r#"
            {
                "data": "Hello, amqprs!"
            }
        "#,
    )
    .into_bytes();



    async fn rpc_handler(_body: Vec<u8>) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        Ok("Ok".into())
    }

    eventbus
        .rpc_server(rpc_handler, &example_event.routing_key, "application/json")
        .await;
    loop {
        let _result = eventbus
            .rpc_client(
                "rpc_exchange",
                &example_event.routing_key,
                content.clone(),
                "application/json",
                1000,
            )
            .await;
    }
}
