use std::error::Error as StdError;
use tokio::time::{sleep, Duration};
mod api;
mod domain;
mod errors;
use crate::{
    api::eventbus::AsyncEventbusRabbitMQ,
    domain::{
        config::{Config, ConfigOptions},
        integration_event::IntegrationEvent,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_url(
        "amqp://guest:guest@localhost:5672",
        ConfigOptions {
            queue_name: "example_queue".to_string(),
            rpc_queue_name: "rpc_queue".to_string(),
            rpc_exchange_name: "rpc_exchange".to_string(),
        },
    )?;

    let eventbus = AsyncEventbusRabbitMQ::new(config).await;
    let example_event = IntegrationEvent::new("teste.iso", "example.exchange");
    async fn handle(_body: Vec<u8>) -> Result<(), Box<dyn StdError + Send + Sync>> {
        Ok(())
    }

    //eventbus.subscribe(&example_event.exchange_name, handle, &example_event.routing_key, "application/json").await?;
    //eventbus.subscribe(&example_event.exchange_name, handle, "asdasd.sda", "application/json").await?;

    let content = String::from(
        r#"
            {
                "publisher": "example"
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
        //eventbus.publish("rpc_exchange", &example_event.routing_key, content.clone(), "application/json").await?;
        let _result = eventbus
            .rpc_client(
                "rpc_exchange",
                &example_event.routing_key,
                content.clone(),
                "application/json",
                1000,
            )
            .await?;
    }
    sleep(Duration::from_secs(50)).await;
    println!("end");

    Ok(())
}
