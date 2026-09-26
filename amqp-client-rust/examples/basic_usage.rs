use std::error::Error as StdError;
use std::time::Duration;
use amqp_client_rust::{
    api::{
        eventbus::AsyncEventbusRabbitMQ,
        utils::{DeliveryMode, Message, PublishOptions, RpcClientOptions},
    },
    domain::config::{Config, ConfigOptions, QoSConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let options = ConfigOptions::new("example_queue", "rpc_queue", "rpc_exchange")
        .with_max_pending_commands(10_000)
        .with_fail_fast_on_disconnect(false)
        .with_default_command_timeout(Duration::from_secs(16));

    let config = Config::from_url(
        "amqp://guest:guest@localhost:5672",
        options,
    )?;

    let qos_config = QoSConfig::default();

    let eventbus = AsyncEventbusRabbitMQ::new(config, qos_config);

    eventbus
        .subscribe(
            "example_exchange",
            "order.*",
            |message: Message| async move {
                let payload = String::from_utf8_lossy(&message.body);
                println!("Received message: {}", payload);
                Ok(())
            },
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(5)),
        )
        .await?;

    eventbus
        .provide_resource(
            "user.get",
            |request: Message| async move {
                let user_id = String::from_utf8_lossy(&request.body);
                let response = format!(r#"{{"id": "{}", "status": "active"}}"#, user_id);
                Ok(Message::from(response.into_bytes()))
            },
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
        )
        .await?;

    let event_payload = br#"{"order_id": 1234, "item": "Rust Book"}"#;
    let pub_options = PublishOptions::new()
        .with_content_type("application/json")
        .with_delivery_mode(DeliveryMode::Persistent)
        .with_command_timeout(Duration::from_secs(5));

    eventbus
        .publish(
            "example_exchange",
            "order.created",
            event_payload,
            &pub_options,
        )
        .await?;

    let rpc_options = RpcClientOptions::new()
        .with_content_type("application/json")
        .with_response_timeout_millis(5000)
        .with_command_timeout(Duration::from_secs(10));

    let rpc_response = eventbus
        .rpc_client(
            "rpc_exchange",
            "user.get",
            b"user_42".to_vec(),
            &rpc_options,
        )
        .await?;

    println!("RPC Response: {}", String::from_utf8_lossy(&rpc_response));

    eventbus.dispose().await?;

    Ok(())
}
