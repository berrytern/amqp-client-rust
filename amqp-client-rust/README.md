# AMQP Client Rust
[![License][license-image]][license-url]

A Rust client library for interacting with RabbitMQ using AMQP. This library provides high-level abstractions for working with RabbitMQ, including automatic queue and exchange management, message publishing, subscribing, and RPC support.

## Features:
- Asynchronous API with Tokio;
- Automatic queue and exchange management;
- RPC (Remote Procedure Call) functionality;
- Reconnection and error handling;

[//]: # (These are reference links used in the body of this note.)
[license-image]: https://img.shields.io/badge/license-Apache%202-blue.svg
[license-url]: https://github.com/berrytern/amqp-client-rust/blob/master/LICENSE

## Getting Started

### Installation
Add the following to your `Cargo.toml`:
```
[dependencies]
amqp-client-rust = "0.0.2-alpha.1"
amqprs = "1.5.1"
async-trait = "0.1.68"
tokio = { version = "1", features = ["rt", "rt-multi-thread", "sync", "net", "io-util", "time", "macros"] }
uuid = { version = "1.3.3", features = ["v4"] }
url = "2.2.2"
```

## Example Usage

Here is an example demonstrating how to use amqp-client-rust to publish and subscribe to messages, as well as handle RPC calls:

```
use std::error::Error as StdError;
use tokio::time::{sleep, Duration};
use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    domain::{
        config::{Config, ConfigOptions},
        integration_event::IntegrationEvent,
    },
    errors::AppError
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

    eventbus
        .subscribe(
            &example_event.event_type(),
            handle,
            &example_event.routing_key,
            "application/json",
        )
        .await?;

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
        eventbus
            .publish(
                &example_event.event_type(),
                &example_event.routing_key,
                content.clone(),
                "application/json",
            )
            .await?;
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
```

## Contributing
Contributions are welcome! Please open issues or pull requests on [GitHub](https://github.com/berrytern/amqp-client-rust/).
## License
This project is licensed under the [Apache 2.0 License](./LICENSE).

## Acknowledgments
This library was inspired by the `amqp-client-python` library, which provides a similar abstraction for RabbitMQ in Python. The design and functionality of `amqp-client-python` greatly influenced the development of this Rust library.

amqp-client-python: [GitHub Repository](https://github.com/nutes-uepb/amqp-client-python) | [PyPI Page](https://pypi.org/project/amqp-client-python/)