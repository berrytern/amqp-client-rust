# AMQP Client Rust
[![License][license-image]][license-url]

A Rust client library for interacting with RabbitMQ using AMQP. This library provides high-level abstractions for working with RabbitMQ, including automatic queue and exchange management, message publishing, subscribing, and RPC support.

## Features:
- Asynchronous API with Tokio;
- Automatic queue, exchange, and dead letter queue (DLQ) management;
- Message publishing with optional Publisher Confirms and delivery guarantees;
- Subscribing with robust error handling, manual/auto-ack, and dead-letter routing;
- High-performance RPC (Remote Procedure Call) client and server functionality;
- Built-in compression and decompression support (`zstd`, `lz4_flex`, `flate2`/`zlib-rs`);
- Optional TLS support;
- Resilient automatic reconnection and recovery.

[//]: # (These are reference links used in the body of this note.)
[license-image]: https://img.shields.io/badge/license-Apache%202-blue.svg
[license-url]: https://github.com/berrytern/amqp-client-rust/blob/master/LICENSE

## Getting Started

### Installation
Add the following to your `Cargo.toml`:
```toml
[dependencies]
amqp-client-rust = "0.0.7"
tokio = { version = "1", features = ["rt", "rt-multi-thread", "sync", "net", "io-util", "time", "macros"] }
```

Optional features:
```toml
amqp-client-rust = { version = "0.0.7", features = ["tls", "zstd", "lz4_flex", "flate2"] }
```

## Example Usage

Here is an example demonstrating how to publish, subscribe, and perform RPC calls using `amqp-client-rust`:

```rust
use std::error::Error as StdError;
use std::time::Duration;
use amqp_client_rust::{
    api::{
        eventbus::AsyncEventbusRabbitMQ,
        utils::{ContentEncoding, DeliveryMode, Message},
    },
    domain::config::{Config, ConfigOptions, QoSConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // 1. Configure the connection
    let config = Config::from_url(
        "amqp://guest:guest@localhost:5672",
        ConfigOptions::new("example_queue", "rpc_queue", "rpc_exchange"),
    )?;

    let eventbus = AsyncEventbusRabbitMQ::new(config, QoSConfig::default());

    // 2. Subscribe to events
    eventbus
        .subscribe(
            "example_exchange",
            "order.created",
            |message: Message| async move {
                println!("Received event: {:?}", String::from_utf8_lossy(&message.body));
                Ok(())
            },
            None,
            Some(Duration::from_secs(5)),
        )
        .await?;

    // 3. Provide an RPC resource (RPC Server)
    eventbus
        .provide_resource(
            "user.get",
            |request: Message| async move {
                let user_id = String::from_utf8_lossy(&request.body);
                let response = format!(r#"{{"id": "{}", "status": "active"}}"#, user_id);
                Ok(Message::from(response.into_bytes()))
            },
            None,
            Some(Duration::from_secs(5)),
        )
        .await?;

    // 4. Publish an event
    let event_payload = br#"{"order_id": 1234, "item": "Rust Book"}"#;
    eventbus
        .publish(
            "example_exchange",
            "order.created",
            event_payload.to_vec(),
            Some("application/json"),
            ContentEncoding::None,
            Some(Duration::from_secs(5)),
            Some(DeliveryMode::Persistent),
            None,
        )
        .await?;

    // 5. Call RPC server (RPC Client)
    let rpc_response = eventbus
        .rpc_client(
            "rpc_exchange",
            "user.get",
            b"user_42".to_vec(),
            "application/json",
            ContentEncoding::None,
            5000, // 5s response timeout in millis
            Some(Duration::from_secs(10)),
            None,
            None,
        )
        .await?;

    println!("RPC Response: {:?}", String::from_utf8_lossy(&rpc_response));

    // 6. Graceful shutdown
    eventbus.dispose().await?;

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