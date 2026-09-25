# AMQP Client Rust
[![License][license-image]][license-url]

A robust, high-performance asynchronous AMQP client library for Rust, designed for mission-critical event-driven architectures with RabbitMQ. Built on top of `amqprs` and `tokio`.

## Features
- **Async Tokio Architecture**: Actor-based connection and channel management with dedicated command queues.
- **Automatic Resource Management**: Automatic declaration and binding of exchanges, queues, and Dead Letter Queues (DLQ).
- **Wildcard Routing**: High-performance AMQP topic matching via specialized Prefix Trie.
- **Publisher Confirms & Guarantees**: Complete streaming Ack/Nack tracking with support for individual and batched (multiple) confirmations.
- **RPC Client & Server**: Bidirectional Remote Procedure Calls with correlation IDs, automatic queue cleanup, and per-call timeouts.
- **Panic Safety & RAII Guards**: Consumer handlers are protected against panics via `catch_unwind`; panics are converted to NACK to DLQ, and in-flight counters are tracked with RAII guards to eliminate shutdown deadlocks.
- **Backpressure & Resiliency**: Configurable limits on pending command buffers, rejecting overflow with `BufferFull` instead of unbounded memory growth.
- **Automatic Reconnection & Healing**: Automatic recovery of connections, channels, exchanges, and topic subscriptions upon broker disconnection or network drop.
- **Compression Support**: Built-in payload compression for `zstd`, `lz4_flex`, and `flate2`/`zlib-rs`.
- **Optional TLS Support**: Secure AMQP connections via `amqprs/tls`.

[license-image]: https://img.shields.io/badge/license-Apache%202-blue.svg
[license-url]: https://github.com/berrytern/amqp-client-rust/blob/master/LICENSE

## Getting Started

### Installation
Add the dependency to your `Cargo.toml`:
```toml
[dependencies]
amqp-client-rust = "0.1.0"
tokio = { version = "1", features = ["rt", "rt-multi-thread", "sync", "net", "io-util", "time", "macros"] }
```

Optional features:
```toml
amqp-client-rust = { version = "0.1.0", features = ["tls", "zstd", "lz4_flex", "flate2"] }
```

---

## Example Usage

```rust
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
    // 1. Configure connection and options
    let options = ConfigOptions::new("example_queue", "rpc_queue", "rpc_exchange")
        .with_max_pending_commands(10_000)
        .with_fail_fast_on_disconnect(false)
        .with_default_command_timeout(Duration::from_secs(16));

    let config = Config::from_url(
        "amqp://guest:guest@localhost:5672",
        options,
    )?;

    // Configure QoS (e.g. publisher confirms, prefetch, auto-ack)
    let qos_config = QoSConfig::default();

    // 2. Initialize the asynchronous eventbus
    let eventbus = AsyncEventbusRabbitMQ::new(config, qos_config);

    // 3. Subscribe to a topic pattern
    eventbus
        .subscribe(
            "example_exchange",
            "order.*",
            |message: Message| async move {
                let payload = String::from_utf8_lossy(&message.body);
                println!("Received message: {}", payload);
                Ok(())
            },
            Some(Duration::from_secs(10)), // Process timeout (sends NACK to DLQ on timeout)
            Some(Duration::from_secs(5)),  // Command timeout
        )
        .await?;

    // 4. Provide an RPC resource (RPC Server)
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

    // 5. Publish an event
    let event_payload = br#"{"order_id": 1234, "item": "Rust Book"}"#;
    let pub_options = PublishOptions::new()
        .with_content_type("application/json")
        .with_delivery_mode(DeliveryMode::Persistent)
        .with_command_timeout(Duration::from_secs(5));

    eventbus
        .publish(
            "example_exchange",
            "order.created",
            event_payload.to_vec(),
            &pub_options,
        )
        .await?;

    // 6. Call an RPC server (RPC Client)
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

    // 7. Graceful shutdown
    // Cancels consumers and waits for all in-flight handlers to finish before closing channels
    eventbus.dispose().await?;

    Ok(())
}
```

---

## Dead Letter Queue (DLQ) Configuration

You can easily route unprocessable or timed-out messages to a Dead Letter Exchange:

```rust
let options = ConfigOptions::new("work_queue", "rpc_queue", "rpc_exchange")
    .dead_letter(Some("my_dlx".to_string()), Some("my_dlq_key".to_string()));
```

---

## Running Tests

Integration tests require a running RabbitMQ instance:

```bash
docker run -d --name lib-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Run the complete test suite including unit tests, property tests, and integration tests:

```bash
cargo test --all-features
```

## License
This project is licensed under the [Apache 2.0 License](./LICENSE).