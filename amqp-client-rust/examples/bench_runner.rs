use std::sync::Arc;
use std::time::{Duration, Instant};
use futures::future::join_all;
use amqp_client_rust::{
    api::{
        eventbus::AsyncEventbusRabbitMQ,
        utils::{Message, PublishOptions, RpcClientOptions},
    },
    domain::config::{Config, ConfigOptions, QoSConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "pub_seq".to_string());
    let total: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(1000);

    let queue_name = format!("bench_rust_{}_{}_{}", mode, total, std::process::id());
    let rpc_exchange = format!("{}_ex", queue_name);
    let rpc_queue = format!("{}_rq", queue_name);

    let options = ConfigOptions::new(&queue_name, &rpc_queue, &rpc_exchange)
        .with_max_pending_commands(100_000)
        .with_fail_fast_on_disconnect(false)
        .with_default_command_timeout(Duration::from_secs(32));

    let config = Config::from_url("amqp://guest:guest@localhost:5672", options)?;
    
    // Uses default QoS (now with rpc_client_auto_ack: true and pub_confirm: true)
    let qos = QoSConfig::default();

    let bus = Arc::new(AsyncEventbusRabbitMQ::new(config, qos));
    tokio::time::sleep(Duration::from_millis(400)).await;

    if mode == "pub_seq" {
        let rk = "bench.pub.seq";
        bus.subscribe(&rpc_exchange, rk, |_m: Message| async move { Ok(()) }, None, None).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;

        let opts = PublishOptions::new()
            .with_content_type("application/json")
            .with_command_timeout(Duration::from_secs(16));
        let payload = br#"{"message": "Hello Rust AMQP", "val": 42}"#;

        // Warmup (10 msgs)
        for _ in 0..10 {
            bus.publish(&rpc_exchange, rk, payload, &opts).await?;
        }

        let start = Instant::now();
        for _ in 0..total {
            bus.publish(&rpc_exchange, rk, payload, &opts).await?;
        }
        let elapsed = start.elapsed().as_secs_f64();
        println!("RESULT:{:.4}:{:.2}", elapsed, total as f64 / elapsed);

    } else if mode == "pub_con" {
        let rk = "bench.pub.con";
        bus.subscribe(&rpc_exchange, rk, |_m: Message| async move { Ok(()) }, None, None).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;

        let opts = PublishOptions::new()
            .with_content_type("application/json")
            .with_command_timeout(Duration::from_secs(16));
        let payload = br#"{"message": "Hello Rust AMQP", "val": 42}"#;

        // Warmup (10 msgs)
        for _ in 0..10 {
            bus.publish(&rpc_exchange, rk, payload, &opts).await?;
        }

        let start = Instant::now();
        let futs = (0..total).map(|_| bus.publish(&rpc_exchange, rk, payload, &opts));
        let results = join_all(futs).await;
        for r in results {
            r?;
        }
        let elapsed = start.elapsed().as_secs_f64();
        println!("RESULT:{:.4}:{:.2}", elapsed, total as f64 / elapsed);

    } else if mode == "rpc_seq" {
        let rk = "bench.rpc.seq";
        bus.provide_resource(rk, |_m: Message| async move {
            Ok(Message::from(b"pong".to_vec()))
        }, None, None).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;

        let opts = RpcClientOptions::new()
            .with_content_type("text/plain")
            .with_response_timeout_millis(100_000);

        // Warmup (10 reqs) - also initializes rpc_client consumer and aux_queue
        for _ in 0..10 {
            bus.rpc_client(&rpc_exchange, rk, b"ping".to_vec(), &opts).await?;
        }

        let start = Instant::now();
        for _ in 0..total {
            bus.rpc_client(&rpc_exchange, rk, b"ping".to_vec(), &opts).await?;
        }
        let elapsed = start.elapsed().as_secs_f64();
        println!("RESULT:{:.4}:{:.2}", elapsed, total as f64 / elapsed);

    } else if mode == "rpc_con" {
        let rk = "bench.rpc.con";
        bus.provide_resource(rk, |_m: Message| async move {
            Ok(Message::from(b"pong".to_vec()))
        }, None, None).await?;
        tokio::time::sleep(Duration::from_millis(400)).await;

        let opts = RpcClientOptions::new()
            .with_content_type("text/plain")
            .with_response_timeout_millis(100_000);

        // Warmup (10 reqs) - initializes rpc_client consumer and aux_queue
        for _ in 0..10 {
            bus.rpc_client(&rpc_exchange, rk, b"ping".to_vec(), &opts).await?;
        }

        let start = Instant::now();
        let futs = (0..total).map(|_| bus.rpc_client(&rpc_exchange, rk, b"ping".to_vec(), &opts));
        let results = join_all(futs).await;
        for r in results {
            r?;
        }
        let elapsed = start.elapsed().as_secs_f64();
        println!("RESULT:{:.4}:{:.2}", elapsed, total as f64 / elapsed);
    }

    bus.dispose().await?;
    Ok(())
}
