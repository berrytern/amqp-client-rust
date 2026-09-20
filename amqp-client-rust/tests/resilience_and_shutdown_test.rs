mod base;
use base::{cleanup_test_resources, create_test_config};
use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    api::utils::ContentEncoding,
    domain::config::QoSConfig,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn test_graceful_shutdown_waits_for_in_flight_messages() {
    let config = create_test_config();
    let qos_config = QoSConfig {
        sub_auto_ack: false,
        ..Default::default()
    };
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), qos_config);

    let exchange_name = format!("test_shutdown_ex_{}", Uuid::new_v4());
    let routing_key = format!("test_shutdown_key_{}", Uuid::new_v4());

    let started = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));

    let started_clone = Arc::clone(&started);
    let completed_clone = Arc::clone(&completed);

    // Subscribe with a slow handler (takes 350ms to process)
    eventbus.subscribe(
        &exchange_name,
        &routing_key,
        move |_msg| {
            let started = Arc::clone(&started_clone);
            let completed = Arc::clone(&completed_clone);
            Box::pin(async move {
                started.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(350)).await;
                completed.store(true, Ordering::SeqCst);
                Ok(())
            })
        },
        None,
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe");

    // Publish message
    eventbus.publish(
        &exchange_name,
        &routing_key,
        b"slow work payload",
        Some("text/plain"),
        ContentEncoding::None,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await.expect("Failed to publish");

    // Wait until the handler actually begins execution (in_flight becomes > 0)
    for _ in 0..50 {
        if started.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(started.load(Ordering::SeqCst), "Handler should have started");

    // Trigger graceful shutdown while work is in flight!
    let dispose_res = eventbus.dispose().await;
    assert!(dispose_res.is_ok(), "Dispose failed: {:?}", dispose_res.err());

    // When dispose() returns, the in-flight handler MUST have completed
    assert!(
        completed.load(Ordering::SeqCst),
        "dispose() must wait for in-flight messages before terminating the channel"
    );

    cleanup_test_resources(&config, &[&exchange_name]).await;
}

#[tokio::test]
async fn test_subscriber_process_timeout_triggers_nack_to_dlq() {
    let dlx_exchange = format!("test_timeout_dlx_{}", Uuid::new_v4());
    let dlq_queue = format!("test_timeout_dlq_{}", Uuid::new_v4());
    let dlq_routing_key = format!("dead_{}", Uuid::new_v4());

    let work_exchange = format!("test_timeout_work_ex_{}", Uuid::new_v4());
    let work_routing_key = format!("timeout_work_{}", Uuid::new_v4());

    // 1. Setup DLX / DLQ in RabbitMQ
    let config = create_test_config();
    {
        use amqprs::channel::{ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments};
        use amqprs::connection::{Connection, OpenConnectionArguments};

        let mut options = OpenConnectionArguments::new(
            &config.host,
            config.port,
            &config.username,
            &config.password,
        );
        options.virtual_host(&config.virtual_host);

        if let Ok(conn) = Connection::open(&options).await {
            if let Ok(ch) = conn.open_channel(None).await {
                let _ = ch.exchange_declare(ExchangeDeclareArguments::new(&dlx_exchange, "topic").durable(true).finish()).await;
                let _ = ch.queue_declare(QueueDeclareArguments::new(&dlq_queue).durable(true).finish()).await;
                let _ = ch.queue_bind(QueueBindArguments::new(&dlq_queue, &dlx_exchange, &dlq_routing_key)).await;
                let _ = ch.close().await;
            }
            let _ = conn.close().await;
        }
    }

    // 2. Configure work eventbus with DLX and DLQ routing
    let mut work_config = create_test_config();
    work_config.options.dead_letter_exchange = Some(dlx_exchange.clone());
    work_config.options.dead_letter_routing_key = Some(dlq_routing_key.clone());

    let qos_config = QoSConfig {
        sub_auto_ack: false,
        ..Default::default()
    };
    let work_eventbus = AsyncEventbusRabbitMQ::new(work_config.clone(), qos_config);

    // 3. Subscribe with a process_timeout of 100ms, but handler sleeps 300ms!
    work_eventbus.subscribe(
        &work_exchange,
        &work_routing_key,
        |_msg| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(())
            })
        },
        Some(Duration::from_millis(100)), // 100ms process timeout
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe");

    // 4. Publish a message to the work exchange
    let test_payload = b"Message that will exceed handler process_timeout";
    work_eventbus.publish(
        &work_exchange,
        &work_routing_key,
        test_payload,
        Some("text/plain"),
        ContentEncoding::None,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await.expect("Failed to publish");

    // 5. Consume from the DLQ to verify that exceeding process_timeout sent NACK and routed to DLQ!
    let mut dlq_config = create_test_config();
    dlq_config.options.queue_name = dlq_queue.clone();
    let dlq_eventbus = AsyncEventbusRabbitMQ::new(dlq_config.clone(), QoSConfig::default());

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let tx = Arc::new(tokio::sync::Mutex::new(tx));

    dlq_eventbus.subscribe(
        &dlx_exchange,
        &dlq_routing_key,
        move |message| {
            let tx = Arc::clone(&tx);
            Box::pin(async move {
                let _ = tx.lock().await.send(message).await;
                Ok(())
            })
        },
        None,
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe to DLQ");

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(received.is_ok(), "Timed out waiting for timed-out message to arrive in DLQ");
    let msg = received.unwrap().expect("DLQ channel closed");
    assert_eq!(&*msg.body, test_payload, "DLQ received wrong payload");

    assert!(work_eventbus.dispose().await.is_ok());
    assert!(dlq_eventbus.dispose().await.is_ok());
    cleanup_test_resources(&work_config, &[&work_exchange, &dlx_exchange]).await;
    cleanup_test_resources(&dlq_config, &[]).await;
}
