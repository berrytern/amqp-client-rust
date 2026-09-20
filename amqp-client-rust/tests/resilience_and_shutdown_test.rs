mod base;
use base::{cleanup_test_resources, create_test_config};
use amqp_client_rust::{
    api::channel::AsyncChannel,
    api::eventbus::AsyncEventbusRabbitMQ,
    api::utils::{Confirmations, ContentEncoding, DeliveryMode, Message, QueueOptions, RPCHandler},
    domain::config::QoSConfig,
};
use amqprs::connection::{Connection, OpenConnectionArguments};
use dashmap::DashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
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

#[tokio::test]
async fn test_rpc_server_graceful_shutdown_waits_for_in_flight_computation() {
    let config = create_test_config();
    let qos_config = QoSConfig::default();
    let server_eventbus = Arc::new(AsyncEventbusRabbitMQ::new(config.clone(), qos_config));
    let client_eventbus = AsyncEventbusRabbitMQ::new(config.clone(), qos_config);

    let routing_key = format!("test_rpc_inflight_key_{}", Uuid::new_v4());
    let server_started = Arc::new(AtomicBool::new(false));
    let server_started_clone = Arc::clone(&server_started);

    server_eventbus
        .provide_resource(
            &routing_key,
            move |_req: Message| {
                let server_started = Arc::clone(&server_started_clone);
                Box::pin(async move {
                    server_started.store(true, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Ok(Message {
                        body: b"delayed response".to_vec().into(),
                        content_type: Some("text/plain".to_string()),
                    })
                })
            },
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("register rpc server");

    // Run client RPC and server dispose concurrently using tokio::join!
    let client_fut = async {
        client_eventbus
            .rpc_client(
                &config.options.rpc_exchange_name,
                &routing_key,
                b"ping",
                "text/plain",
                ContentEncoding::None,
                5000,
                Some(Duration::from_secs(5)),
                Some(DeliveryMode::Transient),
                None,
            )
            .await
    };

    let server_dispose_fut = async {
        // Wait until server starts processing
        for _ in 0..50 {
            if server_started.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(server_started.load(Ordering::SeqCst), "Server should have started processing");

        // Trigger server shutdown while computation is in-flight!
        server_eventbus.dispose().await
    };

    let (client_res, shutdown_res) = tokio::join!(client_fut, server_dispose_fut);

    // Client MUST receive the response cleanly
    let client_res = client_res.expect("RPC call failed");
    assert_eq!(client_res, b"delayed response");

    // Shutdown MUST complete successfully
    assert!(shutdown_res.is_ok(), "Server dispose failed: {:?}", shutdown_res.err());

    let _ = client_eventbus.dispose().await;
    cleanup_test_resources(&config, &[]).await;
}

#[tokio::test]
async fn test_rpc_client_consumer_isolation_and_clean_shutdown() {
    let config = create_test_config();
    let mut options = OpenConnectionArguments::new(
        &config.host,
        config.port,
        &config.username,
        &config.password,
    );
    options.virtual_host(&config.virtual_host);

    let conn = Arc::new(Mutex::new(Connection::open(&options).await.expect("open conn")));
    let ch = conn.lock().await.open_channel(None).await.expect("open channel");
    let (tx, _rx) = mpsc::unbounded_channel();
    let rpc_futures = Arc::new(DashMap::new());

    let mut async_channel = AsyncChannel::new(
        ch,
        conn,
        tx,
        rpc_futures,
        Confirmations::Disables,
        true,
        None,
        None,
    );

    // 1. Initially, consumer_tags must be empty
    assert!(
        async_channel.consumer_tags().await.is_empty(),
        "Initial consumer_tags must be empty"
    );

    // 2. Start the RPC consumer (initializes aux_channel and reply consumer)
    async_channel
        .start_rpc_consumer()
        .await
        .expect("start_rpc_consumer should succeed");

    // 3. Aux reply consumer tag must NOT pollute main channel consumer_tags
    let tags_after_rpc = async_channel.consumer_tags().await;
    assert!(
        tags_after_rpc.is_empty(),
        "aux_channel reply consumer tag must NOT be pushed into main consumer_tags! Found: {:?}",
        tags_after_rpc
    );

    // 4. Register a server consumer on main_channel: this one MUST be tracked
    let server_exchange = format!("test_srv_ex_{}", Uuid::new_v4());
    let server_key = format!("test_srv_key_{}", Uuid::new_v4());
    let server_handler: RPCHandler = Arc::new(|_data| {
        Box::pin(async move {
            Ok(Message {
                body: b"response".to_vec().into(),
                content_type: None,
            })
        })
    });

    let queue_options = QueueOptions::new()
        .auto_delete(false)
        .durable(false)
        .exclusive(false)
        .no_create(false);

    async_channel
        .rpc_server(
            server_handler,
            &server_key,
            &server_exchange,
            "topic",
            &format!("test_srv_q_{}", Uuid::new_v4()),
            None,
            &queue_options,
        )
        .await
        .expect("register rpc_server");

    // Now consumer_tags MUST contain exactly 1 tag (for the main channel consumer)
    let tags_after_server = async_channel.consumer_tags().await;
    assert_eq!(
        tags_after_server.len(),
        1,
        "Only the main-channel rpc_server consumer should be tracked in consumer_tags"
    );

    // 5. Clean teardown via dispose()
    async_channel.dispose().await;

    cleanup_test_resources(&config, &[&server_exchange]).await;
}

