mod base;
use base::create_test_config;
use amqp_client_rust::{
    api::{eventbus::AsyncEventbusRabbitMQ, utils::{ContentEncoding, PublishOptions}},
    domain::config::{Config, ConfigOptions, QoSConfig},
};
use std::process::Command;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use uuid::Uuid;

fn execute_docker_rabbitmqctl(args: &[&str]) -> bool {
    let mut cmd = Command::new("docker");
    cmd.arg("exec").arg("lib-rabbitmq").arg("rabbitmqctl");
    for arg in args {
        cmd.arg(arg);
    }
    cmd.output().map(|o| o.status.success()).unwrap_or(false)
}

#[tokio::test]
async fn test_update_secret_lifecycle() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config, QoSConfig::default());

    // Allow connections to initialize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Call update_secret on active connection
    let res = eventbus
        .update_secret("guest", "token rotation test", Some(Duration::from_secs(5)))
        .await;
    assert!(res.is_ok(), "update_secret should succeed on active connections: {:?}", res);

    let _ = eventbus.dispose().await;
}

#[tokio::test]
async fn test_update_secret_on_unreachable_fails() {
    let mut options = ConfigOptions::new("test_q", "test_rpc_q", "test_rpc_ex");
    options.max_pending_commands = 10;
    let unreachable_config = Config::new(
        "127.0.0.1",
        59998,
        "guest",
        "guest",
        options,
        "/",
    );

    let eventbus = AsyncEventbusRabbitMQ::new(unreachable_config, QoSConfig::default());

    // Attempting update_secret on an unreachable broker with short timeout should fail
    let res = eventbus
        .update_secret("new_pass", "test", Some(Duration::from_millis(150)))
        .await;

    assert!(res.is_err(), "update_secret should fail when disconnected");

    let _ = eventbus.dispose().await;
}

#[tokio::test]
async fn test_reconnection_and_subscription_auto_healing() {
    // Check if docker rabbitmqctl is available; if not, skip test
    if !execute_docker_rabbitmqctl(&["status"]) {
        eprintln!("Docker lib-rabbitmq not available, skipping real reconnection test");
        return;
    }

    let vhost_name = format!("vhost_recon_{}", Uuid::new_v4().simple());

    // 1. Create dedicated isolated vhost in RabbitMQ
    assert!(execute_docker_rabbitmqctl(&["add_vhost", &vhost_name]));
    assert!(execute_docker_rabbitmqctl(&[
        "set_permissions",
        "-p",
        &vhost_name,
        "guest",
        ".*",
        ".*",
        ".*"
    ]));

    let mut config = create_test_config();
    config.virtual_host = vhost_name.clone();

    let qos_config = QoSConfig {
        sub_auto_ack: true,
        ..Default::default()
    };
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), qos_config);

    let exchange_name = format!("ex_recon_{}", Uuid::new_v4().simple());
    let routing_key = format!("rk_recon_{}", Uuid::new_v4().simple());

    let received_count = Arc::new(AtomicUsize::new(0));
    let r_clone = Arc::clone(&received_count);

    // 2. Subscribe consumer
    eventbus
        .subscribe(
            &exchange_name,
            &routing_key,
            move |_msg| {
                let r = Arc::clone(&r_clone);
                Box::pin(async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            },
            None,
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("Initial subscribe failed");

    // 3. Publish first message -> Should be received normally
    let pub_opts = PublishOptions {
        content_type: "text/plain",
        content_encoding: ContentEncoding::None,
        command_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    eventbus
        .publish(
            &exchange_name,
            &routing_key,
            b"message before disconnect",
            &pub_opts,
        )
        .await
        .expect("Initial publish failed");

    // Wait for delivery
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        received_count.load(Ordering::SeqCst),
        1,
        "First message was not received"
    );

    // 4. Force broker to abruptly terminate all connections in this test vhost!
    execute_docker_rabbitmqctl(&[
        "close_all_connections",
        "-p",
        &vhost_name,
        "Simulated connection reset for test",
    ]);

    // 5. Allow ConnectionManager health check to detect drop and reconnect
    // Health check ticks every 1s, initial reconnect delay is 1s (sleep 0s)
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // 6. Publish second message after reconnection -> Auto-healing should route it to consumer
    eventbus
        .publish(
            &exchange_name,
            &routing_key,
            b"message after reconnection",
            &pub_opts,
        )
        .await
        .expect("Publish after reconnection failed");

    // Wait for delivery
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        received_count.load(Ordering::SeqCst),
        2,
        "Auto-healing failed: Consumer did not receive message after reconnection!"
    );

    // 7. Cleanup
    let _ = eventbus.dispose().await;
    execute_docker_rabbitmqctl(&["delete_vhost", &vhost_name]);
}
