use amqp_client_rust::domain::config::{Config, ConfigOptions};

#[tokio::test]
async fn test_config_from_url_default() {
    let config = Config::from_url(
        "amqp://guest:guest@localhost:5672",
        ConfigOptions {
            queue_name: "example_queue".to_string(),
            rpc_queue_name: "rpc_queue".to_string(),
            rpc_exchange_name: "rpc_exchange".to_string(),
        },
    )
    .unwrap();
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 5672);
    assert_eq!(config.username, "guest");
    assert_eq!(config.password, "guest");
    assert_eq!(config.options.queue_name, "example_queue");
    assert_eq!(config.options.rpc_queue_name, "rpc_queue");
    assert_eq!(config.options.rpc_exchange_name, "rpc_exchange");
}

#[tokio::test]
async fn test_config_from_url() {
    let config = Config::from_url(
        "amqp://lkdas:keik231@debian:1562",
        ConfigOptions {
            queue_name: "myqueue".to_string(),
            rpc_queue_name: "rpc_myqueue".to_string(),
            rpc_exchange_name: "rpc_myexchange".to_string(),
        },
    )
    .unwrap();
    assert_eq!(config.host, "debian");
    assert_eq!(config.port, 1562);
    assert_eq!(config.username, "lkdas");
    assert_eq!(config.password, "keik231");
    assert_eq!(config.options.queue_name, "myqueue");
    assert_eq!(config.options.rpc_queue_name, "rpc_myqueue");
    assert_eq!(config.options.rpc_exchange_name, "rpc_myexchange");
}
