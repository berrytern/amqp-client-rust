use amqp_client_rust::domain::config::{Config, ConfigOptions};

fn dummy_options() -> ConfigOptions {
    ConfigOptions {
        queue_name: "test_queue".to_string(),
        rpc_queue_name: "test_rpc_queue".to_string(),
        rpc_exchange_name: "test_rpc_exchange".to_string(),
    }
}

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
    assert_eq!(config.virtual_host, "/");
    assert_eq!(config.options.queue_name, "example_queue");
    assert_eq!(config.options.rpc_queue_name, "rpc_queue");
    assert_eq!(config.options.rpc_exchange_name, "rpc_exchange");
}

#[tokio::test]
async fn test_config_from_url_custom() {
    let config = Config::from_url(
        "amqp://lkdas:keik231@debian:1562/orders_vhost",
        dummy_options(),
    )
    .unwrap();
    assert_eq!(config.host, "debian");
    assert_eq!(config.port, 1562);
    assert_eq!(config.username, "lkdas");
    assert_eq!(config.password, "keik231");
    assert_eq!(config.virtual_host, "orders_vhost");
}

#[tokio::test]
async fn test_config_vhost_trailing_slash_and_empty() {
    let config1 = Config::from_url("amqp://guest:guest@localhost:5672/", dummy_options()).unwrap();
    assert_eq!(config1.virtual_host, "/");

    let config2 = Config::from_url("amqp://guest:guest@localhost:5672", dummy_options()).unwrap();
    assert_eq!(config2.virtual_host, "/");
}

#[tokio::test]
async fn test_config_vhost_percent_encoded() {
    // Standard AMQP URL encoding for default vhost "/" is "%2F"
    let config_upper = Config::from_url("amqp://guest:guest@localhost:5672/%2F", dummy_options()).unwrap();
    assert_eq!(config_upper.virtual_host, "/");

    let config_lower = Config::from_url("amqp://guest:guest@localhost:5672/%2f", dummy_options()).unwrap();
    assert_eq!(config_lower.virtual_host, "/");

    // Custom vhost with space (%20)
    let config_space = Config::from_url("amqp://guest:guest@localhost:5672/my%20vhost", dummy_options()).unwrap();
    assert_eq!(config_space.virtual_host, "my vhost");
}

#[tokio::test]
async fn test_config_custom_and_default_ports() {
    // Default amqp port (5672) when omitted
    let config_amqp = Config::from_url("amqp://guest:guest@rabbit.local", dummy_options()).unwrap();
    assert_eq!(config_amqp.port, 5672);

    // Default amqps port (5671) when omitted
    let config_amqps = Config::from_url("amqps://guest:guest@rabbit.local/secure_vhost", dummy_options()).unwrap();
    assert_eq!(config_amqps.port, 5671);
    assert_eq!(config_amqps.virtual_host, "secure_vhost");

    // Custom port explicitly specified
    let config_custom_amqp = Config::from_url("amqp://guest:guest@localhost:5688/dev", dummy_options()).unwrap();
    assert_eq!(config_custom_amqp.port, 5688);
    assert_eq!(config_custom_amqp.virtual_host, "dev");

    // Custom port with amqps scheme
    let config_custom_amqps = Config::from_url("amqps://guest:guest@localhost:5690/prod", dummy_options()).unwrap();
    assert_eq!(config_custom_amqps.port, 5690);
    assert_eq!(config_custom_amqps.virtual_host, "prod");
}

#[tokio::test]
async fn test_config_new_direct() {
    let config = Config::new(
        "my-rabbitmq-host",
        5679,
        "admin",
        "secret",
        dummy_options(),
        "custom_tenancy_vhost",
    );
    assert_eq!(config.host, "my-rabbitmq-host");
    assert_eq!(config.port, 5679);
    assert_eq!(config.username, "admin");
    assert_eq!(config.password, "secret");
    assert_eq!(config.virtual_host, "custom_tenancy_vhost");
}
