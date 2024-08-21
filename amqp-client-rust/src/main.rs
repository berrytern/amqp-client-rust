use std::collections::HashMap;
use uuid::Uuid;
use std::sync::Arc;
use tokio::sync::Mutex;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        BasicAckArguments, BasicConsumeArguments, BasicPublishArguments, Channel, ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments
    },
    connection::{Connection, OpenConnectionArguments},
    consumer::{AsyncConsumer, DefaultConsumer},
    BasicProperties, Deliver,
};
use tokio::time;
use async_trait::async_trait;
use url::Url;

struct AsyncConnection {
    connection: Option<Connection>,
    channel: Option<Channel>,
    connection_type: ConnectionType,
}

impl AsyncConnection {
    async fn new(connection_type: ConnectionType) -> Self {
        Self {
            connection: None,
            channel: None,
            connection_type,
        }
    }

    pub fn is_open(&self) -> bool {
        self.connection.as_ref().map_or(false, |conn| conn.is_open())
    }

    pub async fn open(&mut self, host: &str, port: u16, username: &str, password: &str) {
        if !self.is_open() {
            let connection = Connection::open(&OpenConnectionArguments::new(
                host,
                port,
                username,
                password,
            ))
            .await
            .unwrap();
            connection
                .register_callback(DefaultConnectionCallback)
                .await
                .unwrap(); 
            self.connection = Some(connection);
        }
    }

    pub async fn create_channel(&self) -> Option<AsyncChannel> {
        if let Some(connection) = &self.connection {
            let channel = connection.open_channel(None).await.unwrap();
            channel
                .register_callback(DefaultChannelCallback)
                .await
                .unwrap();
            Some(AsyncChannel::new(channel))
        } else {
            None
        }
    }
}

pub struct AsyncChannel {
    channel: Channel,
    consumers: HashMap<String, Vec<String>>,
}

impl AsyncChannel {
    pub fn new(channel: Channel) -> Self {
        AsyncChannel {
            channel,
            consumers: HashMap::new(),
        }
    }

    pub async fn setup_exchange(&self, exchange_name: &str, exchange_type: &str, durable: bool) {
        let mut arguments = ExchangeDeclareArguments::default();
        arguments.exchange = exchange_name.to_string();
        arguments.exchange_type = exchange_type.to_string();
        arguments.durable = durable;
        let _ = self.channel.exchange_declare(arguments).await;
    }

    fn generate_consumer_tag(&self) -> String {
        format!("ctag{}", Uuid::new_v4().to_string())
    }

    pub async fn subscribe(&mut self, handler: ExampleHandler, routing_key: &str, exchange_name: &str, exchange_type: &str) {
        self.setup_exchange(exchange_name, exchange_type, true).await;
        let (queue_name, _, _) = self.channel
            .queue_declare(QueueDeclareArguments::durable_client_named(
                "amqprs.examples.basic",
            ))
            .await
            .unwrap()
            .unwrap();
        self.channel
            .queue_bind(QueueBindArguments::new(
                &queue_name,
                exchange_name,
                routing_key,
            ))
            .await
            .unwrap();
        let args = BasicConsumeArguments::new(&queue_name, &self.generate_consumer_tag());
        if !self.consumers.contains_key(&queue_name) {
            self.consumers.insert(queue_name.clone(), Vec::new());
            self.channel
                .basic_consume(handler, args)
                .await
                .unwrap();
        }
    }

    pub async fn publish(&self, exchange_name: &str, routing_key: &str, body: String) {
        let args = BasicPublishArguments::new(exchange_name, routing_key);
        let _ = self.channel.basic_publish(BasicProperties::default(), body.into_bytes(), args).await;
    }
}

// Placeholder for Config struct
pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub options: ConfigOptions,
}
impl Config {
    pub fn from_url(url: &str, options: ConfigOptions) -> Result<Config, Box<dyn std::error::Error>> {
        let parsed_url = Url::parse(url)?;
        let host = parsed_url.host_str().ok_or("No host in URL")?.to_string();
        let port = parsed_url.port().unwrap_or(5672);
        let username = parsed_url.username().to_string();
        let password = parsed_url.password().unwrap_or("").to_string();
        
        Ok(Config {
            host,
            port,
            username,
            password,
            options,
        })
    }

    pub fn new(host: String, port: u16, username: String, password: String, options: ConfigOptions) -> Config {
        Config {
            host,
            port,
            username,
            password,
            options,
        }
    }
}
// Placeholder for ConfigOptions struct
pub struct ConfigOptions {
    pub rpc_queue_name: String,
    pub rpc_exchange_name: String,
    pub queue_name: String,
}


pub struct IntegrationEvent{
    routing_key: String,
    exchange_name: String,
}

impl IntegrationEvent{
    pub fn new(routing_key: &str, exchange_name: &str) -> Self {
        Self {
            routing_key: routing_key.to_string(),
            exchange_name: exchange_name.to_string(),
        }
    }
    fn event_type(&self) -> String {
        self.exchange_name.to_string()
    }
}

// Placeholder for AsyncSubscriberHandler trait
#[async_trait]
pub trait AsyncSubscriberHandler {
    async fn handle(&self, body: Vec<u8>) -> Result<(), Box<dyn std::error::Error>>;
}

enum ConnectionType {
    Publish,
    Subscribe,
    RpcClient,
    RpcServer,
}
pub struct AsyncEventbusRabbitMQ {
    config: Config,
    pub_connection: Arc<Mutex<AsyncConnection>>,
    sub_connection: Arc<Mutex<AsyncConnection>>,
    rpc_client_connection: Arc<Mutex<AsyncConnection>>,
    rpc_server_connection: Arc<Mutex<AsyncConnection>>,
}


struct ConsumerCallback<H: AsyncSubscriberHandler + Send + Sync> {
    handler: Arc<H>,
}

impl AsyncEventbusRabbitMQ {
    // ... (other methods remain the same)
    pub async fn new(config: Config) -> Self {
        Self {
            config,
            pub_connection: Arc::new(Mutex::new(AsyncConnection::new(ConnectionType::Publish).await)),
            sub_connection: Arc::new(Mutex::new(AsyncConnection::new(ConnectionType::Subscribe).await)),
            rpc_client_connection: Arc::new(Mutex::new(AsyncConnection::new(ConnectionType::RpcClient).await)),
            rpc_server_connection: Arc::new(Mutex::new(AsyncConnection::new(ConnectionType::RpcServer).await)),
        }
    }

    pub async fn publish(
        &self,
        event: &IntegrationEvent,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut connection = self.pub_connection.lock().await;
        
        connection.open(&self.config.host, self.config.port, &self.config.username, &self.config.password).await;
        
        if let Some(channel) = connection.create_channel().await {
            let args = BasicPublishArguments::new(
                &event.event_type(),
                routing_key,
            );
            
            channel.publish(&event.event_type(), routing_key, String::from_utf8(body)?).await;
            Ok(())
        } else {
            Err("Failed to create channel".into())
        }
    }

    pub async fn subscribe(
        &self,
        event: &IntegrationEvent,
        handler: ExampleHandler,
        routing_key: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut connection = self.sub_connection.lock().await;
        connection.open(&self.config.host, self.config.port, &self.config.username, &self.config.password).await;
        if let Some(mut channel) = connection.create_channel().await{
            let queue_name = &self.config.options.queue_name;
            let exchange_name = &event.event_type();
            let exchange_type = &String::from("direct");
            channel.subscribe(handler, exchange_name, queue_name, exchange_type).await;
            return  Ok(());
        }  // Changed from get_channel to create_channel
        panic!("error");
        
        
    }


    pub async fn dispose(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Close all connections
        Ok(())
    }


    // ... (other methods remain the same)
}
pub struct ExampleHandler{
    no_ack: bool,
}

#[async_trait]
impl AsyncConsumer for ExampleHandler {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        #[cfg(feature = "traces")]
        info!(
            "consume delivery {} on channel {}, content size: {}",
            deliver,
            channel,
            content.len()
        );

        // ack explicitly if manual ack
        if !self.no_ack {
            #[cfg(feature = "traces")]
            info!("ack to delivery {} on channel {}", deliver, channel);
            let args = BasicAckArguments::new(deliver.delivery_tag(), false);
            channel.basic_ack(args).await.unwrap();
        }
    }
}

#[async_trait]
impl AsyncSubscriberHandler for ExampleHandler {
    async fn handle(&self, body: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        println!("Received message: {}", String::from_utf8_lossy(&body));
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (routing_key, exchange_name, exchange_type) = ("msg.send", "example", "direct");
    let config = Config::from_url("amqp://guest:guest@localhost:5672", ConfigOptions {
        queue_name: "example_queue".to_string(),
        rpc_queue_name: "rpc_queue".to_string(),
        rpc_exchange_name: "rpc_exchange".to_string(),
    })?;
    
    let eventbus = AsyncEventbusRabbitMQ::new(config).await;
    let example_event = IntegrationEvent::new(
        "teste.iso",
        "example.exchange"
    );
    let handler = ExampleHandler{no_ack:true};
    
    eventbus.subscribe(&example_event, handler, "example.routing_key").await?;
    
    
    time::sleep(time::Duration::from_secs(100)).await;

    Ok(())
}