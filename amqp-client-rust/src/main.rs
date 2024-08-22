use core::str;
use std::error::Error as StdError;
use std::collections::HashMap;
use uuid::Uuid;
use std::sync::Arc;
use tokio::sync::Mutex;

use amqprs::{
    callbacks::{ChannelCallback, DefaultConnectionCallback}, channel::{
        BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicPublishArguments, Channel, ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments
    }, connection::{Connection, OpenConnectionArguments}, consumer::AsyncConsumer, Ack, BasicProperties, Cancel, CloseChannel, Deliver, Nack, Return
};
use tokio::time;
use async_trait::async_trait;
use url::Url;

struct AsyncConnection {
    connection: Option<Connection>,
    channel: Option<AsyncChannel>,
    connection_type: ConnectionType,
}
use amqprs::error::Error as AMQPError;
pub type AMQPResult<T> = std::result::Result<T, AMQPError>;
pub struct MyChannelCallback;

#[async_trait]
impl ChannelCallback for MyChannelCallback {
    async fn close(&mut self, channel: &Channel, close: CloseChannel) -> AMQPResult<()> {
        #[cfg(feature = "traces")]
        error!(
            "handle close request for channel {}, cause: {}",
            channel, close
        );
        println!("handle close request for channel {}, cause: {}",
            channel, close);
        Ok(())
    }
    async fn cancel(&mut self, channel: &Channel, cancel: Cancel) -> AMQPResult<()> {
        #[cfg(feature = "traces")]
        warn!(
            "handle cancel request for consumer {} on channel {}",
            cancel.consumer_tag(),
            channel
        );
        println!("handle cancel request for consumer {} on channel {}",
            cancel.consumer_tag(),
            channel);
        Ok(())
    }
    async fn flow(&mut self, channel: &Channel, active: bool) -> AMQPResult<bool> {
        #[cfg(feature = "traces")]
        info!(
            "handle flow request active={} for channel {}",
            active, channel
        );
        println!(
            "handle flow request active={} for channel {}",
            active, channel);
        Ok(true)
    }
    async fn publish_ack(&mut self, channel: &Channel, ack: Ack) {
        #[cfg(feature = "traces")]
        info!(
            "handle publish ack delivery_tag={} on channel {}",
            ack.delivery_tag(),
            channel
        );
        println!(
            "handle publish ack delivery_tag={} on channel {}",
            ack.delivery_tag(),
            channel
        );
    }
    async fn publish_nack(&mut self, channel: &Channel, nack: Nack) {
        #[cfg(feature = "traces")]
        warn!(
            "handle publish nack delivery_tag={} on channel {}",
            nack.delivery_tag(),
            channel
        );
        println!(
            "handle publish nack delivery_tag={} on channel {}",
            nack.delivery_tag(),
            channel
        );        
    }
    async fn publish_return(
        &mut self,
        channel: &Channel,
        ret: Return,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        #[cfg(feature = "traces")]
        warn!(
            "handle publish return {} on channel {}, content size: {}",
            ret,
            channel,
            content.len()
        );
        println!(
            "handle publish return {} on channel {}, content size: {}",
            ret,
            channel,
            content.len()
        );        
    }
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

    pub fn channel_is_open(&self) -> bool{
        if !self.channel.is_none(){
            self.channel.as_ref().unwrap().channel.is_open()
        } else {
            false
        }
    }

    pub async fn create_channel(&mut self) {
        if self.is_open() && !self.channel_is_open(){
            if let Some(connection) = &self.connection {
                if let Ok(channel) = connection.open_channel(None).await {
                    channel
                        .register_callback(MyChannelCallback)
                        .await;
                    self.channel = Some(AsyncChannel::new(channel));
                }
            }
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

    pub async fn subscribe<T:AsyncConsumer + Send + 'static>(&mut self, handler: T, routing_key: &str, exchange_name: &str, exchange_type: &str, queue_name: &str) {
        self.setup_exchange(exchange_name, exchange_type, true).await;
        let (queue_name, _, _) = self.channel
            .queue_declare(QueueDeclareArguments::durable_client_named(
                queue_name,
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

    pub async fn rpc_server(&mut self, handler: RPCServerHandler, routing_key: &str, exchange_name: &str, exchange_type: &str, queue_name: &str) {
        self.setup_exchange(exchange_name, exchange_type, true).await;
        let (queue_name, _, _) = self.channel
            .queue_declare(QueueDeclareArguments::durable_client_named(
                queue_name,
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

    pub async fn publish(&self, exchange_name: &str, routing_key: &str, body: Vec<u8>, content_type: &str) {
        let args = BasicPublishArguments::new(exchange_name, routing_key);
        let mut properties = BasicProperties::default();
        properties.with_content_type(content_type);
        let _ = self.channel.basic_publish(BasicProperties::default(), body, args).await;
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
struct Consumer2Callback<H: AsyncRPCServerHandler + Send + Sync> {
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
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut connection = self.pub_connection.lock().await;
        connection.open(&self.config.host, self.config.port, &self.config.username, &self.config.password).await;
        connection.create_channel().await;
        if let Some(channel) = &connection.channel {
            channel.publish(exchange_name, routing_key, body, content_type).await;
        }
        Ok(())
    }

    pub async fn subscribe<T:AsyncConsumer + Send+ 'static>(
        &self,
        exchange_name: &str,
        handler: T,
        routing_key: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut connection = self.sub_connection.lock().await;
        connection.open(&self.config.host, self.config.port, &self.config.username, &self.config.password).await;
        connection.create_channel().await;
        if let Some(ref mut channel) = connection.channel {
            let queue_name = &self.config.options.queue_name;
            let exchange_type = &String::from("direct");
            channel.subscribe(handler, routing_key, exchange_name, exchange_type, &queue_name).await;
            return  Ok(());
        }  // Changed from get_channel to create_channel
        panic!("error");
        
        
    }

    pub async fn rpc_server(
        &self,
        exchange_name: &str,
        handler: RPCServerHandler,
        routing_key: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut connection = self.sub_connection.lock().await;
        connection.open(&self.config.host, self.config.port, &self.config.username, &self.config.password).await;
        connection.create_channel().await;
        if let Some(ref mut channel) = connection.channel {
            let queue_name = &self.config.options.queue_name;
            let exchange_type = &String::from("direct");
            channel.rpc_server(handler, routing_key, exchange_name, exchange_type, &queue_name).await;
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
pub struct SubscribeHandler{
    no_ack: bool,
    requeue: bool,
}
#[async_trait]
impl AsyncConsumer for SubscribeHandler {
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
        if let Err(err) = self.handle(content).await{
            println!("nack to delivery {} on channel {}", deliver, channel);
            let args = BasicNackArguments::new(deliver.delivery_tag(), false, self.requeue);
            channel.basic_nack(args).await.unwrap();
        }else if !self.no_ack {
            #[cfg(feature = "traces")]
            println!("ack to delivery {} on channel {}", deliver, channel);
            let args = BasicAckArguments::new(deliver.delivery_tag(), false);
            channel.basic_ack(args).await.unwrap();
        }
    }
}

pub struct RPCServerHandler{
    no_ack: bool,
    requeue: bool,
}
#[async_trait]
impl AsyncConsumer for RPCServerHandler {
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
        if let Err(err) = self.handle(content).await{
            println!("nack to delivery {} on channel {}", deliver, channel);
            let args = BasicNackArguments::new(deliver.delivery_tag(), false, self.requeue);
            channel.basic_nack(args).await.unwrap();
        }else if !self.no_ack {
            #[cfg(feature = "traces")]
            println!("ack to delivery {} on channel {}", deliver, channel);
            let args = BasicAckArguments::new(deliver.delivery_tag(), false);
            channel.basic_ack(args).await.unwrap();
        }
    }
}
// Placeholder for AsyncSubscriberHandler trait
#[async_trait]
pub trait AsyncSubscriberHandler: Send + Sync {
    async fn handle(&self, body: Vec<u8>) -> Result<(), Box<dyn StdError + Send + Sync>>;
}
#[async_trait]
impl AsyncSubscriberHandler for SubscribeHandler {
    async fn handle(&self, body: Vec<u8>) -> Result<(), Box<dyn StdError + Send + Sync>> {
        Ok(())
    }
}


// Placeholder for AsyncSubscriberHandler trait
#[async_trait]
pub trait AsyncRPCServerHandler: Send + Sync {
    async fn handle(&self, body: Vec<u8>) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>>;
}
#[async_trait]
impl AsyncRPCServerHandler for RPCServerHandler {
    async fn handle(&self, body: Vec<u8>) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        Ok("ok".into())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let handler = SubscribeHandler{no_ack: false, requeue: false};
    
    eventbus.subscribe(&example_event.exchange_name, handler, &example_event.routing_key).await?;
    let content = String::from(
        r#"
            {
                "publisher": "example"
                "data": "Hello, amqprs!"
            }
        "#,
    )
    .into_bytes();
    
    
    while true {
        eventbus.publish(&example_event.exchange_name, &example_event.routing_key, content.clone(), "application/json").await?;
    }
    time::sleep(time::Duration::from_secs(50)).await;
    println!("end");

    Ok(())
}