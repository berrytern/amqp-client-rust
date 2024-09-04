use core::str;
use std::time::Duration;
use std::error::Error as StdError;
use std::collections::HashMap;
use uuid::Uuid;
use std::sync::Arc;
use tokio::time::timeout;
use tokio::sync::oneshot::Sender;
use tokio::sync::{Mutex, RwLock, oneshot};
mod errors;
use crate::errors::{AppError, AppErrorType};


use amqprs::{
    callbacks::{ChannelCallback, DefaultConnectionCallback}, channel::{
        BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicPublishArguments, Channel, ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments
    }, connection::{Connection, OpenConnectionArguments}, consumer::AsyncConsumer, Ack, BasicProperties, Cancel, CloseChannel, Deliver, Nack, Return
};
use tokio::time;
use async_trait::async_trait;
use url::Url;
use std::future::Future;
use std::pin::Pin;

type Result2<T> = std::result::Result<T, Box<dyn StdError>>;


struct AsyncConnection<T:IntoResponse> {
    connection: Option<Connection>,
    channel: Option<AsyncChannel<T>>,
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

impl<T: IntoResponse + Send + 'static + std::fmt::Debug> AsyncConnection<T> {
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

    
}

impl AsyncConnection<()> {
    pub async fn create_channel(&mut self){
        if self.is_open() && !self.channel_is_open(){
            if let Some(connection) = &self.connection {
                if let Ok(channel) = connection.open_channel(None).await {
                    let _ = channel
                        .register_callback(MyChannelCallback)
                        .await;
                    self.channel = Some(AsyncChannel::new(channel, None, false));
                }
            }
        }
    }
}
impl AsyncConnection<Vec<u8>> {
    pub async fn create_channel(&mut self){
        if self.is_open() && !self.channel_is_open(){
            if let Some(connection) = &self.connection {
                if let Ok(channel) = connection.open_channel(None).await {
                    let _ = channel
                        .register_callback(MyChannelCallback)
                        .await;
                    
                    if let Ok(aux_channel) = connection.open_channel(None).await{
                        self.channel = Some(AsyncChannel::new(channel, Some(Arc::new(aux_channel)), false));
                    }
                }
            }
        }
    }
}

pub struct InternalHandler<T>{

    queue_name: String,
    routing_key: String,
    handler: Box<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<T, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
    content_type: String,
    // response_timeout: i16
}
pub struct InternalRPCHandler{
    queue_name: String,
    routing_key: String,
    handler: Box<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
    content_type: String,
    // response_timeout: i16
}
impl<T> InternalHandler<T> {
    pub fn new<F, Fut>(queue_name: &str, routing_key: &str, handler: F, content_type: &str) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        Self {
            queue_name: queue_name.to_string(),
            routing_key: routing_key.to_string(),
            content_type: content_type.to_string(),
            handler: Box::new(move |body| Box::pin(handler(body))),
        }
    }
}

impl InternalRPCHandler {
    
    pub fn new<F, Fut>(queue_name: &str, routing_key: &str, handler: F, content_type: &str)
     -> Self
     where
     F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
     Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        Self {
            queue_name: queue_name.to_string(),
            routing_key: routing_key.to_string(),
            content_type: content_type.to_string(),
            handler: Box::new(move |body| Box::pin(handler(body))),
        }
    }
}
pub struct BroadHandler< T: IntoResponse>{
    channel: Option<Arc<Channel>>,
    queue_name: String,
    handlers: Arc<RwLock<HashMap<String, HashMap<String, InternalHandler<T>>>>>,
    // response_timeout: i16
}
pub struct BroadRPCHandler{
    handlers: Arc<RwLock<HashMap<String, Sender<String>>>>,
    // response_timeout: i16
}

impl<'a, T: IntoResponse> BroadHandler<T>{
    pub fn new(channel: Option<Arc<Channel>>, queue_name: String, handlers: Arc<RwLock<HashMap<String, HashMap<String, InternalHandler<T>>>>>) -> Self {
        Self {
            channel,
            queue_name,
            handlers,
        }
    }
}

impl BroadRPCHandler{
    pub fn new(handlers: Arc<RwLock<HashMap<String, Sender<String>>>>) -> Self {
        Self {
            handlers,
        }
    }
}

#[async_trait]
impl AsyncConsumer for BroadRPCHandler{
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        _content: Vec<u8>,
    ) {
        if let Some(correlated_id) = basic_properties.correlation_id(){
            
            let handlers = Arc::clone(&self.handlers);
            let mut futures = handlers.write().await;
            if let Some(value) = futures.remove(correlated_id) {
                let _ = value.send("Ok".into());
            }
            drop(futures);
            let args = BasicAckArguments::new(deliver.delivery_tag(), false);
            if let Err(e) = channel.basic_ack(args).await {
                eprintln!("Failed to send ack: {}", e);
            }
        } else {
            let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
            if let Err(err) = channel.basic_nack(args).await {
                eprintln!("Failed to send nack: {}", err);
            }
        }
    }
}
pub trait IntoResponse: Send + Sync {
    fn into_response(self) -> Option<Vec<u8>>;
}

impl IntoResponse for () {
    fn into_response(self) -> Option<Vec<u8>> {
        None
    }
}

impl IntoResponse for Vec<u8> {
    fn into_response(self) -> Option<Vec<u8>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncConsumer for BroadHandler<()>{
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let queue_name = self.queue_name.clone();
        let routing_key = deliver.routing_key().to_string();

        // Clone the Arc to move into the async block
        let handlers = Arc::clone(&self.handlers);

        match async move {
            let rw_handlers = handlers.read().await;
            let queue_handlers = rw_handlers.get(&queue_name).ok_or("Queue not found")?;
            let internal_handler = queue_handlers.get(&routing_key).ok_or("Handler not found")?;
            // Call the handler while still holding the read lock
            (internal_handler.handler)(content).await
        }.await {
            Ok(_) => {
                // Handle successful result
                let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                if let Err(e) = channel.basic_ack(args).await {
                    eprintln!("Failed to send ack: {}", e);
                }
            },
            Err(_) => {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                if let Err(err) = channel.basic_nack(args).await {
                    eprintln!("Failed to send nack: {}", err);
                }
            }
        };
    }
}
#[async_trait]
impl AsyncConsumer for BroadHandler<Vec<u8>>{
    
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let queue_name = self.queue_name.clone();
        let routing_key = deliver.routing_key().to_string();

        // Clone the Arc to move into the async block
        let handlers = Arc::clone(&self.handlers);
        let result = async move {
            let handlers = handlers.read().await;
            let queue_handlers = handlers.get(&queue_name).ok_or("Queue not found")?;
            let internal_handler = queue_handlers.get(&routing_key).ok_or("Handler not found")?;
            
            // Call the handler while still holding the read lock
            (internal_handler.handler)(content).await
        }.await;

        match result {
            Ok(result) => {
                // Handle successful result
                let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                if let Err(e) = channel.basic_ack(args).await {
                    eprintln!("Failed to send ack: {}", e);
                }
                if let Some(reply_to) = basic_properties.reply_to() {
                    if let Some(aux_channel) = &self.channel{
                        let args = BasicPublishArguments::new("".into(), reply_to.as_str());
                        if let Err(e) = aux_channel.basic_publish(basic_properties, result, args).await {
                            eprintln!("Failed to publish response: {}", e);
                        }
                    }
                    
                } else{println!("no reply to");}
                
            },
            Err(_) => {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                if let Err(err) = channel.basic_nack(args).await {
                    eprintln!("Failed to send nack: {}", err);
                }
            }
        }
    }
}


pub struct AsyncChannel<T: IntoResponse> {
    channel: Channel,
    aux_channel: Option<Arc<Channel>>,
    aux_queue_name: String,
    rpc_futures: Arc<RwLock<HashMap<String, oneshot::Sender<String>>>>, 
    rpc_consumer_started: bool,
    consumers: HashMap<String, bool>,
    subscribes: Arc<RwLock<HashMap<String, HashMap<String, InternalHandler<T>>>>>,
    rpc_consumer: bool,
}

impl<'a, T: IntoResponse + Send + 'static + std::fmt::Debug> AsyncChannel<T> {
    pub fn new(channel: Channel, aux_channel: Option<Arc<Channel>>, rpc_consumer: bool) -> Self {
        Self {
            channel,
            aux_channel,
            aux_queue_name: format!("amqp.{}", Uuid::new_v4()),
            rpc_futures: Arc::new(RwLock::new(HashMap::new())),
            rpc_consumer_started: false,
            consumers: HashMap::new(),
            subscribes: Arc::new(RwLock::new(HashMap::new())),
            rpc_consumer,
        }
    }

    fn generate_consumer_tag(&self) -> String {
        format!("ctag{}", Uuid::new_v4().to_string())
    }

    pub fn add_subscribe(
        &mut self,
        handler: InternalHandler<T>,
    ) {
        let subscribes = self.subscribes.clone();
        tokio::spawn(async move {
            let mut subscribes = subscribes.write().await;
            subscribes
                .entry(handler.queue_name.to_string())
                .or_insert_with(HashMap::new)
                .entry(handler.routing_key.to_string())
                .or_insert(handler);
        });
    }
    
    pub async fn setup_exchange(&self, exchange_name: &str, exchange_type: &str, durable: bool) {
        let mut arguments = ExchangeDeclareArguments::default();
        arguments.exchange = exchange_name.to_string();
        arguments.exchange_type = exchange_type.to_string();
        arguments.durable = durable;
        let _ = self.channel.exchange_declare(arguments).await;
    }

    pub async fn publish(&self, exchange_name: &str, routing_key: &str, body: Vec<u8>, content_type: &str) {
        let args = BasicPublishArguments::new(exchange_name, routing_key);
        let mut properties = BasicProperties::default();
        properties.with_content_type(content_type);
        let _ = self.channel.basic_publish(properties, body, args).await;
    }
}
impl<'a> AsyncChannel<()> {

    pub async fn subscribe<'b, F, Fut>(
        &'a mut self,
        handler: F,
        routing_key: &'b str,
        exchange_name: &'b str,
        exchange_type: &'b str,
        queue_name: &'b str,
        content_type: &'b str
    ) 
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
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
        self.add_subscribe(InternalHandler::new(&queue_name, &routing_key, handler, &content_type));
        if !self.consumers.contains_key(&queue_name) {
            self.consumers.insert(queue_name.to_string(), true);
            let sub_handler = BroadHandler::new(None, queue_name, Arc::clone(&self.subscribes));
            self.channel
                .basic_consume(sub_handler, args)
                .await
                .unwrap();
        }
    }
}
impl<'a> AsyncChannel<Vec<u8>> {
    pub async fn rpc_server<'b, F, Fut>(
        &'a mut self,
        handler: F,
        routing_key: &'b str,
        exchange_name: &'b str,
        exchange_type: &'b str,
        queue_name: &'b str,
        content_type: &'b str
    ) 
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static
    {
        self.add_subscribe(InternalHandler::new(&queue_name, &routing_key, handler, &content_type));
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
            self.consumers.insert(queue_name.to_string(), true);
            let sub_handler = BroadHandler::new(self.aux_channel.clone(), queue_name, Arc::clone(&self.subscribes));
            self.channel
                .basic_consume(sub_handler, args)
                .await
                .unwrap();
        }
    }
    async fn start_rpc_consumer(&mut self){
        if !self.rpc_consumer_started {
            if let Some(channel) = &self.aux_channel {
                let mut queue_declare  = QueueDeclareArguments::new(
                    &self.aux_queue_name,
                );
                queue_declare.auto_delete(true);
                let (_, _, _) = channel
                    .queue_declare(queue_declare)
                    .await
                    .unwrap()
                    .unwrap();
                let rpc_handler = BroadRPCHandler::new( Arc::clone( &self.rpc_futures));
                let args = BasicConsumeArguments::new(&self.aux_queue_name, &self.generate_consumer_tag());
                channel
                    .basic_consume(rpc_handler, args)
                    .await
                    .unwrap();
                self.rpc_consumer_started = true;
            }
        }
    }
    pub async fn rpc_client(&mut self, exchange_name: &str, routing_key: &str, body: Vec<u8>, content_type: &str, timeout_millis: u64) -> Result<String, AppError> {
        self.start_rpc_consumer().await;
        let correlated_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        
        let rpc_futures = self.rpc_futures.clone();
        
        let mut rpc_futures = rpc_futures.write().await;
        rpc_futures
            .entry(correlated_id.to_string())
            .or_insert(tx);
        drop(rpc_futures);
        let args = BasicPublishArguments::new(exchange_name, routing_key);
        let mut properties = BasicProperties::default();
        properties.with_content_type(content_type);
        properties.with_correlation_id(&correlated_id);
        properties.with_reply_to(&self.aux_queue_name);
        let _ = self.channel.basic_publish(properties, body, args).await;
        match timeout(Duration::from_millis(timeout_millis), rx).await{
            Ok(intime) => match intime {
                Ok(result) => Ok(result),
                Err(err) => Err(err.into())
            },
            Err(e) => Err(e.into())
        }
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
    pub_connection: Arc<Mutex<AsyncConnection<()>>>,
    sub_connection: Arc<Mutex<AsyncConnection<()>>>,
    rpc_client_connection: Arc<Mutex<AsyncConnection<Vec<u8>>>>,
    rpc_server_connection: Arc<Mutex<AsyncConnection<Vec<u8>>>>,
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

    pub async fn subscribe<'b, F, Fut>(
        &self,
        exchange_name: &'b str,
        handler: F,
        routing_key: &'b str,
        content_type: &'b str,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let mut connection = self.sub_connection.lock().await;
        connection.open(&self.config.host, self.config.port, &self.config.username, &self.config.password).await;
        connection.create_channel().await;
        if let Some(ref mut channel) = connection.channel {
            let queue_name = &self.config.options.queue_name;
            let exchange_type = &String::from("direct");
            channel.subscribe(handler, routing_key, exchange_name, exchange_type, &queue_name, &content_type).await;
            return  Ok(());
        }  // Changed from get_channel to create_channel
        panic!("error");
    }

    pub async fn rpc_client(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
        timeout_millis: u64,
    ) -> Result<String, AppError> {
        let mut connection = self.rpc_client_connection.lock().await;
        connection.open(&self.config.host, self.config.port, &self.config.username, &self.config.password).await;
        connection.create_channel().await;
        if let Some(channel) = connection.channel.as_mut() {
            return channel.rpc_client(exchange_name, routing_key, body, content_type, timeout_millis).await;
        }
        Err(AppError::new(Some("invalid channel".to_string()), None, AppErrorType::InternalError))
    }

    pub async fn rpc_server<'b, F, Fut>(
        &self,
        exchange_name: &'b str,
        handler: F,
        routing_key: &'b str,
        content_type: &'b str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let mut connection = self.rpc_server_connection.lock().await;
        connection.open(&self.config.host, self.config.port, &self.config.username, &self.config.password).await;
        connection.create_channel().await;
        if let Some(ref mut channel) = connection.channel {
            let queue_name = &self.config.options.queue_name;
            let exchange_type = &String::from("direct");
            channel.rpc_server(handler, routing_key, exchange_name, exchange_type, &queue_name, &content_type).await;
            return  Ok("".into());
        }  // Changed from get_channel to create_channel
        panic!("error");
    }




    pub async fn dispose(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Close all connections
        Ok(())
    }


    // ... (other methods remain the same)
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
    /* async fn handle(_body: Vec<u8>) -> Result<(), Box<dyn StdError + Send + Sync>> {
        Ok(())
    } */
    
    //eventbus.subscribe(&example_event.exchange_name, handle, &example_event.routing_key, "application/json").await?;
    
    let content = String::from(
        r#"
            {
                "publisher": "example"
                "data": "Hello, amqprs!"
            }
        "#,
    )
    .into_bytes();
    // let rpc_handler = RPCServerHandler{no_ack: false, requeue: false};
    
    async fn rpc_handler(_body: Vec<u8>) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        Ok("Ok".into())
    }
    eventbus.rpc_server("rpc_exchange", rpc_handler, &example_event.routing_key, "application/json").await?;
    // eventbus.publish("rpc_exchange", &example_event.routing_key, content.clone(), "application/json").await?;
    loop {
        // 
        let _result = eventbus.rpc_client("rpc_exchange", &example_event.routing_key, content.clone(), "application/json", 10000).await?;
    }
    time::sleep(time::Duration::from_secs(50)).await;
    println!("end");

    Ok(())
}