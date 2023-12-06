use std::{io, collections::HashMap};
use uuid::Uuid;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback, ConnectionCallback},
    channel::{
        BasicConsumeArguments, BasicPublishArguments, QueueBindArguments, QueueDeclareArguments, Channel, ExchangeDeclareArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    consumer::DefaultConsumer,
    BasicProperties, Close,
};
use async_trait::async_trait;
use tokio::time;
use amqprs::error::Error;

pub struct AsyncConnection<'a> {
    auto_ack: bool,
    prefetch_count: u32,
    connection: Option<Connection>,
    channel: Option<AsyncChannel<'a>>
}
impl <'a> AsyncConnection<'a> {
    pub fn new(auto_ack: bool, prefetch_count: u32) -> Self {
        AsyncConnection { auto_ack, prefetch_count, connection: None, channel: None }
    }
    pub fn is_open(&self) -> bool {
        if let Some(connection) = &self.connection {
            connection.is_open();
        }
        false
    }
    async fn close(&mut self, connection: &Connection, close: Close) -> Result<(), io::Error> {
        #[cfg(feature = "traces")]
        error!(
            "handle close request for connection {}, cause: {}",
            connection, close
        );
        Ok(())
    }

    async fn blocked(&mut self, connection: &Connection, reason: String) {
        #[cfg(feature = "traces")]
        info!(
            "handle blocked notification for connection {}, reason: {}",
            connection, reason
        );
    }

    async fn unblocked(&mut self, connection: &Connection) {
        #[cfg(feature = "traces")]
        info!(
            "handle unblocked notification for connection {}",
            connection
        );
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
            .register_callback(AsyncConnection{auto_ack:self.auto_ack,prefetch_count:self.prefetch_count, connection: self.connection.clone(), channel: None})
            .await
            .unwrap(); 
            self.connection = Some(connection);
        }
    }
    pub async fn open_channel(&'a mut self){
        if let Some(connection) = &self.connection {
            self.channel = Some(
                AsyncChannel::new(connection, None)
            );
            if let Some(channel) = self.channel.as_mut() {
                channel.open().await;
            }
        }
    }
    pub async fn subscribe(&'a mut self, routing_key: &str, exchange_name: &str, exchange_type: &str){
        if let Some(channel) = self.channel.as_mut() {
            channel.subscribe(routing_key, exchange_name, exchange_type).await;
        }
    }
    pub async fn publish(&'a mut self, exchange_name: &str, routing_key: &str, body: String ){
        if let Some(channel) = self.channel.as_mut() {
            channel.publish(routing_key, exchange_name, body).await;
        }
    }
}
pub struct AsyncChannel<'a> { connection: &'a Connection, channel: Option<Channel>, consumers: HashMap<String,Vec<String>> }
impl <'a> AsyncChannel<'a>{
    pub fn new(connection: &'a Connection, channel: Option<Channel>) -> Self {
        AsyncChannel { connection, channel, consumers: HashMap::new() }
    }

    pub async fn open(&mut self) {
        if self.channel.is_none() || !self.channel.as_mut().unwrap().is_open(){
            self.channel = Some(self.connection.open_channel(None).await.unwrap());
            if let Some(channel) = &self.channel {
                channel
                    .register_callback(DefaultChannelCallback)
                    .await
                    .unwrap();
            }
        }
    }
    // open a channel on the connection
    pub async fn setup_exchange(&self, channel: &Channel, exchange_name: &str, exchange_type: &str, durable: bool){
        let mut arguments = ExchangeDeclareArguments::default();
        arguments.exchange = exchange_name.to_string();
        arguments.exchange_type = exchange_type.to_string();
        arguments.durable = durable;
        let _ = channel.exchange_declare(arguments).await;
    }
    fn generate_consumer_tag(&self) -> String {
        // 'ctag%i.%s' % (self.channel_number, uuid.uuid4().hex)
        let mut result = "ctag".to_string();
        result.push_str(&Uuid::new_v4().to_string());
        result
        
    }

    pub async fn subscribe(&mut self, routing_key: &str, exchange_name: &str, exchange_type: &str){
        if let Some(channel) = &self.channel{
            self.setup_exchange(channel, exchange_name, exchange_type, true).await;
            let (queue_name, _, _) = channel
            .queue_declare(QueueDeclareArguments::durable_client_named(
                "amqprs.examples.basic",
            ))
            .await
            .unwrap()
            .unwrap();
            channel
                .queue_bind(QueueBindArguments::new(
                    &queue_name,
                    exchange_name,
                    routing_key,
                ))
                .await
                .unwrap();
            let args = BasicConsumeArguments::new(&queue_name, &self.generate_consumer_tag());
            if !self.consumers.contains_key(&queue_name){
                self.consumers.insert(queue_name, Vec::new());
                channel
                    .basic_consume(DefaultConsumer::new(args.no_ack), args)
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn publish(&self, exchange_name: &str, routing_key: &str, body: String ) {
        if let Some(channel) = &self.channel{
            let args = BasicPublishArguments::new(exchange_name, routing_key);
            let _ = channel.basic_publish(BasicProperties::default(), body.into_bytes(), args).await;
        }
    }

}

#[async_trait]
impl ConnectionCallback for AsyncConnection<'_> {
    async fn close(&mut self, connection: &Connection, close: Close) -> Result<(), Error> {
        #[cfg(feature = "traces")]
        error!(
            "handle close request for connection {}, cause: {}",
            connection, close
        );
        Ok(())
    }

    async fn blocked(&mut self, connection: &Connection, reason: String) {
        #[cfg(feature = "traces")]
        info!(
            "handle blocked notification for connection {}, reason: {}",
            connection, reason
        );
    }

    async fn unblocked(&mut self, connection: &Connection) {
        #[cfg(feature = "traces")]
        info!(
            "handle unblocked notification for connection {}",
            connection
        );
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    // open a connection to RabbitMQ server
    let (routing_key, exchange_name, exchange_type) = ("msg.send", "example", "direct");
    let mut connection = AsyncConnection::new(true, 0);
    connection.open("localhost", 5672, "guest", "guest").await;
    connection.open_channel().await;
    connection.subscribe(routing_key, exchange_name, exchange_type).await;
    let body = String::from(
        r#"
            {
                "publisher": "example"
                "data": "Hello, amqprs!"
            }
        "#,
    );
    connection.publish(routing_key, exchange_name, body).await;

    
    // keep the `channel` and `connection` object from dropping before pub/sub is done.
    // channel/connection will be closed when drop.
    time::sleep(time::Duration::from_secs(100)).await;
    // explicitly close
    // channel.close().await.unwrap();
}
