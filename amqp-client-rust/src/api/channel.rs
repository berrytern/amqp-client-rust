use crate::{
    api::consumers::{BroadSubscribeHandler, BroadRPCHandler, BroadRPCClientHandler, InternalSubscribeHandler, InternalRPCHandler},
    errors::AppError,
};
use amqprs::{
    channel::{
        BasicConsumeArguments, BasicPublishArguments, Channel, ExchangeDeclareArguments,
        QueueBindArguments, QueueDeclareArguments,
    }, connection::Connection, BasicProperties, DELIVERY_MODE_TRANSIENT
};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

use super::connection::AsyncConnection;

pub struct AsyncChannel {
    pub channel: Channel,
    connection: Arc<Mutex<Connection>>,
    aux_channel: Option<Arc<Channel>>,
    aux_queue_name: String,
    rpc_futures: Arc<RwLock<HashMap<String, oneshot::Sender<String>>>>,
    rpc_consumer_started: bool,
    consumers: HashMap<String, bool>,
    subscribes: Arc<RwLock<HashMap<String, InternalSubscribeHandler>>>,
    rpc_subscribes: Arc<RwLock<HashMap<String, InternalRPCHandler>>>,
}

impl AsyncChannel {
    pub fn new(channel: Channel, connection: Arc<Mutex<Connection>>) -> Self {
        Self {
            channel,
            connection,
            aux_channel: None,
            aux_queue_name: format!("amqp.{}", Uuid::new_v4()),
            rpc_futures: Arc::new(RwLock::new(HashMap::new())),
            rpc_consumer_started: false,
            consumers: HashMap::new(),
            subscribes: Arc::new(RwLock::new(HashMap::new())),
            rpc_subscribes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn generate_consumer_tag(&self) -> String {
        format!("ctag{}", Uuid::new_v4().to_string())
    }

    pub fn add_subscribe(&mut self, handler: InternalSubscribeHandler) {
        let subscribes = self.subscribes.clone();
        tokio::spawn(async move {
            let mut subscribes = subscribes.write().await;
            subscribes
                .entry(format!("{}{}", handler.queue_name, handler.routing_key))
                .or_insert(handler);
        });
    }

    pub fn add_rpc_subscribe(&mut self, handler: InternalRPCHandler) {
        let rpc_subscribes = self.rpc_subscribes.clone();
        tokio::spawn(async move {
            let mut rpc_subscribes = rpc_subscribes.write().await;
            rpc_subscribes
                .entry(format!("{}{}", handler.queue_name, handler.routing_key))
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

    pub async fn publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) {
        let args = BasicPublishArguments::new(exchange_name, routing_key);
        let mut properties = BasicProperties::default();
        properties.with_content_type(content_type);
        let _ = self.channel.basic_publish(properties, body, args).await;
    }
}
impl<'a> AsyncChannel {
    pub async fn subscribe<F, Fut>(
        &mut self,
        handler: Arc<F>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        content_type: &str,
    ) -> Result<(), AppError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        self.setup_exchange(exchange_name, exchange_type, true)
            .await;
        let (queue_name, _, _) = self
            .channel
            .queue_declare(QueueDeclareArguments::durable_client_named(queue_name))
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
        self.add_subscribe(InternalSubscribeHandler::new(
            &queue_name,
            &routing_key,
            handler,
            &content_type,
        ));
        if !self.consumers.contains_key(&queue_name) {
            self.consumers.insert(queue_name.to_string(), true);
            let sub_handler = BroadSubscribeHandler::new(queue_name, Arc::clone(&self.subscribes));
            let _ = self.channel.basic_consume(sub_handler, args).await?;
        }
        Ok(())
    }
}
impl<'a> AsyncChannel{
    pub async fn rpc_server< F, Fut>(
        &'a mut self,
        handler: Arc<F>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        content_type: &str,
    ) where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        if self.aux_channel.is_none() {
            self.aux_channel = Some(Arc::new(self.connection.lock().await.open_channel(None).await.unwrap()));
        }
        self.add_rpc_subscribe(InternalRPCHandler::new(
            &queue_name,
            &routing_key,
            handler,
            &content_type,
        ));
        self.setup_exchange(exchange_name, exchange_type, true)
            .await;
        let (queue_name, _, _) = self
            .channel
            .queue_declare(QueueDeclareArguments::durable_client_named(queue_name))
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
            let sub_handler = BroadRPCHandler::new(
                self.aux_channel.clone(),
                queue_name,
                Arc::clone(&self.rpc_subscribes),
            );
            self.channel.basic_consume(sub_handler, args).await.unwrap();
        }
    }
    async fn start_rpc_consumer(&mut self) {
        if !self.rpc_consumer_started {
            self.aux_channel = Some(Arc::new(self.connection.lock().await.open_channel(None).await.unwrap()));
            if let Some(channel) = &self.aux_channel {
                let mut queue_declare = QueueDeclareArguments::new(&self.aux_queue_name);
                queue_declare.auto_delete(true);
                let (_, _, _) = channel.queue_declare(queue_declare).await.unwrap().unwrap();
                let rpc_handler = BroadRPCClientHandler::new(Arc::clone(&self.rpc_futures));
                let args =
                    BasicConsumeArguments::new(&self.aux_queue_name, &self.generate_consumer_tag());
                channel.basic_consume(rpc_handler, args).await.unwrap();
                self.rpc_consumer_started = true;
            }
        }
    }
    pub async fn rpc_client(
        &mut self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
        timeout_millis: u64,
    ) -> Result<String, AppError> {
        self.start_rpc_consumer().await;
        let (tx, rx) = oneshot::channel();
        let correlated_id = Uuid::new_v4().to_string();

        let rpc_futures = self.rpc_futures.clone();

        let mut rpc_futures = rpc_futures.write().await;
        rpc_futures.entry(correlated_id.to_string()).or_insert(tx);
        drop(rpc_futures);
        let mut args = BasicPublishArguments::new(exchange_name, routing_key);
        args.mandatory(false);
        let mut properties = BasicProperties::default();
        properties.with_content_type(content_type);
        properties.with_correlation_id(&correlated_id);
        properties.with_reply_to(&self.aux_queue_name);
        properties.with_delivery_mode(DELIVERY_MODE_TRANSIENT);
        let _ = self.channel.basic_publish(properties, body, args).await?;
        match timeout(Duration::from_millis(timeout_millis), rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(err.into()),
            Err(err) => Err(err.into()),
        }
    }
}
