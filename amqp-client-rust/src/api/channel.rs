use crate::{
    api::consumers::{BroadHandler, BroadRPCHandler, InternalHandler},
    domain::into_response::IntoResponse,
    errors::AppError,
};
use amqprs::{
    channel::{
        BasicConsumeArguments, BasicPublishArguments, Channel, ExchangeDeclareArguments,
        QueueBindArguments, QueueDeclareArguments,
    },
    BasicProperties, DELIVERY_MODE_TRANSIENT,
};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

pub struct AsyncChannel<T: IntoResponse> {
    pub channel: Channel,
    aux_channel: Option<Arc<Channel>>,
    aux_queue_name: String,
    rpc_futures: Arc<RwLock<HashMap<String, oneshot::Sender<String>>>>,
    rpc_consumer_started: bool,
    consumers: HashMap<String, bool>,
    subscribes: Arc<RwLock<HashMap<String, HashMap<String, InternalHandler<T>>>>>,
}

impl<'a, T: IntoResponse + Send + 'static + std::fmt::Debug> AsyncChannel<T> {
    pub fn new(channel: Channel, aux_channel: Option<Arc<Channel>>) -> Self {
        Self {
            channel,
            aux_channel,
            aux_queue_name: format!("amqp.{}", Uuid::new_v4()),
            rpc_futures: Arc::new(RwLock::new(HashMap::new())),
            rpc_consumer_started: false,
            consumers: HashMap::new(),
            subscribes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn generate_consumer_tag(&self) -> String {
        format!("ctag{}", Uuid::new_v4().to_string())
    }

    pub fn add_subscribe(&mut self, handler: InternalHandler<T>) {
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
impl<'a> AsyncChannel<()> {
    pub async fn subscribe<'b, F, Fut>(
        &'a mut self,
        handler: F,
        routing_key: &'b str,
        exchange_name: &'b str,
        exchange_type: &'b str,
        queue_name: &'b str,
        content_type: &'b str,
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
        self.add_subscribe(InternalHandler::new(
            &queue_name,
            &routing_key,
            handler,
            &content_type,
        ));
        if !self.consumers.contains_key(&queue_name) {
            self.consumers.insert(queue_name.to_string(), true);
            let sub_handler = BroadHandler::new(None, queue_name, Arc::clone(&self.subscribes));
            let _ = self.channel.basic_consume(sub_handler, args).await?;
        }
        Ok(())
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
        content_type: &'b str,
    ) where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        self.add_subscribe(InternalHandler::new(
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
            let sub_handler = BroadHandler::new(
                self.aux_channel.clone(),
                queue_name,
                Arc::clone(&self.subscribes),
            );
            self.channel.basic_consume(sub_handler, args).await.unwrap();
        }
    }
    async fn start_rpc_consumer(&mut self) {
        if !self.rpc_consumer_started {
            if let Some(channel) = &self.aux_channel {
                let mut queue_declare = QueueDeclareArguments::new(&self.aux_queue_name);
                queue_declare.auto_delete(true);
                let (_, _, _) = channel.queue_declare(queue_declare).await.unwrap().unwrap();
                let rpc_handler = BroadRPCHandler::new(Arc::clone(&self.rpc_futures));
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
            Ok(intime) => match intime {
                Ok(result) => Ok(result),
                Err(err) => Err(err.into()),
            },
            Err(err) => Err(err.into()),
        }
    }
}
