use crate::domain::into_response::IntoResponse;
use amqprs::{
    channel::{BasicAckArguments, BasicNackArguments, BasicPublishArguments, Channel},
    consumer::AsyncConsumer,
    BasicProperties, Deliver,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot::Sender;
use tokio::sync::RwLock;

pub struct InternalHandler<T> {
    pub queue_name: String,
    pub routing_key: String,
    handler: Box<
        dyn Fn(
                Vec<u8>,
            )
                -> Pin<Box<dyn Future<Output = Result<T, Box<dyn StdError + Send + Sync>>> + Send>>
            + Send
            + Sync,
    >,
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

pub struct BroadHandler<T: IntoResponse> {
    channel: Option<Arc<Channel>>,
    queue_name: String,
    handlers: Arc<RwLock<HashMap<String, HashMap<String, InternalHandler<T>>>>>,
    // response_timeout: i16
}
pub struct BroadRPCHandler {
    handlers: Arc<RwLock<HashMap<String, Sender<String>>>>,
    // response_timeout: i16
}

impl<'a, T: IntoResponse> BroadHandler<T> {
    pub fn new(
        channel: Option<Arc<Channel>>,
        queue_name: String,
        handlers: Arc<RwLock<HashMap<String, HashMap<String, InternalHandler<T>>>>>,
    ) -> Self {
        Self {
            channel,
            queue_name,
            handlers,
        }
    }
}

impl BroadRPCHandler {
    pub fn new(handlers: Arc<RwLock<HashMap<String, Sender<String>>>>) -> Self {
        Self { handlers }
    }
}

#[async_trait]
impl AsyncConsumer for BroadRPCHandler {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        _content: Vec<u8>,
    ) {
        if let Some(correlated_id) = basic_properties.correlation_id() {
            let handlers = Arc::clone(&self.handlers);
            {
                let mut futures = handlers.write().await;
                if let Some(value) = futures.remove(correlated_id) {
                    tokio::spawn(async move {
                        if let Err(_) = value.send("Ok".into()) {
                            println!("the receiver dropped");
                        }
                    });
                }
            }
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

#[async_trait]
impl AsyncConsumer for BroadHandler<()> {
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
            let internal_handler = queue_handlers
                .get(&routing_key)
                .ok_or("Handler not found")?;
            // Call the handler while still holding the read lock
            (internal_handler.handler)(content).await
        }
        .await
        {
            Ok(_) => {
                // Handle successful result
                let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                if let Err(e) = channel.basic_ack(args).await {
                    eprintln!("Failed to send ack: {}", e);
                }
            }
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
impl AsyncConsumer for BroadHandler<Vec<u8>> {
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
            let internal_handler = queue_handlers
                .get(&routing_key)
                .ok_or("Handler not found")?;
            // Call the handler while still holding the read lock
            (internal_handler.handler)(content).await
        }
        .await;

        match result {
            Ok(result) => {
                // Handle successful result
                let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                if let Err(e) = channel.basic_ack(args).await {
                    eprintln!("Failed to send ack: {}", e);
                }
                if let Some(reply_to) = basic_properties.reply_to() {
                    if let Some(aux_channel) = &self.channel {
                        let args = BasicPublishArguments::new("".into(), reply_to.as_str());
                        if let Err(e) = aux_channel
                            .basic_publish(basic_properties, result, args)
                            .await
                        {
                            eprintln!("Failed to publish response: {}", e);
                        }
                    }
                } else {
                    println!("no reply to");
                }
            }
            Err(_) => {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                if let Err(err) = channel.basic_nack(args).await {
                    eprintln!("Failed to send nack: {}", err);
                }
            }
        }
    }
}
