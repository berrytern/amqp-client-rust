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


pub struct InternalSubscribeHandler {
    pub queue_name: String,
    pub routing_key: String,
    handler: Box<
        dyn Fn(
                Vec<u8>,
            )
                -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
            + Send
            + Sync,
    >,
    content_type: String,
    // response_timeout: i16
}
impl InternalSubscribeHandler {
    pub fn new<F, Fut>(queue_name: &str, routing_key: &str, handler: Arc<F>, content_type: &str) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        Self {
            queue_name: queue_name.to_string(),
            routing_key: routing_key.to_string(),
            content_type: content_type.to_string(),
            handler: Box::new(move |body| Box::pin(handler(body))),
        }
    }
}

pub struct InternalRPCHandler {
    pub queue_name: String,
    pub routing_key: String,
    handler: Box<
        dyn Fn(
                Vec<u8>,
            )
                -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>
            + Send
            + Sync,
    >,
    content_type: String,
    // response_timeout: i16
}
impl InternalRPCHandler {
    pub fn new<F, Fut>(queue_name: &str, routing_key: &str, handler: Arc<F>, content_type: &str) -> Self
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



pub struct BroadSubscribeHandler {
    queue_name: String,
    handlers: Arc<RwLock<HashMap<String, InternalSubscribeHandler>>>,
    // response_timeout: i16
}

pub struct BroadRPCHandler {
    channel: Option<Arc<Channel>>,
    queue_name: String,
    handlers: Arc<RwLock<HashMap<String, InternalRPCHandler>>>,
    // response_timeout: i16
}
pub struct BroadRPCClientHandler {
    handlers: Arc<RwLock<HashMap<String, Sender<String>>>>,
    // response_timeout: i16
}

impl BroadSubscribeHandler {
    pub fn new(
        queue_name: String,
        handlers: Arc<RwLock<HashMap<String, InternalSubscribeHandler>>>,
    ) -> Self {
        Self {
            queue_name,
            handlers,
        }
    }
}
impl BroadRPCHandler {
    pub fn new(
        channel: Option<Arc<Channel>>,
        queue_name: String,
        handlers: Arc<RwLock<HashMap<String, InternalRPCHandler>>>,
    ) -> Self {
        Self {
            channel,
            queue_name,
            handlers,
        }
    }
}

impl BroadRPCClientHandler {
    pub fn new(handlers: Arc<RwLock<HashMap<String, Sender<String>>>>) -> Self {
        Self { handlers }
    }
}

#[async_trait]
impl AsyncConsumer for BroadRPCClientHandler {
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
impl AsyncConsumer for BroadSubscribeHandler {
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
            let internal_handler = rw_handlers.get(&format!("{}{}", queue_name, routing_key)).ok_or("Key not found")?;
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
impl AsyncConsumer for BroadRPCHandler {
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
            let internal_handler = handlers.get(&format!("{}{}", queue_name, routing_key)).ok_or("Key not found")?;
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
                    eprintln!("no reply to");
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
