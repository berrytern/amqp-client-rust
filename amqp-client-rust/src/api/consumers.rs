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
use tokio::sync::{oneshot::Sender, RwLock};
use dashmap::DashMap;


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
    _content_type: String,
    // response_timeout: i16
}
impl InternalSubscribeHandler {
    // Added ?Sized to F
    pub fn new<F, Fut>(queue_name: &str, routing_key: &str, handler: Arc<F>, content_type: &str) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static + ?Sized,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        Self {
            queue_name: queue_name.to_string(),
            routing_key: routing_key.to_string(),
            _content_type: content_type.to_string(),
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
    _content_type: String,
    // response_timeout: i16
}
impl InternalRPCHandler {
    // Added ?Sized to F
    pub fn new<F, Fut>(queue_name: &str, routing_key: &str, handler: Arc<F>, content_type: &str) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static + ?Sized,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        Self {
            queue_name: queue_name.to_string(),
            routing_key: routing_key.to_string(),
            _content_type: content_type.to_string(),
            handler: Box::new(move |body| Box::pin(handler(body))),
        }
    }
}



pub struct BroadSubscribeHandler {
    queue_name: String,
    handlers: Arc<RwLock<HashMap<String, InternalSubscribeHandler>>>,
    auto_ack: bool,
    // response_timeout: i16
}

pub struct BroadRPCHandler {
    channel: Option<Channel>,
    queue_name: String,
    handlers: Arc<RwLock<HashMap<String, InternalRPCHandler>>>,
    auto_ack: bool,
    // response_timeout: i16
}
pub struct BroadRPCClientHandler {
    handlers: Arc<DashMap<String, Sender<Vec<u8>>>>,
    auto_ack: bool,
    // response_timeout: i16
}

impl BroadSubscribeHandler {
    pub fn new(
        queue_name: String,
        handlers: Arc<RwLock<HashMap<String, InternalSubscribeHandler>>>,
        auto_ack: bool,
    ) -> Self {
        Self {
            queue_name,
            handlers,
            auto_ack,
        }
    }
}
impl BroadRPCHandler {
    pub fn new(
        channel: Option<Channel>,
        queue_name: String,
        handlers: Arc<RwLock<HashMap<String, InternalRPCHandler>>>,
        auto_ack: bool,
    ) -> Self {
        Self {
            channel,
            queue_name,
            handlers,
            auto_ack,
        }
    }
}

impl BroadRPCClientHandler {
    pub fn new(handlers: Arc<DashMap<String, Sender<Vec<u8>>>>, auto_ack: bool) -> Self {
        Self { handlers, auto_ack }
    }
}

#[async_trait]
impl AsyncConsumer for BroadRPCClientHandler {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        if let Some(correlated_id) = basic_properties.correlation_id() {
            {
                if let Some(sender) = self.handlers.remove(correlated_id) {
                    tokio::spawn(async move {
                        if let Err(err) = sender.1.send(content) {
                            eprintln!("The receiver dropped {:?}", err);
                        }
                    });
                }
            }
            if !self.auto_ack {
                let channel = channel.clone(); 
                let delivery_tag = deliver.delivery_tag();
                tokio::spawn(async move {
                    let args = BasicAckArguments::new(delivery_tag, false);
                    if let Err(e) = channel.basic_ack(args).await {
                        eprintln!("Failed to send ack: {}", e);
                    }
                });
            }
        } else {
            if !self.auto_ack {
                let channel = channel.clone();
                let delivery_tag = deliver.delivery_tag();
                tokio::spawn(async move {
                    let args = BasicNackArguments::new(delivery_tag, false, false);
                    let _ = channel.basic_nack(args).await;
                });
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
                if !self.auto_ack {
                    let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                    if let Err(e) = channel.basic_ack(args).await {
                        eprintln!("Failed to send ack: {}", e);
                    }
                }
            }
            Err(_) => {
                if !self.auto_ack {
                    let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                    if let Err(err) = channel.basic_nack(args).await {
                        eprintln!("Failed to send nack: {}", err);
                    }
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
                if !self.auto_ack {
                    let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                    if let Err(e) = channel.basic_ack(args).await {
                        eprintln!("Failed to send ack: {}", e);
                    }
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
                    eprintln!("No reply to");
                }
            }
            Err(_) => {
                if !self.auto_ack {
                    let args = BasicNackArguments::new(deliver.delivery_tag(), false, false);
                    if let Err(err) = channel.basic_nack(args).await {
                        eprintln!("Failed to send nack: {}", err);
                    }
                }
            }
        }
    }
}