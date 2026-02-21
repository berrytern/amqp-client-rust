use amqprs::{
    channel::{BasicAckArguments, BasicNackArguments, BasicPublishArguments, Channel},
    consumer::AsyncConsumer,
    BasicProperties, Deliver,
};
use async_trait::async_trait;
use tracing::error;
use std::{collections::HashMap, sync::atomic::{AtomicUsize, Ordering}};
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::{sync::{Notify, OnceCell, RwLock, oneshot::Sender}, time::{Duration, timeout}};
use dashmap::DashMap;

use crate::errors::{AppError, AppErrorType};

type Handler = Arc<
    dyn Fn(
            Vec<u8>,
        )
            -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        + Send
        + Sync,
>;
type RPCHandler = Arc<
    dyn Fn(
            Vec<u8>,
        )
            -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>
        + Send
        + Sync,
>;
pub struct InternalSubscribeHandler {
    pub queue_name: String,
    pub routing_key: String,
    handler: Handler,
    process_timeout: Option<Duration>,
}
impl InternalSubscribeHandler {
    pub fn new<F, Fut>(queue_name: &str, routing_key: &str, handler: Arc<F>, process_timeout: Option<Duration>) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static + ?Sized,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        Self {
            queue_name: queue_name.to_string(),
            routing_key: routing_key.to_string(),
            handler: Arc::new(move |body| Box::pin(handler(body))),
            process_timeout,
        }
    }
}

pub struct InternalRPCHandler {
    pub queue_name: String,
    pub routing_key: String,
    handler: RPCHandler,
    process_timeout: Option<Duration>,
}
impl InternalRPCHandler {
    // Added ?Sized to F
    pub fn new<F, Fut>(queue_name: &str, routing_key: &str, handler: Arc<F>, process_timeout: Option<Duration>) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static + ?Sized,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        Self {
            queue_name: queue_name.to_string(),
            routing_key: routing_key.to_string(),
            handler: Arc::new(move |body| Box::pin(handler(body))),
            process_timeout,
        }
    }
}



pub struct BroadSubscribeHandler {
    queue_name: String,
    handlers: Arc<RwLock<HashMap<String, InternalSubscribeHandler>>>,
    auto_ack: bool,
    in_flight: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
    // response_timeout: i16
}

pub struct BroadRPCHandler {
    channel: Arc<OnceCell<Channel>>,
    queue_name: String,
    handlers: Arc<RwLock<HashMap<String, InternalRPCHandler>>>,
    auto_ack: bool,
    in_flight: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
    // response_timeout: i16
}
pub struct BroadRPCClientHandler {
    handlers: Arc<DashMap<String, Sender<Vec<u8>>>>,
    auto_ack: bool,
    in_flight: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
    // response_timeout: i16
}

impl BroadSubscribeHandler {
    pub fn new(
        queue_name: String,
        handlers: Arc<RwLock<HashMap<String, InternalSubscribeHandler>>>,
        auto_ack: bool,
        in_flight: Arc<AtomicUsize>,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            queue_name,
            handlers,
            auto_ack,
            in_flight,
            shutdown_notify,
        }
    }
}
impl BroadRPCHandler {
    pub fn new(
        channel: Arc<OnceCell<Channel>>,
        queue_name: String,
        handlers: Arc<RwLock<HashMap<String, InternalRPCHandler>>>,
        auto_ack: bool,
        in_flight: Arc<AtomicUsize>,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            channel,
            queue_name,
            handlers,
            auto_ack,
            in_flight,
            shutdown_notify,
        }
    }
}

impl BroadRPCClientHandler {
    pub fn new(handlers: Arc<DashMap<String, Sender<Vec<u8>>>>, auto_ack: bool, in_flight: Arc<AtomicUsize>, shutdown_notify: Arc<Notify>) -> Self {
        Self { handlers, auto_ack, in_flight, shutdown_notify }
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
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if let Some(correlated_id) = basic_properties.correlation_id() {
            if let Some(sender) = self.handlers.remove(correlated_id) {
                if let Err(err) = sender.1.send(content) {
                    error!("The receiver dropped {:?}", err);
                }
            }
            if !self.auto_ack {
                let delivery_tag = deliver.delivery_tag();
                let args = BasicAckArguments::new(delivery_tag, false);
                if let Err(e) = channel.basic_ack(args).await {
                    error!("Failed to send ack: {}", e);
                }
            }
        } else if !self.auto_ack {
            let delivery_tag = deliver.delivery_tag();
            let args = BasicNackArguments::new(delivery_tag, false, false);
            let _ = channel.basic_nack(args).await;
        }
        let previous_count = self.in_flight.fetch_sub(1, Ordering::AcqRel);
        if previous_count == 1 {
            self.shutdown_notify.notify_one();
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
        self.in_flight.fetch_add(1, Ordering::AcqRel);

        let queue_name = self.queue_name.clone();
        let routing_key = deliver.routing_key().to_string();

        let handlers_guard = self.handlers.read().await;
        if let Some(internal_handler) = handlers_guard.get(&format!("{}{}", queue_name, routing_key)) {
            let (handler, process_timeout) = (Arc::clone(&internal_handler.handler), internal_handler.process_timeout);
            drop(handlers_guard);
            match async move {
                match process_timeout {
                    Some(dur) => match timeout(dur, (handler)(content)).await {
                        Ok(res) => res,
                        Err(_) => Err(AppError::new(Some("Response timeout exceed".to_string()), None, AppErrorType::TimeoutError).into()),
                    },
                    None => (handler)(content).await
                }
            }
            .await
            {
                Ok(_) => {
                    if !self.auto_ack {
                        let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                        if let Err(e) = channel.basic_ack(args).await {
                            error!("Failed to send ack: {}", e);
                        }
                    }
                }
                Err(_) => {
                    if !self.auto_ack {
                        let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                        if let Err(err) = channel.basic_nack(args).await {
                            error!("Failed to send nack: {}", err);
                        }
                    }
                }
            };
        } else {
            error!("No handler found for queue {} and routing key {}", queue_name, routing_key);
            if !self.auto_ack {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                if let Err(err) = channel.basic_nack(args).await {
                    error!("Failed to send nack: {}", err);
                }
            }
        }

        let previous_count = self.in_flight.fetch_sub(1, Ordering::AcqRel);
        if previous_count == 1 {
            self.shutdown_notify.notify_one();
        }
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
        self.in_flight.fetch_add(1, Ordering::AcqRel);

        let queue_name = self.queue_name.clone();
        let routing_key = deliver.routing_key().to_string();

        let handlers_guard = self.handlers.read().await;
        if let Some(internal_handler) = handlers_guard.get(&format!("{}{}", queue_name, routing_key)) {
            let (handler, process_timeout) = (Arc::clone(&internal_handler.handler), internal_handler.process_timeout);
            drop(handlers_guard);
            let result = async move {
                match process_timeout {
                    Some(dur) => match timeout(dur, (handler)(content)).await {
                        Ok(res) => res,
                        Err(_) => Err(AppError::new(Some("Response timeout exceed".to_string()), None, AppErrorType::TimeoutError).into()),
                    },
                    None => (handler)(content).await
                }
                
            }
            .await;
            match result {
                Ok(result) => {
                    if !self.auto_ack {
                        let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                        if let Err(e) = channel.basic_ack(args).await {
                            error!("Failed to send ack: {}", e);
                        }
                    }
                    if let Some(reply_to) = basic_properties.reply_to() {
                        if let Some(aux_channel) = self.channel.get() {
                            let args = BasicPublishArguments::new("", reply_to.as_str());
                            if let Err(e) = aux_channel
                                .basic_publish(basic_properties, result, args)
                                .await
                            {
                                error!("Failed to publish response: {}", e);
                            }
                        }
                    } else {
                        error!("No reply to");
                    }
                }
                Err(_) => {
                    if !self.auto_ack {
                        let args = BasicNackArguments::new(deliver.delivery_tag(), false, false);
                        if let Err(err) = channel.basic_nack(args).await {
                            error!("Failed to send nack: {}", err);
                        }
                    }
                }
            }
        } else {
            error!("No handler found for queue {} and routing key {}", queue_name, routing_key);
            if !self.auto_ack {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                if let Err(err) = channel.basic_nack(args).await {
                    error!("Failed to send nack: {}", err);
                }
            }
        }
        let previous_count = self.in_flight.fetch_sub(1, Ordering::AcqRel);
        if previous_count == 1 {
            self.shutdown_notify.notify_one();
        }
    }
}