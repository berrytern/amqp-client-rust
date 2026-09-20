use amqprs::{
    channel::{BasicAckArguments, BasicNackArguments, BasicPublishArguments, Channel},
    consumer::AsyncConsumer,
    BasicProperties, Deliver,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use tracing::error;
use std::{collections::HashMap, sync::atomic::{AtomicUsize, Ordering}};
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;
use tokio::{sync::{Notify, oneshot::Sender}, time::{Duration, timeout}};
use dashmap::DashMap;

use futures::FutureExt;
use crate::{api::utils::{ContentEncoding, Handler, Message, RPCHandler, TopicTrie, compress, decompress}, errors::{AppError, AppErrorType}};

pub(crate) struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
}

impl InFlightGuard {
    pub(crate) fn new(in_flight: Arc<AtomicUsize>, shutdown_notify: Arc<Notify>) -> Self {
        in_flight.fetch_add(1, Ordering::AcqRel);
        Self {
            in_flight,
            shutdown_notify,
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let previous_count = self.in_flight.fetch_sub(1, Ordering::AcqRel);
        if previous_count == 1 {
            self.shutdown_notify.notify_one();
        }
    }
}

fn extract_panic_message(err: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic payload".to_string()
    }
}

#[derive(Clone)]
pub struct InternalSubscribeHandler {
    handler: Handler,
    process_timeout: Option<Duration>,
}
impl InternalSubscribeHandler {
    pub fn new<F, Fut>(handler: Arc<F>, process_timeout: Option<Duration>) -> Self
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static + ?Sized,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        Self {
            handler: Arc::new(move |body| Box::pin(handler(body))),
            process_timeout,
        }
    }
}

#[derive(Clone)]
pub struct InternalRPCHandler {
    handler: RPCHandler,
    process_timeout: Option<Duration>,
}
impl InternalRPCHandler {
    // Added ?Sized to F
    pub fn new(handler: RPCHandler, process_timeout: Option<Duration>) -> Self
    {
        Self {
            handler: Arc::new(move |body| Box::pin(handler(body))),
            process_timeout,
        }
    }
}



pub struct BroadSubscribeHandler {
    handlers: Arc<ArcSwap<TopicTrie<InternalSubscribeHandler>>>,
    auto_ack: bool,
    in_flight: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
    // response_timeout: i16
}

pub struct BroadRPCHandler {
    channel: Arc<Channel>,
    handlers: Arc<ArcSwap<HashMap<String, InternalRPCHandler>>>,
    auto_ack: bool,
    in_flight: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
    // response_timeout: i16
}
pub type RpcFuturesMap = Arc<DashMap<String, Sender<Result<Vec<u8>, AppError>>>>;

pub struct BroadRPCClientHandler {
    handlers: RpcFuturesMap,
    auto_ack: bool,
    in_flight: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
    // response_timeout: i16
}

impl BroadSubscribeHandler {
    pub fn new(
        handlers: Arc<ArcSwap<TopicTrie<InternalSubscribeHandler>>>,
        auto_ack: bool,
        in_flight: Arc<AtomicUsize>,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            handlers,
            auto_ack,
            in_flight,
            shutdown_notify,
        }
    }
}
impl BroadRPCHandler {
    pub fn new(
        channel: Arc<Channel>,
        handlers: Arc<ArcSwap<HashMap<String, InternalRPCHandler>>>,
        auto_ack: bool,
        in_flight: Arc<AtomicUsize>,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            channel,
            handlers,
            auto_ack,
            in_flight,
            shutdown_notify,
        }
    }
}

impl BroadRPCClientHandler {
    pub fn new(handlers: RpcFuturesMap, auto_ack: bool, in_flight: Arc<AtomicUsize>, shutdown_notify: Arc<Notify>) -> Self {
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
        let _guard = InFlightGuard::new(Arc::clone(&self.in_flight), Arc::clone(&self.shutdown_notify));
        if let Some(correlated_id) = basic_properties.correlation_id() {
            if let Some(sender) = self.handlers.remove(correlated_id) {
                let res: Result<Vec<u8>, AppError> = if basic_properties.message_type().map(|s| s.as_str()) == Some("error") {
                    let err_msg = String::from_utf8_lossy(&content).to_string();
                    Err(AppError::new(
                        Some("RPC server returned error".to_string()),
                        Some(err_msg),
                        AppErrorType::UnexpectedResultError,
                    ))
                } else {
                    match decompress(content, basic_properties.content_encoding().map(|e| e.as_str())) {
                        Ok(c) => Ok(c),
                        Err(e) => {
                            error!("Failed to decompress RPC response: {}", e);
                            Err(AppError::new(
                                Some("Failed to decompress RPC response".to_string()),
                                Some(e.to_string()),
                                AppErrorType::InternalError,
                            ))
                        }
                    }
                };
                if let Err(err) = sender.1.send(res) {
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
    }
}

#[async_trait]
impl AsyncConsumer for BroadSubscribeHandler {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let guard = InFlightGuard::new(Arc::clone(&self.in_flight), Arc::clone(&self.shutdown_notify));

        let routing_key = deliver.routing_key().to_string(); // Own the string
        let handlers_guard = self.handlers.load().clone();
        let handlers = handlers_guard.search(&routing_key);

        if handlers.is_empty() {
            error!("No handler found for routing key {}", routing_key);
            if !self.auto_ack {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, false);
                let _ = channel.basic_nack(args).await;
            }
            return;
        }

        let channel = channel.clone();
        let auto_ack = self.auto_ack;

        tokio::spawn(async move {
            let _guard = guard;
            let success = async {
                let decompressed_content = match decompress(content, basic_properties.content_encoding().map(|e| e.as_str())) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to decompress content: {}", e);
                        return false;
                    }
                };

                let futures = handlers.iter().map(|i| {
                    let content_clone = &decompressed_content; 
                    let message = Message {
                        body: Arc::from(&content_clone[..]),
                        content_type: basic_properties.content_type().map(|s| s.to_string()),
                    };
                    let handler = Arc::clone(&i.handler);
                    let process_timeout = i.process_timeout;

                    async move {
                        let res = match std::panic::AssertUnwindSafe(async {
                            match process_timeout {
                                Some(dur) => match timeout(dur, (handler)(message)).await {
                                    Ok(res) => res,
                                    Err(_) => Err(AppError::new(Some("Response timeout exceed".to_string()), None, AppErrorType::TimeoutError).into()),
                                },
                                None => (handler)(message).await
                            }
                        }).catch_unwind().await {
                            Ok(res) => res,
                            Err(panic_err) => {
                                let panic_msg = extract_panic_message(&panic_err);
                                error!("Consumer handler panicked: {}", panic_msg);
                                Err(AppError::new(Some("Consumer handler panicked".to_string()), Some(panic_msg), AppErrorType::InternalError).into())
                            }
                        };

                        if let Err(ref e) = res {
                            error!("Handler execution error: {}", e);
                        }
                        res
                    }
                });

                let results = futures::future::join_all(futures).await;

                results.into_iter().all(|res| res.is_ok())
            }.await;

            if !auto_ack {
                if success {
                    let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                    if let Err(e) = channel.basic_ack(args).await {
                        error!("Failed to send ack: {}", e);
                    }
                } else {
                    let args = BasicNackArguments::new(deliver.delivery_tag(), false, false);
                    if let Err(err) = channel.basic_nack(args).await {
                        error!("Failed to send nack: {}", err);
                    }
                }
            }
            drop(handlers);
            drop(handlers_guard);
        });
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
        let guard = InFlightGuard::new(Arc::clone(&self.in_flight), Arc::clone(&self.shutdown_notify));

        let routing_key = deliver.routing_key().as_str();

        let handlers_guard = self.handlers.load();
        if let Some(internal_handler) = handlers_guard.get(routing_key) {
            let (handler, process_timeout) = (Arc::clone(&internal_handler.handler), internal_handler.process_timeout);
            drop(handlers_guard);
            let channel = channel.clone();
            let aux_channel = Arc::clone(&self.channel);
            let auto_ack = self.auto_ack;
            tokio::spawn(async move {
                let _guard = guard;
                match decompress(content, basic_properties.content_encoding().map(|e| e.as_str())) {
                    Ok(decompressed_content) => {
                        let message = Message {
                            body: Arc::from(&decompressed_content[..]),
                            content_type: basic_properties.content_type().map(|s| s.to_string()),
                        };
                        let result = match std::panic::AssertUnwindSafe(async {
                            match process_timeout {
                                Some(dur) => match timeout(dur, (handler)(message)).await {
                                    Ok(res) => res,
                                    Err(_) => Err(AppError::new(Some("Response timeout exceed".to_string()), None, AppErrorType::TimeoutError).into()),
                                },
                                None => (handler)(message).await
                            }
                        }).catch_unwind().await {
                            Ok(res) => res,
                            Err(panic_err) => {
                                let panic_msg = extract_panic_message(&panic_err);
                                error!("RPC handler panicked: {}", panic_msg);
                                Err(AppError::new(Some("RPC handler panicked".to_string()), Some(panic_msg), AppErrorType::InternalError).into())
                            }
                        };
                        match result {
                            Ok(result) => {
                                if !auto_ack {
                                    let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                                    if let Err(e) = channel.basic_ack(args).await {
                                        error!("Failed to send ack: {}", e);
                                    }
                                }
                                if let Some(reply_to) = basic_properties.reply_to() {
                                    let mut content = result.body;
                                    let mut props = BasicProperties::default();
                                    if let Some(correlation_id) = basic_properties.correlation_id() {
                                        props.with_correlation_id(correlation_id);
                                    }
                                    if let Some(ct) = result.content_type.as_deref().or_else(|| basic_properties.content_type().map(|s| s.as_str())) {
                                        props.with_content_type(ct);
                                    }
                                    if let Some(content_encoding) = basic_properties.content_encoding() {
                                        if let Some(encoding) = ContentEncoding::from_str(content_encoding.as_str()) {
                                            if encoding != ContentEncoding::None {
                                                if let Ok(compressed_body) = compress(content.as_ref(), encoding) {
                                                    props.with_content_encoding(encoding.as_str());
                                                    content = compressed_body.into();
                                                }
                                            }
                                        }
                                    }
                                    props.with_message_type("normal");
                                    let args = BasicPublishArguments::new("", reply_to.as_str());
                                    if let Err(e) = aux_channel
                                        .basic_publish(props, content.to_vec(), args)
                                        .await
                                    {
                                        error!("Failed to publish response: {}", e);
                                    }
                                } else {
                                    error!("No reply to");
                                }
                            }
                            Err(err) => {
                                if !auto_ack {
                                    let args = BasicNackArguments::new(deliver.delivery_tag(), false, false);
                                    if let Err(err) = channel.basic_nack(args).await {
                                        error!("Failed to send nack: {}", err);
                                    }
                                }
                                if let Some(reply_to) = basic_properties.reply_to() {
                                    let mut props = BasicProperties::default();
                                    if let Some(correlation_id) = basic_properties.correlation_id() {
                                        props.with_correlation_id(correlation_id);
                                    }
                                    if let Some(content_type) = basic_properties.content_type() {
                                        props.with_content_type(content_type);
                                    }
                                    props.with_message_type("error");
                                    let args = BasicPublishArguments::new("", reply_to.as_str());
                                    if let Err(e) = aux_channel
                                        .basic_publish(props, err.to_string().as_bytes().to_vec(), args)
                                        .await
                                    {
                                        error!("Failed to publish response: {}", e);
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => {
                        error!("Failed to decompress content: {}", e);
                        if !auto_ack {
                            let args = BasicNackArguments::new(deliver.delivery_tag(), false, false);
                            if let Err(err) = channel.basic_nack(args).await {
                                error!("Failed to send nack: {}", err);
                            }
                        }
                    }
                }
            });
        } else {
            error!("No handler found for routing key {}", routing_key);
            if !self.auto_ack {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, false);
                if let Err(err) = channel.basic_nack(args).await {
                    error!("Failed to send nack: {}", err);
                }
            }
        }
    }
}
