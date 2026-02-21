use crate::{
    api::{consumers::{BroadRPCClientHandler, BroadRPCHandler, BroadSubscribeHandler, InternalRPCHandler, InternalSubscribeHandler}, utils::PendingCmd},
    errors::{AppError, AppErrorType},
};
use amqprs::{
    BasicProperties, DELIVERY_MODE_TRANSIENT, channel::{
        BasicCancelArguments, BasicConsumeArguments, BasicPublishArguments, BasicQosArguments, Channel, ConfirmSelectArguments, ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments
    }, connection::Connection
};
use dashmap::DashMap;
use tracing::error;
use std::{collections::HashMap, sync::atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;
use tokio::{sync::{Mutex, Notify, OnceCell, RwLock, mpsc::UnboundedSender, oneshot}, time::Duration};
use uuid::Uuid;
use crate::api::utils::Confirmations;

#[derive(Clone)]
pub struct AsyncChannel {
    pub channel: Channel,
    connection: Arc<Mutex<Connection>>,
    aux_channel: Arc<OnceCell<Channel>>,
    aux_queue_name: String,
    pub rpc_futures: Arc<DashMap<String, oneshot::Sender<Vec<u8>>>>,
    pub rpc_consumer_started: Arc<AtomicBool>,
    consumers: Arc<DashMap<String, bool>>,
    subscribes: Arc<RwLock<HashMap<String, InternalSubscribeHandler>>>,
    rpc_subscribes: Arc<RwLock<HashMap<String, InternalRPCHandler>>>,
    publisher_confirms: Confirmations,
    auto_ack: bool,
    pre_fetch_count: Option<u16>,
    consumer_tags: Arc<RwLock<Vec<String>>>,
    in_flight: Arc<AtomicUsize>,
    pub shutdown_notify: Arc<Notify>,
}

impl AsyncChannel {
    pub fn new(channel: Channel, connection: Arc<Mutex<Connection>>, rpc_futures: Arc<DashMap<String, oneshot::Sender<Vec<u8>>>>, publisher_confirms: Confirmations, auto_ack: bool, pre_fetch_count: Option<u16>) -> Self {
        Self {
            channel,
            connection,
            aux_channel: Arc::new(OnceCell::new()),
            aux_queue_name: format!("amqp.{}", Uuid::new_v4()),
            rpc_futures,
            rpc_consumer_started: Arc::new(AtomicBool::new(false)),
            consumers: Arc::new(DashMap::new()),
            subscribes: Arc::new(RwLock::new(HashMap::new())),
            rpc_subscribes: Arc::new(RwLock::new(HashMap::new())),
            publisher_confirms,
            auto_ack,
            pre_fetch_count,
            consumer_tags:  Arc::new(RwLock::new(Vec::new())),
            in_flight: Arc::new(AtomicUsize::new(0)),
            shutdown_notify: Arc::new(Notify::new()),
        }
    }

    fn generate_consumer_tag(&self) -> String {
        format!("ctag{}", Uuid::new_v4())
    }

    pub async fn add_subscribe(&self, handler: InternalSubscribeHandler) {
        let mut subscribes = self.subscribes.write().await;
        subscribes
            .entry(format!("{}{}", handler.queue_name, handler.routing_key))
            .or_insert(handler);
    }

    pub async fn add_rpc_subscribe(&self, handler: InternalRPCHandler) {
        let mut rpc_subscribes = self.rpc_subscribes.write().await;
        rpc_subscribes
            .entry(format!("{}{}", handler.queue_name, handler.routing_key))
            .or_insert(handler);
    }

    pub async fn setup_exchange(&self, exchange_name: &str, exchange_type: &str, durable: bool) -> Result<(), AppError> {
        let arguments = ExchangeDeclareArguments{
            exchange: exchange_name.to_string(),
            exchange_type: exchange_type.to_string(),
            durable,
            ..Default::default()
        };
        Ok(self.channel.exchange_declare(arguments).await?)
    }

    pub async fn publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<(), AppError>{
        let args = BasicPublishArguments::new(exchange_name, routing_key);
        let mut properties = BasicProperties::default();
        properties.with_content_type(content_type);
        Ok(self.channel.basic_publish(properties, body, args).await?)
    }
}
impl AsyncChannel {
    pub async fn subscribe<F, Fut>(
        &self,
        handler: Arc<F>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        process_timeout: Option<Duration>,
    ) -> Result<(), AppError>
    where
        // Added + ?Sized here
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static + ?Sized,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        self.setup_exchange(exchange_name, exchange_type, true)
            .await?;
        let (queue_name, _, _) = self
            .channel
            .queue_declare(QueueDeclareArguments::durable_client_named(queue_name))
            .await?
            .ok_or_else(|| AppError::new(Some("Queue declare returned None".to_string()), None, AppErrorType::InternalError))?;
        self.channel
            .queue_bind(QueueBindArguments::new(
                &queue_name,
                exchange_name,
                routing_key,
            ))
            .await?;
        
        // FIXED: Await the add_subscribe to ensure handler is registered before consuming
        self.add_subscribe(InternalSubscribeHandler::new(
            &queue_name,
            routing_key,
            handler,
            process_timeout,
        )).await;

        if !self.consumers.contains_key(&queue_name) {
            if !self.auto_ack && let Some(pre_fetch_count) = self.pre_fetch_count {
                let args = BasicQosArguments::new(0, pre_fetch_count, false);
                let _ = self.channel.basic_qos(args).await;
            }
            self.consumers.insert(queue_name.to_string(), true);
            let mut args = BasicConsumeArguments::new(&queue_name, &self.generate_consumer_tag());
            args.manual_ack(!self.auto_ack);
            let sub_handler = BroadSubscribeHandler::new(queue_name, Arc::clone(&self.subscribes), self.auto_ack, self.in_flight.clone(), self.shutdown_notify.clone());
            let consumer_tag = self.channel.basic_consume(sub_handler, args).await?;
            self.consumer_tags.write().await.push(consumer_tag);
        }
        Ok(())
    }
}
impl AsyncChannel{
    pub async fn rpc_server< F, Fut>(
        &self,
        handler: Arc<F>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        response_timeout: Option<Duration>,
    ) -> Result<(), AppError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static + ?Sized,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        self.aux_channel.get_or_try_init(|| async {
            let ch = self.connection.lock().await.open_channel(None).await?;
            
            if self.publisher_confirms == Confirmations::RPCServerPublisherConfirms {
                let args = ConfirmSelectArguments::default();
                let _ = ch.confirm_select(args).await;
            }
            Ok::<Channel, AppError>(ch)
        }).await?;
        self.add_rpc_subscribe(InternalRPCHandler::new(
            queue_name,
            routing_key,
            handler,
            response_timeout,
        )).await;

        self.setup_exchange(exchange_name, exchange_type, true)
            .await?;
        if let Some((queue_name,_,_)) = self.channel.queue_declare(QueueDeclareArguments::durable_client_named(queue_name)).await? {
            self.channel
                .queue_bind(QueueBindArguments::new(
                    &queue_name,
                    exchange_name,
                    routing_key,
                ))
                .await?;
            if !self.consumers.contains_key(&queue_name) {
                let mut args = BasicConsumeArguments::new(&queue_name, &self.generate_consumer_tag());
                args.manual_ack(!self.auto_ack);
                self.consumers.insert(queue_name.to_string(), true);
                let sub_handler = BroadRPCHandler::new(
                    Arc::clone(&self.aux_channel),
                    queue_name.to_string(),
                    Arc::clone(&self.rpc_subscribes),
                    self.auto_ack,
                    self.in_flight.clone(),
                    self.shutdown_notify.clone(),
                );
                if !self.auto_ack && let Some(pre_fetch_count) = self.pre_fetch_count {
                    let args = BasicQosArguments::new(0, pre_fetch_count, false);
                    let _ = self.channel.basic_qos(args).await;
                }
                let consumer_tag = self.channel.basic_consume(sub_handler, args).await?;
                self.consumer_tags.write().await.push(consumer_tag);
            }
        }
        Ok(())
    }
    
    pub async fn start_rpc_consumer(&self) -> Result<(), AppError> {
        if !self.rpc_consumer_started.load(std::sync::atomic::Ordering::SeqCst) {
            {
                self.aux_channel.get_or_try_init(|| async {
                let ch = self.connection.lock().await.open_channel(None).await?;
                if self.publisher_confirms == Confirmations::RPCClientPublisherConfirms {
                    let args = ConfirmSelectArguments::default();
                    let _ = ch.confirm_select(args).await;
                }
                if !self.auto_ack && let Some(pre_fetch_count) = self.pre_fetch_count {
                    let args = BasicQosArguments::new(0, pre_fetch_count, false);
                    let _ = ch.basic_qos(args).await;
                }
                Ok::<Channel, AppError>(ch)
                }).await?;
            }
            if let Some(channel) = self.aux_channel.get() {
                let mut queue_declare = QueueDeclareArguments::new(&self.aux_queue_name);
                queue_declare.auto_delete(true);
                let (_, _, _) = channel.queue_declare(queue_declare)
                    .await?
                    .ok_or_else(|| AppError::new(Some("Queue declare returned None".to_string()), None, AppErrorType::InternalError))?;
                let rpc_handler = BroadRPCClientHandler::new(Arc::clone(&self.rpc_futures), self.auto_ack, self.in_flight.clone(), self.shutdown_notify.clone());
                let mut args =
                    BasicConsumeArguments::new(&self.aux_queue_name, &self.generate_consumer_tag());
                args.manual_ack(!self.auto_ack);
                let consumer_tag = channel.basic_consume(rpc_handler, args).await?;
                self.consumer_tags.write().await.push(consumer_tag);
                self.rpc_consumer_started.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        Ok(())
    }

    pub async fn rpc_client(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
        timeout_millis: u32,
        expiration: Option<u32>,
        response: oneshot::Sender<Result<Vec<u8>, AppError>>,
        clean_message: UnboundedSender<PendingCmd>,
        message_id: Option<u64>,
    ) -> Result<(), AppError> 
    {
        self.start_rpc_consumer().await?;
        let (tx, rx) = oneshot::channel();
        
        let correlated_id = Uuid::new_v4().to_string();
        self.rpc_futures.insert(correlated_id.to_owned(), tx);
        let mut args = BasicPublishArguments::new(exchange_name, routing_key);
        args.mandatory(false);
        let mut properties = BasicProperties::default();
        properties.with_content_type(content_type);
        properties.with_correlation_id(&correlated_id);
        properties.with_reply_to(&self.aux_queue_name);
        properties.with_delivery_mode(DELIVERY_MODE_TRANSIENT);
        let cn = self.channel.clone();
        if let Some(exp) = expiration {
            properties.with_expiration(&format!("{}", exp));
        }
        tokio::spawn(async move {
            let _ = cn.basic_publish(properties, body, args).await;
            let message = match tokio::time::timeout(std::time::Duration::from_millis(timeout_millis as u64), rx).await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(_)) => Err(AppError::new(Some("Receiver was dropped".to_string()), None, AppErrorType::InternalError)),
                Err(_) => Err(AppError::new(Some("Timeout exceeded".to_string()), None, AppErrorType::TimeoutError)),
            };
            if let Err(_) = response.send(message) && let Some(id) = message_id {
                let _ = clean_message.send(PendingCmd::Nack((id, false)));
            }
        });
        Ok(())
    }
    pub async fn dispose(&self) {
        let cn = self.channel.clone();
        for tag in self.consumer_tags.read().await.iter() {
            let args = BasicCancelArguments::new(tag);
            if let Err(e) = cn.basic_cancel(args).await {
                error!("Failed to cancel consumer {}: {}", tag, e);
            }
        }
        while self.in_flight.load(Ordering::Acquire) > 0 {
            self.shutdown_notify.notified().await;
        }
        if let Err(e) = self.channel.clone().close().await {
            error!("Failed to close main channel: {}", e);
        }
        if let Some(channel) = self.aux_channel.get() {
            if let Err(e) = channel.clone().close().await {
                error!("Failed to close aux channel: {}", e);
            }
        }
    }
}