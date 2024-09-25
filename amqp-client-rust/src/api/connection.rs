use std::{collections::HashMap, future::Future, pin::Pin, sync::{
    atomic::{AtomicBool,Ordering}, Arc
}};
use tokio::{sync::{oneshot, Mutex},time::{sleep, timeout, Duration}};
use crate::{api::{
    callback::MyChannelCallback,
    channel::AsyncChannel,
}, errors::{AppError, AppErrorType}};
use amqprs::connection::{Connection, OpenConnectionArguments};
use crate::domain::config::Config;
use super::callback::MyConnectionCallback;
#[cfg(feature = "tls")]
use amqprs::tls::TlsAdaptor;
use std::error::Error as StdError;


pub enum CallbackType {
    RpcClient {
        exchange_name: String,
        routing_key: String,
        body: Vec<u8>,
        callback: Arc<Box<dyn Fn(Result<Vec<u8>, AppError>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>>,
        content_type: String,
        timeout_millis: u32,
        expiration: Option<u32>,
    },
    RpcServer {
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: String,
        exchange_name: String,
        exchange_type: String,
        queue_name: String,
        content_type: String,
    },
    Subscribe {
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: String,
        exchange_name: String,
        exchange_type: String,
        queue_name: String,
        content_type: String,
    },
    Publish {
        exchange_name: String,
        routing_key: String,
        body: Vec<u8>,
        content_type: String,
    },
}

pub enum CallbackResult {
    // RpcClient(Vec<u8>),
    Void,
}

struct SubscribeBackup{
    queue: String,
    exchange_name: String,
    callback: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
    response_timeout: u16,
}
struct RPCSubscribeBackup{
    queue: String,
    exchange_name: String,
    callback: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
    response_timeout: u16,
}

pub struct AsyncConnection {
    self_connection: Option<Arc<Mutex<AsyncConnection>>>,
    pub connection: Option<Connection>,
    pub channel: Option<AsyncChannel>,
    pub is_closing: AtomicBool,
    config: Arc<Config>,
    reconnecting: AtomicBool,
    reconnect_delay: i8,
    subscribe_backup: HashMap<String, SubscribeBackup>,
    rpc_subscribe_backup: HashMap<String, RPCSubscribeBackup>,
    openning: bool,
    callbacks: Vec<(CallbackType, oneshot::Sender<Result<CallbackResult, AppError>>)>,
}


impl AsyncConnection {
    pub async fn new(config: Arc<Config>) -> Arc<Mutex<Self>> {
        let connection = Arc::new(Mutex::new(Self {
            self_connection: None,
            config,
            connection: None,
            channel: None,
            is_closing: AtomicBool::new(false),
            reconnecting: AtomicBool::new(false),
            reconnect_delay: 0,
            subscribe_backup: HashMap::new(),
            rpc_subscribe_backup: HashMap::new(),
            openning: false,
            callbacks: Vec::new(),
        }));
        {
            let mut inner = connection.lock().await;
            inner.self_connection = Some(Arc::clone(&connection));
        }
        connection
    }

    pub fn set_self_ref(&mut self, self_connection: Arc<Mutex<AsyncConnection>>){
        self.self_connection = Some(self_connection);
    }

    pub fn is_open(&self) -> bool {
        self.connection.as_ref().map_or(false, |conn| conn.is_open())
    }

    pub async fn close(&mut self) {
        if self.is_open() {
            self.is_closing.store(true, std::sync::atomic::Ordering::Release);
            if let Some(cn) = self.connection.clone(){
                let _ = cn.close().await;
            }
        }
    }

    pub fn channel_is_open(&self) -> bool{
        if !self.channel.is_none(){
            self.channel.as_ref().unwrap().channel.is_open()
        } else {
            false
        }
    }
    
    pub async fn reconnect(&mut self) -> Pin<Box<dyn Future<Output = ()>+ Send + '_>>
    {
        Box::pin(async move {
        if !self.is_open() {
            match self.reconnecting.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => { self.retry_connection().await.unwrap() }
                Err(_) => {  }
            }
        }})
    }
    async fn retry_connection(&mut self) -> Result<(), AppError>
    {
        while !self.is_open() {
            if self.reconnect_delay > 30 {
                self.reconnect_delay = 30;
            } else {
                self.reconnect_delay += 1;
            }
            self.open(
                Arc::clone(&self.config)
            ).await;
            if !self.is_open() {
                self.openning = false;
            }
            
            let delay = Duration::from_secs(self.reconnect_delay as u64);
            sleep(delay).await;
        }
        self.create_channel().await;
        let subscriptions: Vec<_> = self.subscribe_backup
            .iter()
            .map(|(key, backup)| {
                (
                    backup.callback.clone(),
                    key.clone(),
                    backup.exchange_name.clone(),
                    backup.queue.clone(),
                )
            })
            .collect();
        for (callback, routing_key, exchange_name, queue) in subscriptions {
            self.subscribe(
                callback,
                &routing_key,
                &exchange_name,
                "direct",
                &queue,
                "application/json"
            ).await?;
        }
        let rpc_subscriptions: Vec<_> = self.rpc_subscribe_backup
            .iter()
            .map(|(key, backup)| {
                (
                    backup.callback.clone(),
                    key.clone(),
                    backup.exchange_name.clone(),
                    backup.queue.clone(),
                )
            })
            .collect();
        for (callback, routing_key, exchange_name, queue) in rpc_subscriptions {
            self.rpc_server(
                callback,
                &routing_key,
                &exchange_name,
                "direct",
                &queue,
                "application/json"
            ).await;
        }
        self.reconnecting.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub async fn open(
        &mut self,
        config: Arc<Config>,
    ) {
        if !self.is_open() && !self.openning {
            self.openning = true;
            self.config = config;
            #[cfg(feature = "default")]
            let connection_options = OpenConnectionArguments::new(
                &self.config.host,
                self.config.port,
                &self.config.username,
                &self.config.password,
            );
            #[cfg(feature = "tls")]
            let mut connection_options = OpenConnectionArguments::new(
                host,
                port,
                username,
                password,
            );
            #[cfg(feature = "tls")]
            if tls_adaptor.is_some() {
                connection_options = connection_options.tls_adaptor(
                    tls_adaptor.unwrap()
                ).finish();
            }
            if let Ok(connection) = Connection::open(&connection_options).await{
                self.openning = false;
                connection
                    .register_callback(MyConnectionCallback{ connection: self.self_connection.clone().unwrap()})
                    .await
                    .unwrap(); 
                self.connection = Some(connection);
            } else {
                /*match self.reconnecting.compare_exchange(false, true,  Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => { self.reconnect().await.await; }
                    Err(_) => { }
                }*/
            }
        }
    }
    
    pub async fn create_channel(&mut self){
        if self.is_open() && !self.channel_is_open(){
            if let Ok(channel) = self.connection.as_ref().unwrap().open_channel(None).await {
                let _ = channel
                    .register_callback(MyChannelCallback{connection: self.self_connection.clone().unwrap()})
                    .await;
                let connection = self.connection.clone().unwrap();
                self.channel = Some(AsyncChannel::new(channel, Arc::new(Mutex::new(connection))));
            }
        }
    }

    pub async fn subscribe(
        & mut self,
        handler:  Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        content_type: &str,
    ) -> Result<(), AppError>
    {
        if let Some(channel) = self.channel.as_mut(){
            let callback = Arc::new(move |payload| {
                Box::pin(handler(payload)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
            });
            self.subscribe_backup.entry(routing_key.to_string()).or_insert(SubscribeBackup {
                queue: queue_name.to_string(),
                exchange_name: exchange_name.to_string(),
                callback: callback.clone(),
                response_timeout: 0,
            });
            channel.subscribe(
                callback,
                routing_key,
                exchange_name,
                exchange_type,
                &queue_name,
                &content_type
            ).await
        } else {
            Err(AppError::new(
                Some("invalid channel".to_string()),
                None,
                AppErrorType::InternalError,
            ))
        }
    }

    pub async fn publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<(), AppError>{
        if let Some(channel) = &self.channel {
            return channel.publish(exchange_name, routing_key, body, content_type).await;
        }
        Ok(())
    }

    pub async fn rpc_server(
        & mut self,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        content_type: &str,
    ) -> Result<(), AppError> {
        if let Some(channel) = self.channel.as_mut(){
            let callback = Arc::new(move |payload| {
                Box::pin(handler(payload)) as Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>
            });
            self.rpc_subscribe_backup.entry(routing_key.to_string()).or_insert(RPCSubscribeBackup {
                queue: queue_name.to_string(),
                exchange_name: exchange_name.to_string(),
                callback: callback.clone(),
                response_timeout: 0,
            });
            return channel.rpc_server(
                callback,
                routing_key,
                exchange_name,
                exchange_type,
                &queue_name,
                &content_type
            ).await
        } else {
            Err(AppError::new(
                Some("invalid channel".to_string()),
                None,
                AppErrorType::InternalError,
            ))
        }
    }

    pub async fn rpc_client(
        &mut self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        callback:  Arc<Box<dyn Fn(Result<Vec<u8>, AppError>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>>,
        content_type: &str,
        timeout_millis: u32,
        expiration: Option<u32>
    ) -> Result<(), AppError> {
        if let Some(channel) = self.channel.as_mut(){
            channel.rpc_client(exchange_name, routing_key, body, callback, content_type, timeout_millis, expiration).await
        } else {
            Err(AppError::new(
                Some("invalid channel".to_string()),
                None,
                AppErrorType::InternalError,
            ))
        }
    }

    async fn execute_callback(&mut self, callback_type: CallbackType) -> Result<CallbackResult, AppError> {
        match callback_type {
            CallbackType::RpcClient { 
                exchange_name, 
                routing_key, 
                body, 
                callback,
                content_type, 
                timeout_millis,
                expiration
            } => {
                self.rpc_client(&exchange_name, &routing_key, body, callback, &content_type, timeout_millis, expiration).await?;
                Ok(CallbackResult::Void)
            },
            CallbackType::RpcServer { 
                handler, 
                routing_key, 
                exchange_name, 
                exchange_type, 
                queue_name, 
                content_type 
            } => {
                self.rpc_server(handler, &routing_key, &exchange_name, &exchange_type, &queue_name, &content_type).await?;
                Ok(CallbackResult::Void)
            },
            CallbackType::Subscribe { 
                handler, 
                routing_key, 
                exchange_name, 
                exchange_type, 
                queue_name, 
                content_type 
            } => {
                self.subscribe(handler, &routing_key, &exchange_name, &exchange_type, &queue_name, &content_type).await?;
                Ok(CallbackResult::Void)
            },
            CallbackType::Publish { 
                exchange_name, 
                routing_key, 
                body, 
                content_type 
            } => {
                self.publish(&exchange_name, &routing_key, body, &content_type).await;
                Ok(CallbackResult::Void)
            },
        }
    }

    pub async fn add_callback(&mut self, callback_type: CallbackType, connection_timeout: Option<Duration>) -> Result<CallbackResult, AppError> {
        if self.is_open() && self.channel_is_open() {
            self.execute_callback(callback_type).await
        } else {
            let (tx, rx) = oneshot::channel();
            self.callbacks.push((callback_type, tx));

            match connection_timeout {
                Some(timeout_duration) => {
                    match timeout(timeout_duration, rx).await {
                        Ok(result) => result?,
                        Err(_) => Err(AppError::new(
                            Some("Timeout: failed to connect, order rejected...".to_string()),
                            None,
                            AppErrorType::TimeoutError,
                        )),
                    }
                }
                None => rx.await?,
            }
        }
    }
}


/*
impl AsyncConnection {
    pub async fn create_channel(&mut self, self_connection: Arc<Mutex<AsyncConnection>>){
        if self.is_open() && !self.channel_is_open(){
            if let Some(connection) = &self.connection {
                if let Ok(channel) = connection.open_channel(None).await {
                    let _ = channel
                        .register_callback(MyChannelCallback{connection: self_connection})
                        .await;
                    
                    if let Ok(aux_channel) = connection.open_channel(None).await{
                        self.channel = Some(AsyncChannel::new(channel, Some(Arc::new(aux_channel))));
                    }
                }
            }
        }
    }
}
 */