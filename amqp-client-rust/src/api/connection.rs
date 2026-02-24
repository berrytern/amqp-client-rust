use std::{collections::{BTreeMap, VecDeque}, future::Future, pin::Pin, sync::{Arc,atomic::{AtomicBool, Ordering}}};
use dashmap::DashMap;
use tokio::{sync::{Mutex, mpsc, oneshot}, time::{Duration, sleep, timeout}};
use tracing::error;
use crate::{api::{
    callback::MyChannelCallback, channel::AsyncChannel, utils::DeliveryMode, utils::{Confirmations, ContentEncoding, PendingCmd, compress}
}, errors::{AppError, AppErrorType}};
use amqprs::{channel::{ConfirmSelectArguments}, connection::{Connection, OpenConnectionArguments}};
use crate::domain::config::Config;
use super::callback::MyConnectionCallback;
#[cfg(feature = "tls")]
use amqprs::tls::TlsAdaptor;
use std::error::Error as StdError;

// Command Enum for Actor Communication
pub enum ConnectionCommand {
    Publish {
        exchange_name: String,
        routing_key: String,
        body: Vec<u8>,
        content_type: String,
        content_encoding: ContentEncoding,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
        response: oneshot::Sender<Result<(), AppError>>,
        confirm: Option<oneshot::Sender<Result<(), AppError>>>,
    },
    Subscribe {
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: String,
        exchange_name: String,
        exchange_type: String,
        queue_name: String,
        response: oneshot::Sender<Result<(), AppError>>,
        process_timeout: Option<Duration>,
    },
    RpcServer {
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: String,
        exchange_name: String,
        exchange_type: String,
        queue_name: String,
        response: oneshot::Sender<Result<(), AppError>>,
        response_timeout: Option<Duration>,
    },
    RpcClient {
        exchange_name: String,
        routing_key: String,
        body: Vec<u8>,
        content_type: String,
        content_encoding: ContentEncoding,
        timeout_millis: u32,
        expiration: Option<u32>,
        response: oneshot::Sender<Result<Vec<u8>, AppError>>,
        confirm: Option<oneshot::Sender<Result<(), AppError>>>,
    },
    Close {
        response: oneshot::Sender<()>,
    },
    CheckConnection {
    },
    UpdateSecret {
        new_secret: String,
        reason: String,
        response: oneshot::Sender<Result<(), AppError>>,
    }
}

// Data structures for backup/restore on reconnection
struct SubscribeBackup {
    queue: String,
    exchange_name: String,
    exchange_type: String,
    handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
    routing_key: String,
    process_timeout: Option<Duration>
}

struct RPCSubscribeBackup {
    queue: String,
    exchange_name: String,
    exchange_type: String,
    handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
    routing_key: String,
    response_timeout: Option<Duration>,
}

// The Handle exposed to the EventBus
#[derive(Clone)]
pub struct AsyncConnection {
    sender: mpsc::UnboundedSender<ConnectionCommand>,
    publisher_confirms: Confirmations,
    is_closing: Arc<AtomicBool>,
}

impl AsyncConnection {
    pub fn new(config: Arc<Config>, publisher_confirms: Confirmations, auto_ack: bool, pre_fetch_count: Option<u16>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let manager = ConnectionManager::new(config, tx.clone(), rx, publisher_confirms, auto_ack, pre_fetch_count);
        tokio::spawn(async move {
            manager.run().await;
        });
        Self { sender: tx, publisher_confirms, is_closing: Arc::new(AtomicBool::new(false)) }
    }

    pub async fn publish(
        &self, exchange_name: &str, routing_key: &str, body: impl Into<Vec<u8>>,
            content_type: &str, content_encoding: ContentEncoding, timeout_duration: Option<Duration>,
            delivery_mode: DeliveryMode, expiration: Option<u32>
        ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_owned()),
                None,
                AppErrorType::InternalError // Or a new ConnectionClosed type
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let body = compress(body, content_encoding)?;
        if self.publisher_confirms == Confirmations::PublisherConfirms {
            let confirmation = oneshot::channel();
        
            let cmd = ConnectionCommand::Publish {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type: content_type.to_string(),
                content_encoding,
                delivery_mode,
                expiration,
                response: resp_tx,
                confirm: Some(confirmation.0),
            };
            let (_, confirm) = tokio::try_join!(self.send_command(cmd, resp_rx, timeout_duration), async {
                confirmation.1.await.map_err(|_| AppError::new(Some("Failed to receive confirmation".to_string()), None, AppErrorType::InternalError))
            })?;
            confirm
        } else {
            let cmd = ConnectionCommand::Publish {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type: content_type.to_string(),
                content_encoding,
                delivery_mode,
                expiration,
                response: resp_tx,
                confirm: None
            };
            self.send_command(cmd, resp_rx, timeout_duration).await
        }
    }

    pub async fn subscribe(
        &self,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        process_timeout: Option<Duration>,
        timeout_duration: Option<Duration>
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_string()),
                None,
                AppErrorType::InternalError // Or a new ConnectionClosed type
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnectionCommand::Subscribe {
            handler,
            routing_key: routing_key.to_string(),
            exchange_name: exchange_name.to_string(),
            exchange_type: exchange_type.to_string(),
            queue_name: queue_name.to_string(),
            response: resp_tx,
            process_timeout,
        };
        self.send_command(cmd, resp_rx, timeout_duration).await
    }

    pub async fn rpc_server(
        &self,
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        response_timeout: Option<Duration>,
        timeout_duration: Option<Duration>
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_string()),
                None,
                AppErrorType::InternalError
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnectionCommand::RpcServer {
            handler,
            routing_key: routing_key.to_string(),
            exchange_name: exchange_name.to_string(),
            exchange_type: exchange_type.to_string(),
            queue_name: queue_name.to_string(),
            response: resp_tx,
            response_timeout,
        };
        self.send_command(cmd, resp_rx, timeout_duration).await
    }

    pub async fn rpc_client(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: impl Into<Vec<u8>>,
        content_type: &str,
        content_encoding: ContentEncoding,
        timeout_millis: u32,
        expiration: Option<u32>,
        timeout_duration: Option<Duration>
    ) -> Result<Vec<u8>, AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_string()),
                None,
                AppErrorType::InternalError // Or a new ConnectionClosed type
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let body = compress(body.into(), content_encoding)?;
        if self.publisher_confirms == Confirmations::PublisherConfirms {
            let confirmation = oneshot::channel();
            let cmd = ConnectionCommand::RpcClient {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type: content_type.to_string(),
                content_encoding,
                timeout_millis,
                expiration,
                response: resp_tx,
                confirm: Some(confirmation.0),
            };
            let confirmation = async {
                confirmation.1.await.map_err(|_| AppError::new(Some("Failed to receive confirmation".to_string()), None, AppErrorType::InternalError))
            };
            let (response, confirm) = tokio::try_join!(self.send_command(cmd, resp_rx, timeout_duration), confirmation)?;
            confirm?;
            Ok(response)
        } else {
            let cmd = ConnectionCommand::RpcClient {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type: content_type.to_string(),
                content_encoding,
                timeout_millis,
                expiration,
                response: resp_tx,
                confirm: None,
            };
            self.send_command(cmd, resp_rx, timeout_duration).await
        }
    }

    pub async fn update_secret(&self, new_secret: &str, reason: &str, command_timeout: Option<Duration>) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_string()),
                None,
                AppErrorType::InternalError
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnectionCommand::UpdateSecret {
            new_secret: new_secret.to_string(),
            reason: reason.to_string(),
            response: resp_tx,
        };
        self.send_command(cmd, resp_rx, command_timeout).await
    }

    async fn send_command<T>(&self, cmd: ConnectionCommand, rx: oneshot::Receiver<Result<T, AppError>>, command_timeout: Option<Duration>) -> Result<T, AppError> {
        if self.sender.send(cmd).is_err() {
            return Err(AppError::new(Some("Connection manager dropped".to_string()), None, AppErrorType::InternalError));
        }
        
        match command_timeout {
            Some(dur) => match timeout(dur, rx).await {
                Ok(Ok(res)) => res,
                Ok(Err(_)) => Err(AppError::new(Some("Response channel closed".to_owned()), None, AppErrorType::InternalError)),
                Err(_) => Err(AppError::new(Some("Timeout waiting for connection".to_owned()), None, AppErrorType::TimeoutError)),
            },
            None => match rx.await {
                Ok(res) => res,
                Err(_) => Err(AppError::new(Some("Response channel closed".to_owned()), None, AppErrorType::InternalError)),
            }
        }
    }

    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error>>  {
        self.is_closing.store(true, Ordering::Release);
        let (tx, rx) = oneshot::channel();
        self.sender.send(ConnectionCommand::Close { response: tx })?;
        rx.await?;
        Ok(())
    }
}


struct ConnectionManager {
    config: Arc<Config>,
    tx: mpsc::UnboundedSender<ConnectionCommand>,
    rx: mpsc::UnboundedReceiver<ConnectionCommand>,
    connection: Option<Connection>,
    channel: Option<AsyncChannel>,
    pending_commands: VecDeque<ConnectionCommand>,
    subscribe_backup: Vec<SubscribeBackup>,
    rpc_subscribe_backup: Vec<RPCSubscribeBackup>,
    publisher_confirms: Confirmations,
    pending_confirmations: BTreeMap<u64, oneshot::Sender<Result<(), AppError>>>,
    pending_rx: mpsc::UnboundedReceiver<PendingCmd>,
    pending_tx: mpsc::UnboundedSender<PendingCmd>,
    message_number: u64,
    auto_ack: bool,
    pre_fetch_count: Option<u16>,
    current_reconnect_delay: u16,
}

impl ConnectionManager {
    fn new(config: Arc<Config>, tx: mpsc::UnboundedSender<ConnectionCommand>, rx: mpsc::UnboundedReceiver<ConnectionCommand>, publisher_confirms: Confirmations, auto_ack: bool, pre_fetch_count: Option<u16>) -> Self {
        let (pending_tx, pending_rx) = mpsc::unbounded_channel();
        Self {
            config,
            tx,
            rx,
            connection: None,
            channel: None,
            pending_commands: VecDeque::new(),
            subscribe_backup: Vec::new(),
            rpc_subscribe_backup: Vec::new(),
            publisher_confirms,
            pending_confirmations: BTreeMap::new(),
            pending_rx,
            pending_tx,
            message_number: 0,
            auto_ack,
            pre_fetch_count,
            current_reconnect_delay: 1,
        }
    }

    async fn run(mut self) {
        self.connect().await;

        let mut health_check_interval = tokio::time::interval(Duration::from_secs(1));
        let mut intentional_close = false;
        loop {
            tokio::select! {
                Some(cmd) = self.pending_rx.recv() => {
                    match cmd {
                        PendingCmd::Ack((tag, multiple)) => {
                            if multiple {
                                while let Some(entry) = self.pending_confirmations.first_entry() {
                                    if entry.key() > &tag {
                                        break;
                                    }
                                    let confirm = entry.remove(); 
                                    let _ = confirm.send(Ok(()));
                                }
                            } else if let Some(confirm) = self.pending_confirmations.remove(&tag) {
                                let _ = confirm.send(Ok(()));
                            }
                        },
                        PendingCmd::Nack((tag, multiple)) => {
                            if multiple {
                                while let Some(entry) = self.pending_confirmations.first_entry() {
                                    if entry.key() > &tag {
                                        break; // Stop if we go past the tag
                                    }
                                    let confirm = entry.remove(); 
                                    let _ = confirm.send(Err(AppError { message: None, description: None, error_type: AppErrorType::NackError }));
                                }
                            } else if let Some(confirm) = self.pending_confirmations.remove(&tag) {
                                let _ = confirm.send(Err(AppError { message: None, description: None, error_type: AppErrorType::NackError }));
                            }
                        },
                    }
                }
                Some(cmd) = self.rx.recv() => {
                    match cmd {
                        ConnectionCommand::Close{ response } => {
                            intentional_close = true;
                            if let Some(channel) = &self.channel {
                                channel.dispose().await;
                            }
                            if let Some(conn) = &self.connection {
                                let _ = conn.clone().close().await;
                            }
                            
                            let _ = response.send(());
                            continue;
                        },
                        ConnectionCommand::CheckConnection{} => {
                            continue;
                        },
                        _ => {
                            if self.is_connected() {
                                self.process_command(cmd).await;
                            } else {
                                self.pending_commands.push_back(cmd);
                            }
                        }
                    }
                },
                _ = health_check_interval.tick() => {
                    if !self.is_connected() && !intentional_close {
                        sleep(Duration::from_secs(self.current_reconnect_delay as u64 -1)).await;
                        self.connect().await;
                        self.current_reconnect_delay = std::cmp::min(self.current_reconnect_delay * 2, 30);
                    }
                }
            }
        }
    }

    fn is_connected(&self) -> bool {
        self.connection.as_ref().is_some_and(|c| c.is_open()) 
            && self.channel.as_ref().is_some_and(|c| c.channel.is_open())
    }

    async fn connect(&mut self) {
        #[cfg(feature = "default")]
        let mut options = OpenConnectionArguments::new(
            &self.config.host,
            self.config.port,
            &self.config.username,
            &self.config.password,
        );
        options.virtual_host(&self.config.virtual_host);
        #[cfg(feature = "tls")]
        if let Some(tls_adaptor) = &self.config.tls_adaptor {
            options = options.tls_adaptor(
                tls_adaptor.clone()
            ).finish();
        }
        match Connection::open(&options).await {
            Ok(conn) => {
                if let Err(e) = conn.register_callback(MyConnectionCallback{sender: self.tx.clone()}).await {
                    error!("Failed to register connection callback: {}", e);
                }
                self.current_reconnect_delay = 1;

                self.connection = Some(conn.clone());
                let conn_mutex = Arc::new(Mutex::new(conn.clone()));
                
                if let Ok(ch) = conn.open_channel(None).await {
                    if let Err(e) = ch.register_callback(MyChannelCallback{sender_pending: self.pending_tx.clone()}).await {
                        error!("Failed to register channel callback: {}", e);
                    }

                    if self.publisher_confirms == Confirmations::PublisherConfirms || self.publisher_confirms == Confirmations::RPCClientPublisherConfirms {
                        let args = ConfirmSelectArguments::default();
                        let _ = ch.confirm_select(args).await;
                    }
                    self.message_number = 0;
                    if let Some(latest_channel) = &self.channel && latest_channel.rpc_consumer_started.load(Ordering::SeqCst){
                        let async_ch = AsyncChannel::new(ch, conn_mutex,latest_channel.rpc_futures.clone(), self.publisher_confirms, self.auto_ack, self.pre_fetch_count);
                        let _ = async_ch.start_rpc_consumer().await;
                        self.channel = Some(async_ch);
                    } else {
                        self.channel = Some(AsyncChannel::new(ch, conn_mutex, Arc::new(DashMap::new()), self.publisher_confirms, self.auto_ack, self.pre_fetch_count));
                    }
                    
                    self.restore_subscriptions().await;
                    
                    while let Some(cmd) = self.pending_commands.pop_front() {
                        self.process_command(cmd).await;
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect: {}", e);
            }
        }
    }

    async fn restore_subscriptions(&mut self) {
        if let Some(channel) = &mut self.channel {
            for sub in &self.subscribe_backup {
                let _ = channel.subscribe(
                    sub.handler.clone(),
                    &sub.routing_key,
                    &sub.exchange_name,
                    &sub.exchange_type,
                    &sub.queue,
                    sub.process_timeout,
                ).await;
            }
            for sub in &self.rpc_subscribe_backup {
                 let _ = channel.rpc_server(
                    sub.handler.clone(),
                    &sub.routing_key,
                    &sub.exchange_name,
                    &sub.exchange_type,
                    &sub.queue,
                    sub.response_timeout,
                ).await;
            }
        }
    }

    async fn process_command(&mut self, cmd: ConnectionCommand) {
        let channel = match &mut self.channel {
            Some(c) => c,
            None => {
                self.pending_commands.push_front(cmd);
                return;
            }
        };

        match cmd {
            ConnectionCommand::Publish { exchange_name, routing_key, body, content_type, content_encoding, delivery_mode, expiration, response , confirm} => {
                if let Some(confirm) = confirm {
                    self.message_number += 1;
                    self.pending_confirmations.insert(self.message_number, confirm);
                }
                let res = channel.publish(&exchange_name, &routing_key, body, &content_type, content_encoding, delivery_mode, expiration).await;
                let _ = response.send(res);
            },
            ConnectionCommand::Subscribe { handler, routing_key, exchange_name, exchange_type, queue_name, response, process_timeout } => {
                self.subscribe_backup.push(SubscribeBackup {
                    queue: queue_name.clone(),
                    exchange_name: exchange_name.clone(),
                    exchange_type: exchange_type.clone(),
                    handler: handler.clone(),
                    routing_key: routing_key.clone(),
                    process_timeout,
                });
                
                let res = channel.subscribe(handler, &routing_key, &exchange_name, &exchange_type, &queue_name, process_timeout).await;
                let _ = response.send(res);
            },
            ConnectionCommand::RpcServer { handler, routing_key, exchange_name, exchange_type, queue_name, response, response_timeout } => {
                self.rpc_subscribe_backup.push(RPCSubscribeBackup {
                    queue: queue_name.clone(),
                    exchange_name: exchange_name.clone(),
                    exchange_type: exchange_type.clone(),
                    handler: handler.clone(),
                    routing_key: routing_key.clone(),
                    response_timeout,
                });
                let res = channel.rpc_server(handler, &routing_key, &exchange_name, &exchange_type, &queue_name, response_timeout).await;
                let _ = response.send(res);
            },
            ConnectionCommand::RpcClient { exchange_name, routing_key, body,
                content_type, content_encoding, timeout_millis, expiration, response, confirm } => {
                if let Some(confirm) = confirm {
                    self.message_number += 1;
                    self.pending_confirmations.insert(self.message_number, confirm);
                    let _ = channel.rpc_client(&exchange_name, &routing_key, body,
                    &content_type, content_encoding, timeout_millis, expiration, response, self.pending_tx.clone(), Some(self.message_number)).await;
                } else {
                    let _ = channel.rpc_client(&exchange_name, &routing_key, body,
                    &content_type, content_encoding, timeout_millis, expiration, response, self.pending_tx.clone(), None).await;
                }
            },
            ConnectionCommand::UpdateSecret { new_secret, reason, response } => {
                if let Some(connection) = &mut self.connection {
                    let _ = response.send(connection.update_secret(new_secret.as_str(), reason.as_str()).await.map_err(AppError::from));
                } else {
                    let _ = response.send(Err(AppError::new(Some("connection is to openned".to_owned()), None, AppErrorType::UnexpectedResultError)));
                }
            },
            _ => {
            }
        }
    }
}