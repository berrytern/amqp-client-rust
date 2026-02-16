use std::{collections::{BTreeMap, BTreeSet, HashMap, VecDeque}, future::Future, pin::Pin, sync::{Arc,atomic::{AtomicU64, Ordering}}};
use tokio::{sync::{mpsc, oneshot, Mutex}, time::{sleep, timeout, Duration}};
use uuid::Uuid;
use crate::{api::{
    callback::MyChannelCallback,
    channel::AsyncChannel, utils::Confirmations,
    utils::PendingCmd,
}, errors::{AppError, AppErrorType}};
use amqprs::{callbacks::DefaultConnectionCallback, channel::{BasicQosArguments, ConfirmSelectArguments}, connection::{Connection, OpenConnectionArguments}};
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
        response: oneshot::Sender<Result<(), AppError>>,
        confirm: Option<oneshot::Sender<Result<(), AppError>>>,
    },
    Subscribe {
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: String,
        exchange_name: String,
        exchange_type: String,
        queue_name: String,
        content_type: String,
        response: oneshot::Sender<Result<(), AppError>>,
    },
    RpcServer {
        handler: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
        routing_key: String,
        exchange_name: String,
        exchange_type: String,
        queue_name: String,
        content_type: String,
        response: oneshot::Sender<Result<(), AppError>>,
    },
    RpcClient {
        exchange_name: String,
        routing_key: String,
        body: Vec<u8>,
        //callback: Arc<Box<dyn Fn(Result<Vec<u8>, AppError>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>>,
        content_type: String,
        timeout_millis: u32,
        expiration: Option<u32>,
        response: oneshot::Sender<Result<Vec<u8>, AppError>>,
    },
    Close {
        response: oneshot::Sender<()>,
    }
}

// Data structures for backup/restore on reconnection
struct SubscribeBackup {
    queue: String,
    exchange_name: String,
    exchange_type: String,
    callback: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
    routing_key: String,
    content_type: String,
}

struct RPCSubscribeBackup {
    queue: String,
    exchange_name: String,
    exchange_type: String,
    callback: Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>,
    routing_key: String,
    content_type: String,
}

// The Handle exposed to the EventBus
#[derive(Clone)]
pub struct AsyncConnection {
    sender: mpsc::Sender<ConnectionCommand>,
    publisher_confirms: Confirmations,
}

impl AsyncConnection {
    pub async fn new(config: Arc<Config>, publisher_confirms: Confirmations, auto_ack: bool, pre_fetch_count: Option<u16>) -> Self {
        let (tx, rx) = mpsc::channel(100);

        let manager = ConnectionManager::new(config, rx, publisher_confirms, auto_ack, pre_fetch_count);
        tokio::spawn(async move {
            manager.run().await;
        });
        Self { sender: tx, publisher_confirms }
    }

    pub async fn publish(&self, exchange_name: &str, routing_key: &str, body: Vec<u8>, content_type: &str, timeout_duration: Option<Duration>) -> Result<(), AppError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.publisher_confirms == Confirmations::PublisherConfirms {
            let confirmation = oneshot::channel();
        
            let cmd = ConnectionCommand::Publish {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type: content_type.to_string(),
                response: resp_tx,
                confirm: Some(confirmation.0),
            };
            self.send_command(cmd, resp_rx, timeout_duration).await?;
            confirmation.1.await.map_err(|_| AppError::new(Some("Failed to receive confirmation".to_string()), None, AppErrorType::InternalError))?
        } else {
            let cmd = ConnectionCommand::Publish {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type: content_type.to_string(),
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
        content_type: &str,
        timeout_duration: Option<Duration>
    ) -> Result<(), AppError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnectionCommand::Subscribe {
            handler,
            routing_key: routing_key.to_string(),
            exchange_name: exchange_name.to_string(),
            exchange_type: exchange_type.to_string(),
            queue_name: queue_name.to_string(),
            content_type: content_type.to_string(),
            response: resp_tx,
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
        content_type: &str,
        timeout_duration: Option<Duration>
    ) -> Result<(), AppError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnectionCommand::RpcServer {
            handler,
            routing_key: routing_key.to_string(),
            exchange_name: exchange_name.to_string(),
            exchange_type: exchange_type.to_string(),
            queue_name: queue_name.to_string(),
            content_type: content_type.to_string(),
            response: resp_tx,
        };
        self.send_command(cmd, resp_rx, timeout_duration).await
    }

    pub async fn rpc_client(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        //callback: Arc<Box<dyn Fn(Result<Vec<u8>, AppError>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>>,
        content_type: &str,
        timeout_millis: u32,
        expiration: Option<u32>,
        timeout_duration: Option<Duration>
    ) -> Result<Vec<u8>, AppError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnectionCommand::RpcClient {
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
            body,
            //callback,
            content_type: content_type.to_string(),
            timeout_millis,
            expiration,
            response: resp_tx,
        };
        self.send_command(cmd, resp_rx, timeout_duration).await
    }

    async fn send_command<T>(&self, cmd: ConnectionCommand, rx: oneshot::Receiver<Result<T, AppError>>, timeout_duration: Option<Duration>) -> Result<T, AppError> {
        if self.sender.send(cmd).await.is_err() {
            return Err(AppError::new(Some("Connection manager dropped".to_string()), None, AppErrorType::InternalError));
        }
        
        match timeout_duration {
            Some(dur) => match timeout(dur, rx).await {
                Ok(Ok(res)) => res,
                Ok(Err(_)) => Err(AppError::new(Some("Response channel closed".to_string()), None, AppErrorType::InternalError)),
                Err(_) => Err(AppError::new(Some("Timeout waiting for connection".to_string()), None, AppErrorType::TimeoutError)),
            },
            None => match rx.await {
                Ok(res) => res,
                Err(_) => Err(AppError::new(Some("Response channel closed".to_string()), None, AppErrorType::InternalError)),
            }
        }
    }
    pub async fn close(&self) {
        let (tx, rx) = oneshot::channel();
        if self.sender.send(ConnectionCommand::Close { response: tx }).await.is_ok() {
            let _ = rx.await;
        }
    }
}


// The Actor Task
struct ConnectionManager {
    config: Arc<Config>,
    rx: mpsc::Receiver<ConnectionCommand>,
    connection: Option<Connection>,
    connection_mutex: Option<Arc<Mutex<Connection>>>, 
    channel: Option<AsyncChannel>,
    pending_commands: VecDeque<ConnectionCommand>,
    subscribe_backup: Vec<SubscribeBackup>,
    rpc_subscribe_backup: Vec<RPCSubscribeBackup>,
    publisher_confirms: Confirmations,
    pending_confirmations: BTreeMap<u64, oneshot::Sender<Result<(), AppError>>>,
    pending_rx: mpsc::UnboundedReceiver<PendingCmd>,
    pending_tx: mpsc::UnboundedSender<PendingCmd>,
    message_number: AtomicU64,
    auto_ack: bool,
    pre_fetch_count: Option<u16>,
}

impl ConnectionManager {
    fn new(config: Arc<Config>, rx: mpsc::Receiver<ConnectionCommand>, publisher_confirms: Confirmations, auto_ack: bool, pre_fetch_count: Option<u16>) -> Self {
        let (pending_tx, pending_rx) = mpsc::unbounded_channel();
        Self {
            config,
            rx,
            connection: None,
            connection_mutex: None, // Placeholder
            channel: None,
            pending_commands: VecDeque::new(),
            subscribe_backup: Vec::new(),
            rpc_subscribe_backup: Vec::new(),
            publisher_confirms,
            pending_confirmations: BTreeMap::new(),
            pending_rx,
            pending_tx,
            message_number: AtomicU64::new(0),
            auto_ack,
            pre_fetch_count,
        }
    }

    async fn run(mut self) {
        self.connect().await;

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
                    if let ConnectionCommand::Close { response } = cmd {
                        if let Some(conn) = &self.connection {
                            let _ = conn.clone().close().await;
                        }
                        let _ = response.send(());
                        continue
                    }

                    if self.is_connected() {
                        self.process_command(cmd).await;
                    } else {
                        self.pending_commands.push_back(cmd);
                    }
                }
            }

            if !self.is_connected() {
                sleep(Duration::from_secs(2)).await;
                self.connect().await;
            }
        }
    }

    fn is_connected(&self) -> bool {
        self.connection.as_ref().map_or(false, |c| c.is_open()) 
            && self.channel.as_ref().map_or(false, |c| c.channel.is_open())
    }

    async fn connect(&mut self) {
        let options = OpenConnectionArguments::new(
            &self.config.host,
            self.config.port,
            &self.config.username,
            &self.config.password,
        );
        
        match Connection::open(&options).await {
            Ok(conn) => {
                // 3. Register Connection Callback
                if let Err(e) = conn.register_callback(MyConnectionCallback{}).await {
                    println!("Failed to register connection callback: {}", e);
                }

                self.connection = Some(conn.clone());
                let conn_mutex = Arc::new(Mutex::new(conn.clone()));
                self.connection_mutex = Some(conn_mutex.clone());
                
                if let Ok(ch) = conn.open_channel(None).await {
                    // 4. Register Channel Callback (Important for Returns/Nacks)
                    if let Err(e) = ch.register_callback(MyChannelCallback{sender_pending: self.pending_tx.clone()}).await {
                        println!("Failed to register channel callback: {}", e);
                    }

                    if self.publisher_confirms == Confirmations::PublisherConfirms || self.publisher_confirms == Confirmations::RPCClientPublisherConfirms {
                        let args = ConfirmSelectArguments::default();
                        let _ = ch.confirm_select(args).await;
                    }
                    self.message_number.store(0, Ordering::SeqCst);
                    let async_ch = AsyncChannel::new(ch, conn_mutex, self.publisher_confirms, self.auto_ack, self.pre_fetch_count);
                    self.channel = Some(async_ch);
                    
                    self.restore_subscriptions().await;
                    
                    while let Some(cmd) = self.pending_commands.pop_front() {
                        self.process_command(cmd).await;
                    }
                }
            }
            Err(e) => {
                println!("Failed to connect: {}", e);
            }
        }
    }

    async fn restore_subscriptions(&mut self) {
        if let Some(channel) = &mut self.channel {
            for sub in &self.subscribe_backup {
                let _ = channel.subscribe(
                    sub.callback.clone(),
                    &sub.routing_key,
                    &sub.exchange_name,
                    &sub.exchange_type,
                    &sub.queue,
                    &sub.content_type,
                ).await;
            }
            for sub in &self.rpc_subscribe_backup {
                 let _ = channel.rpc_server(
                    sub.callback.clone(),
                    &sub.routing_key,
                    &sub.exchange_name,
                    &sub.exchange_type,
                    &sub.queue,
                    &sub.content_type,
                ).await;
            }
        }
    }

    async fn process_command(&mut self, cmd: ConnectionCommand) {
        // If channel is lost during processing, we might fail.
        // In a robust system, we would catch error, push back to pending, and trigger reconnect.
        // Here we attempt execution.
        
        let channel = match &mut self.channel {
            Some(c) => c,
            None => {
                // Should be unreachable if is_connected checked true, but safe guard:
                self.pending_commands.push_front(cmd);
                return;
            }
        };

        match cmd {
            ConnectionCommand::Publish { exchange_name, routing_key, body, content_type, response , confirm} => {
                if let Some(confirm) = confirm {
                    let message_number = self.message_number.fetch_add(1, Ordering::SeqCst);
                    self.pending_confirmations.insert(message_number+1, confirm);
                }
                let res = channel.publish(&exchange_name, &routing_key, body, &content_type).await;
                let _ = response.send(res);
            },
            ConnectionCommand::Subscribe { handler, routing_key, exchange_name, exchange_type, queue_name, content_type, response } => {
                // Backup for reconnection
                self.subscribe_backup.push(SubscribeBackup {
                    queue: queue_name.clone(),
                    exchange_name: exchange_name.clone(),
                    exchange_type: exchange_type.clone(),
                    callback: handler.clone(),
                    routing_key: routing_key.clone(),
                    content_type: content_type.clone(),
                });
                
                let res = channel.subscribe(handler, &routing_key, &exchange_name, &exchange_type, &queue_name, &content_type).await;
                let _ = response.send(res);
            },
            ConnectionCommand::RpcServer { handler, routing_key, exchange_name, exchange_type, queue_name, content_type, response } => {
                 self.rpc_subscribe_backup.push(RPCSubscribeBackup {
                    queue: queue_name.clone(),
                    exchange_name: exchange_name.clone(),
                    exchange_type: exchange_type.clone(),
                    callback: handler.clone(),
                    routing_key: routing_key.clone(),
                    content_type: content_type.clone(),
                });
                let res = channel.rpc_server(handler, &routing_key, &exchange_name, &exchange_type, &queue_name, &content_type).await;
                let _ = response.send(res);
            },
            ConnectionCommand::RpcClient { exchange_name, routing_key, body,
                //callback,
                content_type, timeout_millis, expiration, response } => {
                let res = channel.rpc_client(&exchange_name, &routing_key, body,
                    //callback,
                    &content_type, timeout_millis, expiration, response, Uuid::new_v4()).await;
            },
            ConnectionCommand::Close { response } => {
                // This case is actually handled in the main loop, but if it fell through:
                if let Some(conn) = &self.connection {
                     let _ = conn.clone().close().await;
                }
                let _ = response.send(());
            }
        }
    }
}