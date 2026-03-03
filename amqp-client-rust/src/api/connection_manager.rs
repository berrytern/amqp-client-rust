use super::callback::MyConnectionCallback;
use crate::domain::config::Config;
use crate::{
    api::{
        callback::MyChannelCallback,
        channel::AsyncChannel,
        utils::{
            Confirmations, ContentEncoding, DeliveryMode, Handler, PendingCmd,
            QueueOptions, RPCHandler
        },
    },
    errors::{AppError, AppErrorType},
};
#[cfg(feature = "tls")]
use amqprs::tls::TlsAdaptor;
use amqprs::{
    channel::{ConfirmSelectArguments},
    connection::{Connection, OpenConnectionArguments},
};
use dashmap::DashMap;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::Ordering,
    },
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::{Duration, sleep},
};
use tracing::error;

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
        handler: Handler,
        routing_key: String,
        exchange_name: String,
        exchange_type: String,
        queue_name: String,
        response: oneshot::Sender<Result<(), AppError>>,
        process_timeout: Option<Duration>,
        queue_options: QueueOptions,
    },
    RpcServer {
        handler: RPCHandler,
        routing_key: String,
        exchange_name: String,
        exchange_type: String,
        queue_name: String,
        response: oneshot::Sender<Result<(), AppError>>,
        response_timeout: Option<Duration>,
        queue_options: QueueOptions,
    },
    RpcClient {
        exchange_name: String,
        routing_key: String,
        body: Vec<u8>,
        content_type: String,
        content_encoding: ContentEncoding,
        response_timeout_millis: u32,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
        response: oneshot::Sender<Result<Vec<u8>, AppError>>,
        confirm: Option<oneshot::Sender<Result<(), AppError>>>,
    },
    Close {
        response: oneshot::Sender<()>,
    },
    CheckConnection {},
    UpdateSecret {
        new_secret: String,
        reason: String,
        response: oneshot::Sender<Result<(), AppError>>,
    },
}

// Data structures for backup/restore on reconnection
struct SubscribeBackup {
    exchange_type: String,
    handler: Handler,
    process_timeout: Option<Duration>,
    queue_options: QueueOptions,
}

struct RPCSubscribeBackup {
    exchange_type: String,
    handler: RPCHandler,
    response_timeout: Option<Duration>,
}


pub struct ConnectionManager {
    config: Arc<Config>,
    tx: mpsc::UnboundedSender<ConnectionCommand>,
    rx: mpsc::UnboundedReceiver<ConnectionCommand>,
    connection: Option<Connection>,
    channel: Option<AsyncChannel>,
    pending_commands: VecDeque<ConnectionCommand>,
    subscribe_backup: HashMap<(String, String, String), SubscribeBackup>,
    rpc_subscribe_backup: HashMap<(String, String, String), RPCSubscribeBackup>,
    publisher_confirms: Confirmations,
    pending_confirmations: BTreeMap<u64, oneshot::Sender<Result<(), AppError>>>,
    pending_rx: mpsc::UnboundedReceiver<PendingCmd>,
    pending_tx: mpsc::UnboundedSender<PendingCmd>,
    message_number: u64,
    auto_ack: bool,
    prefetch_count: Option<u16>,
    current_reconnect_delay: u16,
    queues: HashMap<String, (AsyncChannel, QueueOptions)>,
}

impl ConnectionManager {
    pub fn new(
        config: Arc<Config>,
        tx: mpsc::UnboundedSender<ConnectionCommand>,
        rx: mpsc::UnboundedReceiver<ConnectionCommand>,
        publisher_confirms: Confirmations,
        auto_ack: bool,
        prefetch_count: Option<u16>,
    ) -> Self {
        let (pending_tx, pending_rx) = mpsc::unbounded_channel();
        Self {
            config,
            tx,
            rx,
            connection: None,
            channel: None,
            pending_commands: VecDeque::new(),
            subscribe_backup: HashMap::new(),
            rpc_subscribe_backup: HashMap::new(),
            publisher_confirms,
            pending_confirmations: BTreeMap::new(),
            pending_rx,
            pending_tx,
            message_number: 0,
            auto_ack,
            prefetch_count,
            current_reconnect_delay: 1,
            queues: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
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
            options = options.tls_adaptor(tls_adaptor.clone()).finish();
        }
        match Connection::open(&options).await {
            Ok(conn) => {
                if let Err(e) = conn
                    .register_callback(MyConnectionCallback {
                        sender: self.tx.clone(),
                    })
                    .await
                {
                    error!("Failed to register connection callback: {}", e);
                }
                self.current_reconnect_delay = 1;

                self.connection = Some(conn.clone());
                let conn_mutex = Arc::new(Mutex::new(conn.clone()));

                if let Ok(ch) = self.open_channel(&conn, conn_mutex.clone(), self.channel.as_ref()).await {
                    self.channel = Some(ch);
                }
                let old_queues = std::mem::take(&mut self.queues);

                for (queue_name, (latest_channel, options)) in old_queues {
                    if let Ok(ch) = self.open_channel(&conn, conn_mutex.clone(), Some(&latest_channel)).await {
                        self.queues.insert(queue_name, (ch, options));
                    } else {
                        error!("Failed to open channel for queue {} during reconnection", queue_name);
                    }
                }
                
                self.message_number = 0;
                self.restore_subscriptions().await;

                while let Some(cmd) = self.pending_commands.pop_front() {
                    self.process_command(cmd).await;
                }
            }
            Err(e) => {
                error!("Failed to connect: {}", e);
            }
        }
    }

    async fn open_channel(
        &self,
        conn: &Connection,
        conn_mutex: Arc<Mutex<Connection>>,
        latest_channel: Option<&AsyncChannel>,
    ) -> Result<AsyncChannel, AppError> {
        if let Ok(ch) = conn.open_channel(None).await {
            if let Err(e) = ch
                .register_callback(MyChannelCallback {
                    sender_pending: self.pending_tx.clone(),
                })
                .await
            {
                error!("Failed to register channel callback: {}", e);
            }

            if self.publisher_confirms == Confirmations::PublisherConfirms
                || self.publisher_confirms == Confirmations::RPCClientPublisherConfirms
            {
                let args = ConfirmSelectArguments::default();
                let _ = ch.confirm_select(args).await;
            }
            if let Some(latest_channel) = latest_channel
                && latest_channel.rpc_consumer_started.load(Ordering::SeqCst)
            {
                let async_ch = AsyncChannel::new(
                    ch,
                    conn_mutex,
                    latest_channel.rpc_futures.clone(),
                    self.publisher_confirms,
                    self.auto_ack,
                    self.prefetch_count,
                );
                let _ = async_ch.start_rpc_consumer().await;
                Ok(async_ch)
            } else {
                Ok(AsyncChannel::new(
                    ch,
                    conn_mutex,
                    Arc::new(DashMap::new()),
                    self.publisher_confirms,
                    self.auto_ack,
                    self.prefetch_count,
                ))
            }
        } else {
            Err(AppError::new(
                Some("No connection available".to_string()),
                None,
                AppErrorType::InternalError,
            ))
        }
    }

    async fn restore_subscriptions(&mut self) {
        if let Some(channel) = &mut self.channel {
            for (keys, values) in &self.subscribe_backup {
                let _ = channel
                    .subscribe(
                        values.handler.clone(),
                        &keys.1,
                        &keys.2,
                        &values.exchange_type,
                        &keys.0,
                        values.process_timeout,
                        &values.queue_options,
                    )
                    .await;
            }
            for (keys, values) in &self.rpc_subscribe_backup {
                let _ = channel
                    .rpc_server(
                        values.handler.clone(),
                        &keys.1,
                        &keys.2,
                        &values.exchange_type,
                        &keys.0,
                        values.response_timeout,
                    )
                    .await;
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
            ConnectionCommand::Publish {
                exchange_name,
                routing_key,
                body,
                content_type,
                content_encoding,
                delivery_mode,
                expiration,
                response,
                confirm,
            } => {
                if let Some(confirm) = confirm {
                    self.message_number += 1;
                    self.pending_confirmations
                        .insert(self.message_number, confirm);
                }
                let res = channel
                    .publish(
                        &exchange_name,
                        &routing_key,
                        body,
                        &content_type,
                        content_encoding,
                        delivery_mode,
                        expiration,
                    )
                    .await;
                let _ = response.send(res);
            }
            ConnectionCommand::Subscribe {
                handler,
                routing_key,
                exchange_name,
                exchange_type,
                queue_name,
                response,
                process_timeout,
                queue_options,
            } => {
                let conn = self.connection.clone().unwrap();

                let existing_queue = self.queues.get(&queue_name).cloned();

                let channel_result = match existing_queue {
                    Some((ch, args)) if args != queue_options => {
                        self.queues.insert(queue_name.clone(), (ch.clone(), queue_options.clone()));
                        Ok(ch)
                    }
                    Some((ch, _)) => Ok(ch),
                    None => {
                        match self.open_channel(&conn, Arc::new(Mutex::new(conn.clone())), None).await {
                            Ok(ch) => {
                                self.queues.insert(queue_name.clone(), (ch.clone(), queue_options.clone()));
                                Ok(ch)
                            }
                            Err(e) => Err(AppError::new(
                                Some(format!("Channel not Openned: Error {}", e)),
                                None,
                                AppErrorType::InternalError,
                            )),
                        }
                    }
                };

                match channel_result {
                    Ok(ch) => {
                        let res = ch.subscribe(
                            handler.clone(),
                            &routing_key,
                            &exchange_name,
                            &exchange_type,
                            &queue_name,
                            process_timeout,
                            &queue_options,
                        ).await;
                        if res.is_ok() {
                            let key = (queue_name.clone(), routing_key.clone(), exchange_name.clone());
                            self.subscribe_backup.entry(key).or_insert(SubscribeBackup {
                                exchange_type: exchange_type.clone(),
                                handler: handler,
                                process_timeout,
                                queue_options: queue_options.clone(),
                            });
                        }
                        let _ = response.send(res);
                    }
                    Err(err) => {
                        let _ = response.send(Err(err));
                    }
                }
            }
            ConnectionCommand::RpcServer {
                handler,
                routing_key,
                exchange_name,
                exchange_type,
                queue_name,
                response,
                response_timeout,
                queue_options,
            } => {
                let res = channel
                    .rpc_server(
                        handler.clone(),
                        &routing_key,
                        &exchange_name,
                        &exchange_type,
                        &queue_name,
                        response_timeout,
                    )
                    .await;
                if res.is_ok() {
                    let key = (queue_name.clone(), routing_key.clone(), exchange_name.clone());
                    self.rpc_subscribe_backup.entry(key).or_insert(RPCSubscribeBackup {
                        exchange_type: exchange_type.clone(),
                        handler: handler,
                        response_timeout,
                    });
                }
                let _ = response.send(res);
            }
            ConnectionCommand::RpcClient {
                exchange_name,
                routing_key,
                body,
                content_type,
                content_encoding,
                response_timeout_millis,
                delivery_mode,
                expiration,
                response,
                confirm,
            } => {
                if let Some(confirm) = confirm {
                    self.message_number += 1;
                    self.pending_confirmations
                        .insert(self.message_number, confirm);
                    let _ = channel
                        .rpc_client(
                            &exchange_name,
                            &routing_key,
                            body,
                            &content_type,
                            content_encoding,
                            response_timeout_millis,
                            delivery_mode,
                            expiration,
                            response,
                            self.pending_tx.clone(),
                            Some(self.message_number),
                        )
                        .await;
                } else {
                    let _ = channel
                        .rpc_client(
                            &exchange_name,
                            &routing_key,
                            body,
                            &content_type,
                            content_encoding,
                            response_timeout_millis,
                            delivery_mode,
                            expiration,
                            response,
                            self.pending_tx.clone(),
                            None,
                        )
                        .await;
                }
            }
            ConnectionCommand::UpdateSecret {
                new_secret,
                reason,
                response,
            } => {
                if let Some(connection) = &mut self.connection {
                    let _ = response.send(
                        connection
                            .update_secret(new_secret.as_str(), reason.as_str())
                            .await
                            .map_err(AppError::from),
                    );
                } else {
                    let _ = response.send(Err(AppError::new(
                        Some("connection is to openned".to_owned()),
                        None,
                        AppErrorType::UnexpectedResultError,
                    )));
                }
            }
            _ => {}
        }
    }
}
