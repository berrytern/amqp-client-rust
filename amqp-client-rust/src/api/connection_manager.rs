use super::callback::MyConnectionCallback;
use crate::domain::config::Config;
use crate::{
    api::{
        callback::MyChannelCallback,
        channel::{AsyncChannel, ChannelOptions, ChannelRpcClientArgs},
        utils::{
            Confirmations, ContentEncoding, DeliveryMode, Handler, ChannelCmd,
            PublishOptions, QueueOptions, RPCHandler, RouteBinding
        },
    },
    errors::{AppError, AppErrorType},
};
use amqprs::{
    channel::{ConfirmSelectArguments},
    connection::{Connection, OpenConnectionArguments},
};
use dashmap::DashMap;
use std::collections::HashMap;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::Ordering,
    },
};
use tokio::{
    sync::{mpsc, oneshot},
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

impl ConnectionCommand {
    pub fn byte_size(&self) -> usize {
        match self {
            ConnectionCommand::Publish { body, exchange_name, routing_key, .. } => {
                body.len() + exchange_name.len() + routing_key.len()
            }
            ConnectionCommand::RpcClient { body, exchange_name, routing_key, .. } => {
                body.len() + exchange_name.len() + routing_key.len()
            }
            _ => 0,
        }
    }
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
    queue_options: QueueOptions,
}


fn reject_command(cmd: ConnectionCommand, reason: &str, error_type: AppErrorType) {
    match cmd {
        ConnectionCommand::Publish { response, confirm, .. } => {
            let _ = response.send(Err(AppError::new(Some(reason.to_string()), None, error_type)));
            if let Some(conf) = confirm {
                let _ = conf.send(Err(AppError::new(Some(reason.to_string()), None, error_type)));
            }
        }
        ConnectionCommand::Subscribe { response, .. } => {
            let _ = response.send(Err(AppError::new(Some(reason.to_string()), None, error_type)));
        }
        ConnectionCommand::RpcServer { response, .. } => {
            let _ = response.send(Err(AppError::new(Some(reason.to_string()), None, error_type)));
        }
        ConnectionCommand::RpcClient { response, confirm, .. } => {
            let _ = response.send(Err(AppError::new(Some(reason.to_string()), None, error_type)));
            if let Some(conf) = confirm {
                let _ = conf.send(Err(AppError::new(Some(reason.to_string()), None, error_type)));
            }
        }
        ConnectionCommand::UpdateSecret { response, .. } => {
            let _ = response.send(Err(AppError::new(Some(reason.to_string()), None, error_type)));
        }
        ConnectionCommand::Close { response } => {
            let _ = response.send(());
        }
        ConnectionCommand::CheckConnection {} => {}
    }
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
    channel_rx: mpsc::UnboundedReceiver<ChannelCmd>,
    channel_tx: mpsc::UnboundedSender<ChannelCmd>,
    message_number: u64,
    auto_ack: bool,
    prefetch_count: Option<u16>,
    current_reconnect_delay: u16,
    queues: HashMap<String, (AsyncChannel, QueueOptions)>,
    current_pending_bytes: usize,
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
        let (channel_tx, channel_rx) = mpsc::unbounded_channel();
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
            channel_rx,
            channel_tx,
            message_number: 0,
            auto_ack,
            prefetch_count,
            current_reconnect_delay: 1,
            queues: HashMap::new(),
            current_pending_bytes: 0,
        }
    }

    fn abort_pending_confirmations(&mut self, reason: &str) {
        for (_, confirm) in std::mem::take(&mut self.pending_confirmations) {
            let _ = confirm.send(Err(AppError::new(
                Some(reason.to_string()),
                None,
                AppErrorType::ConnectionReset,
            )));
        }
    }

    fn abort_pending_commands(&mut self, reason: &str) {
        while let Some(cmd) = self.pending_commands.pop_front() {
            reject_command(cmd, reason, AppErrorType::ConnectionReset);
        }
        self.current_pending_bytes = 0;
    }

    pub async fn run(mut self) {
        self.connect().await;

        let mut health_check_interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                Some(cmd) = self.channel_rx.recv() => {
                    match cmd {
                        ChannelCmd::PublishAck((tag, multiple)) => {
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
                        ChannelCmd::PublishNack((tag, multiple)) => {
                            if multiple {
                                while let Some(entry) = self.pending_confirmations.first_entry() {
                                    if entry.key() > &tag {
                                        break;
                                    }
                                    let confirm = entry.remove();
                                    let _ = confirm.send(Err(AppError { message: None, description: None, error_type: AppErrorType::NackError }));
                                }
                            } else if let Some(confirm) = self.pending_confirmations.remove(&tag) {
                                let _ = confirm.send(Err(AppError { message: None, description: None, error_type: AppErrorType::NackError }));
                            }
                        },
                        ChannelCmd::ReOpen(channel_id) => {
                            for channel in self.queues.values_mut() {
                                let _ = channel.0.reopen(channel_id).await;
                            }
                        }
                    }
                }
                cmd_opt = self.rx.recv() => {
                    match cmd_opt {
                        Some(cmd) => match cmd {
                            ConnectionCommand::Close{ response } => {
                                let mut dispose_futures = Vec::new();
                                
                                for (channel, _) in self.queues.values() {
                                    dispose_futures.push(channel.dispose());
                                }
        
                                if let Some(channel) = &self.channel {
                                    dispose_futures.push(channel.dispose());
                                }

                                futures::future::join_all(dispose_futures).await;

                                if let Some(conn) = &self.connection {
                                    let _ = conn.clone().close().await;
                                }

                                self.queues.clear();
                                self.subscribe_backup.clear();
                                self.rpc_subscribe_backup.clear();
                                self.channel = None;
                                self.abort_pending_confirmations("Connection closed before publisher confirmation was received");
                                self.abort_pending_commands("Connection closed before command could be processed");

                                let _ = response.send(());
                                break;
                            },
                            ConnectionCommand::CheckConnection{} => {
                                continue;
                            },
                            _ => {
                                if self.is_connected() {
                                    self.process_command(cmd).await;
                                } else {
                                    let fail_fast = self.config.options.fail_fast_on_disconnect
                                        || self.config.options.max_pending_commands == 0;
                                    if fail_fast {
                                        error!("Connection is unavailable; rejecting command immediately (fail-fast)");
                                        reject_command(cmd, "Connection is unavailable; fail-fast is active", AppErrorType::ConnectionUnavailable);
                                    } else {
                                        let cmd_bytes = cmd.byte_size();
                                        let max_pending = self.config.options.max_pending_commands;
                                        let max_pending_bytes = self.config.options.max_pending_bytes;
                                        if self.pending_commands.len() >= max_pending
                                            || (max_pending_bytes > 0 && self.current_pending_bytes + cmd_bytes > max_pending_bytes)
                                        {
                                            error!(
                                                "Pending command buffer reached maximum capacity (count: {}, bytes: {}), rejecting command",
                                                self.pending_commands.len(), self.current_pending_bytes
                                            );
                                            reject_command(cmd, "Pending command buffer full; connection is unavailable", AppErrorType::BufferFull);
                                        } else {
                                            self.current_pending_bytes += cmd_bytes;
                                            self.pending_commands.push_back(cmd);
                                        }
                                    }
                                }
                            }
                        },
                        None => {
                            let mut dispose_futures = Vec::new();
                            for (channel, _) in self.queues.values() {
                                dispose_futures.push(channel.dispose());
                            }
                            if let Some(channel) = &self.channel {
                                dispose_futures.push(channel.dispose());
                            }
                            futures::future::join_all(dispose_futures).await;
                            if let Some(conn) = &self.connection {
                                let _ = conn.clone().close().await;
                            }
                            self.queues.clear();
                            self.subscribe_backup.clear();
                            self.rpc_subscribe_backup.clear();
                            self.channel = None;
                            self.abort_pending_confirmations("Connection dropped before publisher confirmation was received");
                            self.abort_pending_commands("Connection dropped before command could be processed");
                            break;
                        }
                    }
                },
                _ = health_check_interval.tick() => {
                    if !self.is_connected() {
                        sleep(Duration::from_secs(self.current_reconnect_delay as u64 -1)).await;
                        self.connect().await;
                        self.current_reconnect_delay = std::cmp::min(self.current_reconnect_delay * 2, self.config.options.max_reconnect_delay);
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
        let mut options = OpenConnectionArguments::new(
            &self.config.host,
            self.config.port,
            &self.config.username,
            &self.config.password,
        );
        options.virtual_host(&self.config.virtual_host);
        #[cfg(feature = "tls")]
        if let Some(tls_adaptor) = &self.config.tls_adaptor {
            options.tls_adaptor(tls_adaptor.clone());
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

                if let Ok(ch) = self.open_channel(&conn, self.channel.as_ref()).await {
                    self.channel = Some(ch);
                }
                let old_queues = std::mem::take(&mut self.queues);

                for (queue_name, (latest_channel, options)) in old_queues {
                    if let Ok(ch) = self.open_channel(&conn, Some(&latest_channel)).await {
                        self.queues.insert(queue_name, (ch, options));
                    } else {
                        error!("Failed to open channel for queue {} during reconnection", queue_name);
                    }
                }
                
                self.abort_pending_confirmations("Connection reset before publisher confirmation was received");
                self.message_number = 0;
                self.restore_subscriptions().await;

                if self.channel.is_some() {
                    while let Some(cmd) = self.pending_commands.pop_front() {
                        self.current_pending_bytes = self.current_pending_bytes.saturating_sub(cmd.byte_size());
                        self.process_command(cmd).await;
                        if self.channel.is_none() {
                            break;
                        }
                    }
                }
                if self.pending_commands.is_empty() {
                    self.current_pending_bytes = 0;
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
        latest_channel: Option<&AsyncChannel>,
    ) -> Result<AsyncChannel, AppError> {
        if let Ok(ch) = conn.open_channel(None).await {
            if let Err(e) = ch
                .register_callback(MyChannelCallback {
                    channel_tx: self.channel_tx.clone(),
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
                let options = ChannelOptions {
                    publisher_confirms: self.publisher_confirms,
                    auto_ack: self.auto_ack,
                    pre_fetch_count: self.prefetch_count,
                    aux_queue_name: Some(latest_channel.aux_queue_name.clone()),
                };
                let mut async_ch = AsyncChannel::new(
                    ch,
                    conn.clone(),
                    self.channel_tx.clone(),
                    latest_channel.rpc_futures.clone(),
                    options,
                );
                let _ = async_ch.start_rpc_consumer().await;
                Ok(async_ch)
            } else {
                let options = ChannelOptions {
                    publisher_confirms: self.publisher_confirms,
                    auto_ack: self.auto_ack,
                    pre_fetch_count: self.prefetch_count,
                    aux_queue_name: None,
                };
                Ok(AsyncChannel::new(
                    ch,
                    conn.clone(),
                    self.channel_tx.clone(),
                    Arc::new(DashMap::new()),
                    options,
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
        for (keys, values) in &self.subscribe_backup {
            if let Some((isolated_ch, _)) = self.queues.get(&keys.0) {
                let binding = RouteBinding {
                    routing_key: &keys.1,
                    exchange_name: &keys.2,
                    exchange_type: &values.exchange_type,
                    queue_name: &keys.0,
                };
                let _ = isolated_ch.subscribe(
                    values.handler.clone(),
                    binding,
                    values.process_timeout,
                    &values.queue_options,
                )
                .await;
            }
        }
        for (keys, values) in &self.rpc_subscribe_backup {
            if let Some((isolated_ch, _)) = self.queues.get_mut(&keys.0) {
                let binding = RouteBinding {
                    routing_key: &keys.1,
                    exchange_name: &keys.2,
                    exchange_type: &values.exchange_type,
                    queue_name: &keys.0,
                };
                let _ = isolated_ch
                    .rpc_server(
                        values.handler.clone(),
                        binding,
                        values.response_timeout,
                        &values.queue_options
                    )
                    .await;
            }
        }
    }

    async fn process_command(&mut self, cmd: ConnectionCommand) {
        let channel = match &mut self.channel {
            Some(c) => c,
            None => {
                let fail_fast = self.config.options.fail_fast_on_disconnect
                    || self.config.options.max_pending_commands == 0;
                if fail_fast {
                    reject_command(cmd, "Connection is unavailable; fail-fast is active", AppErrorType::ConnectionUnavailable);
                } else {
                    self.current_pending_bytes += cmd.byte_size();
                    self.pending_commands.push_front(cmd);
                }
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
                let opts = PublishOptions {
                    content_type: &content_type,
                    content_encoding,
                    command_timeout: None,
                    delivery_mode,
                    expiration,
                };
                let res = channel
                    .publish(
                        &exchange_name,
                        &routing_key,
                        body,
                        &opts,
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
                let conn = match &self.connection {
                    Some(c) => c.clone(),
                    None => {
                        let _ = response.send(Err(AppError::new(
                            Some("Connection is not open".to_string()),
                            None,
                            AppErrorType::ConnectionUnavailable,
                        )));
                        return;
                    }
                };

                let existing_queue = self.queues.get(&queue_name).cloned();

                let channel_result = match existing_queue {
                    Some((ch, args)) if args != queue_options => {
                        self.queues.insert(queue_name.clone(), (ch.clone(), queue_options.clone()));
                        Ok(ch)
                    }
                    Some((ch, _)) => Ok(ch),
                    None => {
                        match self.open_channel(&conn, None).await {
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
                        let binding = RouteBinding {
                            routing_key: &routing_key,
                            exchange_name: &exchange_name,
                            exchange_type: &exchange_type,
                            queue_name: &queue_name,
                        };
                        let res = ch.subscribe(
                            handler.clone(),
                            binding,
                            process_timeout,
                            &queue_options,
                        ).await;
                        if res.is_ok() {
                            let key = (queue_name.clone(), routing_key.clone(), exchange_name.clone());
                            self.subscribe_backup.entry(key).or_insert(SubscribeBackup {
                                exchange_type: exchange_type.clone(),
                                handler,
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
                let conn = match &self.connection {
                    Some(c) => c.clone(),
                    None => {
                        let _ = response.send(Err(AppError::new(
                            Some("Connection is not open".to_string()),
                            None,
                            AppErrorType::ConnectionUnavailable,
                        )));
                        return;
                    }
                };

                let existing_queue = self.queues.get_mut(&queue_name).cloned();

                let channel_result = match existing_queue {
                    Some((ch, args)) if args != queue_options => {
                        self.queues.insert(queue_name.clone(), (ch.clone(), queue_options.clone()));
                        Ok(ch)
                    }
                    Some((ch, _)) => Ok(ch),
                    None => {
                        match self.open_channel(&conn, None).await {
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
                    Ok(mut ch) => {
                        let binding = RouteBinding {
                            routing_key: &routing_key,
                            exchange_name: &exchange_name,
                            exchange_type: &exchange_type,
                            queue_name: &queue_name,
                        };
                        let res = ch.rpc_server(
                            handler.clone(),
                            binding,
                            response_timeout,
                            &queue_options,
                        )
                        .await;
                        if res.is_ok() {
                            self.queues.insert(queue_name.clone(), (ch.clone(), queue_options.clone()));
                            let key = (queue_name.clone(), routing_key.clone(), exchange_name.clone());
                            self.rpc_subscribe_backup.entry(key).or_insert(RPCSubscribeBackup {
                                exchange_type: exchange_type.clone(),
                                handler,
                                response_timeout,
                                queue_options,
                            });
                        }
                        let _ = response.send(res);
                    }
                    Err(err) => {
                        let _ = response.send(Err(err));
                    }
                }
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
                let message_id = if let Some(confirm) = confirm {
                    self.message_number += 1;
                    self.pending_confirmations
                        .insert(self.message_number, confirm);
                    Some(self.message_number)
                } else {
                    None
                };
                let args = ChannelRpcClientArgs {
                    exchange_name,
                    routing_key,
                    body,
                    content_type,
                    content_encoding,
                    response_timeout_millis,
                    delivery_mode,
                    expiration,
                    response,
                    clean_message: self.channel_tx.clone(),
                    message_id,
                };
                let _ = channel.rpc_client(args).await;
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
                        Some("connection is not open".to_owned()),
                        None,
                        AppErrorType::UnexpectedResultError,
                    )));
                }
            }
            _ => {}
        }
    }
}
