use crate::api::connection_manager::ConnectionCommand;
use crate::domain::config::Config;
use crate::{
    api::{
        connection_manager::ConnectionManager,
        utils::{
            Confirmations, Handler,
            PublishOptions, QueueOptions, RPCHandler, RpcClientOptions, RouteBinding, compress,
        },
    },
    errors::{AppError, AppErrorType},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, timeout},
};


// The Handle exposed to the EventBus
#[derive(Clone)]
pub struct AsyncConnection {
    sender: mpsc::UnboundedSender<ConnectionCommand>,
    publisher_confirms: Confirmations,
    is_closing: Arc<AtomicBool>,
}

impl AsyncConnection {
    pub fn new(
        config: Arc<Config>,
        publisher_confirms: Confirmations,
        auto_ack: bool,
        prefetch_count: Option<u16>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let manager = ConnectionManager::new(
            config.clone(),
            tx.clone(),
            rx,
            publisher_confirms,
            auto_ack,
            prefetch_count,
        );
        tokio::spawn(async move {
            manager.run().await;
        });
        Self {
            sender: tx,
            publisher_confirms,
            is_closing: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: impl Into<Vec<u8>>,
        options: &PublishOptions<'_>,
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is closed or shutting down".to_owned()),
                Some("Operation rejected because the connection was explicitly closed by the application".to_owned()),
                AppErrorType::ConnectionClosed,
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let body = compress(body, options.content_encoding)?;
        let command_timeout = options.command_timeout;
        let content_type = if options.content_type == "application/json" {
            None
        } else {
            Some(options.content_type.to_string())
        };
        if self.publisher_confirms == Confirmations::PublisherConfirms {
            let confirmation = oneshot::channel();

            let cmd = ConnectionCommand::Publish {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type,
                content_encoding: options.content_encoding,
                delivery_mode: options.delivery_mode,
                expiration: options.expiration,
                response: resp_tx,
                confirm: Some(confirmation.0),
            };
            let confirmation_fut = async {
                let confirm_res = match command_timeout {
                    Some(dur) => match timeout(dur, confirmation.1).await {
                        Ok(res) => res,
                        Err(_) => {
                            return Err(AppError::new(
                                Some("Timeout waiting for confirmation".to_owned()),
                                None,
                                AppErrorType::TimeoutError,
                            ));
                        }
                    },
                    None => confirmation.1.await,
                };
                match confirm_res {
                    Ok(res) => res,
                    Err(_) => Err(AppError::new(
                        Some("Confirm channel closed".to_owned()),
                        None,
                        AppErrorType::InternalError,
                    )),
                }
            };
            let (_, _) =
                tokio::try_join!(self.send_command(cmd, resp_rx, command_timeout), confirmation_fut)?;
            Ok(())
        } else {
            let cmd = ConnectionCommand::Publish {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type,
                content_encoding: options.content_encoding,
                delivery_mode: options.delivery_mode,
                expiration: options.expiration,
                response: resp_tx,
                confirm: None,
            };
            self.send_command(cmd, resp_rx, command_timeout).await
        }
    }

    pub async fn subscribe(
        &self,
        handler: Handler,
        binding: RouteBinding<'_>,
        process_timeout: Option<Duration>,
        timeout_duration: Option<Duration>,
        queue_options: QueueOptions,
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is closed or shutting down".to_owned()),
                Some("Operation rejected because the connection was explicitly closed by the application".to_owned()),
                AppErrorType::ConnectionClosed,
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnectionCommand::Subscribe {
            handler,
            routing_key: binding.routing_key.to_string(),
            exchange_name: binding.exchange_name.to_string(),
            exchange_type: binding.exchange_type.to_string(),
            queue_name: binding.queue_name.to_string(),
            response: resp_tx,
            process_timeout,
            queue_options,
        };
        self.send_command(cmd, resp_rx, timeout_duration).await
    }

    pub async fn rpc_server(
        &self,
        handler: RPCHandler,
        binding: RouteBinding<'_>,
        response_timeout: Option<Duration>,
        timeout_duration: Option<Duration>,
        queue_options: QueueOptions,
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is closed or shutting down".to_owned()),
                Some("Operation rejected because the connection was explicitly closed by the application".to_owned()),
                AppErrorType::ConnectionClosed,
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnectionCommand::RpcServer {
            handler,
            routing_key: binding.routing_key.to_string(),
            exchange_name: binding.exchange_name.to_string(),
            exchange_type: binding.exchange_type.to_string(),
            queue_name: binding.queue_name.to_string(),
            response: resp_tx,
            response_timeout,
            queue_options,
        };
        self.send_command(cmd, resp_rx, timeout_duration).await
    }

    pub async fn rpc_client(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: impl Into<Vec<u8>>,
        options: &RpcClientOptions<'_>,
    ) -> Result<Vec<u8>, AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is closed or shutting down".to_owned()),
                Some("Operation rejected because the connection was explicitly closed by the application".to_owned()),
                AppErrorType::ConnectionClosed,
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let body = compress(body.into(), options.content_encoding)?;
        let command_timeout = options.command_timeout;
        let content_type = if options.content_type == "application/json" {
            None
        } else {
            Some(options.content_type.to_string())
        };
        if self.publisher_confirms == Confirmations::RPCClientPublisherConfirms {
            let confirmation = oneshot::channel();
            let cmd = ConnectionCommand::RpcClient {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type,
                content_encoding: options.content_encoding,
                response_timeout_millis: options.response_timeout_millis,
                delivery_mode: options.delivery_mode,
                expiration: options.expiration,
                response: resp_tx,
                confirm: Some(confirmation.0),
            };
            let confirmation_fut = async {
                let confirm_res = match command_timeout {
                    Some(dur) => match timeout(dur, confirmation.1).await {
                        Ok(res) => res,
                        Err(_) => {
                            return Err(AppError::new(
                                Some("Timeout waiting for confirmation".to_owned()),
                                None,
                                AppErrorType::TimeoutError,
                            ));
                        }
                    },
                    None => confirmation.1.await,
                };
                match confirm_res {
                    Ok(res) => res,
                    Err(_) => Err(AppError::new(
                        Some("Confirm channel closed".to_owned()),
                        None,
                        AppErrorType::InternalError,
                    )),
                }
            };
            let (response, _) = tokio::try_join!(
                self.send_command(cmd, resp_rx, command_timeout),
                confirmation_fut
            )?;
            Ok(response)
        } else {
            let cmd = ConnectionCommand::RpcClient {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type,
                content_encoding: options.content_encoding,
                response_timeout_millis: options.response_timeout_millis,
                delivery_mode: options.delivery_mode,
                expiration: options.expiration,
                response: resp_tx,
                confirm: None,
            };
            self.send_command(cmd, resp_rx, command_timeout).await
        }
    }

    pub async fn update_secret(
        &self,
        new_secret: &str,
        reason: &str,
        command_timeout: Option<Duration>,
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is closed or shutting down".to_owned()),
                Some("Operation rejected because the connection was explicitly closed by the application".to_owned()),
                AppErrorType::ConnectionClosed,
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

    async fn send_command<T>(
        &self,
        cmd: ConnectionCommand,
        rx: oneshot::Receiver<Result<T, AppError>>,
        command_timeout: Option<Duration>,
    ) -> Result<T, AppError> {
        if self.sender.send(cmd).is_err() {
            return Err(AppError::new(
                Some("Connection manager dropped".to_string()),
                None,
                AppErrorType::InternalError,
            ));
        }

        match command_timeout {
            Some(dur) => match timeout(dur, rx).await {
                Ok(Ok(res)) => res,
                Ok(Err(_)) => Err(AppError::new(
                    Some("Response channel closed".to_owned()),
                    None,
                    AppErrorType::InternalError,
                )),
                Err(_) => Err(AppError::new(
                    Some("Timeout waiting for connection".to_owned()),
                    None,
                    AppErrorType::TimeoutError,
                )),
            },
            None => match rx.await {
                Ok(res) => res,
                Err(_) => Err(AppError::new(
                    Some("Response channel closed".to_owned()),
                    None,
                    AppErrorType::InternalError,
                )),
            },
        }
    }

    pub async fn close(&self) -> Result<(), AppError> {
        self.is_closing.store(true, Ordering::Release);
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ConnectionCommand::Close { response: tx })
            .map_err(|e| AppError::new(Some("Failed to send close command".to_string()), Some(e.to_string()), AppErrorType::InternalError))?;
        rx.await?;
        Ok(())
    }
}
