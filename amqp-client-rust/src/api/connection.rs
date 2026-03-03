use crate::api::connection_manager::ConnectionCommand;
use crate::domain::config::Config;
use crate::{
    api::{
        connection_manager::ConnectionManager,
        utils::{
            Confirmations, ContentEncoding, DeliveryMode, Handler,
            QueueOptions, RPCHandler, compress,
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
            config,
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
        content_type: &str,
        content_encoding: ContentEncoding,
        command_timeout: Option<Duration>,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_owned()),
                None,
                AppErrorType::InternalError, // Or a new ConnectionClosed type
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
            let (_, _) =
                tokio::try_join!(self.send_command(cmd, resp_rx, command_timeout), async {
                    match timeout(
                        command_timeout.unwrap_or(Duration::from_secs(16)),
                        confirmation.1,
                    )
                    .await
                    {
                        Ok(Ok(res)) => res,
                        Ok(Err(_)) => Err(AppError::new(
                            Some("Confirm channel closed".to_owned()),
                            None,
                            AppErrorType::InternalError,
                        )),
                        Err(_) => Err(AppError::new(
                            Some("Timeout waiting for confirmation".to_owned()),
                            None,
                            AppErrorType::TimeoutError,
                        )),
                    }
                })?;
            Ok(())
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
                confirm: None,
            };
            self.send_command(cmd, resp_rx, command_timeout).await
        }
    }

    pub async fn subscribe(
        &self,
        handler: Handler,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        process_timeout: Option<Duration>,
        timeout_duration: Option<Duration>,
        queue_options: QueueOptions,
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_string()),
                None,
                AppErrorType::InternalError, // Or a new ConnectionClosed type
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
            queue_options,
        };
        self.send_command(cmd, resp_rx, timeout_duration).await
    }

    pub async fn rpc_server(
        &self,
        handler: RPCHandler,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        response_timeout: Option<Duration>,
        timeout_duration: Option<Duration>,
        queue_options: QueueOptions,
    ) -> Result<(), AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_string()),
                None,
                AppErrorType::InternalError,
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
            queue_options,
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
        response_timeout_millis: u32,
        command_timeout: Option<Duration>,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
    ) -> Result<Vec<u8>, AppError> {
        if self.is_closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                Some("Connection is shutting down".to_string()),
                None,
                AppErrorType::InternalError,
            ));
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let body = compress(body.into(), content_encoding)?;
        if self.publisher_confirms == Confirmations::RPCClientPublisherConfirms {
            let confirmation = oneshot::channel();
            let cmd = ConnectionCommand::RpcClient {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type: content_type.to_string(),
                content_encoding,
                response_timeout_millis,
                delivery_mode,
                expiration,
                response: resp_tx,
                confirm: Some(confirmation.0),
            };
            let confirmation = async {
                match timeout(
                    command_timeout.unwrap_or(Duration::from_secs(16)),
                    confirmation.1,
                )
                .await
                {
                    Ok(Ok(res)) => res,
                    Ok(Err(_)) => Err(AppError::new(
                        Some("Confirm channel closed".to_owned()),
                        None,
                        AppErrorType::InternalError,
                    )),
                    Err(_) => Err(AppError::new(
                        Some("Timeout waiting for confirmation".to_owned()),
                        None,
                        AppErrorType::TimeoutError,
                    )),
                }
            };
            let (response, _) = tokio::try_join!(
                self.send_command(cmd, resp_rx, command_timeout),
                confirmation
            )?;
            Ok(response)
        } else {
            let cmd = ConnectionCommand::RpcClient {
                exchange_name: exchange_name.to_string(),
                routing_key: routing_key.to_string(),
                body,
                content_type: content_type.to_string(),
                content_encoding,
                response_timeout_millis,
                delivery_mode,
                expiration,
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
                Some("Connection is shutting down".to_string()),
                None,
                AppErrorType::InternalError,
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

    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.is_closing.store(true, Ordering::Release);
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ConnectionCommand::Close { response: tx })?;
        rx.await?;
        Ok(())
    }
}