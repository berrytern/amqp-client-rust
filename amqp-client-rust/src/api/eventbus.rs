use crate::domain::config::QoSConfig;
use crate::{
    api::connection::AsyncConnection,
    domain::config::Config,
    errors::AppError,
};
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;
use tokio::time::Duration;
use std::pin::Pin;
use crate::api::utils::{Confirmations, ContentEncoding, DeliveryMode};

#[derive(Clone)]
pub struct AsyncEventbusRabbitMQ {
    config: Arc<Config>,
    pub_connection: AsyncConnection,
    sub_connection: AsyncConnection,
    rpc_client_connection: AsyncConnection,
    rpc_server_connection: AsyncConnection,
}

impl AsyncEventbusRabbitMQ {
    pub fn new(config: Config, qos_config: QoSConfig) -> Self {
        let config = Arc::new(config);
        Self {
            config: Arc::clone(&config),
            pub_connection: AsyncConnection::new(Arc::clone(&config), if qos_config.pub_confirm { Confirmations::PublisherConfirms } else { Confirmations::Disables }, false, None),
            sub_connection: AsyncConnection::new(Arc::clone(&config), Confirmations::Disables, qos_config.sub_auto_ack, qos_config.sub_prefetch),
            rpc_client_connection: AsyncConnection::new(Arc::clone(&config), if qos_config.rpc_client_confirm { Confirmations::RPCClientPublisherConfirms } else { Confirmations::Disables }, qos_config.rpc_client_auto_ack, qos_config.rpc_client_prefetch),
            rpc_server_connection: AsyncConnection::new(Arc::clone(&config), if qos_config.rpc_server_confirm { Confirmations::RPCServerPublisherConfirms } else { Confirmations::Disables }, qos_config.rpc_server_auto_ack, qos_config.rpc_server_prefetch),
        }
    }

    pub async fn update_secret(&self, new_secret: &str, reason: &str, command_timeout: Option<Duration>) -> Result<(), AppError> {
        tokio::try_join!(
            self.pub_connection.update_secret(new_secret, reason, command_timeout),
            self.sub_connection.update_secret(new_secret, reason, command_timeout),
            self.rpc_client_connection.update_secret(new_secret, reason, command_timeout),
            self.rpc_server_connection.update_secret(new_secret, reason, command_timeout)
        )?;
        Ok(())
    }
    pub async fn publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: impl Into<Vec<u8>>,
        content_type: Option<&str>,
        content_encoding: ContentEncoding,
        command_timeout: Option<Duration>,
        delivery_mode: Option<DeliveryMode>,
        expiration: Option<u32>,
    ) -> Result<(), AppError> {
        let content_type = content_type.unwrap_or("application/json");
        let delivery_mode = delivery_mode.unwrap_or(DeliveryMode::Transient);
        let command_timeout = command_timeout.or(Some(Duration::from_secs(16)));

        self.pub_connection.publish(
            exchange_name, 
            routing_key, 
            body,
            content_type,
            content_encoding,
            command_timeout,
            delivery_mode,
            expiration,
        ).await
    }

    pub async fn subscribe<F, Fut>(
        &self,
        exchange_name: &str,
        routing_key: &str,
        handler: F,
        process_timeout: Option<Duration>,
        command_timeout: Option<Duration>,
    ) -> Result<(), AppError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let command_timeout = command_timeout.or(Some(Duration::from_secs(16)));
        let queue_name = &self.config.options.queue_name;
        let exchange_type = "topic";
        
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        });

        self.sub_connection.subscribe(
            handler,
            routing_key,
            exchange_name,
            exchange_type,
            queue_name,
            process_timeout,
            command_timeout
        ).await
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
        expiration: Option<u32>
    ) -> Result<Vec<u8>, AppError>
    {
        let command_timeout = command_timeout.or(Some(Duration::from_secs(32)));
    
        self.rpc_client_connection.rpc_client(
            exchange_name,
            routing_key,
            body,
            content_type,
            content_encoding,
            response_timeout_millis,
            expiration,
            command_timeout
        ).await
    }

    pub async fn provide_resource<F, Fut>(
        &self,
        routing_key: &str,
        handler: F,
        process_timeout: Option<Duration>,
        command_timeout: Option<Duration>,
    ) -> Result<(), AppError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let command_timeout = command_timeout.or(Some(Duration::from_secs(16)));
        let queue_name = &self.config.options.rpc_queue_name;
        let exchange_name = &self.config.options.rpc_exchange_name;
        let exchange_type = "topic";
        
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>
        });

        self.rpc_server_connection.rpc_server(
            handler,
            routing_key,
            exchange_name,
            exchange_type,
            queue_name,
            process_timeout,
            command_timeout
        ).await
    }

    pub async fn dispose(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.sub_connection.close().await?;
        self.rpc_server_connection.close().await?;
        self.pub_connection.close().await?;
        self.rpc_client_connection.close().await?;
        Ok(())
    }
}