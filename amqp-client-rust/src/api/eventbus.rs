use crate::{
    api::connection::AsyncConnection,
    domain::config::Config,
    errors::{AppError, AppErrorType},
};
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct AsyncEventbusRabbitMQ {
    config: Config,
    pub_connection: Arc<Mutex<AsyncConnection<()>>>,
    sub_connection: Arc<Mutex<AsyncConnection<()>>>,
    rpc_client_connection: Arc<Mutex<AsyncConnection<Vec<u8>>>>,
    rpc_server_connection: Arc<Mutex<AsyncConnection<Vec<u8>>>>,
}

impl AsyncEventbusRabbitMQ {
    // ... (other methods remain the same)
    pub async fn new(config: Config) -> Self {
        Self {
            config,
            pub_connection: Arc::new(Mutex::new(AsyncConnection::new().await)),
            sub_connection: Arc::new(Mutex::new(AsyncConnection::new().await)),
            rpc_client_connection: Arc::new(Mutex::new(AsyncConnection::new().await)),
            rpc_server_connection: Arc::new(Mutex::new(AsyncConnection::new().await)),
        }
    }

    pub async fn publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut connection = self.pub_connection.lock().await;
        connection
            .open(
                &self.config.host,
                self.config.port,
                &self.config.username,
                &self.config.password,
                #[cfg(feature = "tls")]
                self.config.tls_adaptor.clone(),
            )
            .await;
        connection.create_channel().await;
        if let Some(channel) = &connection.channel {
            channel
                .publish(exchange_name, routing_key, body, content_type)
                .await;
        }
        Ok(())
    }

    pub async fn subscribe<'b, F, Fut>(
        &self,
        exchange_name: &'b str,
        handler: F,
        routing_key: &'b str,
        content_type: &'b str,
    ) -> Result<(), AppError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let mut connection = self.sub_connection.lock().await;
        connection
            .open(
                &self.config.host,
                self.config.port,
                &self.config.username,
                &self.config.password,
                #[cfg(feature = "tls")]
                self.config.tls_adaptor.clone(),
            )
            .await;
        connection.create_channel().await;
        if let Some(ref mut channel) = connection.channel {
            let queue_name = &self.config.options.queue_name;
            let exchange_type = &String::from("direct");
            return channel
                .subscribe(
                    handler,
                    routing_key,
                    exchange_name,
                    exchange_type,
                    &queue_name,
                    &content_type,
                )
                .await;
        } // Changed from get_channel to create_channel
        panic!("error");
    }

    pub async fn rpc_client(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: &str,
        timeout_millis: u64,
    ) -> Result<String, AppError> {
        let mut connection = self.rpc_client_connection.lock().await;
        connection
            .open(
                &self.config.host,
                self.config.port,
                &self.config.username,
                &self.config.password,
                #[cfg(feature = "tls")]
                self.config.tls_adaptor.clone(),
            )
            .await;
        connection.create_channel().await;
        if let Some(channel) = connection.channel.as_mut() {
            return channel
                .rpc_client(
                    exchange_name,
                    routing_key,
                    body,
                    content_type,
                    timeout_millis,
                )
                .await;
        }
        Err(AppError::new(
            Some("invalid channel".to_string()),
            None,
            AppErrorType::InternalError,
        ))
    }

    pub async fn rpc_server<'b, F, Fut>(
        &self,
        handler: F,
        routing_key: &'b str,
        content_type: &'b str,
    ) where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let mut connection = self.rpc_server_connection.lock().await;
        connection
            .open(
                &self.config.host,
                self.config.port,
                &self.config.username,
                &self.config.password,
                #[cfg(feature = "tls")]
                self.config.tls_adaptor.clone(),
            )
            .await;
        connection.create_channel().await;
        if let Some(ref mut channel) = connection.channel {
            let queue_name = &self.config.options.rpc_queue_name;
            let exchange_name = &self.config.options.rpc_exchange_name;
            let exchange_type = &String::from("direct");
            channel
                .rpc_server(
                    handler,
                    routing_key,
                    exchange_name,
                    exchange_type,
                    &queue_name,
                    &content_type,
                )
                .await
        }
    }

    pub async fn dispose(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Close all connections
        Ok(())
    }

    // ... (other methods remain the same)
}
