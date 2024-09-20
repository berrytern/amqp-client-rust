use crate::{
    api::connection::AsyncConnection,
    domain::config::Config,
    errors::AppError,
};
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct AsyncEventbusRabbitMQ {
    config: Arc<Config>,
    pub_connection: Arc<Mutex<AsyncConnection>>,
    sub_connection: Arc<Mutex<AsyncConnection>>,
    rpc_client_connection: Arc<Mutex<AsyncConnection>>,
    rpc_server_connection: Arc<Mutex<AsyncConnection>>,
}

impl AsyncEventbusRabbitMQ {
    // ... (other methods remain the same)
    pub async fn new(config: Config) -> Self {
        let config = Arc::new(config);
        Self {
            config: Arc::clone(&config),
            pub_connection: Arc::new(Mutex::new(AsyncConnection::new(Arc::clone(&config)).await)),
            sub_connection: Arc::new(Mutex::new(AsyncConnection::new(Arc::clone(&config)).await)),
            rpc_client_connection: Arc::new(Mutex::new(AsyncConnection::new(Arc::clone(&config)).await)),
            rpc_server_connection: Arc::new(Mutex::new(AsyncConnection::new(Arc::clone(&config)).await)),
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
                Arc::clone(&self.config)
            )
            .await;
        connection.create_channel(Arc::clone(&self.pub_connection)).await;
        connection.publish(exchange_name, routing_key, body, content_type).await;
        Ok(())
    }

    pub async fn subscribe<F, Fut>(
        &self,
        exchange_name: &str,
        handler: F,
        routing_key: &str,
        content_type: &str,
    ) -> Result<(), AppError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let mut connection: tokio::sync::MutexGuard<'_, AsyncConnection> = self.sub_connection.lock().await;
        connection
            .open(Arc::clone(&self.config))
            .await;
        connection.create_channel(Arc::clone(&self.sub_connection)).await;
        let queue_name = &self.config.options.queue_name;
        let exchange_type = &String::from("direct");
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        });
        connection.subscribe(handler, routing_key, exchange_name, exchange_type, queue_name, content_type).await
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
            .open(Arc::clone(&self.config))
            .await;
        connection.create_channel(Arc::clone(&self.rpc_client_connection)).await;
        connection.rpc_client(exchange_name, routing_key, body, content_type, timeout_millis).await
    }

    pub async fn rpc_server<F, Fut>(
        &self,
        handler: F,
        routing_key: &str,
        content_type: &str,
    ) where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let mut connection = self.rpc_server_connection.lock().await;
        connection
            .open(Arc::clone(&self.config))
            .await;
        connection.create_channel(Arc::clone(&self.rpc_server_connection)).await;
        let queue_name = &self.config.options.rpc_queue_name;
        let exchange_name = &self.config.options.rpc_exchange_name;
        let exchange_type = &String::from("direct");
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>
        });
        connection.rpc_server(handler, routing_key, exchange_name, exchange_type, queue_name, content_type).await;
    }

    pub async fn dispose(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Close all connections
        Ok(())
    }

}
