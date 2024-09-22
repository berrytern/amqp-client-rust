use crate::{
    api::connection::AsyncConnection,
    domain::config::Config,
    errors::{AppError, AppErrorType},
};
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::{time::Duration,sync::Mutex};

use super::connection::{CallbackResult, CallbackType};

pub struct AsyncEventbusRabbitMQ {
    config: Arc<Config>,
    pub_connection: Arc<Mutex<AsyncConnection>>,
    sub_connection: Arc<Mutex<AsyncConnection>>,
    rpc_client_connection: Arc<Mutex<AsyncConnection>>,
    rpc_server_connection: Arc<Mutex<AsyncConnection>>,
}

pub enum DeliveryMode {
    Transient = 1,
    Persistent,
}


impl AsyncEventbusRabbitMQ {
    // ... (other methods remain the same)
    pub async fn new(config: Config) -> Self {
        let config = Arc::new(config);
        Self {
            config: Arc::clone(&config),
            pub_connection: AsyncConnection::new(Arc::clone(&config)).await,
            sub_connection: AsyncConnection::new(Arc::clone(&config)).await,
            rpc_client_connection: AsyncConnection::new(Arc::clone(&config)).await,
            rpc_server_connection: AsyncConnection::new(Arc::clone(&config)).await,
        }
    }

    pub async fn publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
        connection_timeout: Option<Duration>,
    ) -> Result<(), AppError> {
        let content_type = content_type.unwrap_or("application/json");
        // let timeout = timeout.unwrap_or(Duration::from_secs(5));
        let connection_timeout = connection_timeout.unwrap_or(Duration::from_secs(16));
        // let delivery_mode = delivery_mode.unwrap_or(DeliveryMode::Transient);

        let mut connection = self.pub_connection.lock().await;
        connection.open(Arc::clone(&self.config)).await;
        connection.create_channel().await;

        let callback = CallbackType::Publish {
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
            body,
            content_type: content_type.to_string(),
            // timeout: timeout.as_millis() as u64,
            // delivery_mode: delivery_mode,
            // expiration: expiration.map(|s| s.to_string()),
        };

        match connection.add_callback(callback, Some(connection_timeout)).await {
            Ok(CallbackResult::Void) => Ok(()),
            /* Ok(CallbackResult::RpcClient(_)) => Err(AppError::new(
                Some("Unexpected result from publish operation".to_string()),
                None,
                AppErrorType::UnexpectedResultError,
            )), */
            Err(e) => Err(e),
        }
    }

    pub async fn subscribe<F, Fut>(
        &self,
        exchange_name: &str,
        handler: F,
        routing_key: &str,
        content_type: &str,
        connection_timeout: Option<Duration>,
    ) -> Result<(), AppError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let connection_timeout = connection_timeout.unwrap_or(Duration::from_secs(16));
        let mut connection: tokio::sync::MutexGuard<'_, AsyncConnection> = self.sub_connection.lock().await;
        connection
            .open(Arc::clone(&self.config))
            .await;
        connection.create_channel().await;
        let queue_name = &self.config.options.queue_name;
        let exchange_type = &String::from("direct");
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        });
        let callback = CallbackType::Subscribe {
            handler: handler,
            routing_key: routing_key.to_string(),
            exchange_name: exchange_name.to_string(),
            exchange_type: exchange_type.to_string(),
            queue_name: queue_name.to_string(),
            content_type: content_type.to_string()
        };
        match connection.add_callback(callback, Some(connection_timeout)).await {
            Ok(CallbackResult::Void) => Ok(()),
            /* Ok(CallbackResult::RpcClient(_)) => Err(AppError::new(
                Some("Unexpected result from subscribe operation".to_string()),
                None,
                AppErrorType::UnexpectedResultError,
            )), */
            Err(e) => Err(e),
        }
    }

    pub async fn rpc_client<F, Fut>(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        callback: F,
        content_type: &str,
        timeout_millis: u32,
        connection_timeout: Option<Duration>,
        expiration: Option<u32>
    ) -> Result<(), AppError> 
    where
    F: Fn(Result<Vec<u8>, AppError>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let connection_timeout = connection_timeout.unwrap_or(Duration::from_secs(16));
        let mut connection = self.rpc_client_connection.lock().await;
        connection.open(Arc::clone(&self.config)).await;
        connection.create_channel().await;
        let handler = Arc::new(move |data| {
            Box::pin(callback(data)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        });
        let callback = CallbackType::RpcClient {
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
            body,
            callback: handler,
            content_type: content_type.to_string(),
            timeout_millis,
            expiration
        };

        match connection.add_callback(callback, Some(connection_timeout)).await  {
            Ok(CallbackResult::Void) => Ok(()),
            /* Ok(CallbackResult::Void) => Err(AppError::new(
                Some("Unexpected result from rpc_client operation".to_string()),
                None,
                AppErrorType::UnexpectedResultError,
            )),*/
            Err(e) => Err(e),
        }
    }

    pub async fn rpc_server<F, Fut>(
        &self,
        handler: F,
        routing_key: &str,
        content_type: &str,
        connection_timeout: Option<Duration>,
    ) where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let connection_timeout = connection_timeout.unwrap_or(Duration::from_secs(16));
        let mut connection = self.rpc_server_connection.lock().await;
        connection
            .open(Arc::clone(&self.config))
            .await;
        connection.create_channel().await;
        let queue_name = &self.config.options.rpc_queue_name;
        let exchange_name = &self.config.options.rpc_exchange_name;
        let exchange_type = &String::from("direct");
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>
        });
        let callback = CallbackType::RpcServer {
            handler: handler,
            routing_key: routing_key.to_string(),
            exchange_name: exchange_name.to_string(),
            exchange_type: exchange_type.to_string(),
            queue_name: queue_name.to_string(),
            content_type: content_type.to_string()
        };
        match connection.add_callback(callback, Some(connection_timeout)).await {
            Ok(CallbackResult::Void) => (),
            /* Ok(CallbackResult::RpcClient(_)) => {
                println!("Unexpected result from rpc_server operation");
            }, */
            Err(e) => {
                println!("{:?}", e);
            },
        }
    }

    pub async fn dispose(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Close all connections
        Ok(())
    }

}
