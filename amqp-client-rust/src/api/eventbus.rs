use crate::{
    api::connection::AsyncConnection,
    domain::config::Config,
    errors::{AppError, AppErrorType},
};
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;
use tokio::time::Duration;
use std::pin::Pin;
use crate::api::utils::Confirmations;

#[derive(Clone)]
pub struct AsyncEventbusRabbitMQ {
    config: Arc<Config>,
    pub_connection: AsyncConnection,
    sub_connection: AsyncConnection,
    rpc_client_connection: AsyncConnection,
    rpc_server_connection: AsyncConnection,
}

pub enum DeliveryMode {
    Transient = 1,
    Persistent,
}

impl AsyncEventbusRabbitMQ {
    pub async fn new(config: Config, pub_publisher_confirms: bool, rpc_client_publisher_confirms: bool, rpc_server_publisher_confirms: bool) -> Self {
        let config = Arc::new(config);
        // We spawn 4 separate managers, one for each "connection" type, 
        // mimicking the original design but with Actors.
        Self {
            config: Arc::clone(&config),
            pub_connection: AsyncConnection::new(Arc::clone(&config), if pub_publisher_confirms { Confirmations::PublisherConfirms } else { Confirmations::Disables }).await,
            sub_connection: AsyncConnection::new(Arc::clone(&config), Confirmations::Disables).await,
            rpc_client_connection: AsyncConnection::new(Arc::clone(&config), if rpc_client_publisher_confirms { Confirmations::RPCClientPublisherConfirms } else { Confirmations::Disables }).await,
            rpc_server_connection: AsyncConnection::new(Arc::clone(&config), if rpc_server_publisher_confirms { Confirmations::RPCServerPublisherConfirms } else { Confirmations::Disables }).await,
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
        let connection_timeout = connection_timeout.or(Some(Duration::from_secs(16)));

        self.pub_connection.publish(
            exchange_name, 
            routing_key, 
            body, 
            content_type, 
            connection_timeout
        ).await
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
        let connection_timeout = connection_timeout.or(Some(Duration::from_secs(16)));
        let queue_name = &self.config.options.queue_name;
        let exchange_type = "direct";
        
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        });

        self.sub_connection.subscribe(
            handler,
            routing_key,
            exchange_name,
            exchange_type,
            queue_name,
            content_type,
            connection_timeout
        ).await
    }

    pub async fn rpc_client/*<F, Fut>*/(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: Vec<u8>,
        //callback: F,
        content_type: &str,
        timeout_millis: u32,
        connection_timeout: Option<Duration>,
        expiration: Option<u32>
    ) -> Result<Vec<u8>, AppError>
    //where
        //F: Fn(Result<Vec<u8>, AppError>) -> Fut + Send + Sync + 'static,
        //Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let connection_timeout = connection_timeout.or(Some(Duration::from_secs(16)));
        
        //let handler = Arc::new(Box::new(move |data| {
        //    Box::pin(callback(data)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        //}) as Box<dyn Fn(Result<Vec<u8>, AppError>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>> + Send + Sync>);
    
        self.rpc_client_connection.rpc_client(
            exchange_name, 
            routing_key, 
            body, 
            //handler, 
            content_type, 
            timeout_millis, 
            expiration, 
            connection_timeout
        ).await
    }

    pub async fn rpc_server<F, Fut>(
        &self,
        handler: F,
        routing_key: &str,
        content_type: &str,
        connection_timeout: Option<Duration>,
    ) -> Result<(), AppError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let connection_timeout = connection_timeout.or(Some(Duration::from_secs(16)));
        let queue_name = &self.config.options.rpc_queue_name;
        let exchange_name = &self.config.options.rpc_exchange_name;
        let exchange_type = "direct";
        
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<Vec<u8>, Box<dyn StdError + Send + Sync>>> + Send>>
        });

        self.rpc_server_connection.rpc_server(
            handler,
            routing_key,
            exchange_name,
            exchange_type,
            queue_name,
            content_type,
            connection_timeout
        ).await
    }

    pub async fn dispose(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.pub_connection.close().await;
        self.sub_connection.close().await;
        self.rpc_client_connection.close().await;
        self.rpc_server_connection.close().await;
        Ok(())
    }
}