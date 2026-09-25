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
use crate::api::utils::{Confirmations, Message, PublishOptions, QueueOptions, RpcClientOptions, RouteBinding};

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
        options: &PublishOptions<'_>,
    ) -> Result<(), AppError> {
        let mut opts = *options;
        if opts.content_type.is_none() {
            opts.content_type = Some("application/json");
        }
        if opts.command_timeout.is_none() {
            opts.command_timeout = Some(self.config.options.default_command_timeout);
        }

        self.pub_connection.publish(
            exchange_name, 
            routing_key, 
            body,
            &opts,
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
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let command_timeout = command_timeout.or(Some(self.config.options.default_command_timeout));
        let queue_name = &self.config.options.queue_name;
        let exchange_type = "topic";
        
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn StdError + Send + Sync>>> + Send>>
        });
        let mut queue_options = QueueOptions::new()
            .auto_delete(false)
            .durable(true)
            .exclusive(false)
            .no_create(false);

        if let Some(dlx) = &self.config.options.dead_letter_exchange {
            queue_options = queue_options.dead_letter_exchange(dlx);
        }
        if let Some(dlk) = &self.config.options.dead_letter_routing_key {
            queue_options = queue_options.dead_letter_routing_key(dlk);
        }

        let binding = RouteBinding {
            routing_key,
            exchange_name,
            exchange_type,
            queue_name,
        };

        self.sub_connection.subscribe(
            handler,
            binding,
            process_timeout,
            command_timeout,
            queue_options
        ).await
    }

    pub async fn rpc_client(
        &self,
        exchange_name: &str,
        routing_key: &str,
        body: impl Into<Vec<u8>>,
        options: &RpcClientOptions<'_>,
    ) -> Result<Vec<u8>, AppError>
    {
        let mut opts = *options;
        if opts.command_timeout.is_none() {
            opts.command_timeout = Some(self.config.options.default_command_timeout);
        }
    
        self.rpc_client_connection.rpc_client(
            exchange_name,
            routing_key,
            body,
            &opts,
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
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Message, Box<dyn StdError + Send + Sync>>> + Send + 'static,
    {
        let command_timeout = command_timeout.or(Some(self.config.options.default_command_timeout));
        let queue_name = &self.config.options.rpc_queue_name;
        let exchange_name = &self.config.options.rpc_exchange_name;
        let exchange_type = "topic";
        
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<Message, Box<dyn StdError + Send + Sync>>> + Send>>
        });

        let queue_options = QueueOptions::new()
            .auto_delete(false)
            .durable(true)
            .exclusive(false)
            .no_create(false);

        let binding = RouteBinding {
            routing_key,
            exchange_name,
            exchange_type,
            queue_name,
        };

        self.rpc_server_connection.rpc_server(
            handler,
            binding,
            process_timeout,
            command_timeout,
            queue_options
        ).await
    }

    pub async fn dispose(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (r1, r2, r3, r4) = tokio::join!(
            self.sub_connection.close(),
            self.rpc_server_connection.close(),
            self.pub_connection.close(),
            self.rpc_client_connection.close()
        );
        r1?;
        r2?;
        r3?;
        r4?;
        Ok(())
    }
}
