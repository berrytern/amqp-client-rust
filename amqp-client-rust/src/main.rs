use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicPublishArguments,
        Channel, ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    consumer::AsyncConsumer,
    BasicProperties, Deliver, DELIVERY_MODE_TRANSIENT,
};
use async_trait::async_trait;
use std::{
    collections::HashMap, error::Error, future::Future, pin::Pin, sync::Arc, thread::sleep,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub struct BroadHandler<'a, T> {
    channel: Option<Channel>,
    queue_name: String,
    handlers: Box<
        dyn Fn(Vec<u8>) -> BoxFuture<'a, Result<T, Box<dyn Error + Send + Sync>>> + Send + Sync,
    >,
    rpc_futures: Option<Arc<RwLock<HashMap<String, oneshot::Sender<Vec<u8>>>>>>,
    // response_timeout: i16
}

impl<'a, T> BroadHandler<'a, T> {
    pub fn new<F, Fut>(
        channel: Option<Channel>,
        queue_name: String,
        handlers: F,
        rpc_futures: Option<Arc<RwLock<HashMap<String, oneshot::Sender<Vec<u8>>>>>>,
    ) -> Self
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, Box<dyn Error + Send + Sync>>> + Send + 'a,
    {
        Self {
            channel,
            queue_name,
            handlers: Box::new(move |body| Box::pin(handlers(body))),
            rpc_futures,
        }
    }
}
#[async_trait]
impl<'a> AsyncConsumer for BroadHandler<'a, Vec<u8>> {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        match (self.handlers)(content).await {
            Ok(result) => {
                // Handle successful result
                let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                if let Err(e) = channel.basic_ack(args).await {
                    eprintln!("Failed to send ack: {}", e);
                }
                if let Some(reply_to) = basic_properties.reply_to() {
                    let args = BasicPublishArguments::new("".into(), reply_to.as_str());
                    if let Some(channel) = self.channel.as_ref() {
                        let _ = channel.basic_publish(basic_properties, result, args).await;
                    }
                }
            }
            Err(_) => {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                if let Err(err) = channel.basic_nack(args).await {
                    eprintln!("Failed to send nack: {}", err);
                }
            }
        }
    }
}

#[async_trait]
impl<'a> AsyncConsumer for BroadHandler<'a, ()> {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let channel = channel.clone();
        let self_rpc_futures = self.rpc_futures.clone();

        tokio::spawn(async move {
            if let Some(correlated_id) = basic_properties.correlation_id() {
                let args = BasicAckArguments::new(deliver.delivery_tag(), false);
                if let Err(e) = channel.basic_ack(args).await {
                    eprintln!("Failed to send ack: {}", e);
                }
                if let Some(futures) = &self_rpc_futures {
                    let mut futures = futures.write().await;
                    if let Some(value) = futures.remove(correlated_id.as_str()) {
                        let _ = value.send(content);
                    }
                }
            } else {
                let args = BasicNackArguments::new(deliver.delivery_tag(), false, true);
                if let Err(err) = channel.basic_nack(args).await {
                    eprintln!("Failed to send nack: {}", err);
                }
            }
        });
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let routing_key = "example.run";
    let rpc_exchange_name = "rpc_exchange";
    let rpc_queue_name = "rpc_queue";
    let connection_options = OpenConnectionArguments::new("localhost", 5672, "guest", "guest");
    let rpc_server_connection = Connection::open(&connection_options).await.unwrap();
    rpc_server_connection
        .register_callback(DefaultConnectionCallback)
        .await
        .unwrap();
    let rpc_server_channel = rpc_server_connection.open_channel(None).await.unwrap();
    rpc_server_channel
        .register_callback(DefaultChannelCallback)
        .await
        .unwrap();
    let mut exchange_args = ExchangeDeclareArguments::default();
    exchange_args.durable(true);
    exchange_args.exchange("rpc_exchange".to_string());
    rpc_server_channel
        .exchange_declare(exchange_args)
        .await
        .unwrap();
    let mut queue_args = QueueDeclareArguments::default();
    queue_args.durable(true);
    queue_args.queue("rpc_queue".to_string());
    rpc_server_channel.queue_declare(queue_args).await.unwrap();
    let rpc_server_aux_channel = rpc_server_connection.open_channel(None).await.unwrap();
    rpc_server_aux_channel
        .register_callback(DefaultChannelCallback)
        .await
        .unwrap();

    let mut arguments = ExchangeDeclareArguments::default();
    arguments.exchange = rpc_exchange_name.to_string();
    arguments.exchange_type = "direct".to_string();
    arguments.durable = true;
    let _ = rpc_server_channel.exchange_declare(arguments).await;
    let (_, _, _) = rpc_server_channel
        .queue_declare(QueueDeclareArguments::durable_client_named(
            &rpc_exchange_name,
        ))
        .await
        .unwrap()
        .unwrap();
    rpc_server_channel
        .queue_bind(QueueBindArguments::new(
            &rpc_queue_name,
            &rpc_exchange_name,
            routing_key,
        ))
        .await
        .unwrap();
    let rpc_server_handler = |body: Vec<u8>| -> Pin<
        Box<dyn Future<Output = Result<Vec<u8>, Box<dyn Error + Send + Sync>>> + Send>,
    > { Box::pin(async move { Ok("Ok".into()) }) };
    let args = BasicConsumeArguments::new(
        &rpc_queue_name,
        &format!("ctag{}", Uuid::new_v4().to_string()),
    );
    let sub_handler = BroadHandler::new(
        Some(rpc_server_aux_channel),
        rpc_queue_name.to_string(),
        rpc_server_handler,
        None,
    );

    rpc_server_channel
        .basic_consume(sub_handler, args)
        .await
        .unwrap();

    let rpc_client_connection = Connection::open(&connection_options).await.unwrap();
    rpc_client_connection
        .register_callback(DefaultConnectionCallback)
        .await
        .unwrap();
    let rpc_client_channel = rpc_client_connection.open_channel(None).await.unwrap();
    rpc_client_channel
        .register_callback(DefaultChannelCallback)
        .await
        .unwrap();
    let rpc_client_aux_channel = rpc_client_connection.open_channel(None).await.unwrap();
    rpc_client_aux_channel
        .register_callback(DefaultChannelCallback)
        .await
        .unwrap();

    let rpc_client_aux_queue = format!("amqp.{}", Uuid::new_v4());
    let mut queue_declare = QueueDeclareArguments::new(&rpc_client_aux_queue);
    queue_declare.auto_delete(true);
    rpc_client_aux_channel
        .queue_declare(queue_declare)
        .await
        .unwrap()
        .unwrap();

    async fn rpc_client_handler(_body: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    };
    let args = BasicConsumeArguments::new(
        &rpc_client_aux_queue,
        &format!("ctag{}", Uuid::new_v4().to_string()),
    );
    // self.consumers.insert(queue_name.to_string(), true);

    let rpc_futures = Arc::new(RwLock::new(HashMap::new()));

    let sub_handler = BroadHandler::new(
        None,
        rpc_queue_name.to_string(),
        rpc_client_handler,
        Some(Arc::clone(&rpc_futures)),
    );

    rpc_client_aux_channel
        .basic_consume(sub_handler, args)
        .await
        .unwrap();
    loop {
        for _ in 0..500 {
            let (tx, rx) = oneshot::channel();
            let futures = Arc::clone(&rpc_futures);
            let correlated_id = Uuid::new_v4().to_string();
            let mut futures_map = futures.write().await;
            futures_map.entry(correlated_id.to_string()).or_insert(tx);

            let mut args = BasicPublishArguments::new(rpc_exchange_name, routing_key);
            args.mandatory(false);
            let mut properties = BasicProperties::default();
            properties.with_correlation_id(&correlated_id);
            properties.with_reply_to(rpc_client_aux_queue.to_string().as_ref());
            properties.with_delivery_mode(DELIVERY_MODE_TRANSIENT);
            drop(futures_map);
            let channel = rpc_client_channel.clone();
            let _ = channel
                .basic_publish(properties, "Ok".into(), args)
                .await
                .unwrap();
            println!("{:?}", rx.await);
        }
    }
}
