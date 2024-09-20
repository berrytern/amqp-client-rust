use std::sync::Arc;
use amqprs::{
    callbacks::{ChannelCallback, ConnectionCallback},
    channel::Channel,
    connection::Connection,
    error::Error as AMQPError,
    Ack, BasicProperties, Cancel, Close, CloseChannel, Nack, Return
};
use async_trait::async_trait;
use tokio::{sync::Mutex, time::{sleep, Duration}};
use super::connection::AsyncConnection;


pub type AMQPResult<T> = std::result::Result<T, AMQPError>;
pub struct MyChannelCallback{
    pub connection: Arc<Mutex<AsyncConnection>>,
}

#[async_trait]
impl ChannelCallback for MyChannelCallback {
    async fn close(&mut self, _channel: &Channel, _close: CloseChannel) -> AMQPResult<()> {
        #[cfg(feature = "traces")]
        error!(
            "handle close request for channel {}, cause: {}",
            channel, close
        );
        let mut connection = self.connection.lock().await;
        connection.close().await;
        Ok(())
    }
    async fn cancel(&mut self, _channel: &Channel, _cancel: Cancel) -> AMQPResult<()> {
        #[cfg(feature = "traces")]
        warn!(
            "handle cancel request for consumer {} on channel {}",
            cancel.consumer_tag(),
            channel
        );
        Ok(())
    }
    async fn flow(&mut self, _channel: &Channel, _active: bool) -> AMQPResult<bool> {
        #[cfg(feature = "traces")]
        info!(
            "handle flow request active={} for channel {}",
            active, channel
        );
        Ok(true)
    }
    async fn publish_ack(&mut self, _channel: &Channel, _ack: Ack) {
        #[cfg(feature = "traces")]
        info!(
            "handle publish ack delivery_tag={} on channel {}",
            ack.delivery_tag(),
            channel
        );
    }
    async fn publish_nack(&mut self, _channel: &Channel, _nack: Nack) {
        #[cfg(feature = "traces")]
        warn!(
            "handle publish nack delivery_tag={} on channel {}",
            nack.delivery_tag(),
            channel
        );
    }
    async fn publish_return(
        &mut self,
        _channel: &Channel,
        _ret: Return,
        _basic_properties: BasicProperties,
        _content: Vec<u8>,
    ) {
        #[cfg(feature = "traces")]
        warn!(
            "handle publish return {} on channel {}, content size: {}",
            ret,
            channel,
            content.len()
        );
    }
}


pub struct MyConnectionCallback{
    pub connection: Arc<Mutex<AsyncConnection>>,
}


#[async_trait]
impl ConnectionCallback for MyConnectionCallback {

    async fn close(&mut self, _connection: &Connection, _close: Close) -> AMQPResult<()> {
        #[cfg(feature = "traces")]
        error!(
            "handle close request for connection {}, cause: {}",
            connection, close
        );
        let cn = self.connection.clone();
        tokio::spawn(async move {
            let mut cn = cn.lock().await;
            for _ in 0..10 {
                sleep(Duration::from_micros(200)).await;
                if !cn.is_open() {
                    break;
                }
            }
            let _ = cn.reconnect().await.await;
        });
        Ok(())
    }

    async fn blocked(&mut self, _connection: &Connection, _reason: String) {
        #[cfg(feature = "traces")]
        info!(
            "handle blocked notification for connection {}, reason: {}",
            connection, reason
        );
    }

    async fn unblocked(&mut self, _connection: &Connection) {
        #[cfg(feature = "traces")]
        info!(
            "handle unblocked notification for connection {}",
            connection
        );
    }
}