use amqprs::{
    callbacks::{ChannelCallback, ConnectionCallback},
    channel::Channel,
    connection::Connection,
    error::Error as AMQPError,
    Ack, BasicProperties, Cancel, Close, CloseChannel, Nack, Return
};
use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, debug, error, warn};
use crate::api::{connection::ConnectionCommand, utils::PendingCmd};

pub type AMQPResult<T> = std::result::Result<T, AMQPError>;
pub struct MyChannelCallback{
    pub sender_pending: UnboundedSender<PendingCmd>,
}

#[async_trait]
impl ChannelCallback for MyChannelCallback {
    async fn close(&mut self, _channel: &Channel, _close: CloseChannel) -> AMQPResult<()> {
        error!(
            "handle close request for channel {}, cause: {}",
            _channel, _close
        );
        Ok(())
    }
    async fn cancel(&mut self, _channel: &Channel, _cancel: Cancel) -> AMQPResult<()> {
        warn!(
            "handle cancel request for consumer {} on channel {}",
            _cancel.consumer_tag(),
            _channel
        );
        Ok(())
    }
    async fn flow(&mut self, _channel: &Channel, _active: bool) -> AMQPResult<bool> {
        info!(
            "handle flow request active={} for channel {}",
            _active, _channel
        );
        Ok(true)
    }
    async fn publish_ack(&mut self, _channel: &Channel, ack: Ack) {
        info!(
            "handle publish ack delivery_tag={} on channel {}",
            ack.delivery_tag(),
            _channel
        );
        let tag = ack.delivery_tag();
        let multiple = ack.mutiple();
        debug!("Received ACK: tag={}, multiple={}", tag, multiple);
        let sender = self.sender_pending.clone();

        if let Err(e) = sender.send(PendingCmd::Ack((tag, multiple))) {
            error!("Failed to send ACK to connection manager: {}", e);
        }
        
    }
    async fn publish_nack(&mut self, _channel: &Channel, nack: Nack) {
        warn!(
            "handle publish nack delivery_tag={} on channel {}",
            nack.delivery_tag(),
            _channel
        );
        let tag = nack.delivery_tag();
        let multiple = nack.multiple();

        let sender = self.sender_pending.clone();

        if let Err(e) = sender.send(PendingCmd::Nack((tag, multiple))) {
            error!("Failed to send NACK to connection manager: {}", e);
        }
        
    }
    async fn publish_return(
        &mut self,
        _channel: &Channel,
        _ret: Return,
        _basic_properties: BasicProperties,
        _content: Vec<u8>,
    ) {
        warn!(
            "handle publish return {} on channel {}, content size: {}",
            _ret,
            _channel,
            _content.len()
        );
    }
}


pub struct MyConnectionCallback{
    pub sender: UnboundedSender<ConnectionCommand>,
}


#[async_trait]
impl ConnectionCallback for MyConnectionCallback {

    async fn close(&mut self, _connection: &Connection, _close: Close) -> AMQPResult<()> {
        error!(
            "handle close request for connection {}, cause: {}",
            _connection, _close
        );
        let _ = self.sender.send(ConnectionCommand::CheckConnection{});
        Ok(())
    }

    async fn blocked(&mut self, _connection: &Connection, _reason: String) {
        debug!(
            "handle blocked notification for connection {}, reason: {}",
            _connection, _reason
        );
    }

    async fn unblocked(&mut self, _connection: &Connection) {
        debug!(
            "handle unblocked notification for connection {}",
            _connection
        );
    }
}