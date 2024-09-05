use amqprs::{
    callbacks::ChannelCallback, channel::Channel, error::Error as AMQPError, Ack, BasicProperties,
    Cancel, CloseChannel, Nack, Return,
};
use async_trait::async_trait;

pub type AMQPResult<T> = std::result::Result<T, AMQPError>;
pub struct MyChannelCallback;

#[async_trait]
impl ChannelCallback for MyChannelCallback {
    async fn close(&mut self, channel: &Channel, close: CloseChannel) -> AMQPResult<()> {
        #[cfg(feature = "traces")]
        error!(
            "handle close request for channel {}, cause: {}",
            channel, close
        );
        println!(
            "handle close request for channel {}, cause: {}",
            channel, close
        );
        Ok(())
    }
    async fn cancel(&mut self, channel: &Channel, cancel: Cancel) -> AMQPResult<()> {
        #[cfg(feature = "traces")]
        warn!(
            "handle cancel request for consumer {} on channel {}",
            cancel.consumer_tag(),
            channel
        );
        println!(
            "handle cancel request for consumer {} on channel {}",
            cancel.consumer_tag(),
            channel
        );
        Ok(())
    }
    async fn flow(&mut self, channel: &Channel, active: bool) -> AMQPResult<bool> {
        #[cfg(feature = "traces")]
        info!(
            "handle flow request active={} for channel {}",
            active, channel
        );
        println!(
            "handle flow request active={} for channel {}",
            active, channel
        );
        Ok(true)
    }
    async fn publish_ack(&mut self, channel: &Channel, ack: Ack) {
        #[cfg(feature = "traces")]
        info!(
            "handle publish ack delivery_tag={} on channel {}",
            ack.delivery_tag(),
            channel
        );
        println!(
            "handle publish ack delivery_tag={} on channel {}",
            ack.delivery_tag(),
            channel
        );
    }
    async fn publish_nack(&mut self, channel: &Channel, nack: Nack) {
        #[cfg(feature = "traces")]
        warn!(
            "handle publish nack delivery_tag={} on channel {}",
            nack.delivery_tag(),
            channel
        );
        println!(
            "handle publish nack delivery_tag={} on channel {}",
            nack.delivery_tag(),
            channel
        );
    }
    async fn publish_return(
        &mut self,
        channel: &Channel,
        ret: Return,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        #[cfg(feature = "traces")]
        warn!(
            "handle publish return {} on channel {}, content size: {}",
            ret,
            channel,
            content.len()
        );
        println!(
            "handle publish return {} on channel {}, content size: {}",
            ret,
            channel,
            content.len()
        );
    }
}
