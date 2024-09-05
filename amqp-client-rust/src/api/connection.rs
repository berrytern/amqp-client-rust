use crate::{
    api::{callback::MyChannelCallback, channel::AsyncChannel},
    domain::into_response::IntoResponse,
};
use amqprs::{
    callbacks::DefaultConnectionCallback,
    connection::{Connection, OpenConnectionArguments},
};
use std::sync::Arc;

pub struct AsyncConnection<T: IntoResponse> {
    connection: Option<Connection>,
    pub channel: Option<AsyncChannel<T>>,
}
impl<T: IntoResponse + Send + 'static + std::fmt::Debug> AsyncConnection<T> {
    pub async fn new() -> Self {
        Self {
            connection: None,
            channel: None,
        }
    }

    pub fn is_open(&self) -> bool {
        self.connection
            .as_ref()
            .map_or(false, |conn| conn.is_open())
    }

    pub async fn open(&mut self, host: &str, port: u16, username: &str, password: &str) {
        if !self.is_open() {
            let connection = Connection::open(&OpenConnectionArguments::new(
                host, port, username, password,
            ))
            .await
            .unwrap();
            connection
                .register_callback(DefaultConnectionCallback)
                .await
                .unwrap();
            self.connection = Some(connection);
        }
    }

    pub fn channel_is_open(&self) -> bool {
        if !self.channel.is_none() {
            self.channel.as_ref().unwrap().channel.is_open()
        } else {
            false
        }
    }
}

impl AsyncConnection<()> {
    pub async fn create_channel(&mut self) {
        if self.is_open() && !self.channel_is_open() {
            if let Some(connection) = &self.connection {
                if let Ok(channel) = connection.open_channel(None).await {
                    let _ = channel.register_callback(MyChannelCallback).await;
                    self.channel = Some(AsyncChannel::new(channel, None));
                }
            }
        }
    }
}

impl AsyncConnection<Vec<u8>> {
    pub async fn create_channel(&mut self) {
        if self.is_open() && !self.channel_is_open() {
            if let Some(connection) = &self.connection {
                if let Ok(channel) = connection.open_channel(None).await {
                    let _ = channel.register_callback(MyChannelCallback).await;

                    if let Ok(aux_channel) = connection.open_channel(None).await {
                        self.channel =
                            Some(AsyncChannel::new(channel, Some(Arc::new(aux_channel))));
                    }
                }
            }
        }
    }
}
