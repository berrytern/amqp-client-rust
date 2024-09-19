use std::sync::{
    atomic::{AtomicBool,Ordering}, Arc
};
use tokio::sync::Mutex;
use crate::{
    api::{
        callback::MyChannelCallback,
        channel::AsyncChannel
    },
    domain::into_response::IntoResponse
};
use amqprs::{
    callbacks::DefaultConnectionCallback,
    connection::{Connection, OpenConnectionArguments}
};
#[cfg(feature = "tls")]
use amqprs::tls::TlsAdaptor;


pub struct AsyncConnection<T:IntoResponse> {
    pub connection: Option<Connection>,
    pub channel: Option<AsyncChannel<T>>,
    pub is_closing: AtomicBool,
    reconnecting: AtomicBool,
}

impl <T:IntoResponse> AsyncConnection<T> {
    pub async fn new() -> Self {
        Self {
            connection: None,
            channel: None,
            is_closing: AtomicBool::new(false),
            reconnecting: AtomicBool::new(false)
        }
    }
    pub fn is_open(&self) -> bool {
        self.connection.as_ref().map_or(false, |conn| conn.is_open())
    }

    pub async fn close(&mut self) {
        if self.is_open() {
            self.is_closing.store(true, std::sync::atomic::Ordering::Release);
            if let Some(cn) = self.connection.clone(){
                let _ = cn.close().await;
            }
        }
    }

    pub fn channel_is_open(&self) -> bool{
        if !self.channel.is_none(){
            self.channel.as_ref().unwrap().channel.is_open()
        } else {
            false
        }
    }
    
    pub async fn reconnect(&self){
        if !self.is_open() {
            match self.reconnecting.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    // We successfully set the flag to true, so we should reconnect
                    self.retry_connection().await;
                    // self.open(host, port, username, password); // To implement
                }
                Err(_) => {
                    // The flag was already true, so another thread is handling reconnection
                    // We don't need to do anything
                }
            }
        }
    }
    async fn retry_connection(&self){

    }

    
}

impl<T: IntoResponse + Send + 'static + std::fmt::Debug> AsyncConnection<T> {
    pub async fn open(
        &mut self,
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        #[cfg(feature = "tls")]
        tls_adaptor: Option<TlsAdaptor>,
    ) {
        if !self.is_open() {
            
            #[cfg(feature = "default")]
            let connection_options = OpenConnectionArguments::new(
                host,
                port,
                username,
                password,
            );
            #[cfg(feature = "tls")]
            let mut connection_options = OpenConnectionArguments::new(
                host,
                port,
                username,
                password,
            );
            #[cfg(feature = "tls")]
            if tls_adaptor.is_some() {
                connection_options = connection_options.tls_adaptor(
                    tls_adaptor.unwrap()
                ).finish();
            }
            let connection = Connection::open(&connection_options)
            .await
            .unwrap();
            connection
                .register_callback(DefaultConnectionCallback)
                .await
                .unwrap(); 
            self.connection = Some(connection);
        }
    }
    
}

impl AsyncConnection<()> {
    pub async fn create_channel(&mut self, self_connection: Arc<Mutex<AsyncConnection<()>>>){
        if self.is_open() && !self.channel_is_open(){
            if let Some(connection) = &self.connection {
                if let Ok(channel) = connection.open_channel(None).await {
                    let _ = channel
                        .register_callback(MyChannelCallback{connection: self_connection})
                        .await;
                    self.channel = Some(AsyncChannel::new(channel, None));
                }
            }
        }
    }
}

impl AsyncConnection<Vec<u8>> {
    pub async fn create_channel(&mut self, self_connection: Arc<Mutex<AsyncConnection<Vec<u8>>>>){
        if self.is_open() && !self.channel_is_open(){
            if let Some(connection) = &self.connection {
                if let Ok(channel) = connection.open_channel(None).await {
                    let _ = channel
                        .register_callback(MyChannelCallback{connection: self_connection})
                        .await;
                    
                    if let Ok(aux_channel) = connection.open_channel(None).await{
                        self.channel = Some(AsyncChannel::new(channel, Some(Arc::new(aux_channel))));
                    }
                }
            }
        }
    }
}
