use url::Url;
#[cfg(feature = "tls")]
use amqprs::tls::TlsAdaptor;

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub options: ConfigOptions,
    #[cfg(feature = "tls")]
    pub tls_adaptor: Option<TlsAdaptor>,
}
impl Config {
    pub fn from_url(
        url: &str,
        options: ConfigOptions,
        #[cfg(feature = "tls")]
        tls_adaptor: Option<TlsAdaptor>,
    ) -> Result<Config, Box<dyn std::error::Error>> {
        let parsed_url = Url::parse(url)?;
        let host = parsed_url.host_str().ok_or("No host in URL")?.to_string();
        let port = parsed_url.port().unwrap_or(5672);
        let username = parsed_url.username().to_string();
        let password = parsed_url.password().unwrap_or("").to_string();

        Ok(Config {
            host,
            port,
            username,
            password,
            options,
            #[cfg(feature = "tls")]
            tls_adaptor,
        })
    }

    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        options: ConfigOptions,
        #[cfg(feature = "tls")]
        tls_adaptor: Option<TlsAdaptor>,
    ) -> Config {
        Config {
            host,
            port,
            username,
            password,
            options,
            #[cfg(feature = "tls")]
            tls_adaptor,
        }
    }
}
// Placeholder for ConfigOptions struct
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigOptions {
    pub rpc_queue_name: String,
    pub rpc_exchange_name: String,
    pub queue_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QoSConfig {
    pub pub_confirm: bool,
    pub rpc_client_confirm: bool,
    pub rpc_server_confirm: bool,
    pub sub_auto_ack: bool,
    pub rpc_server_auto_ack: bool,
    pub rpc_client_auto_ack: bool,
    pub sub_prefetch: Option<u16>,
    pub rpc_server_prefetch: Option<u16>,
    pub rpc_client_prefetch: Option<u16>,
}
impl QoSConfig {
    pub fn new(
        pub_confirm: bool,
        rpc_client_confirm: bool,
        rpc_server_confirm: bool,
        sub_auto_ack: bool,
        rpc_server_auto_ack: bool,
        rpc_client_auto_ack: bool,
        sub_prefetch: Option<u16>,
        rpc_server_prefetch: Option<u16>,
        rpc_client_prefetch: Option<u16>,
    ) -> Self {
        Self {
            pub_confirm,
            rpc_client_confirm,
            rpc_server_confirm,
            sub_auto_ack,
            rpc_server_auto_ack,
            rpc_client_auto_ack,
            sub_prefetch,
            rpc_server_prefetch,
            rpc_client_prefetch,
        }
    }
}

impl Default for QoSConfig {
    fn default() -> Self {
        Self {
            pub_confirm: true,
            rpc_client_confirm: true,
            rpc_server_confirm: false,
            sub_auto_ack: false,
            rpc_server_auto_ack: false,
            rpc_client_auto_ack: false,
            sub_prefetch: None,
            rpc_server_prefetch: None,
            rpc_client_prefetch: None,
        }
    }
}