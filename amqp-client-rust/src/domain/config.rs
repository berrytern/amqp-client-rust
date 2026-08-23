use url::Url;


#[cfg(feature = "tls")]
pub type TlsAdaptor = amqprs::tls::TlsAdaptor;

#[derive(Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub virtual_host: String,
    pub options: ConfigOptions,
    #[cfg(feature = "tls")]
    pub tls_adaptor: Option<TlsAdaptor>,
}
impl Config {
    pub fn from_url(
        url: &str,
        options: ConfigOptions,
    ) -> Result<Config, Box<dyn std::error::Error>> {
        let parsed_url = Url::parse(url)?;
        let host = parsed_url.host_str().ok_or("No host in URL")?.to_string();
        let default_port = if parsed_url.scheme() == "amqps" { 5671 } else { 5672 };
        let port = parsed_url.port().unwrap_or(default_port);
        let username = parsed_url.username().to_string();
        let password = parsed_url.password().unwrap_or("").to_string();
        let raw_path = parsed_url.path().trim_start_matches('/');
        let virtual_host = if raw_path.is_empty() || raw_path.eq_ignore_ascii_case("%2f") {
            "/".to_string()
        } else {
            url::form_urlencoded::parse(raw_path.as_bytes())
                .map(|(key, _)| key.to_string())
                .next()
                .unwrap_or_else(|| raw_path.to_string())
        };
        Ok(Config {
            host,
            port,
            username,
            password,
            options,
            virtual_host,
            #[cfg(feature = "tls")]
            tls_adaptor: None,
        })
    }

    pub fn new(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        options: ConfigOptions,
        virtual_host: &str,
    ) -> Config {
        Config {
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            options,
            virtual_host: virtual_host.into(),
            #[cfg(feature = "tls")]
            tls_adaptor: None,
        }
    }

    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, tls_adaptor: TlsAdaptor) -> Self {
        self.tls_adaptor = Some(tls_adaptor);
        self
    }
}
// Placeholder for ConfigOptions struct
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigOptions {
    pub rpc_queue_name: String,
    pub rpc_exchange_name: String,
    pub queue_name: String,
    pub dead_letter_exchange: Option<String>,
    pub dead_letter_routing_key: Option<String>,
}

impl ConfigOptions {
    pub fn new(queue_name: impl Into<String>, rpc_queue_name: impl Into<String>, rpc_exchange_name: impl Into<String>) -> Self {
        Self {
            queue_name: queue_name.into(),
            rpc_queue_name: rpc_queue_name.into(),
            rpc_exchange_name: rpc_exchange_name.into(),
            dead_letter_exchange: None,
            dead_letter_routing_key: None,
        }
    }

    pub fn with_dead_letter(mut self, exchange: impl Into<String>, routing_key: Option<impl Into<String>>) -> Self {
        self.dead_letter_exchange = Some(exchange.into());
        self.dead_letter_routing_key = routing_key.map(|k| k.into());
        self
    }
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
