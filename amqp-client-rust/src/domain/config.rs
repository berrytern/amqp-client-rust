use url::Url;

pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub options: ConfigOptions,
}
impl Config {
    pub fn from_url(
        url: &str,
        options: ConfigOptions,
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
        })
    }

    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        options: ConfigOptions,
    ) -> Config {
        Config {
            host,
            port,
            username,
            password,
            options,
        }
    }
}
// Placeholder for ConfigOptions struct
pub struct ConfigOptions {
    pub rpc_queue_name: String,
    pub rpc_exchange_name: String,
    pub queue_name: String,
}
