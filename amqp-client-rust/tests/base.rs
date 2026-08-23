use std::env;

use amqp_client_rust::domain::config::{Config, ConfigOptions};
use uuid::Uuid;

pub fn create_test_config() -> Config {
    let queue_uuid = Uuid::new_v4();
    Config::new(
        &env::var("RABBITMQ_HOST").unwrap_or_else(|_| "localhost".to_string()),
        env::var("RABBITMQ_PORT").unwrap_or_else(|_| "5672".to_string()).parse().unwrap(),
        &env::var("RABBITMQ_USER").unwrap_or_else(|_| "guest".to_string()),
        &env::var("RABBITMQ_PASS").unwrap_or_else(|_| "guest".to_string()),
        ConfigOptions {
            queue_name: format!("test_queue_{}", queue_uuid),
            rpc_queue_name: format!("test_rpc_queue_{}", queue_uuid),
            rpc_exchange_name: format!("test_rpc_exchange_{}", queue_uuid),
            dead_letter_exchange: None,
            dead_letter_routing_key: None,
        },
        &env::var("RABBITMQ_VHOST").unwrap_or_else(|_| "/".to_string()),
    )
}

#[allow(dead_code)]
pub async fn cleanup_test_resources(config: &Config, extra_exchanges: &[&str]) {
    use amqprs::channel::{ExchangeDeleteArguments, QueueDeleteArguments};
    use amqprs::connection::{Connection, OpenConnectionArguments};

    let mut options = OpenConnectionArguments::new(
        &config.host,
        config.port,
        &config.username,
        &config.password,
    );
    options.virtual_host(&config.virtual_host);
    #[cfg(feature = "tls")]
    if let Some(tls_adaptor) = &config.tls_adaptor {
        options.tls_adaptor(tls_adaptor.clone());
    }

    if let Ok(conn) = Connection::open(&options).await {
        if let Ok(ch) = conn.open_channel(None).await {
            let _ = ch.queue_delete(QueueDeleteArguments::new(&config.options.queue_name)).await;
            let _ = ch.queue_delete(QueueDeleteArguments::new(&config.options.rpc_queue_name)).await;
            let _ = ch.exchange_delete(ExchangeDeleteArguments::new(&config.options.rpc_exchange_name)).await;
            for ex in extra_exchanges {
                let _ = ch.exchange_delete(ExchangeDeleteArguments::new(ex)).await;
            }
            let _ = ch.close().await;
        }
        let _ = conn.close().await;
    }
}

#[allow(dead_code)]
const KX: u32 = 123456789;
#[allow(dead_code)]
const KY: u32 = 362436069;
#[allow(dead_code)]
const KZ: u32 = 521288629;
#[allow(dead_code)]
const KW: u32 = 88675123;

#[allow(dead_code)]
pub struct Rand {
    x: u32, y: u32, z: u32, w: u32
}

#[allow(dead_code)]
impl Rand{
    pub fn new(seed: u32) -> Rand {
        Rand{
            x: KX^seed, y: KY^seed,
            z: KZ, w: KW
        }
    }

    // Xorshift 128, taken from German Wikipedia
    pub fn rand(&mut self) -> u32 {
        let t = self.x^self.x.wrapping_shl(11);
        self.x = self.y; self.y = self.z; self.z = self.w;
        self.w ^= self.w.wrapping_shr(19)^t^t.wrapping_shr(8);
        return self.w;
    }

    pub fn shuffle<T>(&mut self, a: &mut [T]) {
        if a.len()==0 {return;}
        let mut i = a.len()-1;
        while i>0 {
            let j = (self.rand() as usize)%(i+1);
            a.swap(i,j);
            i-=1;
        }
    }

    pub fn rand_range(&mut self, a: i32, b: i32) -> i32 {
        let m = (b-a+1) as u32;
        return a+(self.rand()%m) as i32;
    }

    pub fn rand_float(&mut self) -> f64 {
        (self.rand() as f64)/(<u32>::max_value() as f64)
    }
}