use std::env;

use amqp_client_rust::domain::config::{Config, ConfigOptions};
use uuid::Uuid;

pub fn create_test_config() -> Config {
    let queue_uuid = Uuid::new_v4();
    Config {
        host: env::var("RABBITMQ_HOST").unwrap_or_else(|_| "localhost".to_string()),
        port: env::var("RABBITMQ_PORT").unwrap_or_else(|_| "5672".to_string()).parse().unwrap(),
        username: env::var("RABBITMQ_USER").unwrap_or_else(|_| "guest".to_string()),
        password: env::var("RABBITMQ_PASS").unwrap_or_else(|_| "guest".to_string()),
        virtual_host: env::var("RABBITMQ_VHOST").unwrap_or_else(|_| "/".to_string()),
        options: ConfigOptions {
            queue_name: format!("test_queue_{}", queue_uuid),
            rpc_queue_name: format!("test_rpc_queue_{}", queue_uuid),
            rpc_exchange_name: format!("test_rpc_exchange_{}", queue_uuid),
        },
    }
}

const KX: u32 = 123456789;
const KY: u32 = 362436069;
const KZ: u32 = 521288629;
const KW: u32 = 88675123;


pub struct Rand {
    x: u32, y: u32, z: u32, w: u32
}

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