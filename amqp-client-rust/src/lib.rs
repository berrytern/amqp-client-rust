mod errors;

use amqprs::{
    callbacks,
    security::SecurityCredentials,
    connection::{OpenConnectionArguments, Connection},
};

pub async fn mount_connection() {
    println!("Hello, world!");
    let args = OpenConnectionArguments::new("localhost", 5672, "guest", "guest");
    let connection = Connection::open(&args).await.unwrap();
    connection.register_callback(callbacks::DefaultConnectionCallback).await.unwrap();
    let channel = connection.open_channel(None).await.unwrap();
    channel.register_callback(callbacks::DefaultChannelCallback).await.unwrap();
    channel.flow(true).await.unwrap();
}
