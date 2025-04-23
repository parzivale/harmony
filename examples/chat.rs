use base64::{Engine as _, engine::general_purpose};
use core::panic;
use futures_util::{SinkExt, StreamExt, sink::Send};
use harmony_core::BrokerBuilder;
use iroh::{NodeId, SecretKey};
use keyring::Entry;
use protocol::message::v1::Message;
use rand::random;
use std::{env::args, io::stdin, str::FromStr};

#[tokio::main]
async fn main() {
    let args = args();
    let entry = Entry::new("harmony", &whoami::username());
    let entry = entry.unwrap();

    let key = match entry.get_secret() {
        Err(keyring::Error::NoEntry) => {
            let key = SecretKey::generate(rand::rngs::OsRng);
            let encoded = key.secret().as_bytes();
            println!(
                "you did not have a key in disk your current key is {:?}",
                key.public()
            );
            entry.set_secret(encoded).unwrap();
            return;
        }
        Ok(secret) => SecretKey::from_bytes(&secret.try_into().unwrap()),
        Err(err) => {
            let key = SecretKey::generate(rand::rngs::OsRng);
            let encoded = key.secret().as_bytes();
            entry.set_secret(encoded).unwrap();
            panic!("{:?}", err);
        }
    };
    println!("I am listening on key: {}", key.public());
    let builder = BrokerBuilder::new(key.clone()).await.unwrap();

    let broker = builder.add_protocol::<Message>().build().await.unwrap();
    let send_broker = broker.clone();
    let recieve_broker = broker.clone();
    let mut spawned = if let Some(node_id) = args.into_iter().nth(1) {
        tokio::spawn(async move {
            println!("Spawned input handler");
            let stdin = stdin();
            let mut buffer = String::new();
            let node_id = NodeId::from_str(&node_id).unwrap();
            let mut send_stream = send_broker
                .send_packet_sink::<Message>(node_id)
                .await
                .unwrap();
            println!("Acquired send stream");
            loop {
                stdin.read_line(&mut buffer).unwrap();
                send_stream.send(Message::new(&buffer)).await.unwrap();
                buffer.clear();
            }
        });
        true
    } else {
        println!("Your current public key is: {:?}", key.public());
        false
    };

    let send_broker = broker.clone();
    tokio::spawn(async move {
        let mut stream = recieve_broker.recieve_packet_stream::<Message>().unwrap();
        while let Some(message) = stream.next().await {
            let message = message.unwrap();

            let from = message.from_node();

            println!("{}", message.data());
            let send_broker = send_broker.clone();
            if !spawned {
                tokio::spawn(async move {
                    println!("Spawned Input handler");
                    let stdin = stdin();
                    let mut buffer = String::new();
                    let mut send_stream =
                        send_broker.send_packet_sink::<Message>(from).await.unwrap();
                    println!("Aquired Send Stream");
                    loop {
                        stdin.read_line(&mut buffer).unwrap();
                        send_stream.send(Message::new(&buffer)).await.unwrap();
                        buffer.clear();
                    }
                });
                spawned = true;
            }
        }
    })
    .await
    .unwrap();
}
