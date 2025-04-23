use core::panic;
use futures_util::{SinkExt, StreamExt};
use harmony_core::BrokerBuilder;
use iroh::{NodeId, SecretKey};
use keyring::Entry;
use protocol::message::v1::Message;
use std::{env::args, str::FromStr};
use tokio::io::{self, AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() {
    let args = args();
    let entry = Entry::new("harmony2", &whoami::username());
    let entry = entry.unwrap();

    let key = match entry.get_secret() {
        Err(keyring::Error::NoEntry) => {
            let key = SecretKey::generate(rand::rngs::OsRng);
            let encoded = key.secret().as_bytes();
            println!(
                "you did not have a key in disk your current key is {}",
                key.public()
            );
            entry.set_secret(encoded).unwrap();
            key
        }
        Ok(secret) => SecretKey::from_bytes(&secret.try_into().unwrap()),
        Err(err) => {
            let key = SecretKey::generate(rand::rngs::OsRng);
            let encoded = key.secret().as_bytes();
            entry.set_secret(encoded).unwrap();
            panic!("{:?}", err);
        }
    };
    println!("public key {}", key.public());
    let builder = BrokerBuilder::new(key.clone()).await.unwrap();

    let broker = builder.add_protocol::<Message>().build().await.unwrap();
    let send_broker = broker.clone();
    let recieve_broker = broker.clone();
    let mut spawned = if let Some(node_id) = args.into_iter().nth(1) {
        tokio::spawn(async move {
            let stdin = io::stdin();
            let mut lines = BufReader::new(stdin).lines();
            let node_id = NodeId::from_str(&node_id).unwrap();
            let mut send_stream = send_broker
                .send_packet_sink::<Message>(node_id)
                .await
                .unwrap();
            println!("Connected to {}", node_id);
            loop {
                let line = lines.next_line().await.unwrap().unwrap();
                println!("sending {} to {}", line, node_id);
                send_stream.send(Message::new(&line)).await.unwrap();
            }
        });
        true
    } else {
        false
    };

    let send_broker = broker.clone();
    tokio::spawn(async move {
        let mut stream = recieve_broker.recieve_packet_stream::<Message>().unwrap();
        while let Some(message) = stream.next().await {
            let message = message.unwrap();

            let from = message.from_node();

            println!("{}: {}", from, message.data());
            let send_broker = send_broker.clone();
            if !spawned {
                tokio::spawn(async move {
                    let stdin = io::stdin();
                    let mut lines = BufReader::new(stdin).lines();
                    let mut send_stream =
                        send_broker.send_packet_sink::<Message>(from).await.unwrap();
                    loop {
                        let line = lines.next_line().await.unwrap().unwrap();
                        send_stream.send(Message::new(&line)).await.unwrap();
                    }
                });
                spawned = true;
            }
        }
    })
    .await
    .unwrap();
}
