use base64::{Engine as _, engine::general_purpose};
use core::panic;
use futures_util::{SinkExt, StreamExt};
use harmony_core::BrokerBuilder;
use iroh::{NodeId, SecretKey};
use keyring::Entry;
use protocol::message::v1::Message;
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
        Err(err) => panic!("{:?}", err),
    };
    let node_id = if let Some(id) = args.into_iter().nth(1) {
        id
    } else {
        let _ = BrokerBuilder::new(key.clone()).await.unwrap();
        println!("Your current public key is: {:?}", key.public());
        return;
    };

    println!("I am listening on key: {}", key.public());
    println!("Connecting to peer: {:?}", node_id);
    let builder = BrokerBuilder::new(key).await.unwrap();

    let broker = builder.add_protocol::<Message>().build().await.unwrap();
    let send_broker = broker.clone();
    let recieve_broker = broker.clone();
    tokio::spawn(async move {
        let mut stream = recieve_broker.recieve_packet_stream::<Message>().unwrap();
        while let Some(message) = stream.next().await {
            let message = message.unwrap();
            println!("{}", message.data());
        }
    });

    tokio::spawn(async move {
        let stdin = stdin();
        let mut buffer = String::new();
        let node_id = NodeId::from_str(&node_id).unwrap();
        let mut send_stream = send_broker
            .send_packet_sink::<Message>(node_id)
            .await
            .unwrap();
        loop {
            stdin.read_line(&mut buffer).unwrap();
            send_stream.send(Message::new(&buffer)).await.unwrap();
            buffer.clear();
        }
    })
    .await
    .unwrap();
}
