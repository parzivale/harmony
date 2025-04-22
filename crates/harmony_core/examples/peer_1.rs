use std::{any::TypeId, str::FromStr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use harmony_core::{Broker, BrokerBuilder, ProtocolPacket};
use iroh::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use tokio::{task::JoinHandle, time::sleep};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message {
    data: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message2 {
    data: Vec<char>,
}

impl ProtocolPacket<'_> for Message {
    const APLN: &'static str = "message";
}

impl ProtocolPacket<'_> for Message2 {
    const APLN: &'static str = "message2";
}

#[tokio::main]
async fn main() {
    let key = SecretKey::from_bytes(&[2; 32]);
    println!("key is {:#?}", key.public());

    let builder = BrokerBuilder::new(key).await.unwrap();
    let server =
        PublicKey::from_str("8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c")
            .unwrap();

    let broker = builder
        .add_protocol::<Message>()
        .add_protocol::<Message2>()
        .build()
        .await
        .unwrap();

    spawn_handler::<Message>(broker.clone());
    spawn_handler::<Message2>(broker.clone());

    let message = Message {
        data: "HAII".to_string(),
    };

    let message2 = Message2 {
        data: vec!['h', 'a', 'i'],
    };

    let mut stream = broker.send_packet_sink::<Message>(server).await.unwrap();
    let mut stream2 = broker.send_packet_sink::<Message2>(server).await.unwrap();

    loop {
        stream.send(message.clone()).await.unwrap();
        sleep(Duration::from_secs(1)).await;
        stream2.send(message2.clone()).await.unwrap();
        sleep(Duration::from_secs(1)).await;
    }
}

fn spawn_handler<T>(broker: Broker) -> JoinHandle<()>
where
    for<'de> T: ProtocolPacket<'de> + 'static,
{
    tokio::spawn(async move {
        let mut stream = broker.recieve_packet_stream::<T>().unwrap();
        while let Some(packet) = stream.next().await {
            let packet = packet.unwrap();
            println!("Packet from {:?} recieved", packet.from_node());
            println!("Packet type is: {:?}", TypeId::of::<T>());
            println!("Packet Contents: {:?}", packet.data());
        }
    })
}
