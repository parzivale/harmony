use std::{str::FromStr, sync::Arc, time::Duration};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use harmony_core::{
    Broker, BrokerBuilder, ProtocolPacket,
    broker::builder::OrgTriple,
    service::{
        ProtocolService, ProtocolServiceDefinition, ProtocolServiceDefinitionMethods,
        recieve::ProtocolServiceReceiveDefinition,
        send::{ProtocolSendService, ProtocolServiceSendDefinition},
    },
};
use iroh::{
    NodeId, PublicKey, SecretKey,
    endpoint::{ConnectOptions, TransportConfig},
};
use serde::{Deserialize, Serialize};

const PEER_2_ADDR: &str = "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c";

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message {
    data: String,
}

impl From<String> for Message {
    fn from(value: String) -> Self {
        Self { data: value }
    }
}

impl From<Message> for String {
    fn from(value: Message) -> Self {
        value.data
    }
}

impl ProtocolPacket<'_> for Message {
    const APLN: &'static str = "message";
}

pub struct ExampleServiceDefinition;

impl ProtocolServiceDefinition for ExampleServiceDefinition {
    type Protocols = (Message,);
    type Tables = ();
}

impl ProtocolServiceSendDefinition for ExampleServiceDefinition {
    type SinkItem = String;

    type Error = anyhow::Error;

    async fn send_sink(
        definition: Arc<Self>,
        broker: &Broker,
        node: NodeId,
    ) -> anyhow::Result<impl Sink<Self::SinkItem, Error = Self::Error>> {
        let sink = definition.get_send_connection(broker, node).await?;
        Ok(Box::pin(
            sink.with(async |message| Ok(Message::from(message))),
        ))
    }
}

impl ProtocolServiceReceiveDefinition for ExampleServiceDefinition {
    type StreamItem = String;

    type Error = anyhow::Error;

    fn recv_stream(
        definition: Arc<Self>,
        broker: &Broker,
    ) -> anyhow::Result<impl Stream<Item = Self::StreamItem>, Self::Error> {
        let stream = definition.get_receive_connection(broker)?;
        Ok(stream.map(|x| {
            println!("received message");
            x.unwrap().data().into()
        }))
    }
}

#[tokio::main]
async fn main() {
    let key = SecretKey::from_bytes(&[2; 32]);
    println!("key is {:#?}", key.public());

    let triple = OrgTriple::new("com", "harmony", "peer1");
    let builder = BrokerBuilder::new(key, triple).await.unwrap();

    let broker = builder
        .add_service::<ExampleServiceDefinition>()
        .unwrap()
        .build()
        .await
        .unwrap();

    let peer = PublicKey::from_str(PEER_2_ADDR).unwrap();
    let service = ProtocolService::new(&broker, ExampleServiceDefinition);

    let (send_service, recv_service) = service.service_channels();

    async fn send_handler(
        send_service: ProtocolSendService<ExampleServiceDefinition>,
        peer: NodeId,
    ) {
        let mut send_service = send_service.send_sink(peer).await.unwrap();
        loop {
            send_service.send("HAII FROM PEER_1".into()).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    tokio::spawn(send_handler(send_service, peer));

    tokio::spawn(async move {
        let mut recv = recv_service.receieve_stream().unwrap();
        while let Some(packet) = recv.next().await {
            println!(" {}", packet);
        }
    })
    .await
    .unwrap();
}
