use std::{str::FromStr, sync::Arc, time::Duration};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use harmony_core::{
    Broker, BrokerBuilder, ProtocolPacket,
    broker::builder::OrgTriple,
    service::{
        ProtocolService, ProtocolServiceDefinition, ProtocolServiceDefinitionMethods,
        recieve::ProtocolServiceReceiveDefinition, send::ProtocolServiceSendDefinition,
    },
};
use iroh::{
    NodeId, PublicKey, SecretKey,
    endpoint::{ConnectOptions, TransportConfig},
};
use serde::{Deserialize, Serialize};

const PEER_1_ADDR: &str = "8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394";

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
    ) -> Result<impl Sink<Self::SinkItem, Error = Self::Error>, Self::Error> {
        let mut transport_options = TransportConfig::default();
        transport_options.max_idle_timeout(None);
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
    ) -> Result<impl Stream<Item = Self::StreamItem>, Self::Error> {
        let stream = definition.get_receive_connection(broker)?;
        Ok(stream.map(|x| x.unwrap().data().into()))
    }
}

#[tokio::main]
async fn main() {
    let key = SecretKey::from_bytes(&[1; 32]);
    println!("key is {:#?}", key.public());

    let triple = OrgTriple::new("com", "harmony", "peer2");
    let builder = BrokerBuilder::new(key, triple).await.unwrap();

    let broker = builder
        .add_service::<ExampleServiceDefinition>()
        .unwrap()
        .build()
        .await
        .unwrap();

    let peer = PublicKey::from_str(PEER_1_ADDR).unwrap();
    let service = ProtocolService::new(&broker, ExampleServiceDefinition);

    let (send_service, recv_service) = service.service_channels();
    tokio::spawn(async move {
        while let Ok(()) = send_service.send("HAIIII FROM PEER_2".into(), peer).await {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    tokio::spawn(async move {
        let mut recv = recv_service.receieve_stream().unwrap();
        while let Some(packet) = recv.next().await {
            println!("{}", packet);
        }
    })
    .await
    .unwrap();
}
