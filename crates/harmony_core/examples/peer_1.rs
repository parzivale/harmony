use std::{str::FromStr, sync::Arc, time::Duration};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use harmony_core::{
    BrokerBuilder, ProtocolPacket, ProtocolService, ProtocolServiceDefinition,
    ProtocolServiceDefinitionMethods, ProtocolServiceReceiveDefinition,
    ProtocolServiceSendDefinition, builder::Broker,
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

pub struct ExampleService;

impl ProtocolServiceDefinition for ExampleService {
    type Protocols = (Message,);
}

impl ProtocolServiceSendDefinition for ExampleService {
    type SinkItem = String;

    type Error = anyhow::Error;

    async fn send_sink(
        &self,
        broker: &Broker,
        node: NodeId,
    ) -> anyhow::Result<impl Sink<Self::SinkItem, Error = Self::Error>> {
        let mut transport_options = TransportConfig::default();
        transport_options.max_idle_timeout(None);
        let connection_options =
            ConnectOptions::new().with_transport_config(Arc::new(transport_options));
        let sink = self
            .get_send_connection_with_options(broker, node, connection_options)
            .await?;
        Ok(Box::pin(
            sink.with(async |message| Ok(Message::from(message))),
        ))
    }
}

impl ProtocolServiceReceiveDefinition for ExampleService {
    type StreamItem = String;

    type Error = anyhow::Error;

    fn recv_stream(
        &self,
        broker: &Broker,
    ) -> anyhow::Result<impl Stream<Item = Self::StreamItem>, Self::Error> {
        let stream = self.get_receive_connection(broker)?;
        Ok(stream.map(|x| x.unwrap().data().into()))
    }
}

#[tokio::main]
async fn main() {
    let key = SecretKey::from_bytes(&[2; 32]);
    println!("key is {:#?}", key.public());

    let builder = BrokerBuilder::new(key).await.unwrap();

    let broker = builder
        .add_service::<ExampleService>()
        .build()
        .await
        .unwrap();

    let peer = PublicKey::from_str(PEER_2_ADDR).unwrap();
    let service = ProtocolService::new(&broker, ExampleService);

    let (send_service, recv_service) = service.service_channels();

    tokio::spawn(async move {
        loop {
            while let Ok(()) = send_service.send("HAIIII".into(), peer).await {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });

    tokio::spawn(async move {
        let mut recv = recv_service.receieve_stream().unwrap();
        while let Some(packet) = recv.next().await {
            println!(" {}", packet);
        }
    })
    .await
    .unwrap();
}
