use std::{str::FromStr, sync::Arc, time::Duration};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use harmony_core::{
    Broker, BrokerBuilder, ProtocolPacket, ProtocolService, ProtocolServiceMethods,
    ServiceConstructor,
};
use iroh::{
    PublicKey, SecretKey,
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

#[derive(Clone)]
pub struct ExampleService {
    peer: PublicKey,
    broker: Broker,
}

impl<'a> ServiceConstructor<'a> for ExampleService {
    async fn new(broker: &'a Broker) -> ExampleService {
        let server = PublicKey::from_str(PEER_1_ADDR).unwrap();
        Self {
            peer: server,
            broker: broker.clone(),
        }
    }
}

impl ProtocolService<'_, String, String> for ExampleService {
    type Protocols = (Message,);

    type StreamError = anyhow::Error;

    type SinkError = anyhow::Error;
    type SinkInnerError = anyhow::Error;

    fn stream(&self) -> anyhow::Result<impl Stream<Item = String>, Self::StreamError> {
        let stream = Self::get_receive_connection(&self.broker)?;
        Ok(stream.map(|x| x.unwrap().data().into()))
    }

    async fn sink(
        &self,
    ) -> Result<impl Sink<String, Error = Self::SinkInnerError>, Self::SinkError> {
        let mut transport_options = TransportConfig::default();
        transport_options.max_idle_timeout(None);
        let connection_options =
            ConnectOptions::new().with_transport_config(Arc::new(transport_options));
        let sink =
            Self::get_send_connection_with_options(&self.broker, self.peer, connection_options)
                .await?;
        Ok(Box::pin(
            sink.with(async |message| Ok(Message::from(message))),
        ))
    }
}

#[tokio::main]
async fn main() {
    let key = SecretKey::from_bytes(&[1; 32]);
    println!("key is {:#?}", key.public());

    let builder = BrokerBuilder::new(key).await.unwrap();

    let broker = builder
        .add_service::<ExampleService, String, String>()
        .build()
        .await
        .unwrap();

    let service = broker.as_service::<ExampleService, String, String>().await;

    let send_service = service.clone();
    let recv_service = service.clone();
    tokio::spawn(async move {
        loop {
            let mut send = send_service.sink().await.unwrap();
            while let Ok(()) = send.send("HAIIII".into()).await {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });

    tokio::spawn(async move {
        let mut recv = recv_service.stream().unwrap();
        while let Some(packet) = recv.next().await {
            println!("{}", packet);
        }
    })
    .await
    .unwrap();
}
