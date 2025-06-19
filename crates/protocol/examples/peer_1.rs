use std::{str::FromStr, sync::Arc};

use harmony_core::{
    Broker, BrokerBuilder, ProtocolPacket,
    broker::builder::OrgTriple,
    service::{
        ProtocolService, ProtocolServiceDefinition, ProtocolServiceDefinitionMethods,
        recieve::ProtocolServiceReceiveDefinition, send::ProtocolServiceSendDefinition,
    },
};
use iroh::{NodeId, PublicKey, SecretKey};
use protocol::message::v1::{
    message::Message,
    service::{MessageServiceDefinition, MessageSink, SendMessageError},
};

const PEER_2_ADDR: &str = "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c";

#[tokio::main]
async fn main() {
    let key = SecretKey::from_bytes(&[2; 32]);
    println!("key is {:#?}", key.public());

    let triple = OrgTriple::new("com", "harmony", "peer1");
    let builder = BrokerBuilder::new(key, triple).await.unwrap();

    let broker = builder
        .add_service::<MessageServiceDefinition>()
        .unwrap()
        .build()
        .await
        .unwrap();

    let peer = PublicKey::from_str(PEER_2_ADDR).unwrap();
    let service = ProtocolService::new(&broker, MessageServiceDefinition);

    let (send_service, recv_service) = service.service_channels();

    tokio::spawn(async move {
        let mut send_sink = send_service.send_sink(peer).await.unwrap();
        send_sink.send("HAII FROM PEER_1".into()).await.unwrap();
        send_sink.close().await.unwrap();
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
