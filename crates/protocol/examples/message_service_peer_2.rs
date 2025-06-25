use std::{str::FromStr, time::Duration};

use harmony_core::{
    BrokerBuilder, SinkExt, StreamExt, broker::builder::OrgTriple, service::ProtocolService,
};
use iroh::{PublicKey, SecretKey};
use protocol::message::v1::service::MessageService;

const PEER_1_ADDR: &str = "8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394";

#[tokio::main]
async fn main() {
    let key = SecretKey::from_bytes(&[1; 32]);
    println!("key is {:#?}", key.public());

    let triple = OrgTriple::new("com", "harmony", "peer2");
    let builder = BrokerBuilder::new(key, triple).await.unwrap();

    let broker = builder
        .add_service::<MessageService>()
        .unwrap()
        .build()
        .await
        .unwrap();

    let peer = PublicKey::from_str(PEER_1_ADDR).unwrap();
    let message_service = MessageService::new(&broker);
    let (service, _) = ProtocolService::new(broker, message_service);

    let (send_service, recv_service) = service.service_channels();
    tokio::spawn(async move {
        let mut send_sink = send_service.send_sink(peer).await.unwrap();

        while let Ok(()) = send_sink.send("HAIIII FROM PEER_2".into()).await {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        send_sink.close().await.unwrap();
    });

    tokio::spawn(async move {
        let mut recv = recv_service.receieve_stream().unwrap();
        while let Some(packet) = recv.next().await {
            println!("{:?}", packet);
        }
    })
    .await
    .unwrap();
}
