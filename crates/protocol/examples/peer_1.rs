use std::{str::FromStr, time::Duration};

use harmony_core::{
    BrokerBuilder, SinkExt, StreamExt, broker::builder::OrgTriple, service::ProtocolService,
};
use iroh::{PublicKey, SecretKey};
use protocol::message::v1::service::MessageService;

const PEER_2_ADDR: &str = "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c";

#[tokio::main]
async fn main() {
    let key = SecretKey::from_bytes(&[2; 32]);
    println!("key is {:#?}", key.public());

    let triple = OrgTriple::new("com", "harmony", "peer1");
    let builder = BrokerBuilder::new(key, triple).await.unwrap();

    let broker = builder
        .add_service::<MessageService>()
        .unwrap()
        .build()
        .await
        .unwrap();

    let peer = PublicKey::from_str(PEER_2_ADDR).unwrap();
    let message_service = MessageService::new(&broker);
    let (service, _) = ProtocolService::new(broker, message_service);

    let (send_service, recv_service) = service.service_channels();

    tokio::spawn(async move {
        let mut send_sink = send_service.send_sink(peer).await.unwrap();
        while let Ok(()) = send_sink.send("HAII FROM PEER_1".into()).await {
            tokio::time::sleep(Duration::from_secs(1)).await
        }

        send_sink.close().await.unwrap();
    });

    tokio::spawn(async move {
        let mut recv = recv_service.receieve_stream().unwrap();
        println!("setup recv");
        while let Some(packet) = recv.next().await {
            println!("entered body");
            println!("{:?}", packet);
        }
    })
    .await
    .unwrap();
}
