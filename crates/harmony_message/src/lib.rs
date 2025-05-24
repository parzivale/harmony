use harmony_core::{
    NodeId, ProtocolServiceDefinition, ProtocolServiceDefinitionMethods,
    ProtocolServiceSendDefinition, SinkExt, builder::SendBrokerError, send::SendConnection,
};
use protocol::message::v1::{message::Message, retransmit::Retransmit};
use thiserror::Error;

pub struct MessageServiceDefinition;

impl ProtocolServiceDefinition for MessageServiceDefinition {
    type Protocols = (Message, Retransmit);
}

#[derive(Debug, Error)]
pub enum SendMessageError {
    #[error(transparent)]
    SendBrokerError(#[from] SendBrokerError),
}

impl ProtocolServiceSendDefinition for MessageServiceDefinition {
    type SinkItem = String;

    type Error = SendMessageError;

    async fn send_sink(
        &self,
        broker: &harmony_core::Broker,
        node: NodeId,
    ) -> Result<impl harmony_core::Sink<Self::SinkItem, Error = Self::Error>, Self::Error> {
        let connection: SendConnection<Message> = self.get_send_connection(broker, node).await?;

        Ok(Box::pin(connection.with(async |message| Ok()))
    }
}
