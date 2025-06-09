use std::{array::TryFromSliceError, sync::Arc, time::SystemTime};

use crate::message::v1::{message::Message, retransmit::Retransmit};
use blake3::Hash;
use harmony_core::{
    Broker, FutureExt, NodeId, PacketDispatcherError, Sink, SinkExt,
    broker::SendBrokerError,
    connection::send::SendConnection,
    database::{
        DatabaseTable, table_collection::DatabaseError, transaction::ReadTransactionResult,
    },
    ready,
    service::{
        DatabaseManagerMethods, DatabaseTableDefinitionMethods, ProtocolServiceDefinition,
        ProtocolServiceDefinitionMethods, TransactionFutureError,
        send::ProtocolServiceSendDefinition,
    },
};
use redb::{StorageError, TableError, TransactionError};
use thiserror::Error;

use super::message::MessageHasher;

pub struct MessagesTable;
pub struct LastMessageTable;

#[derive(Debug, Error)]
pub enum MessagesTableError {}

impl DatabaseTable for MessagesTable {
    type Key = Hash;

    type Value = Message;

    const NAME: &'static str = "Messages";

    type Error = MessagesTableError;

    fn serialize_key<'s>(_: Self::Key) -> Result<&'s [u8], Self::Error> {
        todo!()
    }

    fn deserialize_key(_: &[u8]) -> Result<Self::Key, Self::Error> {
        todo!()
    }

    fn serialize_value<'s>(_: Self::Value) -> Result<&'s [u8], Self::Error> {
        todo!()
    }

    fn deserialize_value(_: &[u8]) -> Result<Self::Value, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Error)]
pub enum LastMessageTableError {}

impl DatabaseTable for LastMessageTable {
    type Key = NodeId;

    type Value = Message;

    const NAME: &'static str = "Messages";

    type Error = MessagesTableError;

    fn serialize_key<'s>(_: Self::Key) -> Result<&'s [u8], Self::Error> {
        todo!()
    }

    fn deserialize_key(_: &[u8]) -> Result<Self::Key, Self::Error> {
        todo!()
    }

    fn serialize_value<'s>(_: Self::Value) -> Result<&'s [u8], Self::Error> {
        todo!()
    }

    fn deserialize_value(_: &[u8]) -> Result<Self::Value, Self::Error> {
        todo!()
    }
}
pub struct MessageServiceDefinition;

impl ProtocolServiceDefinition for MessageServiceDefinition {
    type Protocols = (Message, Retransmit);

    type Tables = (MessagesTable, LastMessageTable);
}

#[derive(Debug, Error)]
pub enum SendMessageError {
    #[error(transparent)]
    SendBrokerError(#[from] SendBrokerError),
    #[error(transparent)]
    TransactionError(#[from] TransactionError),
    #[error(transparent)]
    TableError(#[from] TableError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    PacketDispatcherError(#[from] PacketDispatcherError),
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    TryFromSliceError(#[from] TryFromSliceError),
    #[error(transparent)]
    ReadTransactionFutureError(#[from] TransactionFutureError),
}

pub struct MessageSink {
    broker: Broker,
    definition: Arc<MessageServiceDefinition>,
    previous: Option<Message>,
    message_hasher: MessageHasher,
    sink: SendConnection<Message>,
    node: NodeId,
    messages: Vec<(String, SystemTime)>,
    flushing: bool,
}

impl MessageSink {
    pub fn new(
        broker: &Broker,
        definition: Arc<MessageServiceDefinition>,
        node: NodeId,
        sink: SendConnection<Message>,
    ) -> Self {
        Self {
            broker: broker.clone(),
            definition,
            previous: None,
            sink,
            node,
            messages: Vec::new(),
            flushing: false,
            message_hasher: MessageHasher::new(),
        }
    }
}

impl Sink<String> for MessageSink {
    type Error = SendMessageError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.sink.poll_ready_unpin(cx).map_err(Into::into)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.messages.push((item, SystemTime::now()));
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.flushing {
            match this.sink.poll_flush_unpin(cx) {
                std::task::Poll::Ready(result) => {
                    this.flushing = false;
                    std::task::Poll::Ready(result.map_err(Into::into))
                }
                std::task::Poll::Pending => todo!(),
            }
        } else {
            let previous = if this.previous.is_some() {
                &mut this.previous
            } else {
                let nodeid = this.node;
                &mut ready!(
                    this.definition
                        .read_transaction(
                            &this.broker,
                            move |definition,
                                  transaction|
                                  -> Result<
                                ReadTransactionResult<Message>,
                                DatabaseError<LastMessageTable>,
                            > {
                                let message = definition
                                    .with_table::<LastMessageTable>()
                                    .read_get(nodeid, &transaction)?;
                                Ok(transaction.finish(message))
                            }
                        )
                        .poll_unpin(cx)
                )?
            };

            for (message, time) in &mut this.messages {
                let _ = Message::new(
                    message,
                    previous
                        .as_mut()
                        .map(|message| message.as_hash(&mut this.message_hasher))
                        .transpose()?,
                    *time,
                    &this.broker,
                );
            }
            this.sink.poll_flush_unpin(cx).map_err(Into::into)
        }
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.sink.poll_close_unpin(cx).map_err(Into::into)
    }
}

impl ProtocolServiceSendDefinition for MessageServiceDefinition {
    type SinkItem = String;

    type Error = SendMessageError;

    async fn send_sink(
        definition: Arc<Self>,
        broker: &harmony_core::Broker,
        node: NodeId,
    ) -> Result<impl harmony_core::Sink<Self::SinkItem, Error = Self::Error>, Self::Error> {
        let connection: SendConnection<Message> =
            definition.get_send_connection(broker, node).await?;
        Ok(MessageSink::new(broker, definition, node, connection))
    }
}
