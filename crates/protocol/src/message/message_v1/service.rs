use std::{array::TryFromSliceError, sync::Arc, time::SystemTime};

use crate::message::v1::{message::Message, retransmit::Retransmit};
use blake3::{Hash, Hasher};
use harmony_core::{
    Broker, NodeId, PacketDispatcherError, ProtocolPacket, Sink, SinkExt,
    broker::SendBrokerError,
    connection::send::SendConnection,
    database::DatabaseTable,
    service::{
        ProtocolServiceDefinition, ProtocolServiceDefinitionMethods,
        send::ProtocolServiceSendDefinition,
    },
};
use redb::{Database, StorageError, TableDefinition, TableError, TransactionError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MESSAGE_TABLE: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("messages");
const LAST_MESSAGE: TableDefinition<&[u8; 32], &[u8; 32]> = TableDefinition::new("last_message");


pub struct MessagesTable;

#[derive(Debug, Error)]
pub enum MessagesTableError {}

impl DatabaseTable for MessagesTable {
    type Key = Hash;

    type Value = Message;

    const NAME: &'static str = "Messages";

    type Error = MessagesTableError;

    fn serialize_key<'s>(data: Self::Key) -> Result<&'s [u8], Self::Error> {
        todo!()
    }

    fn deserialize_key(data: &[u8]) -> Result<Self::Key, Self::Error> {
        todo!()
    }

    fn serialize_value<'s>(data: Self::Value) -> Result<&'s [u8], Self::Error> {
        todo!()
    }

    fn deserialize_value(data: &[u8]) -> Result<Self::Value, Self::Error> {
        todo!()
    }
}

pub struct MessageServiceDefinition {
    db: Arc<Database>,
}

impl MessageServiceDefinition {
    pub fn get_database(&self) -> Arc<Database> {
        Arc::clone(&self.db)
    }
}

impl ProtocolServiceDefinition for MessageServiceDefinition {
    type Protocols = (Message, Retransmit);

    type Tables = (MessagesTable,);
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
}

pub struct MessageSink {
    broker: Broker,
    definition: Arc<MessageServiceDefinition>,
    hasher: Hasher,
    hash_buffer: Vec<u8>,
    message_buffer: Vec<u8>,
    sink: SendConnection<Message>,
    node: NodeId,
}

impl MessageSink {
    pub fn new(
        broker: &Broker,
        definition: Arc<MessageServiceDefinition>,
        node: NodeId,
        sink: SendConnection<Message>,
    ) -> Self {
        let hasher = Hasher::new();
        let hash_buffer = Vec::new();
        let message_buffer = Vec::new();
        Self {
            broker: broker.clone(),
            definition,
            hasher,
            hash_buffer,
            message_buffer,
            sink,
            node,
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
        let last_message = this.broker.read_transation(|transaction| {
            
        })
        let last_message = this
            .definition
            .db
            .begin_read()?
            .open_table(LAST_MESSAGE)?
            .get(this.node.as_bytes())?
            .map(|slice| Hash::from_slice(slice.value()))
            .transpose()?;
        let message = Message::new(&item, last_message, SystemTime::now(), &this.broker);
        let message_hash = message.as_hash(&mut this.hash_buffer, &mut this.hasher)?;
        this.definition
            .db
            .begin_write()?
            .open_table(MESSAGE_TABLE)?
            .insert(
                message_hash.as_bytes(),
                message.as_bytes(&mut this.message_buffer)?.as_slice(),
            )?;
        this.message_buffer.clear();
        this.definition
            .db
            .begin_write()?
            .open_table(LAST_MESSAGE)?
            .insert(this.node.as_bytes(), message_hash.as_bytes())?;
        this.sink.start_send_unpin(message).map_err(Into::into)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.sink.poll_flush_unpin(cx).map_err(Into::into)
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
