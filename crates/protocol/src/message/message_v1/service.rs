use std::{
    any::Any,
    array::TryFromSliceError,
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use crate::message::v1::{message::Message, retransmit::Retransmit};
use blake3::Hash;
use harmony_core::{
    Broker, FutureExt, NodeId, PacketDispatcherError, Sink, SinkExt, Stream,
    broker::SendBrokerError,
    connection::send::SendConnection,
    database::{
        DatabaseTable,
        transaction::{ReadTransactionResult, WriteTransactionResult},
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
pub enum MessagesTableError {
    #[error(transparent)]
    Postcard(#[from] postcard::Error),
}

impl DatabaseTable for MessagesTable {
    type Key = Hash;

    type Value = Message;

    const NAME: &'static str = "Messages";

    type Error = MessagesTableError;

    fn serialize_key(key: Self::Key) -> Result<Vec<u8>, Self::Error> {
        postcard::to_stdvec(&key).map_err(Into::into)
    }

    fn deserialize_key(bytes: &[u8]) -> Result<Self::Key, Self::Error> {
        postcard::from_bytes(bytes).map_err(Into::into)
    }

    fn serialize_value(key: Self::Value) -> Result<Vec<u8>, Self::Error> {
        postcard::to_stdvec(&key).map_err(Into::into)
    }

    fn deserialize_value(bytes: &[u8]) -> Result<Self::Value, Self::Error> {
        postcard::from_bytes(bytes).map_err(Into::into)
    }
}

#[derive(Debug, Error)]
pub enum LastMessageTableError {}

impl DatabaseTable for LastMessageTable {
    type Key = NodeId;

    type Value = Hash;

    const NAME: &'static str = "Messages";

    type Error = MessagesTableError;

    fn serialize_key(key: Self::Key) -> Result<Vec<u8>, Self::Error> {
        postcard::to_stdvec(&key).map_err(Into::into)
    }

    fn deserialize_key(bytes: &[u8]) -> Result<Self::Key, Self::Error> {
        postcard::from_bytes(bytes).map_err(Into::into)
    }

    fn serialize_value(key: Self::Value) -> Result<Vec<u8>, Self::Error> {
        postcard::to_stdvec(&key).map_err(Into::into)
    }

    fn deserialize_value(bytes: &[u8]) -> Result<Self::Value, Self::Error> {
        postcard::from_bytes(bytes).map_err(Into::into)
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

pub type MessageSinkFuture<T> =
    Option<Pin<Box<(dyn Future<Output = Result<T, TransactionFutureError>> + Send)>>>;

pub struct MessageSink {
    broker: Broker,
    definition: Arc<MessageServiceDefinition>,
    previous: Option<Hash>,
    message_hasher: MessageHasher,
    sink: SendConnection<Message>,
    node: NodeId,
    unprocessed_messages: VecDeque<(String, SystemTime)>,
    processed_messages: Arc<Mutex<Vec<(Message, Hash)>>>,
    flushing: bool,
    new_last_message_fut: MessageSinkFuture<()>,
}

impl MessageSink {
    pub fn new(
        broker: &Broker,
        definition: Arc<MessageServiceDefinition>,
        node: NodeId,
        sink: SendConnection<Message>,
        previous: Option<Hash>,
    ) -> Self {
        Self {
            broker: broker.clone(),
            definition,
            previous,
            sink,
            node,
            unprocessed_messages: VecDeque::new(),
            processed_messages: Arc::new(Mutex::new(Vec::new())),
            flushing: false,
            message_hasher: MessageHasher::new(),
            new_last_message_fut: None,
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
        if this.flushing {
            return std::task::Poll::Pending;
        }
        this.sink.poll_ready_unpin(cx).map_err(Into::into)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.unprocessed_messages
            .push_back((item, SystemTime::now()));
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        // if the sink is still flushing we shouldn't
        // push more messages through
        ready!(this.sink.poll_ready_unpin(cx))?;
        let mut processed_messages = match this.processed_messages.try_lock() {
            Ok(queue) => queue,
            Err(std::sync::TryLockError::WouldBlock) => return std::task::Poll::Pending,
            Err(e) => panic!("{}", e),
        };
        this.flushing = true;

        // needs to be last as we use .pop to remove
        // elements as its o(1) time
        while let Some((message_text, time)) = this.unprocessed_messages.front() {
            let signature = this.broker.sign(message_text.as_bytes());
            let message = Message::new(message_text, this.previous, *time, signature);

            // these will be moved/copied into the futures
            let message_hash = message.as_hash(&mut this.message_hasher)?;

            this.sink.start_send_unpin(message.clone())?;
            processed_messages.push((message, message_hash));

            this.previous = Some(message_hash);
            this.unprocessed_messages.pop_front();
        }
        if let Some(prev) = this.previous {
            let poll = this
                .new_last_message_fut
                .get_or_insert_with(|| {
                    let messages = Arc::clone(&this.processed_messages);
                    let node = this.node;
                    this.definition
                            .write_transaction(
                                &this.broker,
                                move |manager,
                                      tx|
                                      -> Result<
                                    WriteTransactionResult<()>,
                                    TransactionFutureError,
                                > {
                                    manager
                                        .with_table::<LastMessageTable>()
                                        .insert(node, prev, &tx)?;

                                    while let Some((message, hash)) = messages.lock().unwrap().pop()
                                    {
                                        manager
                                            .with_table::<MessagesTable>()
                                            .insert(hash, message, &tx)?;
                                    }
                                    Ok(tx.finish(()))
                                },
                            )
                            .boxed()
                })
                .poll_unpin(cx);

            ready!(poll)?;
            this.new_last_message_fut = None;

            ready!(this.sink.flush().poll_unpin(cx))?;
            this.flushing = false;
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        ready!(this.poll_flush_unpin(cx))?;
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

        let last_message = definition
            .read_transaction(
                broker,
                move |manager, tx| -> Result<ReadTransactionResult<Hash>, TransactionFutureError> {
                    let msg = manager
                        .with_table::<LastMessageTable>()
                        .read_get(node, &tx)?;
                    Ok(tx.finish(msg))
                },
            )
            .await?;

        Ok(MessageSink::new(
            broker,
            definition,
            node,
            connection,
            last_message,
        ))
    }
}

pub struct MessageStream {
    broker: Broker,
    definition: Arc<MessageServiceDefinition>,
}

impl MessageStream {
    pub fn new(broker: &Broker, definition: Arc<MessageServiceDefinition>) -> Self {
        Self {
            broker: broker.clone(),
            definition: Arc::clone(&definition),
        }
    }
}

impl Stream for MessageStream {
    type Item = Message;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}
