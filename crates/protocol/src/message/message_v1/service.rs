use std::{
    array::TryFromSliceError,
    collections::{BTreeMap, VecDeque, btree_map::Entry},
    pin::Pin,
    sync::Arc,
    time::SystemTime,
};

use crate::message::v1::{message::Message, retransmit::Retransmit};
use blake3::Hash;
use harmony_core::{
    Broker, NodeId, PacketDispatcherError, Sink, SinkExt, Stream, StreamExt,
    broker::{RecieveBrokerError, SendBrokerError},
    connection::{receive::RecieveConnection, send::SendConnection},
    database::{
        DatabaseTable,
        transaction::{ReadTransactionResult, WriteTransactionResult},
    },
    ready,
    service::{
        DatabaseManagerMethods, DatabaseTableDefinitionMethods, ProtocolServiceDefinition,
        ProtocolServiceDefinitionMethods, TransactionFutureError,
        recieve::ProtocolServiceReceiveDefinition, send::ProtocolServiceSendDefinition,
    },
};
use redb::{StorageError, TableError, TransactionError};
use thiserror::Error;
use tokio::{
    sync::{
        Mutex,
        mpsc::{UnboundedSender, error::SendError, unbounded_channel},
    },
    task::JoinHandle,
};

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

    const NAME: &'static str = "LastMessage";

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

pub struct MessageService {
    send: UnboundedSender<(NodeId, Vec<(Hash, Message)>, Hash)>,
    send_handle: JoinHandle<()>,
    previous_map: Mutex<BTreeMap<NodeId, Option<Hash>>>,
}

impl MessageService {
    pub fn new<Services: Send + 'static>(broker: &Broker<Services>) -> MessageService {
        let (send, mut recv) = unbounded_channel::<(NodeId, Vec<(Hash, Message)>, Hash)>();
        let cloned_broker = broker.clone();
        let send_handle = tokio::task::spawn(async move {
            while let Some((node, mut messages, prev)) = recv.recv().await {
                Self::write_transaction(
                    &cloned_broker,
                    move |manager, tx| -> Result<WriteTransactionResult<()>, TransactionFutureError> {
                        manager
                            .with_table::<LastMessageTable>()
                            .insert(node, prev, &tx)?;

                        while let Some((hash, message)) = messages.pop() {
                            manager
                                .with_table::<MessagesTable>()
                                .insert(hash, message, &tx)?;
                        }
                        Ok(tx.finish(()))
                    },
                );
            }
        });
        Self {
            send,
            send_handle,
            previous_map: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn fetch_latest_message_hash(
        broker: &Broker,
        node: NodeId,
    ) -> Result<Option<Hash>, TransactionFutureError> {
        match MessageService::read_transaction(
            broker,
            move |manager, tx| -> Result<ReadTransactionResult<Hash>, TransactionFutureError> {
                let hash = manager
                    .with_table::<LastMessageTable>()
                    .read_get(node, &tx)?;
                Ok(tx.finish(hash))
            },
        )
        .await
        {
            Ok(res) => Ok(res),
            Err(TransactionFutureError::TableError(TableError::TableDoesNotExist(_))) => {
                MessageService::write_transaction(broker, move |manager, tx|  -> Result<WriteTransactionResult<()>, TransactionFutureError>{
                    manager.with_table::<LastMessageTable>().create_table(&tx)?;
                    Ok(tx.finish(()))
                })
                .await?;

                MessageService::read_transaction(
                    broker,
                    move |manager, tx| -> Result<ReadTransactionResult<Hash>, TransactionFutureError> {
                    let hash = manager
                        .with_table::<LastMessageTable>()
                        .read_get(node, &tx)?;
                    Ok(tx.finish(hash))
                }).await
            }
            Err(err) => Err(err),
        }
    }
}

impl ProtocolServiceDefinition for MessageService {
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
    #[error(transparent)]
    SendError(#[from] SendError<(NodeId, Vec<(Hash, Message)>, Hash)>),
}

pub type MessageSinkFuture<T> =
    Option<Pin<Box<(dyn Future<Output = Result<T, TransactionFutureError>> + Send)>>>;

struct MessageSink {
    broker: Broker,
    definition: Arc<MessageService>,
    message_hasher: MessageHasher,
    sink: SendConnection<Message>,
    node: NodeId,
    unprocessed_messages: VecDeque<(String, SystemTime)>,
    processed_messages: Vec<(Hash, Message)>,
}

impl MessageSink {
    pub fn new(
        broker: &Broker,
        definition: Arc<MessageService>,
        node: NodeId,
        sink: SendConnection<Message>,
    ) -> Self {
        Self {
            broker: broker.clone(),
            definition,
            sink,
            node,
            unprocessed_messages: VecDeque::new(),
            processed_messages: Vec::new(),
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
        this.unprocessed_messages
            .push_back((item, SystemTime::now()));
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        ready!(this.sink.poll_flush_unpin(cx)?);
        let mut map = match this.definition.previous_map.try_lock() {
            Ok(ok) => ok,
            Err(_) => return std::task::Poll::Pending,
        };

        if let Some(current_prev) = map.get_mut(&this.node) {
            while let Some((message_text, time)) = this.unprocessed_messages.front() {
                let signature = this.broker.sign(message_text.as_bytes());
                let message = Message::new(message_text, *current_prev, *time, signature);

                // these will be moved/copied into the futures
                let message_hash = message.as_hash(&mut this.message_hasher)?;
                this.sink.start_send_unpin(message.clone())?;
                this.processed_messages.push((message_hash, message));

                *current_prev = Some(message_hash);
                this.unprocessed_messages.pop_front();
            }
            if let Some(prev) = current_prev {
                this.definition.send.send((
                    this.node,
                    this.processed_messages.drain(..).collect(),
                    *prev,
                ))?;
                ready!(this.sink.poll_flush_unpin(cx)?);
                std::task::Poll::Ready(Ok(()))
            } else {
                std::task::Poll::Ready(Ok(()))
            }
        } else {
            todo!();
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

impl ProtocolServiceSendDefinition for MessageService {
    type SinkItem = String;

    type Error = SendMessageError;

    async fn send_sink(
        definition: Arc<Self>,
        broker: &harmony_core::Broker,
        node: NodeId,
    ) -> Result<impl harmony_core::Sink<Self::SinkItem, Error = Self::Error>, Self::Error> {
        let connection: SendConnection<Message> =
            definition.get_send_connection(broker, node).await?;

        if let Entry::Vacant(e) = definition.previous_map.lock().await.entry(node) {
            e.insert(MessageService::fetch_latest_message_hash(broker, node).await?);
        }

        Ok(MessageSink::new(broker, definition, node, connection))
    }
}

pub struct MessageStream<'a> {
    broker: Broker,
    definition: Arc<MessageService>,
    stream: RecieveConnection<'a, Message>,
}

impl<'a> MessageStream<'a> {
    pub fn new(
        broker: &Broker,
        definition: Arc<MessageService>,
        stream: RecieveConnection<'a, Message>,
    ) -> Self {
        Self {
            broker: broker.clone(),
            definition: Arc::clone(&definition),
            stream,
        }
    }
}

#[derive(Debug)]
pub struct MessageWithNode {
    message: Message,
    from: NodeId,
}

#[derive(Debug, Error)]
pub enum RecieveMessageError {
    #[error(transparent)]
    RecieveBrokerError(#[from] RecieveBrokerError),
}

impl<'a> Stream for MessageStream<'a> {
    type Item = MessageWithNode;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let packet = match ready!(this.stream.poll_next_unpin(cx)) {
            Some(Ok(message)) => message,
            _ => return std::task::Poll::Ready(None),
        };

        let from = packet.from_node();
        let data = packet.data();

        std::task::Poll::Ready(Some(MessageWithNode {
            message: data,
            from,
        }))
    }
}

impl ProtocolServiceReceiveDefinition for MessageService {
    type StreamItem = MessageWithNode;

    type Error = RecieveMessageError;

    fn recv_stream(
        definition: Arc<Self>,
        broker: &Broker,
    ) -> Result<impl Stream<Item = Self::StreamItem>, Self::Error> {
        let connection: RecieveConnection<'_, Message> = broker.recieve_packet_stream()?;

        Ok(MessageStream::new(broker, definition, connection))
    }
}
