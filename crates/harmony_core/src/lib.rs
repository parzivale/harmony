use anyhow::Result;
use futures_util::{Sink, Stream, StreamExt, future::BoxFuture, ready};
use iroh::{
    Endpoint, NodeId, RelayMode, SecretKey,
    endpoint::{
        ClosedStream, Connection, ConnectionError, ReadError, RecvStream, SendStream, WriteError,
    },
    protocol::{ProtocolHandler, Router, RouterBuilder},
};
use serde::{Deserialize, Serialize};
use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    io::{self},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::{
    io::AsyncReadExt,
    sync::{
        Mutex, MutexGuard,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
};

// unpin needed for future::stream impl
// debug needed for iroh protocolHandler
pub trait ProtocolPacket<'de>:
    Serialize + Deserialize<'de> + Send + Sync + Unpin + std::fmt::Debug
{
    const APLN: &'static str;

    fn take_from_bytes(buf: &'de [u8]) -> postcard::Result<(Self, &'de [u8])> {
        postcard::take_from_bytes(buf)
    }

    fn into_bytes(self, buf: &mut Vec<u8>) -> postcard::Result<&mut Vec<u8>> {
        postcard::to_io(&self, buf)
    }
}

pub struct PacketDispatcher<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    send_stream: SendStream,
    buffer: Vec<u8>,
    written: usize,
    flushing: bool,
    _phantom: PhantomData<T>,
}

impl<T> From<SendStream> for PacketDispatcher<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: SendStream) -> Self {
        Self {
            send_stream: value,
            buffer: Vec::new(),
            written: 0,
            flushing: false,
            _phantom: PhantomData,
        }
    }
}

#[derive(Error, Debug)]
pub enum PacketDispatcherError {
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    WriteError(#[from] WriteError),
    #[error(transparent)]
    StreamClosed(#[from] ClosedStream),
}

impl<T> Sink<T> for PacketDispatcher<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    type Error = PacketDispatcherError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.flushing {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.into_bytes(&mut this.buffer)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let total_len = this.buffer.len();
        this.flushing = true;
        while this.written < total_len {
            let n = ready!(SendStream::poll_write(
                Pin::new(&mut this.send_stream),
                cx,
                &this.buffer[this.written..],
            )?);
            this.written += n;
        }
        this.flushing = false;
        this.buffer.clear();
        this.written = 0;
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        ready!(Sink::poll_flush(Pin::new(this), cx))?;
        this.send_stream.finish()?;
        Poll::Ready(Ok(()))
    }
}

pub struct PacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    recv_stream: RecvStream,
    buffer: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<T> PacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn take_from_bytes(&mut self) -> Result<NextPacketStatus<T>, postcard::Error> {
        match T::take_from_bytes(&self.buffer) {
            Ok((packet, remaining)) => {
                let used = self.buffer.len() - remaining.len();
                self.buffer.drain(..used);
                Ok(NextPacketStatus::Packet(packet))
            }
            Err(postcard::Error::DeserializeUnexpectedEnd) => {
                Ok(NextPacketStatus::BytesRemaining(self.buffer.len()))
            }
            Err(err) => Err(err),
        }
    }
}

impl<T> Stream for PacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    type Item = Result<T, PacketHandlerError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if !this.buffer.is_empty() {
            return match this.take_from_bytes() {
                Ok(NextPacketStatus::Packet(packet)) => Poll::Ready(Some(Ok(packet))),
                Ok(NextPacketStatus::BytesRemaining(_)) => Poll::Pending,
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            };
        }
        let read_fut = this.recv_stream.read_buf(&mut this.buffer);
        tokio::pin!(read_fut);

        match ready!(read_fut.as_mut().poll(cx)) {
            Ok(0) => Poll::Ready(None),
            Ok(_) => match this.take_from_bytes() {
                Ok(NextPacketStatus::Packet(packet)) => Poll::Ready(Some(Ok(packet))),
                Ok(NextPacketStatus::BytesRemaining(_)) => Poll::Pending,
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            },
            Err(err) => match err.downcast::<ReadError>() {
                Ok(err) => match err {
                    ReadError::ConnectionLost(ConnectionError::ApplicationClosed(_)) => {
                        Poll::Ready(None)
                    }
                    err => Poll::Ready(Some(Err(err.into()))),
                },
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            },
        }
    }
}

impl<T> From<RecvStream> for PacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: RecvStream) -> Self {
        Self {
            recv_stream: value,
            buffer: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

pub enum NextPacketStatus<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    Packet(T),
    BytesRemaining(usize),
}

#[derive(Error, Debug)]
pub enum PacketHandlerError {
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    ReadError(#[from] ReadError),
    #[error(transparent)]
    IoError(#[from] io::Error),
}

pub struct Packet<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    data: T,
    from: NodeId,
}

impl<T> Packet<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    pub fn from_node(&self) -> NodeId {
        self.from
    }

    pub fn data(self) -> T {
        self.data
    }
}

impl<T> From<(T, NodeId)> for Packet<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: (T, NodeId)) -> Self {
        Self {
            data: value.0,
            from: value.1,
        }
    }
}

#[derive(Debug)]
struct IrohPacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    sender: UnboundedSender<(NodeId, Box<dyn Any + Send + Sync>)>,
    _phantom: PhantomData<T>,
}

impl<T> IrohPacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    pub fn new(sender: UnboundedSender<(NodeId, Box<dyn Any + Send + Sync>)>) -> Self {
        Self {
            sender,
            _phantom: PhantomData,
        }
    }
}

impl<T> ProtocolHandler for IrohPacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de> + 'static,
{
    fn accept(&self, connection: Connection) -> BoxFuture<'static, Result<()>> {
        let cloned_sender = self.sender.clone();
        Box::pin(async move {
            let recv = connection.accept_uni().await;
            let recv = recv?;
            let from = connection
                .remote_node_id()
                .expect("Remote node should have an ID");

            let mut handler: PacketHandler<T> = recv.into();
            while let Some(packet) = handler.next().await {
                cloned_sender.send((from, Box::new(packet)))?;
            }
            Ok(())
        })
    }
}

pub struct RecieveConnection<'a, T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    connections: MutexGuard<'a, IncomingPackets>,
    _phantom: PhantomData<T>,
}

impl<'a, T> From<MutexGuard<'a, IncomingPackets>> for RecieveConnection<'a, T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: MutexGuard<'a, IncomingPackets>) -> RecieveConnection<'a, T> {
        Self {
            connections: value,
            _phantom: PhantomData,
        }
    }
}

impl<T> Stream for RecieveConnection<'_, T>
where
    for<'de> T: ProtocolPacket<'de> + 'static,
{
    type Item = Result<Packet<T>, PacketHandlerError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some((from, recv)) = ready!(this.connections.poll_recv(cx)) {
            let packet: Result<T, PacketHandlerError> = *recv.downcast().expect("T should match generic parameter, this means poll_next was called with non T parameter, this should be impossible");
            let packet = packet.map(|data| Packet::from((data, from)));
            Poll::Ready(Some(packet))
        } else {
            Poll::Ready(None)
        }
    }
}

pub struct SendConnection<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    dispatcher: PacketDispatcher<T>,
    _phantom: PhantomData<T>,
}

impl<T> From<SendStream> for SendConnection<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: SendStream) -> Self {
        Self {
            dispatcher: value.into(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Sink<T> for SendConnection<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    type Error = PacketDispatcherError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.dispatcher).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> std::result::Result<(), Self::Error> {
        let this = self.get_mut();
        Pin::new(&mut this.dispatcher).start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.dispatcher).poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        Pin::new(&mut this.dispatcher).poll_close(cx)
    }
}

type IncomingPackets = UnboundedReceiver<(NodeId, Box<dyn Any + Send + Sync>)>;

pub struct BrokerBuilder {
    alpns: Vec<&'static str>,
    handlers: BTreeMap<TypeId, Mutex<IncomingPackets>>,
    builder: RouterBuilder,
}

impl BrokerBuilder {
    pub async fn new(key: SecretKey) -> Result<Self> {
        let endpoint = Endpoint::builder()
            .secret_key(key)
            .discovery_n0()
            .discovery_local_network()
            .relay_mode(RelayMode::Default)
            .bind()
            .await?;
        Ok(Self {
            alpns: Vec::new(),
            handlers: BTreeMap::new(),
            builder: Router::builder(endpoint),
        })
    }

    pub fn add_protocol<T>(mut self) -> Self
    where
        for<'de> T: ProtocolPacket<'de> + 'static,
    {
        if self.alpns.contains(&T::APLN) {
            panic!("Cannot add APLN {:?} more than once", T::APLN);
        }

        let (sender, connections) = unbounded_channel();

        let alpns = self.alpns;
        let builder = self
            .builder
            .accept(T::APLN, IrohPacketHandler::<T>::new(sender));
        self.handlers
            .insert(TypeId::of::<T>(), Mutex::new(connections));

        let handlers = self.handlers;
        Self {
            alpns,
            builder,
            handlers,
        }
    }

    pub async fn build(self) -> Result<Broker> {
        Ok(Broker {
            alpns: Arc::new(self.alpns),
            router: Arc::new(self.builder.spawn().await?),
            handlers: Arc::new(self.handlers),
        })
    }
}

pub struct Broker {
    alpns: Arc<Vec<&'static str>>,
    router: Arc<Router>,
    handlers: Arc<BTreeMap<TypeId, Mutex<IncomingPackets>>>,
}

impl Clone for Broker {
    fn clone(&self) -> Self {
        Self {
            alpns: Arc::clone(&self.alpns),
            router: Arc::clone(&self.router),
            handlers: Arc::clone(&self.handlers),
        }
    }
}

#[derive(Debug, Error)]
pub enum RecieveBrokerError {
    #[error("Protocol for {0} not found")]
    ProtocolNotFound(String),
    #[error("Lock for receive connection for protocol {0} currently in use")]
    LockNotAvailable(String),
}
#[derive(Debug, Error)]
pub enum SendBrokerError {
    #[error(transparent)]
    IrohError(#[from] anyhow::Error),
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
}

impl<'a> Broker {
    pub fn recieve_packet_stream<T>(
        &'a self,
    ) -> Result<RecieveConnection<'a, T>, RecieveBrokerError>
    where
        for<'de> T: ProtocolPacket<'de> + 'static,
    {
        Ok(RecieveConnection::<T>::from(
            self.handlers
                .get(&TypeId::of::<T>())
                .ok_or(RecieveBrokerError::ProtocolNotFound(T::APLN.to_string()))?
                .try_lock()
                .map_err(|_| RecieveBrokerError::LockNotAvailable(T::APLN.to_string()))?,
        ))
    }

    pub async fn send_packet_sink<T>(
        &'a self,
        node: NodeId,
    ) -> Result<SendConnection<T>, SendBrokerError>
    where
        for<'de> T: ProtocolPacket<'de>,
    {
        let connection = self
            .router
            .endpoint()
            .connect(node, T::APLN.as_bytes())
            .await?;
        let send_stream = connection.open_uni().await?;

        Ok(send_stream.into())
    }
}
