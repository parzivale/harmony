use anyhow::Result;
use futures_util::{Sink, Stream};
use iroh::{
    Endpoint, NodeId, RelayMode, SecretKey,
    endpoint::{ConnectOptions, ConnectionError},
    protocol::{Router, RouterBuilder},
};
pub use packet::ProtocolPacket;
use protocol_handler::IrohPacketHandler;
use receive::{IncomingPackets, RecieveConnection};
use send::SendConnection;

use std::{any::TypeId, collections::BTreeMap, sync::Arc};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc::unbounded_channel};

mod dispatcher;
mod handler;
pub mod packet;
mod protocol_handler;
pub mod receive;
pub mod send;

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

    pub fn add_service<'a, Service, Si, St>(self) -> Self
    where
        Service: ProtocolService<'a, Si, St>,
    {
        <Service as ProtocolService<Si, St>>::Protocols::add_protocols(self)
    }

    fn add_protocol<T>(mut self) -> Self
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
    #[error("Cannot connect to self connecting to {0} which is the same address as this device")]
    ConnectToSelfError(NodeId),
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
        self.send_packet_sink_with_options(node, ConnectOptions::default())
            .await
    }

    pub async fn send_packet_sink_with_options<T>(
        &'a self,
        node: NodeId,
        options: ConnectOptions,
    ) -> Result<SendConnection<T>, SendBrokerError>
    where
        for<'de> T: ProtocolPacket<'de>,
    {
        if node == self.router.endpoint().node_id() {
            return Err(SendBrokerError::ConnectToSelfError(node));
        }

        let connecting = self
            .router
            .endpoint()
            .connect_with_opts(node, T::APLN.as_bytes(), options)
            .await?;

        let connection = connecting.await?;

        let send_stream = connection.open_uni().await?;

        Ok(send_stream.into())
    }

    pub async fn as_service<Service, Si, St>(&'a self) -> Service
    where
        Service: ProtocolService<'a, Si, St>,
    {
        Service::new(self).await
    }
}

pub trait ProtocolService<'a, Si, St>: Send + Sync {
    type Protocols: ProtocolCollection;

    type StreamError;
    type SinkError;
    type SinkInnerError;

    fn new(broker: &'a Broker) -> impl std::future::Future<Output = Self> + Send;

    fn broker(&self) -> &Broker;

    fn stream(&self) -> Result<impl Stream<Item = St>, Self::StreamError>;

    fn sink(
        &self,
    ) -> impl std::future::Future<
        Output = Result<impl Sink<Si, Error = Self::SinkInnerError>, Self::SinkError>,
    > + Send;
}

pub trait ProtocolServiceMethods<'a, T, Si, St, Index>: ProtocolService<'a, Si, St>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn get_receive_connection(&self) -> Result<RecieveConnection<T>, RecieveBrokerError>;

    fn get_send_connection(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<SendConnection<T>, SendBrokerError>> + Send + Sync;

    fn get_send_connection_with_options(
        &self,
        node: NodeId,
        options: ConnectOptions,
    ) -> impl Future<Output = Result<SendConnection<T>, SendBrokerError>> + Send + Sync;
}

impl<'a, Service, Si, St, T, Index> ProtocolServiceMethods<'a, T, Si, St, Index> for Service
where
    Service: ProtocolService<'a, Si, St>,
    for<'de> T: ProtocolPacket<'de> + 'static,
    Service::Protocols: GetConnection<T, Index>,
{
    fn get_receive_connection(&self) -> Result<RecieveConnection<T>, RecieveBrokerError> {
        <Self::Protocols as GetConnection<T, Index>>::get_receive_connection(self.broker())
    }

    async fn get_send_connection(
        &self,
        node: NodeId,
    ) -> Result<SendConnection<T>, SendBrokerError> {
        <Self::Protocols as GetConnection<T, Index>>::get_send_connection(self.broker(), node).await
    }

    async fn get_send_connection_with_options(
        &self,
        node: NodeId,
        options: ConnectOptions,
    ) -> Result<SendConnection<T>, SendBrokerError> {
        <Self::Protocols as GetConnection<T, Index>>::get_send_connection_with_options(
            self.broker(),
            node,
            options,
        )
        .await
    }
}

pub trait ProtocolCollection {
    fn add_protocols(builder: BrokerBuilder) -> BrokerBuilder;
}

pub struct Here;
pub struct Later<T>(std::marker::PhantomData<T>);

trait HasTypeAt<Index, T>
where
    for<'de> T: ProtocolPacket<'de>,
{
}
impl<T, Tail> HasTypeAt<Here, T> for (T, Tail) where for<'de> T: ProtocolPacket<'de> {}
impl<T, U, I, Tail> HasTypeAt<Later<I>, T> for (U, Tail)
where
    Tail: HasTypeAt<I, T>,
    for<'de> T: ProtocolPacket<'de>,
{
}

pub trait HasPacket<T, Index>
where
    for<'de> T: ProtocolPacket<'de>,
{
}

impl<T, D, Index> HasPacket<T, Index> for D
where
    for<'de> T: ProtocolPacket<'de>,
    D: AsNestedTuple,
    D::Nested: HasTypeAt<Index, T>,
{
}

trait GetConnection<T, Index>
where
    for<'de> T: ProtocolPacket<'de> + 'static,
{
    fn get_receive_connection(broker: &Broker) -> Result<RecieveConnection<T>, RecieveBrokerError> {
        broker.recieve_packet_stream::<T>()
    }

    fn get_send_connection(
        broker: &Broker,
        node: NodeId,
    ) -> impl Future<Output = Result<SendConnection<T>, SendBrokerError>> + Send + Sync {
        async move { broker.send_packet_sink(node).await }
    }

    fn get_send_connection_with_options(
        broker: &Broker,
        node: NodeId,
        options: ConnectOptions,
    ) -> impl Future<Output = Result<SendConnection<T>, SendBrokerError>> + Send + Sync {
        async move { broker.send_packet_sink_with_options(node, options).await }
    }
}

impl<T, D, Index> GetConnection<T, Index> for D
where
    for<'de> T: ProtocolPacket<'de> + 'static,
    D: HasPacket<T, Index>,
{
}

pub trait AsNestedTuple {
    type Nested;
}

macro_rules! nested_tuple {
    () => {
        ()
    };
    ($head:ty $(, $tail:ty)*) => {
        ($head, nested_tuple!($($tail),*))
    };
}

macro_rules! impl_as_nested_tuple_inner {
    ($($types:ident),+) => {
        impl<$($types),+> AsNestedTuple for ($($types,)+) {
            type Nested = nested_tuple!($($types),*);
        }
    };
}

macro_rules! impl_as_nested_tuple {
    ($head:ident $(, $tail:ident)* $(,)?) => {
        impl_as_nested_tuple!(@impl $head $(, $tail)*);
        impl_as_nested_tuple!($($tail),*);
    };
    () => {
    };

    (@impl $($name:ident),+) => {
        impl_as_nested_tuple_inner!($($name),+);
     };
}

impl_as_nested_tuple!(T, D, F, G, H, J, K, L, Z, X, C, V, B, N, M, Q);

impl<T> ProtocolCollection for T
where
    for<'de> T: ProtocolPacket<'de> + 'static,
{
    fn add_protocols(builder: BrokerBuilder) -> BrokerBuilder {
        builder.add_protocol::<T>()
    }
}

macro_rules! impl_protocol_collection {
    ($head:ident $(, $tail:ident)* $(,)?) => {
        impl_protocol_collection!(@impl $head $(, $tail)*);
        impl_protocol_collection!($($tail),*);
    };
    () => {};

    (@impl $($name:ident),+) => {
        impl<$($name),+> ProtocolCollection for ($($name),+,)
        where
            $(
                for<'de> $name: ProtocolPacket<'de> + 'static,
            )+
        {
            fn add_protocols(builder: BrokerBuilder) -> BrokerBuilder {
                builder $(.add_protocol::<$name>())+
            }
        }
    };
}

impl_protocol_collection!(T, D, F, G, H, J, K, L, Z, X, C, V, B, N, M, Q);
