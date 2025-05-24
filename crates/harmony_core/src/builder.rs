use std::{any::TypeId, collections::BTreeMap, marker::PhantomData, sync::Arc};

use iroh::{
    Endpoint, NodeId, RelayMode, SecretKey,
    endpoint::{ConnectOptions, ConnectionError},
    protocol::{Router, RouterBuilder},
};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc::unbounded_channel};

use crate::{
    ProtocolCollection, ProtocolPacket, ProtocolServiceDefinition,
    protocol_handler::IrohPacketHandler,
    receive::{IncomingPackets, RecieveConnection},
    send::SendConnection,
};

pub struct BrokerBuilder<Services = ()> {
    alpns: Vec<&'static str>,
    handlers: BTreeMap<TypeId, Mutex<IncomingPackets>>,
    builder: RouterBuilder,
    services: PhantomData<Services>,
}

impl BrokerBuilder {
    pub async fn new(key: SecretKey) -> anyhow::Result<Self> {
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
            services: PhantomData,
        })
    }
}
impl<Services> BrokerBuilder<Services> {
    pub fn add_service<Service>(self) -> BrokerBuilder<(Service, Services)>
    where
        Service: ProtocolServiceDefinition,
    {
        let this: BrokerBuilder<(Service, Services)> = BrokerBuilder {
            alpns: self.alpns,
            handlers: self.handlers,
            builder: self.builder,
            services: PhantomData,
        };
        <Service as ProtocolServiceDefinition>::Protocols::add_protocols(this)
    }

    pub fn add_protocol<T>(mut self) -> BrokerBuilder<Services>
    where
        for<'de> T: ProtocolPacket<'de> + 'static,
    {
        if self.handlers.contains_key(&TypeId::of::<T>()) {
            return self;
        }

        if self.alpns.contains(&T::APLN) {
            panic!("Cannot add APLN {:?} more than once", T::APLN);
        }

        let (sender, connections) = unbounded_channel();

        let builder = self
            .builder
            .accept(T::APLN, IrohPacketHandler::<T>::new(sender));
        self.handlers
            .insert(TypeId::of::<T>(), Mutex::new(connections));

        BrokerBuilder {
            alpns: self.alpns,
            handlers: self.handlers,
            builder,
            services: PhantomData,
        }
    }

    pub async fn build(self) -> anyhow::Result<Broker<Services>> {
        Ok(Broker {
            alpns: Arc::new(self.alpns),
            router: Arc::new(self.builder.spawn().await?),
            handlers: Arc::new(self.handlers),
            protocols: PhantomData,
        })
    }
}
pub struct Broker<Protocols = ()> {
    alpns: Arc<Vec<&'static str>>,
    router: Arc<Router>,
    handlers: Arc<BTreeMap<TypeId, Mutex<IncomingPackets>>>,
    protocols: PhantomData<Protocols>,
}
impl<Services> Clone for Broker<Services> {
    fn clone(&self) -> Self {
        Self {
            alpns: Arc::clone(&self.alpns),
            router: Arc::clone(&self.router),
            handlers: Arc::clone(&self.handlers),
            protocols: PhantomData,
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
impl<'a, Services> Broker<Services> {
    pub fn clear_type(self) -> Broker {
        Broker {
            alpns: self.alpns,
            router: self.router,
            handlers: self.handlers,
            protocols: PhantomData,
        }
    }

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
}
