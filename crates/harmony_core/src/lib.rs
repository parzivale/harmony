use std::sync::Arc;

pub use builder::{Broker, BrokerBuilder};
use builder::{RecieveBrokerError, SendBrokerError};
pub use futures_util::{Sink, SinkExt, Stream, StreamExt};
pub use iroh::{NodeId, endpoint::ConnectOptions};
pub use packet::ProtocolPacket;
use receive::RecieveConnection;
use send::SendConnection;

use protocol_collection::{GetConnection, HasTypeAt, ProtocolCollection};
use tokio::pin;

pub mod builder;
mod dispatcher;
mod handler;
pub mod packet;
mod protocol_collection;
mod protocol_handler;
pub mod receive;
pub mod send;

pub struct ProtocolService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceDefinition,
{
    definition: Arc<ServiceDefinition>,
    broker: Broker,
}

impl<ServiceDefinition> Clone for ProtocolService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceDefinition,
{
    fn clone(&self) -> Self {
        Self {
            definition: Arc::clone(&self.definition),
            broker: self.broker.clone(),
        }
    }
}

pub struct ProtocolSendService<ServiceSendDefinition>
where
    ServiceSendDefinition: ProtocolServiceSendDefinition,
{
    definition: Arc<ServiceSendDefinition>,
    broker: Broker,
}

impl<ServiceSendDefinition> Clone for ProtocolSendService<ServiceSendDefinition>
where
    ServiceSendDefinition: ProtocolServiceSendDefinition,
{
    fn clone(&self) -> Self {
        Self {
            definition: Arc::clone(&self.definition),
            broker: self.broker.clone(),
        }
    }
}

pub struct ProtocolRecieveService<ServiceRecieveDefinition>
where
    ServiceRecieveDefinition: ProtocolServiceReceiveDefinition,
{
    definition: Arc<ServiceRecieveDefinition>,
    broker: Broker,
}

impl<ServiceDefinition> ProtocolService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceDefinition,
{
    pub fn new<Services, Index>(broker: &Broker<Services>, definition: ServiceDefinition) -> Self
    where
        Services: HasTypeAt<Index, ServiceDefinition>,
    {
        Self {
            definition: Arc::new(definition),
            broker: broker.clone().clear_type(),
        }
    }
}

impl<ServiceDefinition> ProtocolService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceDefinition + ProtocolServiceSendDefinition,
{
    pub fn send_service(self) -> ProtocolSendService<ServiceDefinition> {
        ProtocolSendService {
            definition: self.definition.clone(),
            broker: self.broker.clone(),
        }
    }
}

impl<ServiceDefinition> ProtocolService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceDefinition + ProtocolServiceReceiveDefinition,
{
    pub fn receieve_service(self) -> ProtocolRecieveService<ServiceDefinition> {
        ProtocolRecieveService {
            definition: self.definition,
            broker: self.broker,
        }
    }
}

impl<ServiceDefinition> ProtocolService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceDefinition
        + ProtocolServiceReceiveDefinition
        + ProtocolServiceSendDefinition,
{
    pub fn service_channels(
        self,
    ) -> (
        ProtocolSendService<ServiceDefinition>,
        ProtocolRecieveService<ServiceDefinition>,
    ) {
        (
            ProtocolSendService {
                definition: self.definition.clone(),
                broker: self.broker.clone(),
            },
            ProtocolRecieveService {
                definition: self.definition,
                broker: self.broker,
            },
        )
    }
}

impl<T> ProtocolServiceDefinition for Arc<T>
where
    T: ProtocolServiceDefinition,
{
    type Protocols = T::Protocols;
}

impl<ServiceDefinition> ProtocolSendService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceSendDefinition,
{
    pub async fn send(
        &self,
        item: ServiceDefinition::SinkItem,
        node: NodeId,
    ) -> Result<(), ServiceDefinition::Error> {
        let fut = self.definition.send_sink(&self.broker, node).await?;
        pin!(fut);
        fut.send(item).await
    }

    pub async fn send_sink(
        &self,
        node: NodeId,
    ) -> Result<impl Sink<ServiceDefinition::SinkItem>, ServiceDefinition::Error> {
        self.definition.send_sink(&self.broker, node).await
    }
}
impl<ServiceDefinition> ProtocolRecieveService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceReceiveDefinition,
{
    pub fn receieve_stream(
        &self,
    ) -> Result<impl Stream<Item = ServiceDefinition::StreamItem>, ServiceDefinition::Error> {
        self.definition.recv_stream(&self.broker)
    }
}

pub trait ProtocolServiceDefinition: Send + Sync {
    type Protocols: ProtocolCollection;
}

pub trait ProtocolServiceDefinitionMethods<T, Index>: ProtocolServiceDefinition
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn get_receive_connection<'a>(
        &self,
        broker: &'a Broker,
    ) -> Result<RecieveConnection<'a, T>, RecieveBrokerError>;

    fn get_send_connection(
        &self,
        broker: &Broker,
        node: NodeId,
    ) -> impl Future<Output = Result<SendConnection<T>, SendBrokerError>> + Send + Sync;

    fn get_send_connection_with_options(
        &self,
        broker: &Broker,
        node: NodeId,
        options: ConnectOptions,
    ) -> impl Future<Output = Result<SendConnection<T>, SendBrokerError>> + Send + Sync;
}

impl<Service, T, Index> ProtocolServiceDefinitionMethods<T, Index> for Service
where
    Service: ProtocolServiceDefinition,
    for<'de> T: ProtocolPacket<'de> + 'static,
    Service::Protocols: GetConnection<T, Index>,
{
    fn get_receive_connection<'a>(
        &self,
        broker: &'a Broker,
    ) -> Result<RecieveConnection<'a, T>, RecieveBrokerError> {
        <Self::Protocols as GetConnection<T, Index>>::get_receive_connection(broker)
    }

    async fn get_send_connection(
        &self,
        broker: &Broker,
        node: NodeId,
    ) -> Result<SendConnection<T>, SendBrokerError> {
        <Self::Protocols as GetConnection<T, Index>>::get_send_connection(broker, node).await
    }

    async fn get_send_connection_with_options(
        &self,
        broker: &Broker,
        node: NodeId,
        options: ConnectOptions,
    ) -> Result<SendConnection<T>, SendBrokerError> {
        <Self::Protocols as GetConnection<T, Index>>::get_send_connection_with_options(
            broker, node, options,
        )
        .await
    }
}

pub trait ProtocolServiceSendDefinition: Send + Sync {
    type SinkItem;
    type Error;

    fn send_sink(
        &self,
        broker: &Broker,
        node: NodeId,
    ) -> impl std::future::Future<
        Output = Result<impl Sink<Self::SinkItem, Error = Self::Error>, Self::Error>,
    > + Send;
}

pub trait ProtocolServiceReceiveDefinition: Send + Sync {
    type StreamItem;
    type Error;
    fn recv_stream(
        &self,
        broker: &Broker,
    ) -> Result<impl Stream<Item = Self::StreamItem>, Self::Error>;
}
