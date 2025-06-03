use std::sync::Arc;

use iroh::{NodeId, endpoint::ConnectOptions};
use recieve::{ProtocolRecieveService, ProtocolServiceReceiveDefinition};
use send::{ProtocolSendService, ProtocolServiceSendDefinition};

use crate::{
    Broker, DatabaseTableCollection, ProtocolPacket,
    broker::{RecieveBrokerError, SendBrokerError},
    connection::{receive::RecieveConnection, send::SendConnection},
    database::{
        DatabaseTable,
        table_collection::{DatabaseError, TableMethods},
        transaction::{ReadTransaction, WriteTransaction},
    },
    protocol::protocol_collection::{GetConnection, ProtocolCollection},
    tuple_utils::HasTypeAt,
};

pub mod recieve;
pub mod send;

pub struct ProtocolService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceDefinition,
{
    definition: Arc<ServiceDefinition>,
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

pub trait ProtocolServiceDefinition: Send + Sync {
    type Protocols: ProtocolCollection;
    type Tables: DatabaseTableCollection;
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
    ) -> impl Future<Output = Result<SendConnection<T>, SendBrokerError>> + Send;

    fn get_send_connection_with_options(
        &self,
        broker: &Broker,
        node: NodeId,
        options: ConnectOptions,
    ) -> impl Future<Output = Result<SendConnection<T>, SendBrokerError>> + Send;
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
pub trait DatabaseTableDefinitionMethods<T, Index>: ProtocolServiceDefinition
where
    T: DatabaseTable,
{
    fn read_get(
        key: T::Key,
        transaction: ReadTransaction,
    ) -> impl Future<Output = Result<Option<T::Value>, DatabaseError<T>>> + Send;

    fn write_get(
        key: T::Key,
        transaction: WriteTransaction,
    ) -> impl Future<Output = Result<Option<T::Value>, DatabaseError<T>>> + Send;

    fn insert(
        key: T::Key,
        value: T::Value,
        transaction: WriteTransaction,
    ) -> impl Future<Output = Result<Option<T::Value>, DatabaseError<T>>> + Send;
}

impl<Service, T, Index> DatabaseTableDefinitionMethods<T, Index> for Service
where
    Service: ProtocolServiceDefinition,
    T: DatabaseTable,
    Service::Tables: TableMethods<T, Index>,
{
    async fn read_get(
        key: <T as DatabaseTable>::Key,
        transaction: ReadTransaction,
    ) -> Result<Option<<T as DatabaseTable>::Value>, DatabaseError<T>> {
        <Self::Tables as TableMethods<T, Index>>::read_get(key, transaction).await
    }

    async fn write_get(
        key: <T as DatabaseTable>::Key,
        transaction: WriteTransaction,
    ) -> Result<Option<<T as DatabaseTable>::Value>, DatabaseError<T>> {
        <Self::Tables as TableMethods<T, Index>>::write_get(key, transaction).await
    }

    async fn insert(
        key: <T as DatabaseTable>::Key,
        value: <T as DatabaseTable>::Value,
        transaction: WriteTransaction,
    ) -> Result<Option<<T as DatabaseTable>::Value>, DatabaseError<T>> {
        <Self::Tables as TableMethods<T, Index>>::insert(key, value, transaction).await
    }
}
impl<T> ProtocolServiceDefinition for Arc<T>
where
    T: ProtocolServiceDefinition,
{
    type Protocols = T::Protocols;
    type Tables = T::Tables;
}
