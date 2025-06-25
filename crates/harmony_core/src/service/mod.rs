use std::{any::Any, marker::PhantomData, sync::Arc};

use futures_util::{FutureExt, ready};
use iroh::{NodeId, endpoint::ConnectOptions};
use recieve::{ProtocolRecieveService, ProtocolServiceReceiveDefinition};
use redb::{CommitError, Database, StorageError, TableError, TransactionError};
use send::{ProtocolSendService, ProtocolServiceSendDefinition};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};

use crate::{
    Broker, DatabaseTableCollection, ProtocolPacket,
    broker::{RecieveBrokerError, SendBrokerError},
    connection::{receive::RecieveConnection, send::SendConnection},
    database::{
        DatabaseTable,
        table_collection::{DatabaseError, TableMethods},
        transaction::{
            ReadTransaction, ReadTransactionResult, WriteTransaction, WriteTransactionResult,
        },
    },
    protocol::protocol_collection::{GetConnection, ProtocolCollection},
    tuple_utils::{HasTypeAt, Remove},
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
    pub fn new<Services, Index>(
        broker: Broker<Services>,
        definition: ServiceDefinition,
    ) -> (
        Self,
        Broker<<Services as Remove<Index, ServiceDefinition>>::Output>,
    )
    where
        Services: HasTypeAt<Index, ServiceDefinition> + Remove<Index, ServiceDefinition>,
    {
        (
            Self {
                definition: Arc::new(definition),
                broker: broker.clone().clear_type(),
            },
            Broker {
                alpns: broker.alpns,
                tables: broker.tables,
                router: broker.router,
                handlers: broker.handlers,
                db: broker.db,
                protocols: PhantomData,
            },
        )
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

pub struct NoTable;

pub struct TableManager<T, Table = NoTable>
where
    T: ProtocolServiceDefinition,
{
    _inner: PhantomData<(T, Table)>,
}

impl<S, A> TableManager<S, A>
where
    S: ProtocolServiceDefinition,
{
    pub fn with_table<T>(&self) -> TableManager<S, T>
    where
        T: DatabaseTable,
    {
        TableManager {
            _inner: PhantomData,
        }
    }
}

impl<T> Clone for TableManager<T>
where
    T: ProtocolServiceDefinition,
{
    fn clone(&self) -> Self {
        Self {
            _inner: PhantomData,
        }
    }
}

impl<T> Default for TableManager<T>
where
    T: ProtocolServiceDefinition,
{
    fn default() -> Self {
        TableManager {
            _inner: PhantomData,
        }
    }
}

impl<T> TableManager<T>
where
    T: ProtocolServiceDefinition,
{
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct ReadTransactionFuture<Service, F, Err, A>
where
    A: Send,
    Service: ProtocolServiceDefinition + Unpin + 'static,
    Err: Into<TransactionFutureError> + Send + Unpin,
    F: FnMut(TableManager<Service>, ReadTransaction) -> Result<ReadTransactionResult<A>, Err>
        + Send
        + Unpin
        + 'static,
{
    task: Option<JoinHandle<Result<ReadTransactionResult<A>, TransactionFutureError>>>,
    db: Arc<Database>,
    manager: TableManager<Service>,
    func: Option<F>,
}

#[derive(Debug, Error)]
pub enum TransactionFutureError {
    #[error(transparent)]
    TransactionError(#[from] TransactionError),
    #[error(transparent)]
    CommitError(#[from] CommitError),
    #[error(transparent)]
    JoinError(#[from] JoinError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    TableError(#[from] TableError),
    #[error("Received a database Serialization error")]
    DatabaseSerializationError(Box<dyn Any + Send>),
    #[error("Received a database Deserialization error")]
    DatabaseDeserializationError(Box<dyn Any + Send>),
}

impl<T: DatabaseTable> From<DatabaseError<T>> for TransactionFutureError {
    fn from(value: DatabaseError<T>) -> Self {
        match value {
            DatabaseError::DeserializationError(err) => {
                Self::DatabaseDeserializationError(Box::new(err))
            }
            DatabaseError::SerializationError(err) => {
                Self::DatabaseSerializationError(Box::new(err))
            }
            DatabaseError::StorageError(storage_error) => storage_error.into(),
            DatabaseError::TableError(table_error) => table_error.into(),
            DatabaseError::JoinError(join_error) => join_error.into(),
        }
    }
}

impl<Service, F, Err, A> Future for ReadTransactionFuture<Service, F, Err, A>
where
    A: Send + 'static,
    Service: ProtocolServiceDefinition + Unpin + 'static,
    Err: Into<TransactionFutureError> + Send + Unpin,
    F: FnMut(TableManager<Service>, ReadTransaction) -> Result<ReadTransactionResult<A>, Err>
        + Send
        + Unpin
        + 'static,
{
    type Output = Result<Option<A>, TransactionFutureError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        fn finish_transaction<A>(
            task: &mut JoinHandle<Result<ReadTransactionResult<A>, TransactionFutureError>>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<Option<A>, TransactionFutureError>> {
            let result = ready!(task.poll_unpin(cx))??;
            result.transaction.close()?;
            std::task::Poll::Ready(Ok(result.result))
        }

        let this = self.get_mut();
        match &mut this.task {
            Some(task) => finish_transaction(task, cx),
            None => {
                let manager = this.manager.clone();
                let db = Arc::clone(&this.db);
                let func = this.func.take();
                let mut task = tokio::task::spawn_blocking(move || {
                    if let Some(mut func) = func {
                        func(manager, db.begin_read()?.into()).map_err(Into::into)
                    } else {
                        panic!("Somehow the func for the readtransaction future was stolen");
                    }
                });

                match finish_transaction(&mut task, cx) {
                    std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
                    std::task::Poll::Pending => {
                        this.task = Some(task);
                        std::task::Poll::Pending
                    }
                }
            }
        }
    }
}

pub struct WriteTransactionFuture<Service, F, Err, A>
where
    A: Send,
    Service: ProtocolServiceDefinition + Unpin + 'static,
    Err: Into<TransactionFutureError> + Send + Unpin,
    F: FnMut(TableManager<Service>, WriteTransaction) -> Result<WriteTransactionResult<A>, Err>
        + Send
        + Unpin
        + 'static,
{
    task: Option<JoinHandle<Result<WriteTransactionResult<A>, TransactionFutureError>>>,
    db: Arc<Database>,
    manager: TableManager<Service>,
    func: Option<F>,
}

impl<Service, F, Err, A> Future for WriteTransactionFuture<Service, F, Err, A>
where
    A: Send + 'static,
    Service: ProtocolServiceDefinition + Unpin + 'static,
    Err: Into<TransactionFutureError> + Send + Unpin,
    F: FnMut(TableManager<Service>, WriteTransaction) -> Result<WriteTransactionResult<A>, Err>
        + Send
        + Unpin
        + 'static,
{
    type Output = Result<A, TransactionFutureError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        fn finish_transaction<A>(
            task: &mut JoinHandle<Result<WriteTransactionResult<A>, TransactionFutureError>>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<A, TransactionFutureError>> {
            let result = ready!(task.poll_unpin(cx))??;
            result.transaction.commit()?;
            std::task::Poll::Ready(Ok(result.result))
        }

        let this = self.get_mut();
        match &mut this.task {
            Some(task) => finish_transaction(task, cx),
            None => {
                let manager = this.manager.clone();
                let db = Arc::clone(&this.db);
                let func = this.func.take();
                let mut task = tokio::task::spawn_blocking(move || {
                    if let Some(mut func) = func {
                        func(manager, db.begin_write()?.into()).map_err(Into::into)
                    } else {
                        panic!("Somehow the func for the writetransaction future was stolen");
                    }
                });

                match finish_transaction(&mut task, cx) {
                    std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
                    std::task::Poll::Pending => {
                        this.task = Some(task);
                        std::task::Poll::Pending
                    }
                }
            }
        }
    }
}

pub trait DatabaseTableDefinitionMethods: ProtocolServiceDefinition + Sized
where
    Self: Unpin + 'static,
{
    fn read_transaction<F, Err, A, Services>(
        broker: &Broker<Services>,
        func: F,
    ) -> ReadTransactionFuture<Self, F, Err, A>
    where
        F: FnMut(TableManager<Self>, ReadTransaction) -> Result<ReadTransactionResult<A>, Err>
            + Send
            + Unpin
            + 'static,
        Err: Into<TransactionFutureError> + Send + Unpin,
        A: Send + 'static,
        Services: Send + 'static,
    {
        ReadTransactionFuture {
            task: None,
            db: Arc::clone(&broker.db),
            manager: TableManager::new(),
            func: Some(func),
        }
    }

    fn write_transaction<F, Err, A, Services>(
        broker: &Broker<Services>,
        func: F,
    ) -> WriteTransactionFuture<Self, F, Err, A>
    where
        F: FnMut(TableManager<Self>, WriteTransaction) -> Result<WriteTransactionResult<A>, Err>
            + Send
            + Unpin
            + 'static,
        Err: Into<TransactionFutureError> + Send + Unpin,
        A: Send + 'static,
        Services: Send + 'static,
    {
        WriteTransactionFuture {
            task: None,
            db: Arc::clone(&broker.db),
            manager: TableManager::new(),
            func: Some(func),
        }
    }
}

impl<Service> DatabaseTableDefinitionMethods for Service where
    Service: ProtocolServiceDefinition + Unpin + 'static
{
}

pub trait DatabaseManagerMethods<T, Index>
where
    T: DatabaseTable,
{
    fn create_table(&self, transaction: &WriteTransaction) -> Result<(), DatabaseError<T>>;

    fn read_get(
        &self,
        key: impl Into<T::Key>,
        transaction: &ReadTransaction,
    ) -> Result<Option<T::Value>, DatabaseError<T>>;

    fn write_get(
        &self,
        key: impl Into<T::Key>,
        transaction: &WriteTransaction,
    ) -> Result<Option<T::Value>, DatabaseError<T>>;

    fn insert(
        &self,
        key: impl Into<T::Key>,
        value: impl Into<T::Value>,
        transaction: &WriteTransaction,
    ) -> Result<Option<T::Value>, DatabaseError<T>>;
}

impl<Service, T, Index> DatabaseManagerMethods<T, Index> for TableManager<Service, T>
where
    Service: ProtocolServiceDefinition,
    T: DatabaseTable,
    Service::Tables: TableMethods<T, Index>,
{
    fn create_table(&self, transaction: &WriteTransaction) -> Result<(), DatabaseError<T>> {
        <Service::Tables as TableMethods<T, Index>>::create_table(transaction)
    }

    fn read_get(
        &self,
        key: impl Into<<T as DatabaseTable>::Key>,
        transaction: &ReadTransaction,
    ) -> Result<Option<T::Value>, DatabaseError<T>> {
        <Service::Tables as TableMethods<T, Index>>::read_get(key.into(), transaction)
    }

    fn write_get(
        &self,
        key: impl Into<<T as DatabaseTable>::Key>,
        transaction: &WriteTransaction,
    ) -> Result<Option<<T as DatabaseTable>::Value>, DatabaseError<T>> {
        <Service::Tables as TableMethods<T, Index>>::write_get(key.into(), transaction)
    }

    fn insert(
        &self,
        key: impl Into<<T as DatabaseTable>::Key>,
        value: impl Into<<T as DatabaseTable>::Value>,
        transaction: &WriteTransaction,
    ) -> Result<Option<<T as DatabaseTable>::Value>, DatabaseError<T>> {
        <Service::Tables as TableMethods<T, Index>>::insert(key.into(), value.into(), transaction)
    }
}

impl<T> ProtocolServiceDefinition for Arc<T>
where
    T: ProtocolServiceDefinition,
{
    type Protocols = T::Protocols;
    type Tables = T::Tables;
}
