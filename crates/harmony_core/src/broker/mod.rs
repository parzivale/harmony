pub mod builder;
use std::{any::TypeId, collections::BTreeMap, marker::PhantomData, sync::Arc};

use directories::ProjectDirs;
use ed25519_dalek::Signature;
use iroh::{
    NodeId,
    endpoint::{ConnectOptions, ConnectionError},
    protocol::Router,
};
use redb::{CommitError, Database, DatabaseError, TransactionError};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    ProtocolPacket,
    connection::{
        receive::{IncomingPackets, RecieveConnection},
        send::SendConnection,
    },
    database::{
        DatabaseTable,
        table_collection::DatabaseTableCollection,
        transaction::{
            ReadTransaction, ReadTransactionResult, WriteTransaction, WriteTransactionResult,
        },
    },
};

pub fn get_project_dir() -> ProjectDirs {
    ProjectDirs::from("com", "parzivale", "harmony").unwrap()
}

pub fn get_database() -> Result<Database, DatabaseError> {
    let database_path = get_project_dir().project_path().join("database.redb");
    Database::create(database_path)
}

pub struct Broker<Protocols = ()> {
    pub(crate) alpns: Arc<Vec<&'static str>>,
    pub(crate) tables: Arc<Vec<&'static str>>,
    pub(crate) router: Arc<Router>,
    pub(crate) handlers: Arc<BTreeMap<TypeId, Mutex<IncomingPackets>>>,
    pub(crate) db: Arc<Database>,
    pub(crate) protocols: PhantomData<Protocols>,
}
impl<Services> Clone for Broker<Services> {
    fn clone(&self) -> Self {
        Self {
            alpns: Arc::clone(&self.alpns),
            router: Arc::clone(&self.router),
            handlers: Arc::clone(&self.handlers),
            db: Arc::clone(&self.db),
            protocols: PhantomData,
            tables: Arc::clone(&self.tables),
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
    pub(crate) fn clear_type(self) -> Broker {
        Broker {
            alpns: self.alpns,
            router: self.router,
            handlers: self.handlers,
            protocols: PhantomData,
            tables: self.tables,
            db: self.db,
        }
    }

    pub async fn read_transaction<F, E, Fut, FutErr, T, Def>(&self, func: F) -> Result<T, E>
    where
        F: Fn(ReadTransaction) -> Fut,
        Fut: Future<Output = Result<ReadTransactionResult<T>, FutErr>>,
        E: From<TransactionError>,
        FutErr: Into<E>,
        Def: DatabaseTableCollection,
    {
        func(self.db.begin_read()?.into())
            .await
            .map(|result| result.1)
            .map_err(Into::into)
    }

    pub async fn write_transaction<F, E, Fut, FutErr, T>(&self, func: F) -> Result<T, E>
    where
        F: Fn(WriteTransaction) -> Fut,
        Fut: Future<Output = Result<WriteTransactionResult<T>, FutErr>>,
        E: From<TransactionError> + From<CommitError>,
        FutErr: Into<E>,
    {
        let result = func(self.db.begin_write()?.into())
            .await
            .map_err(Into::into)?;
        result.0.commit()?;
        Ok(result.1)
    }

    pub fn sign(&self, msg: &[u8]) -> Signature {
        self.router.endpoint().secret_key().sign(msg)
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
