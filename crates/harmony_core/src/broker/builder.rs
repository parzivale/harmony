use std::{any::TypeId, collections::BTreeMap, fs, marker::PhantomData, sync::Arc};

use anyhow::Context;
use directories::ProjectDirs;
use iroh::{
    Endpoint, RelayMode, SecretKey,
    protocol::{Router, RouterBuilder},
};
use redb::{Database, DatabaseError};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc::unbounded_channel};

use crate::{
    ProtocolCollection, ProtocolPacket, connection::receive::IncomingPackets,
    database::DatabaseTable, database::table_collection::DatabaseTableCollection,
    protocol_handler::IrohPacketHandler, service::ProtocolServiceDefinition,
};

use super::Broker;

pub struct BrokerBuilder<Services = ()> {
    alpns: Vec<&'static str>,
    handlers: BTreeMap<TypeId, Mutex<IncomingPackets>>,
    builder: RouterBuilder,
    services: PhantomData<Services>,
    tables: Vec<&'static str>,
    triple: OrgTriple,
}

#[derive(Debug, Error)]
pub enum BrokerBuilderError {
    #[error("Table {0} has already been added")]
    TableAlreadyExists(&'static str),
    #[error("Alpn {0} has already been added")]
    AplnAlreadyExists(&'static str),
}

pub struct OrgTriple {
    qualifier: &'static str,
    organization: &'static str,
    application: &'static str,
}

impl From<&OrgTriple> for ProjectDirs {
    fn from(value: &OrgTriple) -> Self {
        ProjectDirs::from(value.qualifier, value.organization, value.application).unwrap()
    }
}

impl OrgTriple {
    pub fn new(
        qualifier: &'static str,
        organization: &'static str,
        application: &'static str,
    ) -> Self {
        Self {
            qualifier,
            organization,
            application,
        }
    }
}

impl BrokerBuilder {
    pub async fn new(key: SecretKey, triple: OrgTriple) -> anyhow::Result<Self> {
        let endpoint = Endpoint::builder()
            .secret_key(key)
            .discovery_n0()
            .discovery_local_network()
            .relay_mode(RelayMode::Default)
            .bind()
            .await?;
        Ok(Self {
            triple,
            alpns: Vec::new(),
            handlers: BTreeMap::new(),
            builder: Router::builder(endpoint),
            services: PhantomData,
            tables: Vec::new(),
        })
    }
}

impl<Services> BrokerBuilder<Services> {
    pub fn add_service<Service>(
        self,
    ) -> Result<BrokerBuilder<(Service, Services)>, BrokerBuilderError>
    where
        Service: ProtocolServiceDefinition,
    {
        let this: BrokerBuilder<(Service, Services)> = BrokerBuilder {
            triple: self.triple,
            alpns: self.alpns,
            handlers: self.handlers,
            builder: self.builder,
            services: PhantomData,
            tables: self.tables,
        };
        let this = <Service as ProtocolServiceDefinition>::Protocols::add_protocols(this)?;
        <Service as ProtocolServiceDefinition>::Tables::add_tables(this)
    }

    // This method should never be exposed to endusers as it is used internally for service registration
    pub(crate) fn add_protocol<T>(mut self) -> Result<BrokerBuilder<Services>, BrokerBuilderError>
    where
        for<'de> T: ProtocolPacket<'de> + 'static,
    {
        if self.handlers.contains_key(&TypeId::of::<T>()) {
            return Ok(self);
        }

        if self.alpns.contains(&T::APLN) {
            return Err(BrokerBuilderError::AplnAlreadyExists(T::APLN));
        }

        let (sender, connections) = unbounded_channel();

        let builder = self
            .builder
            .accept(T::APLN, IrohPacketHandler::<T>::new(sender));
        self.handlers
            .insert(TypeId::of::<T>(), Mutex::new(connections));

        Ok(BrokerBuilder {
            triple: self.triple,
            alpns: self.alpns,
            handlers: self.handlers,
            builder,
            services: PhantomData,
            tables: self.tables,
        })
    }

    // This method should never be exposed to endusers as it is used internally for service registration
    pub(crate) fn add_table<T>(mut self) -> Result<BrokerBuilder<Services>, BrokerBuilderError>
    where
        for<'a> T: DatabaseTable,
    {
        if self.tables.contains(&T::NAME) {
            return Err(BrokerBuilderError::TableAlreadyExists(T::NAME));
        }

        self.tables.push(T::NAME);

        Ok(BrokerBuilder { ..self })
    }

    fn get_database(&self) -> Result<Database, DatabaseError> {
        let project_dir: ProjectDirs = (&self.triple).into();
        let project_path = project_dir.data_dir();

        if !project_path.exists() {
            fs::create_dir(project_path)?;
        }

        let database_path = project_path.join("database.redb");

        if !database_path.exists() {
            fs::File::create(&database_path)?;
        }

        Database::create(database_path)
    }

    pub async fn build(self) -> anyhow::Result<Broker<Services>> {
        Ok(Broker {
            db: Arc::new(
                self.get_database()
                    .context("Failed to construct database for the broker")?,
            ),
            alpns: Arc::new(self.alpns),
            router: Arc::new(self.builder.spawn().await?),
            handlers: Arc::new(self.handlers),
            protocols: PhantomData,
            tables: Arc::new(self.tables),
        })
    }
}
