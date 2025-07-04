use crate::database::DatabaseTable;
use crate::database::table_collection::DatabaseTableCollection;
pub use broker::{
    Broker,
    builder::{BrokerBuilder, BrokerBuilderError},
};
pub use dispatcher::PacketDispatcherError;
pub use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt, ready};
pub use iroh::{NodeId, endpoint::ConnectOptions};
pub use protocol::packet::ProtocolPacket;
use protocol::protocol_collection::ProtocolCollection;
pub use tokio::pin;
use tuple_utils::AsNestedTuple;

pub mod broker;
pub mod connection;
pub mod database;
mod dispatcher;
pub mod handler;
pub mod protocol;
mod protocol_handler;
pub mod service;
mod tuple_utils;

macro_rules! setup_impls {
    ($($name:ident),*) => {
        impl_as_nested_tuple!($($name),*);
        impl_protocol_collection!($($name),*);
        impl_table_collection!($($name),*);
    };
}

setup_impls!(T, D, F, G, H, J, K, L, Z, X, C, V, B, N, M, Q);
