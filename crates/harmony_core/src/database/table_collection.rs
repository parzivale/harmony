use std::sync::Arc;

use crate::{
    BrokerBuilder,
    broker::builder::BrokerBuilderError,
    database::DatabaseTable,
    tuple_utils::{AsNestedTuple, HasTypeAt},
};
use redb::{ReadableTable, StorageError, TableError};
use thiserror::Error;

use super::transaction::{ReadTransaction, WriteTransaction};

pub trait DatabaseTableCollection<Tail = ()>: AsNestedTuple<Tail> {
    fn add_tables<Services>(
        builder: BrokerBuilder<Services>,
    ) -> Result<BrokerBuilder<Services>, BrokerBuilderError>;
}

pub trait HasTable<T, Index>
where
    T: DatabaseTable,
{
}

impl<T, D, Index> HasTable<T, Index> for D
where
    T: DatabaseTable,
    D: AsNestedTuple<()>,
    D::Nested: HasTypeAt<Index, T>,
{
}

pub trait TableMethods<T, Index>
where
    T: DatabaseTable,
{
    fn read_get(
        key: T::Key,
        transaction: ReadTransaction,
    ) -> impl Future<Output = Result<Option<T::Value>, DatabaseError<T>>> + Send {
        async move {
            tokio::task::spawn_blocking(move || {
                transaction
                    .0
                    .open_table(T::definition())?
                    .get(T::serialize_key(key).map_err(DatabaseError::from_serializer_error)?)
                    .map(|value| {
                        value
                            .map(|value| T::deserialize_value(value.value()))
                            .transpose()
                            .map_err(DatabaseError::from_deserializer_error)
                    })?
            })
            .await?
        }
    }

    fn write_get(
        key: T::Key,
        transaction: WriteTransaction,
    ) -> impl Future<Output = Result<Option<T::Value>, DatabaseError<T>>> + Send {
        async move {
            tokio::task::spawn_blocking(move || {
                transaction
                    .0
                    .open_table(T::definition())?
                    .get(T::serialize_key(key).map_err(DatabaseError::from_serializer_error)?)
                    .map(|value| {
                        value
                            .map(|value| T::deserialize_value(value.value()))
                            .transpose()
                            .map_err(DatabaseError::from_deserializer_error)
                    })?
            })
            .await?
        }
    }

    fn insert(
        key: T::Key,
        value: T::Value,
        transaction: WriteTransaction,
    ) -> impl Future<Output = Result<Option<T::Value>, DatabaseError<T>>> + Send {
        async move {
            tokio::task::spawn_blocking(move || {
                transaction
                    .0
                    .open_table(T::definition())?
                    .insert(
                        T::serialize_key(key).map_err(DatabaseError::from_serializer_error)?,
                        T::serialize_value(value).map_err(DatabaseError::from_serializer_error)?,
                    )
                    .map(|value| {
                        value
                            .map(|value| T::deserialize_value(value.value()))
                            .transpose()
                            .map_err(DatabaseError::from_deserializer_error)
                    })?
            })
            .await?
        }
    }
}

impl<T, Index, Y> TableMethods<T, Index> for Y
where
    T: DatabaseTable,
    Y: HasTable<T, Index>,
{
}

#[derive(Error, Debug)]
pub enum DatabaseError<T: DatabaseTable> {
    #[error(transparent)]
    DeserializationError(T::Error),
    #[error(transparent)]
    SerializationError(T::Error),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    TableError(#[from] TableError),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
}

impl<T> DatabaseError<T>
where
    T: DatabaseTable,
{
    pub fn from_serializer_error(err: T::Error) -> Self {
        Self::SerializationError(err)
    }
    pub fn from_deserializer_error(err: T::Error) -> Self {
        Self::DeserializationError(err)
    }
}

impl DatabaseTableCollection for () {
    fn add_tables<Services>(
        builder: BrokerBuilder<Services>,
    ) -> Result<BrokerBuilder<Services>, BrokerBuilderError> {
        Ok(builder)
    }
}

#[macro_export]
macro_rules! reversed_add_tables {
    // Entry point
    ($builder:expr; $($name:ident),+) => {
        reversed_add_tables!(@rev $builder; ;  $($name),+)
    };

    // Recursive case: move $head from the input to the reversed list
    (@rev $builder:expr; $($rev:ident),* ; $head:ident $(, $tail:ident)*) => {
        reversed_add_tables!(@rev $builder; $head $(, $rev)* ; $($tail),*)
    };

    // Base case: when input is empty, emit the builder chain
    (@rev $builder:expr; $($rev:ident),* ;) => {
        $builder$(.add_table::<$rev>()?)*
    };
}
#[macro_export]
macro_rules! impl_table_collection{
    ($head:ident $(, $tail:ident)* $(,)?) => {
        impl_table_collection!(@impl $head $(, $tail)*);
        impl_table_collection!($($tail),*);
    };
    () => {};

    (@impl $($name:ident),+) => {
        impl<$($name),+> DatabaseTableCollection for ($($name),+,)
        where
            $(
                for<'a> $name: DatabaseTable,
            )+
        {

            fn add_tables<Services>(builder: BrokerBuilder<Services>) -> Result<BrokerBuilder<Services>, BrokerBuilderError> {
                Ok(reversed_add_tables!(builder; $($name),+))
             }
        }

    };
}
