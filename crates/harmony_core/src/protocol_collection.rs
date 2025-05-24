use iroh::{NodeId, endpoint::ConnectOptions};

use crate::{
    Broker, BrokerBuilder, ProtocolPacket,
    builder::{RecieveBrokerError, SendBrokerError},
    receive::RecieveConnection,
    send::SendConnection,
};

pub struct Here;
pub struct Later<T>(std::marker::PhantomData<T>);

pub trait HasTypeAt<Index, T> {}
impl<T, Tail> HasTypeAt<Here, T> for (T, Tail) {}
impl<T, U, I, Tail> HasTypeAt<Later<I>, T> for (U, Tail) where Tail: HasTypeAt<I, T> {}

pub trait AsNestedTuple<Tail> {
    type Nested;
}

impl<Tail> AsNestedTuple<Tail> for () {
    type Nested = ();
}

pub trait ProtocolCollection<Tail = ()>: AsNestedTuple<Tail> {
    fn add_protocols<Services>(builder: BrokerBuilder<Services>) -> BrokerBuilder<Services>;
}
pub trait HasPacket<T, Index>
where
    for<'de> T: ProtocolPacket<'de>,
{
}

impl<T, D, Index> HasPacket<T, Index> for D
where
    for<'de> T: ProtocolPacket<'de>,
    D: AsNestedTuple<()>,
    D::Nested: HasTypeAt<Index, T>,
{
}

pub trait GetConnection<T, Index>
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
macro_rules! nested_tuple {
    (; $end:ty) => {
        $end
    };
    ($head:ty $(, $tail:ty)* ; $end:ty) => {
        ($head, nested_tuple!($($tail),*; $end))
    };
}

macro_rules! impl_as_nested_tuple_inner {
    ($($types:ident),+) => {
        impl<$($types),+, Tail> AsNestedTuple<Tail> for ($($types,)+) {
            type Nested = nested_tuple!($($types),*; Tail);
        }
    };
}
macro_rules! reversed_add_protocols {
    // Entry point
    ($builder:expr; $($name:ident),+) => {
        reversed_add_protocols!(@rev $builder; ;  $($name),+)
    };

    // Recursive case: move $head from the input to the reversed list
    (@rev $builder:expr; $($rev:ident),* ; $head:ident $(, $tail:ident)*) => {
        reversed_add_protocols!(@rev $builder; $head $(, $rev)* ; $($tail),*)
    };

    // Base case: when input is empty, emit the builder chain
    (@rev $builder:expr; $($rev:ident),* ;) => {
        $builder$(.add_protocol::<$rev>())*
    };
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

            fn add_protocols<Services>(builder: BrokerBuilder<Services>) -> BrokerBuilder<Services> {
                reversed_add_protocols!(builder; $($name),+)
             }
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
macro_rules! setup_impls {
    ($($name:ident),*) => {
        impl_as_nested_tuple!($($name),*);
        impl_protocol_collection!($($name),*);
    };
}

setup_impls!(T, D, F, G, H, J, K, L, Z, X, C, V, B, N, M, Q);
