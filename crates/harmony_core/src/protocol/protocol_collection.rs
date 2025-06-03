use iroh::{NodeId, endpoint::ConnectOptions};

use crate::{
    Broker, BrokerBuilder, BrokerBuilderError, ProtocolPacket,
    broker::{RecieveBrokerError, SendBrokerError},
    connection::{receive::RecieveConnection, send::SendConnection},
    tuple_utils::{AsNestedTuple, HasTypeAt},
};
pub trait ProtocolCollection<Tail = ()>: AsNestedTuple<Tail> {
    fn add_protocols<Services>(
        builder: BrokerBuilder<Services>,
    ) -> Result<BrokerBuilder<Services>, BrokerBuilderError>;
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

#[macro_export]
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
        $builder$(.add_protocol::<$rev>()?)*
    };
}
#[macro_export]
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

            fn add_protocols<Services>(builder: BrokerBuilder<Services>) -> Result<BrokerBuilder<Services>, BrokerBuilderError> {
                Ok(reversed_add_protocols!(builder; $($name),+))
             }
        }

    };
}
