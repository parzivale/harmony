use std::sync::Arc;

use futures_util::Stream;

use crate::Broker;

pub struct ProtocolRecieveService<ServiceRecieveDefinition>
where
    ServiceRecieveDefinition: ProtocolServiceReceiveDefinition,
{
    pub(crate) definition: Arc<ServiceRecieveDefinition>,
    pub(crate) broker: Broker,
}

impl<ServiceDefinition> ProtocolRecieveService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceReceiveDefinition,
{
    pub fn receieve_stream(
        &self,
    ) -> Result<impl Stream<Item = ServiceDefinition::StreamItem>, ServiceDefinition::Error> {
        <ServiceDefinition as ProtocolServiceReceiveDefinition>::recv_stream(
            Arc::clone(&self.definition),
            &self.broker,
        )
    }
}

pub trait ProtocolServiceReceiveDefinition: Send + Sync {
    type StreamItem;
    type Error: Send;
    fn recv_stream(
        definition: Arc<Self>,
        broker: &Broker,
    ) -> Result<impl Stream<Item = Self::StreamItem>, Self::Error>;
}
