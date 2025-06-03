use std::sync::Arc;

use futures_util::{Sink, SinkExt};
use iroh::NodeId;
use tokio::pin;

use crate::Broker;

pub struct ProtocolSendService<ServiceSendDefinition>
where
    ServiceSendDefinition: ProtocolServiceSendDefinition,
{
    pub(crate) definition: Arc<ServiceSendDefinition>,
    pub(crate) broker: Broker,
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
impl<ServiceDefinition> ProtocolSendService<ServiceDefinition>
where
    ServiceDefinition: ProtocolServiceSendDefinition,
{
    pub async fn send(
        &self,
        item: ServiceDefinition::SinkItem,
        node: NodeId,
    ) -> Result<(), ServiceDefinition::Error> {
        let fut = <ServiceDefinition as ProtocolServiceSendDefinition>::send_sink(
            Arc::clone(&self.definition),
            &self.broker,
            node,
        )
        .await?;
        pin!(fut);
        fut.send(item).await
    }

    pub async fn send_sink(
        &self,
        node: NodeId,
    ) -> Result<impl Sink<ServiceDefinition::SinkItem>, ServiceDefinition::Error> {
        <ServiceDefinition as ProtocolServiceSendDefinition>::send_sink(
            Arc::clone(&self.definition),
            &self.broker,
            node,
        )
        .await
    }
}
pub trait ProtocolServiceSendDefinition: Send + Sync {
    type SinkItem;
    type Error: Send;

    fn send_sink(
        definition: Arc<Self>,
        broker: &Broker,
        node: NodeId,
    ) -> impl std::future::Future<
        Output = Result<impl Sink<Self::SinkItem, Error = Self::Error>, Self::Error>,
    > + Send;
}
