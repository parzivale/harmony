use harmony_core::{Broker, ProtocolService, Sink, Stream};
use protocol::discovery::v1::{ping::Ping, status::Status};

pub struct DiscoveryService {
    broker: Broker,
}

impl ProtocolService<'_> for DiscoveryService {
    type Protocols = (Ping, Status);

    type StreamItem = Status;

    type SinkItem = ();

    type StreamError = anyhow::Error;

    type SinkError = anyhow::Error;

    type SinkInnerError = anyhow::Error;

    fn new(broker: &'_ Broker) -> impl std::future::Future<Output = Self> + Send {
        Box::pin(async move {
            Self {
                broker: broker.clone(),
            }
        })
    }

    fn broker(&self) -> &Broker {
        &self.broker
    }

    fn recv(&self) -> Result<impl Stream<Item = Self::StreamItem>, Self::StreamError> {
        :
    }

    fn send(
        &self,
    ) -> impl std::future::Future<
        Output = Result<impl Sink<Self::SinkItem, Error = Self::SinkInnerError>, Self::SinkError>,
    > + Send {
        todo!()
    }
}
