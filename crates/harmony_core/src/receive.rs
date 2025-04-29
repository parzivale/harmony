use std::{
    any::Any,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Stream, ready};
use iroh::NodeId;
use tokio::sync::{MutexGuard, mpsc::UnboundedReceiver};

use crate::{ProtocolPacket, handler::PacketHandlerError, packet::Packet};

pub type IncomingPackets = UnboundedReceiver<(NodeId, Box<dyn Any + Send + Sync>)>;

pub struct RecieveConnection<'a, T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    connections: MutexGuard<'a, IncomingPackets>,
    _phantom: PhantomData<T>,
}

impl<'a, T> From<MutexGuard<'a, IncomingPackets>> for RecieveConnection<'a, T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: MutexGuard<'a, IncomingPackets>) -> RecieveConnection<'a, T> {
        Self {
            connections: value,
            _phantom: PhantomData,
        }
    }
}

impl<T> Stream for RecieveConnection<'_, T>
where
    for<'de> T: ProtocolPacket<'de> + 'static,
{
    type Item = Result<Packet<T>, PacketHandlerError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some((from, recv)) = ready!(this.connections.poll_recv(cx)) {
            let packet: Result<T, PacketHandlerError> = *recv.downcast().expect("T should match generic parameter, this means poll_next was called with non T parameter, this should be impossible");
            let packet = packet.map(|data| Packet::from((data, from)));
            Poll::Ready(Some(packet))
        } else {
            Poll::Ready(None)
        }
    }
}
