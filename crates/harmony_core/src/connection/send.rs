use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Sink, SinkExt};
use iroh::endpoint::SendStream;

use crate::{
    ProtocolPacket,
    dispatcher::{PacketDispatcher, PacketDispatcherError},
};

// It is important to explicitly call .close() on the connection to ensure the peer has recieved
// all remaning data. Otherwise if the connection is dropped while open the SendConnection will
// forceably terminate the connection and possibly cause the peer to lose data.
pub struct SendConnection<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    dispatcher: PacketDispatcher<T>,
    _phantom: PhantomData<T>,
}

impl<T> From<SendStream> for SendConnection<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: SendStream) -> Self {
        Self {
            dispatcher: value.into(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Sink<T> for SendConnection<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    type Error = PacketDispatcherError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        this.dispatcher.poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> std::result::Result<(), Self::Error> {
        let this = self.get_mut();
        this.dispatcher.start_send_unpin(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        this.dispatcher.poll_flush_unpin(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        this.dispatcher.poll_close_unpin(cx)
    }
}
