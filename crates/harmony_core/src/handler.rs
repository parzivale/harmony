use std::{
    io,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Stream, ready};
use iroh::endpoint::{ConnectionError, ReadError, RecvStream};
use thiserror::Error;
use tokio::io::AsyncReadExt;

use crate::ProtocolPacket;

pub struct PacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    recv_stream: RecvStream,
    buffer: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<T> PacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn take_from_bytes(&mut self) -> Result<NextPacketStatus<T>, postcard::Error> {
        match T::take_from_bytes(&self.buffer) {
            Ok((packet, remaining)) => {
                let used = self.buffer.len() - remaining.len();
                self.buffer.drain(..used);
                Ok(NextPacketStatus::Packet(packet))
            }
            Err(postcard::Error::DeserializeUnexpectedEnd) => Ok(NextPacketStatus::BytesRemaining),
            Err(err) => Err(err),
        }
    }
}

impl<T> Stream for PacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    type Item = Result<T, PacketHandlerError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if !this.buffer.is_empty() {
            return match this.take_from_bytes() {
                Ok(NextPacketStatus::Packet(packet)) => Poll::Ready(Some(Ok(packet))),
                Ok(NextPacketStatus::BytesRemaining) => Poll::Pending,
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            };
        }
        let read_fut = this.recv_stream.read_buf(&mut this.buffer);
        tokio::pin!(read_fut);

        match ready!(read_fut.as_mut().poll(cx)) {
            Ok(0) => Poll::Ready(None),
            Ok(_) => match this.take_from_bytes() {
                Ok(NextPacketStatus::Packet(packet)) => Poll::Ready(Some(Ok(packet))),
                Ok(NextPacketStatus::BytesRemaining) => Poll::Pending,
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            },
            Err(err) => match err.downcast::<ReadError>() {
                Ok(err) => match err {
                    ReadError::ConnectionLost(ConnectionError::ApplicationClosed(_)) => {
                        Poll::Ready(None)
                    }
                    err => Poll::Ready(Some(Err(err.into()))),
                },
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            },
        }
    }
}

impl<T> From<RecvStream> for PacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: RecvStream) -> Self {
        Self {
            recv_stream: value,
            buffer: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

pub enum NextPacketStatus<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    BytesRemaining,
    Packet(T),
}

#[derive(Error, Debug)]
pub enum PacketHandlerError {
    #[error(transparent)]
    Postcard(#[from] postcard::Error),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Io(#[from] io::Error),
}
