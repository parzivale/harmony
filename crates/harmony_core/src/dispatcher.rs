use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures_util::{Sink, ready};
use iroh::endpoint::{ClosedStream, SendStream, StoppedError, WriteError};
use thiserror::Error;

use crate::ProtocolPacket;

pub struct PacketDispatcher<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    send_stream: SendStream,
    buffer: Vec<u8>,
    written: usize,
    flushing: bool,
    waker: Option<Waker>,
    _phantom: PhantomData<T>,
}

impl<T> From<SendStream> for PacketDispatcher<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: SendStream) -> Self {
        Self {
            send_stream: value,
            buffer: Vec::new(),
            written: 0,
            flushing: false,
            waker: None,
            _phantom: PhantomData,
        }
    }
}

#[derive(Error, Debug)]
pub enum PacketDispatcherError {
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    WriteError(#[from] WriteError),
    #[error(transparent)]
    StreamClosed(#[from] ClosedStream),
    #[error(transparent)]
    StoppedError(#[from] StoppedError),
}

impl<T> Sink<T> for PacketDispatcher<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    type Error = PacketDispatcherError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.flushing {
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.as_bytes(&mut this.buffer)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let total_len = this.buffer.len();
        this.flushing = true;
        while this.written < total_len {
            let n = ready!(SendStream::poll_write(
                Pin::new(&mut this.send_stream),
                cx,
                &this.buffer[this.written..],
            )?);
            this.written += n;
        }
        this.flushing = false;
        if let Some(waker) = this.waker.take() {
            waker.wake();
        }
        this.buffer.clear();
        this.written = 0;
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        ready!(Sink::poll_flush(Pin::new(this), cx))?;
        this.send_stream.finish()?;
        let fut = this.send_stream.stopped();
        tokio::pin!(fut);
        ready!(fut.poll(cx))?;
        Poll::Ready(Ok(()))
    }
}
