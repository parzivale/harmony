use std::{any::Any, marker::PhantomData};

use crate::{ProtocolPacket, handler::PacketHandler};
use anyhow::Result;
use futures_util::{StreamExt, future::BoxFuture};
use iroh::{
    NodeId,
    endpoint::{Connection, ConnectionClose, TransportErrorCode},
    protocol::ProtocolHandler,
};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub struct IrohPacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    sender: UnboundedSender<(NodeId, Box<dyn Any + Send + Sync>)>,
    _phantom: PhantomData<T>,
}

impl<T> IrohPacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    pub fn new(sender: UnboundedSender<(NodeId, Box<dyn Any + Send + Sync>)>) -> Self {
        Self {
            sender,
            _phantom: PhantomData,
        }
    }
}

impl<T> ProtocolHandler for IrohPacketHandler<T>
where
    for<'de> T: ProtocolPacket<'de> + 'static,
{
    fn accept(&self, connection: Connection) -> BoxFuture<'static, Result<()>> {
        let cloned_sender = self.sender.clone();
        Box::pin(async move {
            let recv = match connection.accept_uni().await {
                Ok(recv) => recv,
                Err(err) => match err {
                    iroh::endpoint::ConnectionError::ConnectionClosed(ConnectionClose {
                        error_code: TransportErrorCode::NO_ERROR,
                        frame_type: _,
                        reason: _,
                    }) => {
                        return Ok(());
                    }
                    err => return Err(err.into()),
                },
            };
            let from = connection
                .remote_node_id()
                .expect("Remote node should have an ID");

            let mut handler: PacketHandler<T> = recv.into();
            while let Some(packet) = handler.next().await {
                cloned_sender.send((from, Box::new(packet)))?;
            }
            Ok(())
        })
    }
}
