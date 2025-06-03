use iroh::NodeId;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

// unpin needed for future::stream impl
// debug needed for iroh protocolHandler
pub trait ProtocolPacket<'de>: Serialize + Deserialize<'de> + Send + Sync + Unpin + Debug {
    const APLN: &'static str;

    fn take_from_bytes(buf: &'de [u8]) -> postcard::Result<(Self, &'de [u8])> {
        postcard::take_from_bytes(buf)
    }

    fn as_bytes<'a>(&self, buf: &'a mut Vec<u8>) -> postcard::Result<&'a mut Vec<u8>> {
        postcard::to_io(self, buf)
    }
}

pub struct Packet<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    data: T,
    from: NodeId,
}

impl<T> Packet<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    pub fn from_node(&self) -> NodeId {
        self.from
    }

    pub fn data(self) -> T {
        self.data
    }
}

impl<T> From<(T, NodeId)> for Packet<T>
where
    for<'de> T: ProtocolPacket<'de>,
{
    fn from(value: (T, NodeId)) -> Self {
        Self {
            data: value.0,
            from: value.1,
        }
    }
}
