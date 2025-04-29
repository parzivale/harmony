use harmony_core::ProtocolPacket;
use serde::{Deserialize, Serialize};

use crate::discovery;

#[derive(Debug, Serialize, Deserialize)]
pub struct Ping;

impl ProtocolPacket<'_> for Ping {
    const APLN: &'static str = discovery!("v1", "ping");
}
