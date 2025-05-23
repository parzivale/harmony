use harmony_core::ProtocolPacket;
use serde::{Deserialize, Serialize};

use crate::discovery;

#[derive(Debug, Serialize, Deserialize)]
pub enum Status {
    Active,
    Afk,
}

impl ProtocolPacket<'_> for Status {
    const APLN: &'static str = discovery!("v1", "status");
}
