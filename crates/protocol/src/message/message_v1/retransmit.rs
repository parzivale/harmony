use blake3::Hash;
use harmony_core::ProtocolPacket;
use serde::{Deserialize, Serialize};

use crate::message;

use super::message::Message;

#[derive(Debug, Serialize, Deserialize)]
pub enum Retransmit {
    Request(Hash),
    Response(Message),
}

impl ProtocolPacket<'_> for Retransmit {
    const APLN: &'static str = message!("v1", "retransmit");
}
