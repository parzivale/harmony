use std::fmt::Display;

use harmony_core::ProtocolPacket;
use serde::{Deserialize, Serialize};

use crate::message;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    message: String,
}

impl Message {
    pub fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl ProtocolPacket<'_> for Message {
    const APLN: &'static str = message!("v1", "message");
}
