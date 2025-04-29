use blake3::{Hash, Hasher};
use harmony_core::ProtocolPacket;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use crate::message;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    previous: Option<Hash>,
    message: String,
}

impl Message {
    pub fn new(message: &str, previous: Option<Hash>) -> Self {
        Self {
            message: message.to_string(),
            previous,
        }
    }

    pub fn as_hash(&self) -> postcard::Result<Hash> {
        let mut buf = Vec::new();
        let mut hasher = Hasher::new();
        let as_bytes = self.as_bytes(&mut buf)?;
        hasher.update(as_bytes);
        Ok(hasher.finalize())
    }

    pub fn matches_hash(&self, hash: Option<Hash>) -> bool {
        self.previous == hash
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
