use blake3::{Hash, Hasher};
use ed25519_dalek::Signature;
use harmony_core::{Broker, ProtocolPacket};
use iroh::PublicKey;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, time::SystemTime};

use crate::message;

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    previous: Option<Hash>,
    message: String,
    sent_at: u64,
    signature: Signature,
}

impl Message {
    pub fn new(
        message: &str,
        previous: Option<Hash>,
        sent_at: SystemTime,
        broker: &Broker,
    ) -> Self {
        let sent_at = sent_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("We traveled back in time before 1970")
            .as_secs();

        Self {
            message: message.to_string(),
            previous,
            sent_at,
            signature: broker.sign(message.as_bytes()),
        }
    }

    pub fn as_hash(&self, buf: &mut Vec<u8>, hasher: &mut Hasher) -> postcard::Result<Hash> {
        let as_bytes = postcard::to_io(&self, &mut *buf)?;
        hasher.update(as_bytes);
        buf.clear();
        Ok(hasher.finalize())
    }

    pub fn matches_hash(&self, hash: Option<Hash>) -> bool {
        self.previous == hash
    }

    pub fn is_from(&self, public_key: PublicKey) -> bool {
        public_key
            .verify(self.message.as_bytes(), &self.signature)
            .is_ok()
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
