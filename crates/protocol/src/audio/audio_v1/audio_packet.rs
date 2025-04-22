use crate::audio;
use harmony_core::ProtocolPacket;
use serde::{Deserialize, Serialize};

use super::AudioId;

#[derive(Serialize, Deserialize, Debug)]
pub struct OpusStreamPacket<'a> {
    stream_id: AudioId,
    data: &'a [u8],
}

impl<'de> ProtocolPacket<'de> for OpusStreamPacket<'de> {
    const APLN: &'static str = audio!("v1", "packet");
}
