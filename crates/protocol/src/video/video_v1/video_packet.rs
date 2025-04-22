use crate::video;
use harmony_core::ProtocolPacket;
use serde::{Deserialize, Serialize};

use super::VideoId;

#[derive(Serialize, Deserialize, Debug)]
pub struct Av1StreamPacket<'a> {
    stream_id: VideoId,
    data: &'a [u8],
}

impl<'a> ProtocolPacket<'a> for Av1StreamPacket<'a> {
    const APLN: &'static str = video!("v1", "packet");
}
