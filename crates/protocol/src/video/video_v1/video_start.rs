use crate::video;
use harmony_core::ProtocolPacket;
use serde::{Deserialize, Serialize};

use super::VideoId;

#[derive(Serialize, Deserialize, Debug)]
pub enum Rotation {
    Bottom,
    Left,
    Right,
    Top,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Size {
    height: u32,
    width: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartStreamSettings {
    pub stream_id: VideoId,
    pub size: Size,
    pub rotation: Rotation,
    pub frequecy: f32,
    pub name: String,
}

impl ProtocolPacket<'_> for StartStreamSettings {
    const APLN: &'static str = video!("v1", "start");
}
