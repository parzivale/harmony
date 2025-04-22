use serde::{Deserialize, Serialize};

pub mod video_packet;
pub mod video_start;

#[derive(Serialize, Deserialize, Debug)]
pub struct VideoId(u8);
