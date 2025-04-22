use serde::{Deserialize, Serialize};

pub mod audio_packet;
pub mod audio_start;

#[derive(Serialize, Deserialize, Debug)]
pub struct AudioId(u8);
