pub mod audio_v1;
pub use audio_v1 as v1;

#[macro_export]
macro_rules! audio {
    ($module_version:expr, $extension:expr) => {
        concat!("audio/", $module_version, "/", $extension)
    };
}
