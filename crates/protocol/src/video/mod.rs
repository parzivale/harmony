pub mod video_v1;
pub use video_v1 as v1;

#[macro_export]
macro_rules! video {
    ($module_version:expr, $extension:expr) => {
        concat!("video/", $module_version, "/", $extension)
    };
}
