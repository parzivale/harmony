pub mod message_v1;
pub use message_v1 as v1;

#[macro_export]
macro_rules! message {
    ($module_version:expr, $extension:expr) => {
        concat!("message/", $module_version, "/", $extension)
    };
}
