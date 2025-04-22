pub mod error_v1;
pub use error_v1 as v1;

#[macro_export]
macro_rules! error {
    ($module_version:expr, $extension:expr) => {
        concat!("error/", $module_version, "/", $extension)
    };
}
