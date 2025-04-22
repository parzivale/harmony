pub mod discovery_v1;
pub use discovery_v1 as v1;

#[macro_export]
macro_rules! discovery {
    ($module_version:expr, $extension:expr) => {
        concat!("discovery/", $module_version, "/", $extension)
    };
}
