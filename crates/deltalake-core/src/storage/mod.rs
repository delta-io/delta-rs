//! Object storage backend abstraction layer for Delta Table transaction logs and data

use std::sync::Arc;

use lazy_static::lazy_static;

pub mod config;
pub mod file;
pub mod utils;

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
pub mod s3;

pub use object_store::path::{Path, DELIMITER};
pub use object_store::{
    DynObjectStore, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result as ObjectStoreResult,
};
pub use utils::*;

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

/// Return the uri of commit version.
pub fn commit_uri_from_version(version: i64) -> Path {
    let version = format!("{version:020}.json");
    DELTA_LOG_PATH.child(version.as_str())
}

/// Sharable reference to [`ObjectStore`]
pub type ObjectStoreRef = Arc<dyn ObjectStore>;
