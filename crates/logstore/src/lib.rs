//! # DeltaLake storage system
//!
//! Interacting with storage systems is a crucial part of any table format.
//! On one had the storage abstractions need to provide certain guarantees
//! (e.g. atomic rename, ...) and meet certain assumptions (e.g. sorted list results)
//! on the other hand can we exploit our knowledge about the general file layout
//! and access patterns to optimize our operations in terms of cost and performance.

pub mod config;
pub(crate) mod default_logstore;
pub mod error;
pub(crate) mod factories;
mod logstore_impl;
pub(crate) mod storage;

// Compatibility shim for DeltaConfig derive macro which expects crate::logstore::config
pub mod logstore {
    pub mod config {
        pub use crate::config::*;
    }
    pub mod error {
        pub use crate::error::LogStoreResult;
    }
}

pub use self::config::StorageConfig;
pub use self::default_logstore::DefaultLogStore;
pub use self::error::{LogStoreError, LogStoreResult};
pub use self::factories::{
    logstore_factories, object_store_factories, store_for, LogStoreFactory,
    LogStoreFactoryRegistry, ObjectStoreFactory, ObjectStoreFactoryRegistry,
};
pub use self::storage::utils::commit_uri_from_version;
pub use self::storage::{
    DefaultObjectStoreRegistry, DeltaIOStorageBackend, IORuntime, ObjectStoreRef,
    ObjectStoreRegistry, ObjectStoreRetryExt,
};
pub use ::object_store;
pub use logstore_impl::{DELTA_LOG_PATH, DELTA_LOG_REGEX, *};
pub use object_store::path::Path;
pub use object_store::Error as ObjectStoreError;
