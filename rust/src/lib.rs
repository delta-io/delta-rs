//! Native Delta Lake implementation in Rust
//!
//! # Usage
//!
//! Load a Delta Table by path:
//!
//! ```rust
//! async {
//!   let table = deltalake::open_table("./tests/data/simple_table").await.unwrap();
//!   let files = table.get_files();
//! };
//! ```
//!
//! Load a specific version of Delta Table by path then filter files by partitions:
//!
//! ```rust
//! async {
//!   let table = deltalake::open_table_with_version("./tests/data/simple_table", 0).await.unwrap();
//!   let files = table.get_files_by_partitions(&[deltalake::PartitionFilter {
//!       key: "month",
//!       value: deltalake::PartitionValue::Equal("12"),
//!   }]);
//! };
//! ```
//!
//! Load a specific version of Delta Table by path and datetime:
//!
//! ```rust
//! async {
//!   let table = deltalake::open_table_with_ds(
//!       "./tests/data/simple_table",
//!       "2020-05-02T23:47:31-07:00",
//!   ).await.unwrap();
//!   let files = table.get_files();
//! };
//! ```
//!
//! # Optional cargo package features
//!
//! - `s3`, `gcs`, `azure` - enable the storage backends for AWS S3, Google Cloud Storage (GCS),
//!   or Azure Blob Storage / Azure Data Lake Storage Gen2 (ADLS2). Use `s3-native-tls` to use native TLS
//!   instead of Rust TLS implementation.
//! - `glue` - enable the Glue data catalog to work with Delta Tables with AWS Glue.
//! - `datafusion` - enable the `datafusion::datasource::TableProvider` trait implementation
//!   for Delta Tables, allowing them to be queried using [DataFusion](https://github.com/apache/arrow-datafusion).
//! - `datafusion-ext` - DEPRECATED: alias for `datafusion` feature.
//! - `parquet2` - use parquet2 for checkpoint deserialization. Since `arrow` and `parquet` features
//!   are enabled by default for backwards compatibility, this feature needs to be used with `--no-default-features`.
//!
//! # Querying Delta Tables with Datafusion
//!
//! Querying from local filesystem:
//! ```ignore
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//!
//! async {
//!   let mut ctx = SessionContext::new();
//!   let table = deltalake::open_table("./tests/data/simple_table")
//!       .await
//!       .unwrap();
//!   ctx.register_table("demo", Arc::new(table)).unwrap();
//!
//!   let batches = ctx
//!       .sql("SELECT * FROM demo").await.unwrap()
//!       .collect()
//!       .await.unwrap();
//! };
//! ```

#![deny(warnings)]
#![deny(missing_docs)]
#![allow(rustdoc::invalid_html_tags)]

#[cfg(all(feature = "parquet", feature = "parquet2"))]
compile_error!(
    "Features parquet and parquet2 are mutually exclusive and cannot be enabled together"
);

#[cfg(all(feature = "s3", feature = "s3-native-tls"))]
compile_error!(
    "Features s3 and s3-native-tls are mutually exclusive and cannot be enabled together"
);

pub mod action;
pub mod builder;
pub mod data_catalog;
pub mod delta;
pub mod delta_config;
pub mod errors;
pub mod operations;
pub mod partitions;
pub mod schema;
pub mod storage;
pub mod table_state;
pub mod time_utils;

#[cfg(all(feature = "arrow"))]
pub mod table_state_arrow;

#[cfg(all(feature = "arrow", feature = "parquet"))]
pub mod delta_arrow;
#[cfg(feature = "datafusion")]
pub mod delta_datafusion;
#[cfg(all(feature = "arrow", feature = "parquet"))]
pub mod writer;

pub use self::builder::*;
pub use self::data_catalog::{get_data_catalog, DataCatalog, DataCatalogError};
pub use self::delta::*;
pub use self::delta_config::*;
pub use self::partitions::*;
pub use self::schema::*;
pub use errors::*;
pub use object_store::{path::Path, Error as ObjectStoreError, ObjectMeta, ObjectStore};
pub use operations::DeltaOps;

// convenience exports for consumers to avoid aligning crate versions
#[cfg(all(feature = "arrow", feature = "parquet"))]
pub use action::checkpoints;
#[cfg(feature = "arrow")]
pub use arrow;
#[cfg(feature = "datafusion")]
pub use datafusion;
#[cfg(feature = "parquet")]
pub use parquet;
#[cfg(feature = "parquet2")]
pub use parquet2;

// needed only for integration tests
// TODO can / should we move this into the test crate?
#[cfg(feature = "integration_test")]
pub mod test_utils;
