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
//!   or Azure Data Lake Storage Gen2 (ADLS2). Use `s3-rustls` to use Rust TLS instead of native
//!   TLS implementation.
//! - `glue` - enable the Glue data catalog to work with Delta Tables with AWS Glue.
//! - `datafusion-ext` - enable the `datafusion::datasource::TableProvider` trait implementation
//!   for Delta Tables, allowing them to be queried using [DataFusion](https://github.com/apache/arrow-datafusion).
//!
//! # Querying Delta Tables with Datafusion
//!
//! Querying from local filesystem:
//! ```ignore
//! use std::sync::Arc;
//! use datafusion::execution::context::ExecutionContext;
//!
//! async {
//!   let mut ctx = ExecutionContext::new();
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
//!
//! It's important to note that the DataFusion library is evolving quickly, often with breaking api
//! changes, and this may cause compilation issues as a result.  If you are having issues with the most
//! recently released `delta-rs` you can set a specific branch or commit in your `Cargo.toml`.
//!
//! ```toml
//! datafusion = { git = "https://github.com/apache/arrow-datafusion.git", rev = "07bc2c754805f536fe1cd873dbe6adfc0a21cbb3" }
//! ```

#![deny(warnings)]
#![deny(missing_docs)]

pub mod action;
pub mod builder;
pub mod checkpoints;
pub mod data_catalog;
mod delta;
pub mod delta_arrow;
pub mod delta_config;
#[cfg(feature = "datafusion-ext")]
pub mod operations;
pub mod optimize;
pub mod partitions;
pub mod schema;
pub mod storage;
mod table_state;
pub mod time_utils;
pub mod vacuum;
pub mod writer;

#[cfg(feature = "datafusion-ext")]
pub mod delta_datafusion;

#[cfg(feature = "rust-dataframe-ext")]
mod delta_dataframe;

pub use self::builder::*;
pub use self::data_catalog::{get_data_catalog, DataCatalog, DataCatalogError};
pub use self::delta::*;
pub use self::partitions::*;
pub use self::schema::*;
pub use self::storage::StorageError;
pub use object_store::{path::Path, Error as ObjectStoreError, ObjectMeta, ObjectStore};

// convenience exports for consumers to avoid aligning crate versions
pub use arrow;
#[cfg(feature = "datafusion-ext")]
pub use datafusion;
