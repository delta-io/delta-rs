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
//! - `s3` - enable the S3 storage backend to work with Delta Tables in AWS S3.
//! - `azure` - enable the Azure storage backend to work with Delta Tables in Azure Data Lake Storage Gen2 accounts.
//! - `datafusion-ext` - enable the `datafusion::datasource::TableProvider` trait implementation for Delta Tables, allowing them to be queried using [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion).

#![deny(warnings)]
#![deny(missing_docs)]

extern crate log;

extern crate arrow;
extern crate chrono;
extern crate lazy_static;
extern crate parquet;
extern crate regex;
extern crate serde;
#[cfg(test)]
#[macro_use]
extern crate serde_json;
extern crate thiserror;

pub mod action;
pub mod checkpoints;
mod delta;
pub mod delta_arrow;
pub mod partitions;
mod schema;
pub mod storage;
pub mod writer;

#[cfg(feature = "datafusion-ext")]
pub mod delta_datafusion;

#[cfg(feature = "rust-dataframe-ext")]
mod delta_dataframe;

pub use self::delta::*;
pub use self::partitions::*;
pub use self::schema::*;
pub use self::storage::{
    get_backend_for_uri, parse_uri, StorageBackend, StorageError, Uri, UriError,
};
