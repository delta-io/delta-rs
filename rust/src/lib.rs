//! Native Delta Lake implementation in Rust
//!
//! # Usage
//!
//! Load a Delta Table by path:
//!
//! ```rust
//! let table = deltalake::open_table("./tests/data/simple_table").unwrap();
//! println!(table.get_files());
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
extern crate serde_json;
extern crate thiserror;

#[cfg(feature = "dynamodb")]
#[macro_use]
extern crate maplit;

pub mod action;
mod delta;
mod delta_arrow;
pub mod partitions;
mod schema;
mod storage;

#[cfg(feature = "datafusion-ext")]
mod delta_datafusion;

#[cfg(feature = "rust-dataframe-ext")]
mod delta_dataframe;

pub use self::delta::*;
pub use self::partitions::*;
pub use self::schema::*;
pub use self::storage::*;
