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
