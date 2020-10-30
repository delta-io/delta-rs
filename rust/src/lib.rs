#![deny(warnings)]

#[macro_use]
extern crate log;

extern crate arrow;
extern crate chrono;
extern crate parquet;
extern crate regex;
extern crate serde;
extern crate serde_json;
extern crate thiserror;

#[cfg(feature = "rust-dataframe-ext")]
extern crate rust_dataframe;

pub mod action;
mod delta;
mod schema;
mod storage;

#[cfg(feature = "datafusion-ext")]
mod delta_datafusion;

#[cfg(feature = "rust-dataframe-ext")]
mod delta_dataframe;

pub use self::delta::*;
#[cfg(feature = "rust-dataframe-ext")]
pub use self::delta_dataframe::*;
pub use self::schema::*;
pub use self::storage::*;
