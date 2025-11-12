//! Delta Lake action types
//!
//! This crate contains the fundamental action types used in Delta Lake.
//! Actions are the fundamental unit of work in Delta Lake. Each action performs a single atomic
//! operation on the state of a Delta table. Actions are stored in the `_delta_log` directory of a
//! Delta table in JSON format. The log is a time series of actions that represent all the changes
//! made to a table.

mod actions;
mod serde_path;

pub use actions::*;
pub use delta_kernel::actions::{Metadata, Protocol};
