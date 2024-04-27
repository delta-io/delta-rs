//! Logical operators and physical executions for CDF

use arrow_schema::{DataType, Field, TimeUnit};
use lazy_static::lazy_static;
use std::collections::HashMap;

pub(crate) use scan::*;
pub(crate) use scan_utils::*;

use crate::kernel::{Add, AddCDCFile};

mod scan;
mod scan_utils;

/// Change type column name
pub const CHANGE_TYPE_COL: &str = "_change_type";
/// Commit version column name
pub const COMMIT_VERSION_COL: &str = "_commit_version";
/// Commit Timestamp column name
pub const COMMIT_TIMESTAMP_COL: &str = "_commit_timestamp";

lazy_static! {
    pub(crate) static ref CDC_PARTITION_SCHEMA: Vec<Field> = vec![
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true
        )
    ];
    pub(crate) static ref ADD_PARTITION_SCHEMA: Vec<Field> = vec![
        Field::new(CHANGE_TYPE_COL, DataType::Utf8, true),
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true
        ),
    ];
}

#[derive(Debug)]
pub(crate) struct CdcDataSpec<F: FileAction> {
    version: i64,
    timestamp: i64,
    actions: Vec<F>,
}

impl<F: FileAction> CdcDataSpec<F> {
    pub fn new(version: i64, timestamp: i64, actions: Vec<F>) -> Self {
        Self {
            version,
            timestamp,
            actions,
        }
    }
}

/// This trait defines a generic set of operations used by CDF Reader
pub trait FileAction {
    /// Adds partition values
    fn partition_values(&self) -> &HashMap<String, Option<String>>;
    /// Physical Path to the data
    fn path(&self) -> String;
    /// Byte size of the physical file
    fn size(&self) -> usize;
}

impl FileAction for Add {
    fn partition_values(&self) -> &HashMap<String, Option<String>> {
        &self.partition_values
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> usize {
        self.size as usize
    }
}

impl FileAction for AddCDCFile {
    fn partition_values(&self) -> &HashMap<String, Option<String>> {
        &self.partition_values
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> usize {
        self.size as usize
    }
}
