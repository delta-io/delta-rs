//! Logical operators and physical executions for CDF
use std::collections::HashMap;

use arrow_schema::{DataType, Field, TimeUnit};
use lazy_static::lazy_static;

pub(crate) use self::scan::*;
pub(crate) use self::scan_utils::*;
use crate::kernel::{Add, AddCDCFile, Remove};
use crate::DeltaResult;

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
    fn partition_values(&self) -> DeltaResult<&HashMap<String, Option<String>>>;
    /// Physical Path to the data
    fn path(&self) -> String;
    /// Byte size of the physical file
    fn size(&self) -> DeltaResult<usize>;
}

impl FileAction for Add {
    fn partition_values(&self) -> DeltaResult<&HashMap<String, Option<String>>> {
        Ok(&self.partition_values)
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> DeltaResult<usize> {
        Ok(self.size as usize)
    }
}

impl FileAction for AddCDCFile {
    fn partition_values(&self) -> DeltaResult<&HashMap<String, Option<String>>> {
        Ok(&self.partition_values)
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> DeltaResult<usize> {
        Ok(self.size as usize)
    }
}

impl FileAction for Remove {
    fn partition_values(&self) -> DeltaResult<&HashMap<String, Option<String>>> {
        // If extended_file_metadata is true, it should be required to have this filled in
        if self.extended_file_metadata.unwrap_or_default() {
            Ok(self.partition_values.as_ref().unwrap())
        } else {
            match self.partition_values {
                Some(ref part_map) => Ok(part_map),
                _ => Err(crate::DeltaTableError::Protocol {
                    source: crate::protocol::ProtocolError::InvalidField(
                        "partition_values".to_string(),
                    ),
                }),
            }
        }
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> DeltaResult<usize> {
        // If extended_file_metadata is true, it should be required to have this filled in
        if self.extended_file_metadata.unwrap_or_default() {
            Ok(self.size.unwrap() as usize)
        } else {
            match self.size {
                Some(size) => Ok(size as usize),
                _ => Err(crate::DeltaTableError::Protocol {
                    source: crate::protocol::ProtocolError::InvalidField("size".to_string()),
                }),
            }
        }
    }
}
