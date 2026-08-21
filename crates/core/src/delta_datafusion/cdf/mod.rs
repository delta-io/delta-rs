//! Logical operators and physical executions for CDF
use std::collections::HashMap;
use std::sync::LazyLock;

use arrow_schema::{DataType, Field, TimeUnit};

pub(crate) use self::scan_utils::*;
use crate::DeltaResult;
use crate::kernel::{Add, AddCDCFile, DeletionVectorDescriptor, Remove, Version};

/// Scan-related types and helpers for reading Change Data Feed (CDF) batches.
pub mod scan;
mod scan_utils;

/// Change type column name
pub const CHANGE_TYPE_COL: &str = "_change_type";
/// Commit version column name
pub const COMMIT_VERSION_COL: &str = "_commit_version";
/// Commit Timestamp column name
pub const COMMIT_TIMESTAMP_COL: &str = "_commit_timestamp";

pub(crate) static CDC_PARTITION_SCHEMA: LazyLock<Vec<Field>> = LazyLock::new(|| {
    vec![
        Field::new(COMMIT_VERSION_COL, DataType::UInt64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]
});
pub(crate) static ADD_PARTITION_SCHEMA: LazyLock<Vec<Field>> = LazyLock::new(|| {
    vec![
        Field::new(CHANGE_TYPE_COL, DataType::Utf8, true),
        Field::new(COMMIT_VERSION_COL, DataType::UInt64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]
});

/// Sometimes when a data file is updated after a delete commit the data file is not re-written, but
/// the deletion vector is updated. This manifests itself in the commit log as the data file being
/// removed (with a DV) and then added back with a new DV. In this case, the difference of the two
/// bitmaps is what is both deleted from this file and rows that haven't been touched.
#[derive(Debug)]
pub(crate) struct ResolvedPair {
    pub version: Version,
    pub timestamp: i64,
    pub add: Add,
    pub rm_dv: Option<DeletionVectorDescriptor>,
}

#[derive(Debug)]
pub(crate) struct CdcDataSpec<F: FileAction> {
    pub version: Version,
    pub timestamp: i64,
    pub actions: Vec<F>,
}

impl<F: FileAction> CdcDataSpec<F> {
    pub fn new(version: Version, timestamp: i64, actions: Vec<F>) -> Self {
        Self {
            version,
            timestamp,
            actions,
        }
    }

    pub fn into_parts(self) -> (Version, i64, Vec<F>) {
        (self.version, self.timestamp, self.actions)
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
    /// Possibly provide the deletion vector for the action
    fn deletion_vector(&self) -> Option<DeletionVectorDescriptor>;
    /// Whether this file action contains a deletion vector
    fn has_deletion_vector(&self) -> bool {
        false
    }
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

    fn deletion_vector(&self) -> Option<DeletionVectorDescriptor> {
        self.deletion_vector.clone()
    }

    fn has_deletion_vector(&self) -> bool {
        self.deletion_vector.is_some()
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

    fn deletion_vector(&self) -> Option<DeletionVectorDescriptor> {
        None
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
                _ => Err(crate::DeltaTableError::MetadataError(
                    "Remove action is missing required field: 'partition_values'".to_string(),
                )),
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
                _ => Err(crate::DeltaTableError::MetadataError(
                    "Remove action is missing required field: 'size'".to_string(),
                )),
            }
        }
    }

    fn deletion_vector(&self) -> Option<DeletionVectorDescriptor> {
        self.deletion_vector.clone()
    }

    fn has_deletion_vector(&self) -> bool {
        self.deletion_vector.is_some()
    }
}
