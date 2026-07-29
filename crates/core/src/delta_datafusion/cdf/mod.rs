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

#[derive(Debug)]
pub(crate) struct CdcDataSpec<F: FileAction> {
    version: Version,
    timestamp: i64,
    actions: Vec<F>,
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

/// Depending on the kind of file action we build the access plan differently. Adds are a deletion
/// vector while deletes are a selection vector. For CDCFiles we don't build anything
#[derive(Debug)]
pub enum FileActionType {
    /// An add action (and deletion vector)
    Add,
    /// A delete action (and selection vector)
    Delete,
    /// We do nothing with this variant
    CdcFile,
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
    /// Return what variant of file action this action is
    fn action_type(&self) -> FileActionType;
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

    fn action_type(&self) -> FileActionType {
        FileActionType::Add
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

    fn action_type(&self) -> FileActionType {
        FileActionType::CdcFile
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

    fn action_type(&self) -> FileActionType {
        FileActionType::Delete
    }

    fn has_deletion_vector(&self) -> bool {
        self.deletion_vector.is_some()
    }
}
