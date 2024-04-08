//! Logical operators and physical executions for CDF

use crate::kernel::{Add, AddCDCFile, CommitInfo, Remove};
use std::collections::HashMap;

mod scan;
mod scan_utils;

pub(crate) use scan::*;
pub(crate) use scan_utils::*;

#[derive(Debug)]
pub(crate) struct CdcDataSpec<F: FileAction> {
    version: i64,
    timestamp: i64,
    actions: Vec<F>,
    commit_info: Option<CommitInfo>,
}

impl<F: FileAction> CdcDataSpec<F> {
    pub fn new(
        version: i64,
        timestamp: i64,
        actions: Vec<F>,
        commit_info: Option<CommitInfo>,
    ) -> Self {
        Self {
            version,
            timestamp,
            actions,
            commit_info,
        }
    }
}

/// This trait defines a generic set of operations used by CDF Reader
pub trait FileAction {
    /// Adds partition values
    fn partition_values(&self) -> HashMap<String, Option<String>>;
    /// Physical Path to the data
    fn path(&self) -> String;
    /// Byte size of the physical file
    fn size(&self) -> usize;
}

impl FileAction for Add {
    fn partition_values(&self) -> HashMap<String, Option<String>> {
        self.partition_values.clone()
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> usize {
        self.size as usize
    }
}

impl FileAction for AddCDCFile {
    fn partition_values(&self) -> HashMap<String, Option<String>> {
        self.partition_values.clone()
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> usize {
        self.size as usize
    }
}

impl FileAction for Remove {
    fn partition_values(&self) -> HashMap<String, Option<String>> {
        self.partition_values.clone().unwrap_or_default()
    }
    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> usize {
        self.size.unwrap_or_default() as usize
    }
}
