//! Logical operators and physical executions for CDF

use std::collections::HashMap;

pub(crate) use scan::*;
pub(crate) use scan_utils::*;

use crate::kernel::{Add, AddCDCFile};

mod scan;
mod scan_utils;

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
