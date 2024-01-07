use crate::kernel::{Add, AddCDCFile, CommitInfo, Remove};
use std::collections::HashMap;

pub mod scan;
mod scan_utils;

pub use scan::DeltaCdfScan;

#[derive(Debug)]
pub struct CdcDataSpec<F: FileAction> {
    version: i64,
    timestamp: i64,
    actions: Vec<F>,
    commit_info: Option<CommitInfo>,
}

pub trait FileAction {
    fn partition_values(&self) -> HashMap<String, Option<String>>;
    fn path(&self) -> String;
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
