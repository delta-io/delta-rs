//! The workload specs of the DAT corpus: one JSON file per workload, in `<table>/specs/`.
//!
//! Only the fields that say what to run are read. The expected results are not: the test
//! compares the executor with the kernel's default engine instead.

use std::path::Path;

use delta_kernel::Version;
use serde::Deserialize;
use url::Url;

/// One workload of a DAT table.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Spec {
    Read(ReadSpec),
    /// The protocol and metadata of a snapshot.
    Snapshot(TimeTravel),
    Cdf(CdfSpec),
    /// `write`, `checkpoint` and `crc`: these write to the table, which the test does not do.
    #[serde(other)]
    Write,
}

/// A table scan, optionally at a version, filtered, and projected.
#[derive(Debug, Deserialize)]
pub struct ReadSpec {
    #[serde(flatten)]
    pub at: TimeTravel,
    pub predicate: Option<String>,
    pub columns: Option<Vec<String>>,
}

/// A change data feed scan between two versions.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CdfSpec {
    pub start_version: Version,
    pub end_version: Option<Version>,
    pub predicate: Option<String>,
}

/// The table version a workload reads: the latest, a version, or the version as of a timestamp.
#[derive(Debug, Deserialize)]
pub struct TimeTravel {
    /// Signed: some workloads ask for a negative version and expect an error.
    pub version: Option<i64>,
    /// `yyyy-MM-dd HH:mm:ss.SSS`, in UTC.
    pub timestamp: Option<String>,
}

/// A spec file and the table it runs against.
#[derive(Debug)]
pub struct Workload {
    /// The spec's file name without `.json`.
    pub name: String,
    pub spec: Spec,
    pub table_root: Url,
}

impl Workload {
    /// Loads `<table>/specs/<workload>.json`; the table is `<table>/delta`.
    pub fn load(spec_path: &Path) -> Self {
        let name = spec_path.file_stem().unwrap().to_str().unwrap().to_string();
        let spec = std::fs::read_to_string(spec_path).unwrap();
        let spec = serde_json::from_str(&spec)
            .unwrap_or_else(|err| panic!("{}: {err}", spec_path.display()));
        let table_dir = spec_path.parent().unwrap().parent().unwrap().join("delta");
        let table_root = Url::from_directory_path(table_dir).unwrap();
        Self {
            name,
            spec,
            table_root,
        }
    }
}
