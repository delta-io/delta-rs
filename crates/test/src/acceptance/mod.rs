//! The DAT workload corpus (`make setup-dat`): one JSON spec per workload in `<table>/specs/`,
//! run against the table in `<table>/delta`.

use std::future::Future;
use std::path::Path;

use chrono::NaiveDateTime;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::Version;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableBuilder, DeltaTableError};
use serde::Deserialize;
use url::Url;

/// One workload of a DAT table.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Spec {
    Read(ReadSpec),
    Snapshot(SnapshotSpec),
    /// `cdf`, `write`, `checkpoint` and `crc`.
    #[serde(other)]
    Other,
}

/// A table scan, optionally at a version, filtered, and projected.
#[derive(Debug, Deserialize)]
pub struct ReadSpec {
    #[serde(flatten)]
    pub at: TimeTravel,
    /// A Spark SQL predicate.
    pub predicate: Option<String>,
    pub columns: Option<Vec<String>>,
    /// `None` when the read is expected to fail.
    pub expected: Option<ReadExpected>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadExpected {
    pub row_count: usize,
}

/// The protocol and metadata of a snapshot.
#[derive(Debug, Deserialize)]
pub struct SnapshotSpec {
    #[serde(flatten)]
    pub at: TimeTravel,
    /// `None` when loading the snapshot is expected to fail.
    pub expected: Option<Box<SnapshotExpected>>,
}

#[derive(Debug, Deserialize)]
pub struct SnapshotExpected {
    pub protocol: Protocol,
    pub metadata: Metadata,
}

/// The table version a workload reads: the latest, a version, or the version as of a timestamp.
#[derive(Debug, Deserialize)]
pub struct TimeTravel {
    /// Signed: some workloads ask for a negative version and expect an error.
    pub version: Option<i64>,
    /// `yyyy-MM-dd HH:mm:ss.SSS`, in UTC.
    pub timestamp: Option<String>,
}

impl TimeTravel {
    /// Loads the table at `table_root` at this version.
    pub async fn load(&self, table_root: &Url) -> DeltaResult<DeltaTable> {
        let mut builder = DeltaTableBuilder::from_url(table_root.clone())?;
        if let Some(version) = self.version {
            let version = Version::try_from(version)
                .map_err(|_| DeltaTableError::generic(format!("negative version {version}")))?;
            builder = builder.with_version(version);
        } else if let Some(timestamp) = &self.timestamp {
            let timestamp = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.f")
                .map_err(|err| DeltaTableError::generic(format!("{timestamp}: {err}")))?;
            builder = builder.with_timestamp(timestamp.and_utc());
        }
        builder.load().await
    }
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
    /// Loads `<table>/specs/<workload>.json`.
    pub fn load(spec_path: &Path) -> Self {
        let name = spec_path.file_stem().unwrap().to_str().unwrap().to_string();
        let spec = std::fs::read_to_string(spec_path).unwrap();
        let spec = serde_json::from_str(&spec)
            .unwrap_or_else(|err| panic!("{}: {err}", spec_path.display()));
        let table_dir = spec_path.parent().unwrap().parent().unwrap().join("delta");
        let table_root = Url::from_directory_path(std::path::absolute(table_dir).unwrap()).unwrap();
        Self {
            name,
            spec,
            table_root,
        }
    }
}

/// Workloads that are known to fail, by reason.
pub type KnownFailures = &'static [(&'static str, &'static [&'static str])];

/// Runs `check` for the workload `name`, which fails with an error or a panic.
///
/// A workload in `known_failures` has to fail still, so an entry goes once its bug is fixed.
pub async fn check_workload<F>(name: &str, known_failures: KnownFailures, check: F)
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let known_failure = known_failures
        .iter()
        .find(|(_, workloads)| workloads.contains(&name))
        .map(|(reason, _)| *reason);
    let outcome = tokio::spawn(check).await;
    match (known_failure, outcome) {
        (None, Ok(Ok(()))) => {}
        (None, Ok(Err(err))) => panic!("{name}: {err}"),
        (None, Err(panic)) => std::panic::resume_unwind(panic.into_panic()),
        (Some(_), Ok(Err(_)) | Err(_)) => {}
        (Some(reason), Ok(Ok(()))) => {
            panic!("{name} passes now ({reason}): remove it from the known failures")
        }
    }
}
