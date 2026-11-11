//! The DAT workload corpus (`make setup-dat`): the specs from [`deltalake_dat`], and what a
//! deltalake-core test needs to run them.

use std::future::Future;

use chrono::NaiveDateTime;
use delta_kernel::Version;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableBuilder, DeltaTableError};
pub use deltalake_dat::*;
use url::Url;

/// Loads the table at `table_root` at the version `at` asks for.
pub async fn load_table(table_root: &Url, at: &TimeTravel) -> DeltaResult<DeltaTable> {
    let mut builder = DeltaTableBuilder::from_url(table_root.clone())?;
    if let Some(version) = at.version {
        let version = Version::try_from(version)
            .map_err(|_| DeltaTableError::generic(format!("negative version {version}")))?;
        builder = builder.with_version(version);
    } else if let Some(timestamp) = &at.timestamp {
        let timestamp = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.f")
            .map_err(|err| DeltaTableError::generic(format!("{timestamp}: {err}")))?;
        builder = builder.with_timestamp(timestamp.and_utc());
    }
    builder.load().await
}

/// The component whose behavior makes a workload fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Cause {
    /// delta-kernel: kernel alone, on its default engine, fails the workload the same way.
    Kernel,
    /// deltalake-core: kernel alone does what the spec expects.
    Core,
    /// The test: DataFusion's SQL cannot run the workload's Spark SQL predicate.
    DataFusionSql,
}

impl std::fmt::Display for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Cause::Kernel => "delta-kernel",
            Cause::Core => "deltalake-core",
            Cause::DataFusionSql => "DataFusion SQL",
        })
    }
}

/// Workloads that fail for one reason.
pub struct KnownFailure {
    pub cause: Cause,
    /// What `cause` does that the spec does not expect.
    pub reason: &'static str,
    pub workloads: &'static [&'static str],
}

/// The workloads a test expects to fail.
pub type KnownFailures = &'static [KnownFailure];

/// Runs `check` for the workload `name`, which fails with an error or a panic.
///
/// A workload in `known_failures` has to fail still, so an entry goes once its bug is fixed.
pub async fn check_workload<F>(name: &str, known_failures: KnownFailures, check: F)
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let known_failure = known_failures
        .iter()
        .find(|failure| failure.workloads.contains(&name));
    let outcome = tokio::spawn(check).await;
    match (known_failure, outcome) {
        (None, Ok(Ok(()))) => {}
        (None, Ok(Err(err))) => panic!("{name}: {err}"),
        (None, Err(panic)) => std::panic::resume_unwind(panic.into_panic()),
        (Some(_), Ok(Err(_)) | Err(_)) => {}
        (Some(KnownFailure { cause, reason, .. }), Ok(Ok(()))) => {
            panic!("{name} passes now (cause {cause}: {reason}): remove it from the known failures")
        }
    }
}
