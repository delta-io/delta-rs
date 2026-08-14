//! Full-mode vacuum listing benchmarks.
//!
//! Fixture strategy: generate a multi-level partitioned Delta table **once**,
//! then dry-run full vacuum many times (parallel vs flat). Dry-run does not
//! delete, so the same table can be reused across samples.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Duration as ChronoDuration;
use deltalake_core::arrow;
use deltalake_core::kernel::{DataType, PrimitiveType, StructField, StructType};
use deltalake_core::operations::vacuum::{VacuumMetrics, VacuumMode};
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableBuilder, DeltaTableError};
use object_store::local::LocalFileSystem;
use url::Url;

use crate::latency_store::LatencyStore;

/// Parameters for the multi-level vacuum fixture.
///
/// Layout: partitions `date` (string YYYY-MM-DD) and `group` (string).
#[derive(Debug, Clone, Copy)]
pub struct VacuumFixtureParams {
    /// Number of distinct `date` partition values (one commit per day).
    pub days: usize,
    /// Number of distinct `group` values per day (`0..groups`).
    pub groups: usize,
    /// Rows written per (date, group) partition.
    pub rows_per_partition: usize,
}

impl Default for VacuumFixtureParams {
    fn default() -> Self {
        // ~30 * 500 = 15k leaf partitions — enough to exercise listing without
        // multi-hour generation.
        Self {
            days: 30,
            groups: 500,
            rows_per_partition: 1,
        }
    }
}

/// Whether full-mode vacuum uses the parallel multi-level scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VacuumScanMode {
    /// Default: parallel prefix expansion when the table has >1 partition cols.
    Parallel,
    /// Force flat `list(None)` via [`VacuumBuilder::disable_parallel_scan`].
    Flat,
}

impl VacuumScanMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Parallel => "parallel",
            Self::Flat => "flat",
        }
    }
}

impl std::fmt::Display for VacuumScanMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Marker file written next to a generated fixture for resume/skip.
const FIXTURE_META: &str = "vacuum_fixture_meta.txt";

/// True if `dir` already looks like a complete vacuum fixture.
pub fn fixture_exists(dir: &Path) -> bool {
    dir.join(FIXTURE_META).is_file() && dir.join("_delta_log").is_dir()
}

/// Generate (or skip if present) a multi-level partitioned table at `dir`.
///
/// This is partitioned by `date` and `group`.
/// One commit per day containing all groups for that day.
///
/// Depending on the size of dataset it may take a long time (hours).
/// For example: for 1095 days (`date`) and 5001 groups (`group`)
/// it will take 1 to 2 hours depending on the machine it's executed.
pub async fn generate_vacuum_fixture(
    dir: &Path,
    params: &VacuumFixtureParams,
    force: bool,
) -> DeltaResult<PathBuf> {
    if !force && fixture_exists(dir) {
        return Ok(fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf()));
    }

    if dir.exists() {
        fs::remove_dir_all(dir).map_err(|e| {
            DeltaTableError::generic(format!(
                "failed to clear fixture dir {}: {e}",
                dir.display()
            ))
        })?;
    }
    fs::create_dir_all(dir).map_err(|e| {
        DeltaTableError::generic(format!(
            "failed to create fixture dir {}: {e}",
            dir.display()
        ))
    })?;

    // Url::from_directory_path requires an absolute path; --out is often relative.
    let dir = fs::canonicalize(dir)
        .map_err(|e| DeltaTableError::generic(format!("canonicalize {}: {e}", dir.display())))?;

    let table_url = Url::from_directory_path(&dir)
        .map_err(|_| DeltaTableError::generic(format!("invalid table path: {}", dir.display())))?;

    let schema = StructType::try_new(vec![
        StructField::new("date", DataType::Primitive(PrimitiveType::String), false),
        StructField::new("group", DataType::Primitive(PrimitiveType::String), false),
        StructField::new("id", DataType::Primitive(PrimitiveType::Long), false),
        StructField::new("value", DataType::Primitive(PrimitiveType::String), true),
    ])?;

    let mut table = DeltaTable::try_from_url(table_url)
        .await?
        .create()
        .with_columns(schema.fields().cloned())
        .with_partition_columns(["date", "group"])
        .await?;

    let start_day = days_ago_ymd(params.days.saturating_sub(1) as i64);
    let mut global_id: i64 = 0;

    for day_idx in 0..params.days {
        let date = add_days(&start_day, day_idx as i64);
        let batch = day_batch(
            &date,
            params.groups,
            params.rows_per_partition,
            &mut global_id,
        )?;
        table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .with_partition_columns(["date", "group"])
            .await?;

        if day_idx == 0 || day_idx + 1 == params.days || (day_idx + 1) % 10 == 0 {
            eprintln!(
                "vacuum fixture: committed day {}/{} ({date})",
                day_idx + 1,
                params.days
            );
        }
    }

    let meta = format!(
        "days={}\ngroups={}\nrows_per_partition={}\npartitions_approx={}\n",
        params.days,
        params.groups,
        params.rows_per_partition,
        params.days.saturating_mul(params.groups)
    );
    fs::write(dir.join(FIXTURE_META), meta)
        .map_err(|e| DeltaTableError::generic(format!("failed to write fixture meta: {e}")))?;

    Ok(dir)
}

/// Open a previously generated fixture from the local filesystem.
pub async fn open_vacuum_fixture(dir: &Path) -> DeltaResult<DeltaTable> {
    open_vacuum_fixture_with_list_latency(dir, Duration::ZERO).await
}

/// Open a fixture, optionally wrapping the store so every LIST sleeps
/// `list_latency` (simulates cloud LIST RTT on local disk).
pub async fn open_vacuum_fixture_with_list_latency(
    dir: &Path,
    list_latency: Duration,
) -> DeltaResult<DeltaTable> {
    if !fixture_exists(dir) {
        return Err(DeltaTableError::generic(format!(
            "vacuum fixture not found at {} — run generate first",
            dir.display()
        )));
    }
    let dir = fs::canonicalize(dir)
        .map_err(|e| DeltaTableError::generic(format!("canonicalize {}: {e}", dir.display())))?;
    let table_url = Url::from_directory_path(&dir)
        .map_err(|_| DeltaTableError::generic(format!("invalid table path: {}", dir.display())))?;

    if list_latency.is_zero() {
        return DeltaTableBuilder::from_url(table_url)?.load().await;
    }

    // Root-level local FS (not prefix-stripped). Table location in the URL
    // selects the table path; LatencyStore only delays LIST-family calls.
    let store = Arc::new(LatencyStore::new(
        Arc::new(LocalFileSystem::new()),
        list_latency,
    ));
    DeltaTableBuilder::from_url(table_url.clone())?
        .with_storage_backend(store, table_url)
        .load()
        .await
}

/// Dry-run full vacuum once
///
/// Note: Clones `table` so the shared fixture can be reused across iterations.
pub async fn run_vacuum_full_dry_run(
    table: &DeltaTable,
    mode: VacuumScanMode,
    scan_concurrency: Option<usize>,
) -> DeltaResult<(DeltaTable, VacuumMetrics)> {
    let mut builder = table
        .clone()
        .vacuum()
        .with_mode(VacuumMode::Full)
        .with_dry_run(true)
        .with_enforce_retention_duration(false)
        // Listing-cost bench: retention 0. Live add files stay referenced so the
        // delete set should be empty on a clean fixture (no planted orphans).
        .with_retention_period(ChronoDuration::seconds(0));

    if let Some(n) = scan_concurrency {
        builder = builder.with_scan_concurrency(n);
    }
    if mode == VacuumScanMode::Flat {
        builder = builder.parallel_scan(false);
    }

    builder.await
}

fn day_batch(
    date: &str,
    groups: usize,
    rows_per_partition: usize,
    global_id: &mut i64,
) -> DeltaResult<arrow::record_batch::RecordBatch> {
    let n = groups.saturating_mul(rows_per_partition);
    let mut dates = Vec::with_capacity(n);
    let mut group_vals = Vec::with_capacity(n);
    let mut ids = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);

    for g in 0..groups {
        let group = g.to_string();
        for r in 0..rows_per_partition {
            dates.push(date.to_string());
            group_vals.push(group.clone());
            ids.push(*global_id);
            values.push(format!("v-{date}-{g}-{r}"));
            *global_id += 1;
        }
    }

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("date", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("group", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Utf8, true),
    ]));

    Ok(arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(dates)),
            Arc::new(arrow::array::StringArray::from(group_vals)),
            Arc::new(arrow::array::Int64Array::from(ids)),
            Arc::new(arrow::array::StringArray::from(values)),
        ],
    )?)
}

/// Calendar day string `YYYY-MM-DD` for `days_ago` days before today (UTC).
fn days_ago_ymd(days_ago: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let secs = now - days_ago.saturating_mul(86_400);
    format_utc_ymd(secs)
}

fn add_days(start_ymd: &str, days: i64) -> String {
    let start_secs = parse_utc_ymd(start_ymd).unwrap_or(0);
    format_utc_ymd(start_secs + days.saturating_mul(86_400))
}

fn parse_utc_ymd(s: &str) -> Option<i64> {
    let mut parts = s.split('-');
    let y: i32 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    Some(ymd_to_unix_days(y, m, d).saturating_mul(86_400))
}

fn format_utc_ymd(unix_secs: i64) -> String {
    let days = unix_secs.div_euclid(86_400);
    let (y, m, d) = unix_days_to_ymd(days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn ymd_to_unix_days(y: i32, m: u32, d: u32) -> i64 {
    let y = y as i64;
    let m = m as i64;
    let d = d as i64;
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn unix_days_to_ymd(days: i64) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

/// Default fixture directory under the benchmarks crate data folder.
pub fn default_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/vacuum_bench/d30_g500")
}
