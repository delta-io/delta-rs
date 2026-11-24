//! Test-only VACUUM statements and SQL views of their observable effects.
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use chrono::Duration;
use datafusion::prelude::SessionContext;
use deltalake_core::{DeltaTable, operations::vacuum::VacuumMode};
use deltalake_test::clock::TestClock;
use futures::TryStreamExt;

use super::{LabResult, Url, open_table};

pub(super) fn statement_mode(sql: &str) -> Option<VacuumMode> {
    let mut words = sql.trim().trim_end_matches(';').split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("vacuum")
        || !words.next()?.eq_ignore_ascii_case("actual")
    {
        return None;
    }
    let mode = words.next()?;
    match (mode.to_ascii_uppercase().as_str(), words.next()) {
        ("LITE", None) => Some(VacuumMode::Lite),
        ("FULL", None) => Some(VacuumMode::Full),
        _ => None,
    }
}

async fn parquet_keys(table: &DeltaTable) -> LabResult<Vec<String>> {
    let mut files = table.object_store().list(None);
    let mut keys = Vec::new();
    while let Some(file) = files.try_next().await? {
        let key = file.location.as_ref();
        if !key.starts_with("_delta_log/") && key.ends_with(".parquet") {
            keys.push(key.to_owned());
        }
    }
    Ok(keys)
}

pub(super) async fn run(ctx: &SessionContext, uri: &Url, mode: VacuumMode) -> LabResult<()> {
    let table = open_table(uri.clone()).await?;
    let before = parquet_keys(&table).await?;
    let start_version = table.version();
    // Expire the fixture's tombstones while honoring the default seven-day retention.
    let clock = TestClock::from_systemtime();
    clock.tick(Duration::days(8));
    let (table, metrics) = table
        .vacuum()
        .with_mode(mode)
        .with_clock(Arc::new(clock))
        .with_dry_run(false)
        .await?;
    let after = parquet_keys(&table).await?;
    let mut sources = Vec::new();
    let mut paths = Vec::new();
    for (source, keys) in [
        ("before", before),
        ("after", after),
        ("reported", metrics.files_deleted),
    ] {
        sources.extend(std::iter::repeat_n(source, keys.len()));
        paths.extend(keys);
    }
    let files = RecordBatch::try_from_iter([
        ("source", Arc::new(StringArray::from(sources)) as ArrayRef),
        ("path", Arc::new(StringArray::from(paths)) as ArrayRef),
    ])?;

    let counts = if table.version() == start_version {
        None // No candidates, so delta-rs did not create a commit.
    } else {
        let mut end = table
            .history(Some(1))
            .try_next()
            .await?
            .ok_or("missing VACUUM END commit")?;
        if end.operation.as_deref() != Some("VACUUM END") {
            return Err(format!("expected VACUUM END, found {:?}", end.operation).into());
        }
        Some(
            end.info
                .remove("operationMetrics")
                .ok_or("missing VACUUM END metrics")?,
        )
    };
    let summary = RecordBatch::try_from_iter(
        [
            ("deleted", "numDeletedFiles"),
            ("already_absent", "numAlreadyAbsentFiles"),
        ]
        .map(|(name, metric)| {
            let values = counts.iter().map(|row| row[metric].as_i64());
            (name, Arc::new(Int64Array::from_iter(values)) as ArrayRef)
        }),
    )?;
    for (name, batch) in [("vacuum_files", files), ("vacuum_end", summary)] {
        ctx.deregister_table(name)?;
        ctx.register_batch(name, batch)?;
    }
    Ok(())
}
