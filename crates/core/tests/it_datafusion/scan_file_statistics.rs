//! Regression tests for #4522: `DeltaScanNext` should not attach per-file column
//! statistics when a scan has no predicate (nothing to prune on), but must keep them
//! when a predicate is present so file pruning is preserved.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::datasource::TableProvider;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionContext, col, lit};
use deltalake_core::DeltaTable;
use deltalake_core::delta_datafusion::{DeltaScanConfig, DeltaScanNext};
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::protocol::SaveMode;
use deltalake_test::TestResult;

const NUM_COLUMNS: usize = 20;
const NUM_FILES: usize = 6;

fn wide_delta_columns() -> Vec<StructField> {
    (0..NUM_COLUMNS)
        .map(|i| {
            StructField::new(
                format!("c{i}"),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )
        })
        .collect()
}

fn wide_batch(seed: i32) -> TestResult<RecordBatch> {
    let schema = ArrowSchema::new(
        (0..NUM_COLUMNS)
            .map(|i| ArrowField::new(format!("c{i}"), ArrowDataType::Int32, true))
            .collect::<Vec<_>>(),
    );
    let columns: Vec<ArrayRef> = (0..NUM_COLUMNS)
        .map(|i| {
            let base = seed + i as i32;
            Arc::new(Int32Array::from(vec![base, base + 1])) as ArrayRef
        })
        .collect();
    Ok(RecordBatch::try_new(Arc::new(schema), columns)?)
}

async fn wide_table_with_many_files() -> TestResult<DeltaTable> {
    let mut table = DeltaTable::new_in_memory()
        .create()
        .with_columns(wide_delta_columns())
        .await?;
    for file in 0..NUM_FILES {
        table = table
            .write(vec![wide_batch(file as i32 * 100)?])
            .with_save_mode(SaveMode::Append)
            .await?;
    }
    Ok(table)
}

/// Counts statistics entries, allocated slots, and files in the physical plan.
fn count_per_file_column_stats(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize, usize) {
    let mut total_column_stats = 0usize;
    let mut total_capacity = 0usize;
    let mut files = 0usize;
    collect(
        plan,
        &mut total_column_stats,
        &mut total_capacity,
        &mut files,
    );
    (total_column_stats, total_capacity, files)
}

fn collect(
    plan: &Arc<dyn ExecutionPlan>,
    total_column_stats: &mut usize,
    total_capacity: &mut usize,
    files: &mut usize,
) {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
    {
        for group in config.file_groups.iter() {
            for partitioned_file in group.files() {
                *files += 1;
                if let Some(stats) = &partitioned_file.statistics {
                    *total_column_stats += stats.column_statistics.len();
                    *total_capacity += stats.column_statistics.capacity();
                }
            }
        }
    }
    for child in plan.children() {
        collect(child, total_column_stats, total_capacity, files);
    }
}

#[tokio::test]
async fn no_predicate_scan_attaches_no_per_file_column_statistics() -> TestResult {
    let table = wide_table_with_many_files().await?;
    let session = SessionContext::new().state();
    table.update_datafusion_session(&session)?;

    let provider = DeltaScanNext::new(
        table.snapshot()?.snapshot().clone(),
        DeltaScanConfig::default(),
    )?;
    let plan = provider.scan(&session, None, &[], None).await?;

    let (total_column_stats, _, files) = count_per_file_column_stats(&plan);
    assert_eq!(files, NUM_FILES, "expected one planned file per append");
    assert_eq!(
        total_column_stats, 0,
        "a no-predicate scan should not allocate per-file column statistics; \
         got {total_column_stats} column-stat entries across {files} files"
    );
    Ok(())
}

#[tokio::test]
async fn predicate_scan_retains_statistics_for_pruning() -> TestResult {
    let table = wide_table_with_many_files().await?;
    let session = SessionContext::new().state();
    table.update_datafusion_session(&session)?;

    let provider = DeltaScanNext::new(
        table.snapshot()?.snapshot().clone(),
        DeltaScanConfig::default(),
    )?;
    // A predicate on `c0` must keep per-file statistics so file skipping still works.
    let predicate = col("c0").gt(lit(50i32));
    let plan = provider.scan(&session, None, &[predicate], None).await?;

    let (total_column_stats, total_capacity, files) = count_per_file_column_stats(&plan);
    assert!(
        total_column_stats > 0,
        "a predicate scan must retain per-file statistics for pruning; \
         got {total_column_stats} column-stat entries across {files} files"
    );
    assert_eq!(
        total_capacity, total_column_stats,
        "per-file column statistics should keep no spare capacity; \
         got {total_capacity} slots for {total_column_stats} entries across {files} files"
    );
    Ok(())
}
