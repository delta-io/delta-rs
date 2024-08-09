use std::time::Duration;
use std::{error::Error, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow_select::concat::concat_batches;
use deltalake_core::errors::DeltaTableError;
use deltalake_core::kernel::{Action, DataType, PrimitiveType, StructField};
use deltalake_core::operations::optimize::{
    create_merge_plan, MetricDetails, Metrics, OptimizeType,
};
use deltalake_core::operations::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::operations::DeltaOps;
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::storage::ObjectStoreRef;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use deltalake_core::{DeltaTable, PartitionFilter, Path};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use rand::prelude::*;
use serde_json::json;
use tempfile::TempDir;

struct Context {
    pub tmp_dir: TempDir,
    pub table: DeltaTable,
}

async fn setup_test(partitioned: bool) -> Result<Context, Box<dyn Error>> {
    let columns = vec![
        StructField::new(
            "x".to_owned(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "y".to_owned(),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "date".to_owned(),
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
    ];

    let partition_columns = if partitioned {
        vec!["date".to_owned()]
    } else {
        vec![]
    };

    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();
    let dt = DeltaOps::try_from_uri(table_uri)
        .await?
        .create()
        .with_columns(columns)
        .with_partition_columns(partition_columns)
        .await?;

    Ok(Context { tmp_dir, table: dt })
}

fn generate_random_batch<T: Into<String>>(
    rows: usize,
    partition: T,
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut x_vec: Vec<i32> = Vec::with_capacity(rows);
    let mut y_vec: Vec<i32> = Vec::with_capacity(rows);
    let mut date_vec = Vec::with_capacity(rows);
    let mut rng = rand::thread_rng();
    let s = partition.into();

    for _ in 0..rows {
        x_vec.push(rng.gen());
        y_vec.push(rng.gen());
        date_vec.push(s.clone());
    }

    let x_array = Int32Array::from(x_vec);
    let y_array = Int32Array::from(y_vec);
    let date_array = StringArray::from(date_vec);

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("x", ArrowDataType::Int32, false),
            Field::new("y", ArrowDataType::Int32, false),
            Field::new("date", ArrowDataType::Utf8, false),
        ])),
        vec![Arc::new(x_array), Arc::new(y_array), Arc::new(date_array)],
    )?)
}

fn tuples_to_batch<T: Into<String>>(
    tuples: Vec<(i32, i32)>,
    partition: T,
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut x_vec: Vec<i32> = Vec::new();
    let mut y_vec: Vec<i32> = Vec::new();
    let mut date_vec = Vec::new();
    let s = partition.into();

    for t in tuples {
        x_vec.push(t.0);
        y_vec.push(t.1);
        date_vec.push(s.clone());
    }

    let x_array = Int32Array::from(x_vec);
    let y_array = Int32Array::from(y_vec);
    let date_array = StringArray::from(date_vec);

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("x", ArrowDataType::Int32, false),
            Field::new("y", ArrowDataType::Int32, false),
            Field::new("date", ArrowDataType::Utf8, false),
        ])),
        vec![Arc::new(x_array), Arc::new(y_array), Arc::new(date_array)],
    )?)
}

fn records_for_size(size: usize) -> usize {
    //12 bytes to account of overhead
    size / 12
}

#[tokio::test]
async fn test_optimize_non_partitioned_table() -> Result<(), Box<dyn Error>> {
    let context = setup_test(false).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 1), (2, 3), (2, 3)], "2022-05-23")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(3, 1), (3, 3), (3, 3)], "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(4, 1), (4, 3), (4, 3)], "2022-05-23")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(4_000_000), "2022-05-22")?,
    )
    .await?;

    let version = dt.version();
    assert_eq!(dt.get_files_count(), 5);

    let optimize = DeltaOps(dt).optimize().with_target_size(2_000_000);
    let (dt, metrics) = optimize.await?;

    assert_eq!(version + 1, dt.version());
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 4);
    assert_eq!(metrics.total_considered_files, 5);
    assert_eq!(metrics.partitions_optimized, 1);
    assert_eq!(dt.get_files_count(), 2);

    let commit_info = dt.history(None).await?;
    let last_commit = &commit_info[0];
    let parameters = last_commit.operation_parameters.clone().unwrap();
    assert_eq!(parameters["targetSize"], json!("2000000"));
    assert_eq!(parameters["predicate"], "[]");

    Ok(())
}

async fn write(
    writer: &mut RecordBatchWriter,
    table: &mut DeltaTable,
    batch: RecordBatch,
) -> Result<(), DeltaTableError> {
    writer.write(batch).await?;
    writer.flush_and_commit(table).await?;
    table.update().await?;
    Ok(())
}

#[tokio::test]
async fn test_optimize_with_partitions() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 1), (2, 3), (2, 3)], "2022-05-23")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(3, 1), (3, 3), (3, 3)], "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(4, 1), (4, 3), (4, 3)], "2022-05-23")?,
    )
    .await?;

    let version = dt.version();
    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = DeltaOps(dt).optimize().with_filters(&filter);
    let (dt, metrics) = optimize.await?;

    assert_eq!(version + 1, dt.version());
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);
    assert_eq!(dt.get_files_count(), 3);

    let partition_adds = dt
        .get_active_add_actions_by_partitions(&filter)?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(partition_adds.len(), 1);
    let partition_values = partition_adds[0].partition_values()?;
    assert_eq!(
        partition_values.get("date"),
        Some(&delta_kernel::expressions::Scalar::String(
            "2022-05-22".to_string()
        ))
    );

    Ok(())
}

#[tokio::test]
/// Validate that optimize fails when a remove action occurs
async fn test_conflict_for_remove_actions() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
    )
    .await?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 2), (2, 3), (2, 4)], "2022-05-22")?,
    )
    .await?;

    let version = dt.version();

    //create the merge plan, remove a file, and execute the plan.
    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];
    let plan = create_merge_plan(
        OptimizeType::Compact,
        dt.snapshot()?,
        &filter,
        None,
        WriterProperties::builder().build(),
    )?;

    let uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let other_dt = deltalake_core::open_table(uri).await?;
    let add = &other_dt.snapshot()?.log_data().into_iter().next().unwrap();
    let remove = add.remove_action(true);

    let operation = DeltaOperation::Delete { predicate: None };
    CommitBuilder::default()
        .with_actions(vec![Action::Remove(remove)])
        .build(Some(other_dt.snapshot()?), other_dt.log_store(), operation)
        .await?;

    let maybe_metrics = plan
        .execute(
            dt.log_store(),
            dt.snapshot()?,
            1,
            20,
            None,
            CommitProperties::default(),
        )
        .await;

    assert!(maybe_metrics.is_err());
    dt.update().await?;
    assert_eq!(dt.version(), version + 1);
    Ok(())
}

#[tokio::test]
/// Validate that optimize succeeds when only add actions occur for a optimized partition
async fn test_no_conflict_for_append_actions() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
    )
    .await?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 2), (2, 3), (2, 4)], "2022-05-22")?,
    )
    .await?;

    let version = dt.version();

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];
    let plan = create_merge_plan(
        OptimizeType::Compact,
        dt.snapshot()?,
        &filter,
        None,
        WriterProperties::builder().build(),
    )?;

    let uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let mut other_dt = deltalake_core::open_table(uri).await?;
    let mut writer = RecordBatchWriter::for_table(&other_dt)?;
    write(
        &mut writer,
        &mut other_dt,
        tuples_to_batch(vec![(3, 2), (3, 3), (3, 4)], "2022-05-22")?,
    )
    .await?;

    let metrics = plan
        .execute(
            dt.log_store(),
            dt.snapshot()?,
            1,
            20,
            None,
            CommitProperties::default(),
        )
        .await?;
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);

    dt.update().await.unwrap();
    assert_eq!(dt.version(), version + 2);
    Ok(())
}

#[tokio::test]
/// Validate that optimize creates multiple commits when min_commin_interval is set
async fn test_commit_interval() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    // expect it to perform 2 merges, one in each partition
    for partition in ["2022-05-22", "2022-05-23"] {
        for _i in 0..2 {
            write(
                &mut writer,
                &mut dt,
                tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], partition)?,
            )
            .await?;
        }
    }

    let version = dt.version();

    let plan = create_merge_plan(
        OptimizeType::Compact,
        dt.snapshot()?,
        &[],
        None,
        WriterProperties::builder().build(),
    )?;

    let metrics = plan
        .execute(
            dt.log_store(),
            dt.snapshot()?,
            1,
            20,
            Some(Duration::from_secs(0)), // this will cause as many commits as num_files_added
            CommitProperties::default(),
        )
        .await?;
    assert_eq!(metrics.num_files_added, 2);
    assert_eq!(metrics.num_files_removed, 4);

    dt.update().await.unwrap();
    assert_eq!(dt.version(), version + 2);
    Ok(())
}

#[tokio::test]
/// Validate that bin packing is idempotent.
async fn test_idempotent() -> Result<(), Box<dyn Error>> {
    //TODO: Compression makes it hard to get the target file size...
    //Maybe just commit files with a known size
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(6_000_000), "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(9_000_000), "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(2_000_000), "2022-05-22")?,
    )
    .await?;

    let version = dt.version();

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = DeltaOps(dt)
        .optimize()
        .with_filters(&filter)
        .with_target_size(10_000_000);
    let (dt, metrics) = optimize.await?;
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);
    assert_eq!(dt.version(), version + 1);

    let optimize = DeltaOps(dt)
        .optimize()
        .with_filters(&filter)
        .with_target_size(10_000_000);
    let (dt, metrics) = optimize.await?;

    assert_eq!(metrics.num_files_added, 0);
    assert_eq!(metrics.num_files_removed, 0);
    assert_eq!(dt.version(), version + 1);

    Ok(())
}

#[tokio::test]
/// Validate Metrics when no files are optimized
async fn test_idempotent_metrics() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(1_000_000), "2022-05-22")?,
    )
    .await?;

    let version = dt.version();
    let optimize = DeltaOps(dt).optimize().with_target_size(10_000_000);
    let (dt, metrics) = optimize.await?;

    let expected_metric_details = MetricDetails {
        min: 0,
        max: 0,
        avg: 0.0,
        total_files: 0,
        total_size: 0,
    };

    let expected = Metrics {
        num_files_added: 0,
        num_files_removed: 0,
        partitions_optimized: 0,
        num_batches: 0,
        total_considered_files: 1,
        total_files_skipped: 1,
        preserve_insertion_order: true,
        files_added: expected_metric_details.clone(),
        files_removed: expected_metric_details,
    };

    assert_eq!(expected, metrics);
    assert_eq!(version, dt.version());
    Ok(())
}

#[tokio::test]
/// Validate that multiple bins packing is idempotent.
async fn test_idempotent_with_multiple_bins() -> Result<(), Box<dyn Error>> {
    //TODO: Compression makes it hard to get the target file size...
    //Maybe just commit files with a known size
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(6_000_000), "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(3_000_000), "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(6_000_000), "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(3_000_000), "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(9_900_000), "2022-05-22")?,
    )
    .await?;

    let version = dt.version();

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = DeltaOps(dt)
        .optimize()
        .with_filters(&filter)
        .with_target_size(10_000_000);
    let (dt, metrics) = optimize.await?;
    assert_eq!(metrics.num_files_added, 2);
    assert_eq!(metrics.num_files_removed, 4);
    assert_eq!(dt.version(), version + 1);

    let optimize = DeltaOps(dt)
        .optimize()
        .with_filters(&filter)
        .with_target_size(10_000_000);
    let (dt, metrics) = optimize.await?;
    assert_eq!(metrics.num_files_added, 0);
    assert_eq!(metrics.num_files_removed, 0);
    assert_eq!(dt.version(), version + 1);

    Ok(())
}

#[tokio::test]
/// Validate operation data and metadata was written
async fn test_commit_info() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
    )
    .await?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 2), (2, 3), (2, 4)], "2022-05-22")?,
    )
    .await?;

    let version = dt.version();

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = DeltaOps(dt)
        .optimize()
        .with_target_size(2_000_000)
        .with_filters(&filter);
    let (dt, metrics) = optimize.await?;

    let commit_info = dt.history(None).await?;
    let last_commit = &commit_info[0];

    let commit_metrics =
        serde_json::from_value::<Metrics>(last_commit.info["operationMetrics"].clone())?;

    assert_eq!(commit_metrics, metrics);
    assert_eq!(last_commit.read_version, Some(version));
    let parameters = last_commit.operation_parameters.clone().unwrap();
    assert_eq!(parameters["targetSize"], json!("2000000"));
    assert_eq!(parameters["predicate"], "[\"date = '2022-05-22'\"]");

    Ok(())
}

#[tokio::test]
async fn test_zorder_rejects_zero_columns() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let dt = context.table;

    // Rejects zero columns
    let result = DeltaOps(dt)
        .optimize()
        .with_type(OptimizeType::ZOrder(vec![]))
        .await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Z-order requires at least one column"));
    Ok(())
}

#[tokio::test]
async fn test_zorder_rejects_nonexistent_columns() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let dt = context.table;

    // Rejects non-existent columns
    let result = DeltaOps(dt)
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["non-existent".to_string()]))
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains(
        "Z-order columns must be present in the table schema. Unknown columns: [\"non-existent\"]"
    ));
    Ok(())
}

#[tokio::test]
async fn test_zorder_rejects_partition_column() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
    )
    .await?;
    // Rejects partition columns
    let result = DeltaOps(dt)
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["date".to_string()]))
        .await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Z-order columns cannot be partition columns. Found: [\"date\"]"));

    Ok(())
}

#[tokio::test]
async fn test_zorder_unpartitioned() -> Result<(), Box<dyn Error>> {
    let context = setup_test(false).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 1), (1, 2), (1, 2)], "1970-01-01")?,
    )
    .await?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 1), (2, 2), (1, 2)], "1970-01-04")?,
    )
    .await?;

    let optimize = DeltaOps(dt).optimize().with_type(OptimizeType::ZOrder(vec![
        "date".to_string(),
        "x".to_string(),
        "y".to_string(),
    ]));
    let (dt, metrics) = optimize.await?;

    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);
    assert_eq!(metrics.total_files_skipped, 0);
    assert_eq!(metrics.total_considered_files, 2);

    // Check data
    let files = dt.get_files_iter()?.collect::<Vec<_>>();
    assert_eq!(files.len(), 1);

    let actual = read_parquet_file(&files[0], dt.object_store()).await?;
    let expected = RecordBatch::try_new(
        actual.schema(),
        // Note that the order is not hierarchically sorted.
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 1, 1, 1, 2])),
            Arc::new(Int32Array::from(vec![1, 1, 2, 2, 2, 2])),
            Arc::new(StringArray::from(vec![
                "1970-01-01",
                "1970-01-04",
                "1970-01-01",
                "1970-01-01",
                "1970-01-04",
                "1970-01-04",
            ])),
        ],
    )?;

    assert_eq!(actual, expected);

    Ok(())
}

#[tokio::test]
async fn test_zorder_partitioned() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    // Write data in sorted order. Each value is a power of 2, so will affect
    // a new bit in the z-ordering.
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 1), (1, 2), (1, 4)], "2022-05-22")?,
    )
    .await?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 1), (2, 2), (2, 4)], "2022-05-22")?,
    )
    .await?;

    // This batch doesn't matter; we just use it to test partition filtering
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 1), (1, 1), (1, 1)], "2022-05-23")?,
    )
    .await?;

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = DeltaOps(dt)
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["x".to_string(), "y".to_string()]))
        .with_filters(&filter);
    let (dt, metrics) = optimize.await?;

    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);

    // Check data
    let files = dt.get_files_by_partitions(&filter)?;
    assert_eq!(files.len(), 1);

    let actual = read_parquet_file(&files[0], dt.object_store()).await?;
    let expected = RecordBatch::try_new(
        actual.schema(),
        // Note that the order is not hierarchically sorted.
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 1, 2, 1, 2])),
            Arc::new(Int32Array::from(vec![1, 1, 2, 2, 4, 4])),
        ],
    )?;

    assert_eq!(actual, expected);

    Ok(())
}

#[tokio::test]
async fn test_zorder_respects_target_size() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(6_000_000), "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(9_000_000), "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_random_batch(records_for_size(2_000_000), "2022-05-22")?,
    )
    .await?;

    let optimize = DeltaOps(dt)
        .optimize()
        .with_writer_properties(
            WriterProperties::builder()
                .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                // Easier to hit the target size with a smaller row group size
                .set_max_row_group_size(64 * 1024)
                .build(),
        )
        .with_type(OptimizeType::ZOrder(vec!["x".to_string(), "y".to_string()]))
        .with_target_size(10_000_000);
    let (_, metrics) = optimize.await?;

    assert_eq!(metrics.num_files_added, 2);
    assert_eq!(metrics.num_files_removed, 3);

    // Allow going a little over the target size
    assert!(metrics.files_added.max < 11_000_000);

    Ok(())
}

async fn read_parquet_file(
    path: &Path,
    object_store: ObjectStoreRef,
) -> Result<RecordBatch, Box<dyn Error>> {
    let file = object_store.head(path).await?;
    let file_reader = ParquetObjectReader::new(object_store, file);
    let batches = ParquetRecordBatchStreamBuilder::new(file_reader)
        .await?
        .build()?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(concat_batches(&batches[0].schema(), &batches)?)
}
