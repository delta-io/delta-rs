use std::num::NonZeroU64;
use std::time::Duration;
use std::{error::Error, sync::Arc};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow_select::concat::concat_batches;
use datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::DeltaSessionContext;
use deltalake_core::ensure_table_uri;
use deltalake_core::errors::DeltaTableError;
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, DataType, PrimitiveType, StructField};
use deltalake_core::logstore::ObjectStoreRef;
use deltalake_core::operations::optimize::{
    MetricDetails, Metrics, OptimizeType, PlannerStrategy, create_merge_plan,
};
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use deltalake_core::{DeltaTable, PartitionFilter, Path};
use futures::TryStreamExt;
use object_store::ObjectStoreExt as _;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::prelude::*;
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

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
    let dt = DeltaTable::try_from_url(
        url::Url::from_directory_path(std::path::Path::new(table_uri)).unwrap(),
    )
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
    let mut rng = rand::rng();
    let s = partition.into();

    for _ in 0..rows {
        x_vec.push(rng.random());
        y_vec.push(rng.random());
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

fn generate_constant_batch<T: Into<String>>(
    rows: usize,
    value: i32,
    partition: T,
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut x_vec: Vec<i32> = Vec::with_capacity(rows);
    let mut y_vec: Vec<i32> = Vec::with_capacity(rows);
    let mut date_vec = Vec::with_capacity(rows);
    let s = partition.into();

    for _ in 0..rows {
        x_vec.push(value);
        y_vec.push(value.saturating_mul(2));
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

fn ordered_range_batch(
    start: i32,
    len: usize,
    partition: &str,
) -> Result<RecordBatch, Box<dyn Error>> {
    let x_values = (start..start + len as i32).collect::<Vec<_>>();
    let y_values = x_values.iter().map(|value| value * 10).collect::<Vec<_>>();
    let partitions = vec![partition.to_string(); len];

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("x", ArrowDataType::Int32, false),
            Field::new("y", ArrowDataType::Int32, false),
            Field::new("date", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(x_values)),
            Arc::new(Int32Array::from(y_values)),
            Arc::new(StringArray::from(partitions)),
        ],
    )?)
}

async fn active_file_ranges(table: &DeltaTable) -> Result<Vec<(i32, i32, i64)>, Box<dyn Error>> {
    let files = table
        .get_active_add_actions_by_partitions(&[])
        .try_collect::<Vec<_>>()
        .await?;
    let object_store = table.object_store();
    let mut ranges = Vec::with_capacity(files.len());

    for file in files {
        let path = match Path::parse(file.path().as_ref()) {
            Ok(path) => path,
            Err(_) => Path::from(file.path().as_ref()),
        };
        let size = object_store.head(&path).await?.size as i64;
        let batch = read_parquet_file(&path, object_store.clone()).await?;
        let x_values = batch
            .column_by_name("x")
            .ok_or_else(|| std::io::Error::other("missing x column"))?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| std::io::Error::other("x column is not Int32"))?;
        let min = x_values
            .iter()
            .flatten()
            .min()
            .ok_or_else(|| std::io::Error::other("empty parquet file"))?;
        let max = x_values
            .iter()
            .flatten()
            .max()
            .ok_or_else(|| std::io::Error::other("empty parquet file"))?;
        ranges.push((min, max, size));
    }

    ranges.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));

    Ok(ranges)
}

fn overlapping_bytes(ranges: &[(i32, i32, i64)], lower_bound: i32) -> i64 {
    ranges
        .iter()
        .filter(|(_, max, _)| *max >= lower_bound)
        .map(|(_, _, size)| *size)
        .sum()
}

fn overlap_is_suffix_like(ranges: &[(i32, i32, i64)], lower_bound: i32) -> bool {
    ranges
        .iter()
        .map(|(_, max, _)| *max >= lower_bound)
        .skip_while(|overlaps| !*overlaps)
        .all(|overlaps| overlaps)
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

    let version = dt.version().unwrap();
    assert_eq!(dt.snapshot().unwrap().log_data().num_files(), 5);

    let optimize = dt
        .optimize()
        .with_target_size(NonZeroU64::new(2_000_000).unwrap());
    let (dt, metrics) = optimize.await?;

    assert_eq!(version + 1, dt.version().unwrap());
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 4);
    assert_eq!(metrics.total_considered_files, 5);
    assert_eq!(metrics.partitions_optimized, 1);
    assert_eq!(dt.snapshot().unwrap().log_data().num_files(), 2);

    let commit_info: Vec<_> = dt.history(Some(1)).await?.collect();
    let last_commit = &commit_info[0];
    let parameters = last_commit.operation_parameters.clone().unwrap();
    assert_eq!(parameters["targetSize"], json!("2000000"));
    assert_eq!(parameters["predicate"], "[]");

    Ok(())
}

#[tokio::test]
async fn test_write_default_writer_properties_include_delta_rs_created_by()
-> Result<(), Box<dyn Error>> {
    let context = setup_test(false).await?;
    let dt = context.table;

    let dt = dt
        .write(vec![tuples_to_batch(
            vec![(1, 2), (1, 3), (1, 4)],
            "2022-05-22",
        )?])
        .await?;

    let files = dt.get_files_by_partitions(&[]).await?;
    assert_eq!(files.len(), 1);

    let metadata = read_parquet_metadata(&files[0], dt.object_store()).await?;
    let expected_created_by = format!("delta-rs version {}", deltalake_core::crate_version());
    assert_eq!(
        metadata.file_metadata().created_by(),
        Some(expected_created_by.as_str())
    );
    assert_eq!(
        metadata.row_group(0).column(0).compression(),
        Compression::SNAPPY
    );

    Ok(())
}

#[tokio::test]
async fn test_optimize_default_writer_properties_include_delta_rs_created_by()
-> Result<(), Box<dyn Error>> {
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
        tuples_to_batch(vec![(2, 1), (2, 3), (2, 4)], "2022-05-23")?,
    )
    .await?;

    let (dt, metrics) = dt
        .optimize()
        .with_target_size(NonZeroU64::new(1_000_000).unwrap())
        .await?;
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);

    let files = dt.get_files_by_partitions(&[]).await?;
    assert_eq!(files.len(), 1);

    let metadata = read_parquet_metadata(&files[0], dt.object_store()).await?;
    let expected_created_by = format!("delta-rs version {}", deltalake_core::crate_version());
    assert_eq!(
        metadata.file_metadata().created_by(),
        Some(expected_created_by.as_str())
    );
    assert!(
        matches!(
            metadata.row_group(0).column(0).compression(),
            Compression::ZSTD(_)
        ),
        "expected optimize to use ZSTD compression by default"
    );

    Ok(())
}

async fn write(
    writer: &mut RecordBatchWriter,
    table: &mut DeltaTable,
    batch: RecordBatch,
) -> Result<(), DeltaTableError> {
    writer.write(batch).await?;
    writer.flush_and_commit(table).await?;
    table.update_state().await?;
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

    let version = dt.version().unwrap();
    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = dt.optimize().with_filters(&filter);
    let (dt, metrics) = optimize.await?;

    assert_eq!(version + 1, dt.version().unwrap());
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);
    assert_eq!(dt.snapshot().unwrap().log_data().num_files(), 3);

    let partition_adds = dt
        .get_active_add_actions_by_partitions(&filter)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(partition_adds.len(), 1);
    let partition_values = partition_adds[0].partition_values().unwrap();
    let data_idx = partition_values
        .fields()
        .iter()
        .position(|field| field.name() == "date");
    assert_eq!(
        data_idx.map(|idx| &partition_values.values()[idx]),
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

    let version = dt.version().unwrap();

    let df_context: SessionContext = DeltaSessionContext::default().into();

    //create the merge plan, remove a file, and execute the plan.
    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];
    let plan = create_merge_plan(
        &dt.log_store(),
        OptimizeType::Compact,
        dt.snapshot()?.snapshot(),
        &filter,
        None,
        WriterProperties::builder().build(),
        df_context.state(),
    )
    .await?;

    let uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let table_url = ensure_table_uri(uri).unwrap();
    let other_dt = deltalake_core::open_table(table_url).await?;
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
            dt.snapshot()?.snapshot(),
            1,
            None,
            CommitProperties::default(),
            Uuid::new_v4(),
            None,
        )
        .await;

    assert!(maybe_metrics.is_err());
    dt.update_state().await?;
    assert_eq!(dt.version().unwrap(), version + 1);
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

    let version = dt.version().unwrap();

    let df_context: SessionContext = DeltaSessionContext::default().into();

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];
    let plan = create_merge_plan(
        &dt.log_store(),
        OptimizeType::Compact,
        dt.snapshot()?.snapshot(),
        &filter,
        None,
        WriterProperties::builder().build(),
        df_context.state(),
    )
    .await?;

    let uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let table_url = ensure_table_uri(uri).unwrap();
    let mut other_dt = deltalake_core::open_table(table_url).await?;
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
            dt.snapshot()?.snapshot(),
            1,
            None,
            CommitProperties::default(),
            Uuid::new_v4(),
            None,
        )
        .await?;
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);

    dt.update_state().await.unwrap();
    assert_eq!(dt.version().unwrap(), version + 2);
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

    let version = dt.version().unwrap();

    let context: SessionContext = DeltaSessionContext::default().into();

    let plan = create_merge_plan(
        &dt.log_store(),
        OptimizeType::Compact,
        dt.snapshot()?.snapshot(),
        &[],
        None,
        WriterProperties::builder().build(),
        context.state(),
    )
    .await?;

    let metrics = plan
        .execute(
            dt.log_store(),
            dt.snapshot()?.snapshot(),
            1,
            Some(Duration::from_secs(0)), // this will cause as many commits as num_files_added
            CommitProperties::default(),
            Uuid::new_v4(),
            None,
        )
        .await?;
    assert_eq!(metrics.num_files_added, 2);
    assert_eq!(metrics.num_files_removed, 4);

    dt.update_state().await.unwrap();
    assert_eq!(dt.version().unwrap(), version + 2);
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

    let version = dt.version().unwrap();

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = dt
        .optimize()
        .with_filters(&filter)
        .with_target_size(NonZeroU64::new(10_000_000).unwrap());
    let (dt, metrics) = optimize.await?;
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);
    assert_eq!(dt.version().unwrap(), version + 1);

    let optimize = dt
        .optimize()
        .with_filters(&filter)
        .with_target_size(NonZeroU64::new(10_000_000).unwrap());
    let (dt, metrics) = optimize.await?;

    assert_eq!(metrics.num_files_added, 0);
    assert_eq!(metrics.num_files_removed, 0);
    assert_eq!(dt.version().unwrap(), version + 1);

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
    let optimize = dt
        .optimize()
        .with_target_size(NonZeroU64::new(10_000_000).unwrap());
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
        planner_strategy: PlannerStrategy::PreserveLocality,
        preserved_stable_order: true,
        max_bin_span_files: 0,
        files_added: expected_metric_details.clone(),
        files_removed: expected_metric_details,
    };

    assert_eq!(expected, metrics);
    assert_eq!(version, dt.version());
    Ok(())
}

#[test]
fn test_legacy_optimize_metrics_deserialize_with_unknown_planner_strategy()
-> Result<(), Box<dyn Error>> {
    let legacy_metrics = json!({
        "numFilesAdded": 1,
        "numFilesRemoved": 2,
        "filesAdded": "{\"avg\":10.0,\"max\":10,\"min\":10,\"totalFiles\":1,\"totalSize\":10}",
        "filesRemoved": "{\"avg\":5.0,\"max\":8,\"min\":2,\"totalFiles\":2,\"totalSize\":10}",
        "partitionsOptimized": 1,
        "numBatches": 3,
        "totalConsideredFiles": 2,
        "totalFilesSkipped": 0,
        "preserveInsertionOrder": true
    });

    let metrics = serde_json::from_value::<Metrics>(legacy_metrics)?;

    assert_eq!(metrics.planner_strategy, PlannerStrategy::UnknownLegacy);
    assert!(metrics.preserve_insertion_order);
    assert!(!metrics.preserved_stable_order);
    assert_eq!(metrics.max_bin_span_files, 0);

    Ok(())
}

#[tokio::test]
async fn test_optimize_rejects_target_size_larger_than_i64_max() -> Result<(), Box<dyn Error>> {
    let context = setup_test(false).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 1), (1, 2), (1, 3)], "1970-01-01")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 1), (2, 2), (2, 3)], "1970-01-02")?,
    )
    .await?;

    let err = dt
        .optimize()
        .with_target_size(NonZeroU64::new(i64::MAX as u64 + 1).unwrap())
        .await
        .unwrap_err();

    assert!(err.to_string().contains("optimize target_size"), "{err:?}");
    assert!(err.to_string().contains("i64::MAX"), "{err:?}");

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

    let version = dt.version().unwrap();

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = dt
        .optimize()
        .with_filters(&filter)
        .with_target_size(NonZeroU64::new(10_000_000).unwrap());
    let (dt, metrics) = optimize.await?;
    assert_eq!(metrics.num_files_added, 2);
    assert_eq!(metrics.num_files_removed, 4);
    assert_eq!(dt.version().unwrap(), version + 1);

    let optimize = dt
        .optimize()
        .with_filters(&filter)
        .with_target_size(NonZeroU64::new(10_000_000).unwrap());
    let (dt, metrics) = optimize.await?;
    assert_eq!(metrics.num_files_added, 0);
    assert_eq!(metrics.num_files_removed, 0);
    assert_eq!(dt.version().unwrap(), version + 1);

    Ok(())
}

#[tokio::test]
/// Validate that compact rewrite does not re-split a selected bin by target size.
/// https://github.com/delta-io/delta-rs/issues/3855
async fn test_compact_rewrite_is_unbounded_and_idempotent() -> Result<(), Box<dyn Error>> {
    let context = setup_test(false).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?.with_writer_properties(
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(4).unwrap(),
            ))
            .build(),
    );

    write(
        &mut writer,
        &mut dt,
        generate_constant_batch(400_000, 1, "2022-05-22")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        generate_constant_batch(400_000, 2, "2022-05-22")?,
    )
    .await?;

    let source_adds: Vec<_> = dt.snapshot().unwrap().log_data().into_iter().collect();
    assert_eq!(source_adds.len(), 2);
    let source_total_size: u64 = source_adds
        .iter()
        .map(|add| u64::try_from(add.size()).unwrap())
        .sum();
    let target_size = NonZeroU64::new(source_total_size + 1).unwrap();

    let make_uncompressed_writer_properties = || {
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .set_max_row_group_size(64 * 1024)
            .build()
    };

    let (dt, first_metrics) = dt
        .optimize()
        .with_target_size(target_size)
        .with_writer_properties(make_uncompressed_writer_properties())
        .await?;
    assert_eq!(first_metrics.num_files_removed, 2);
    assert_eq!(
        first_metrics.num_files_added, 1,
        "Compact rewrite should not re-split a single bin by target size"
    );

    let (_, second_metrics) = dt
        .optimize()
        .with_target_size(target_size)
        .with_writer_properties(make_uncompressed_writer_properties())
        .await?;
    assert_eq!(second_metrics.num_files_added, 0);
    assert_eq!(second_metrics.num_files_removed, 0);

    Ok(())
}

#[tokio::test]
async fn test_compact_does_not_merge_across_skipped_large_file_gap() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let writer_properties = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .build();
    let mut writer = RecordBatchWriter::for_table(&dt)?.with_writer_properties(writer_properties);
    let partition = "2022-05-22";

    write(
        &mut writer,
        &mut dt,
        ordered_range_batch(0, 200, partition)?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        ordered_range_batch(1_000, 10_000, partition)?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        ordered_range_batch(20_000, 200, partition)?,
    )
    .await?;

    let before_ranges = active_file_ranges(&dt).await?;
    let small_total_size = u64::try_from(before_ranges[0].2 + before_ranges[2].2)?;
    let large_file_size = u64::try_from(before_ranges[1].2)?;
    let target_size = NonZeroU64::new(small_total_size + 1).unwrap();

    assert!(large_file_size > target_size.get(), "{before_ranges:?}");

    let (dt, metrics) = dt.optimize().with_target_size(target_size).await?;
    let after_ranges = active_file_ranges(&dt).await?;

    assert_eq!(metrics.num_files_added, 0);
    assert_eq!(metrics.num_files_removed, 0);
    assert_eq!(after_ranges, before_ranges);

    Ok(())
}

#[tokio::test]
async fn test_compact_preserves_tail_locality_after_small_recent_appends()
-> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let partition = "2022-05-22";
    let old_rows = 5_000;
    let recent_rows = 1_250;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    for batch_idx in 0..6 {
        write(
            &mut writer,
            &mut dt,
            ordered_range_batch(batch_idx * old_rows, old_rows as usize, partition)?,
        )
        .await?;
    }

    let base_ranges = active_file_ranges(&dt).await?;
    let base_size = base_ranges[0].2 as u64;
    let target_size = NonZeroU64::new(base_size * 2 + (base_size / 2)).unwrap();

    let (optimized, _) = dt.optimize().with_target_size(target_size).await?;
    dt = optimized;

    let recent_start = 6 * old_rows;
    let mut writer = RecordBatchWriter::for_table(&dt)?;
    for batch_idx in 0..2 {
        write(
            &mut writer,
            &mut dt,
            ordered_range_batch(
                recent_start + (batch_idx * recent_rows),
                recent_rows as usize,
                partition,
            )?,
        )
        .await?;
    }

    let (dt, _) = dt.optimize().with_target_size(target_size).await?;
    let ranges = active_file_ranges(&dt).await?;
    let overlap_bytes = overlapping_bytes(&ranges, recent_start);
    let tail_bytes: i64 = ranges.iter().rev().take(2).map(|(_, _, size)| *size).sum();

    assert!(overlap_is_suffix_like(&ranges, recent_start), "{ranges:?}");
    assert!(overlap_bytes <= tail_bytes, "{ranges:?}");

    Ok(())
}

#[tokio::test]
async fn test_compact_tail_overlap_is_suffix_like_after_repeated_compaction()
-> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let partition = "2022-05-22";
    let old_rows = 5_000;
    let recent_rows = 1_250;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    for batch_idx in 0..8 {
        write(
            &mut writer,
            &mut dt,
            ordered_range_batch(batch_idx * old_rows, old_rows as usize, partition)?,
        )
        .await?;
    }

    let base_ranges = active_file_ranges(&dt).await?;
    let base_size = base_ranges[0].2 as u64;
    let target_size = NonZeroU64::new(base_size * 2 + (base_size / 2)).unwrap();

    let (optimized, _) = dt.optimize().with_target_size(target_size).await?;
    dt = optimized;

    let mut next_start = 8 * old_rows;
    let mut last_recent_start = next_start;
    for _ in 0..3 {
        let mut writer = RecordBatchWriter::for_table(&dt)?;
        last_recent_start = next_start;
        for batch_idx in 0..2 {
            write(
                &mut writer,
                &mut dt,
                ordered_range_batch(
                    next_start + (batch_idx * recent_rows),
                    recent_rows as usize,
                    partition,
                )?,
            )
            .await?;
        }
        next_start += 2 * recent_rows;
        let (optimized, _) = dt.optimize().with_target_size(target_size).await?;
        dt = optimized;
    }

    let ranges = active_file_ranges(&dt).await?;
    let overlap_bytes = overlapping_bytes(&ranges, last_recent_start);
    let overlapping_files = ranges
        .iter()
        .filter(|(_, max, _)| *max >= last_recent_start)
        .count();
    let max_file_bytes = ranges
        .iter()
        .map(|(_, _, size)| *size)
        .max()
        .unwrap_or_default();

    assert!(overlapping_files <= 1, "{ranges:?}");
    assert!(overlap_bytes <= max_file_bytes, "{ranges:?}");

    Ok(())
}

#[tokio::test]
async fn test_compact_equal_sized_appends_remain_stable() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let partition = "2022-05-22";
    let base_rows = 5_000;
    let append_rows = 1_250;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    for batch_idx in 0..6 {
        write(
            &mut writer,
            &mut dt,
            ordered_range_batch(batch_idx * base_rows, base_rows as usize, partition)?,
        )
        .await?;
    }

    let base_ranges = active_file_ranges(&dt).await?;
    let base_size = base_ranges[0].2 as u64;
    let target_size = NonZeroU64::new(base_size * 2 + (base_size / 2)).unwrap();

    let (optimized, _) = dt.optimize().with_target_size(target_size).await?;
    dt = optimized;

    let recent_start = 6 * base_rows;
    let mut writer = RecordBatchWriter::for_table(&dt)?;
    for batch_idx in 0..4 {
        write(
            &mut writer,
            &mut dt,
            ordered_range_batch(
                recent_start + (batch_idx * append_rows),
                append_rows as usize,
                partition,
            )?,
        )
        .await?;
    }

    let (dt, _) = dt.optimize().with_target_size(target_size).await?;
    let ranges = active_file_ranges(&dt).await?;
    let mut sorted_ranges = ranges.clone();
    sorted_ranges.sort_by_key(|(min, _, _)| *min);

    assert!(
        sorted_ranges
            .windows(2)
            .all(|window| window[0].1 < window[1].0),
        "{ranges:?}"
    );
    assert!(
        overlap_is_suffix_like(&sorted_ranges, recent_start),
        "{ranges:?}"
    );

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

    let version = dt.version().unwrap();

    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];

    let optimize = dt
        .optimize()
        .with_target_size(NonZeroU64::new(2_000_000).unwrap())
        .with_filters(&filter);
    let (dt, metrics) = optimize.await?;

    let commit_info: Vec<_> = dt.history(Some(1)).await?.collect();
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
async fn test_optimize_metrics_expose_planner_strategy() -> Result<(), Box<dyn Error>> {
    let context = setup_test(false).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 1), (1, 2), (1, 3)], "1970-01-01")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 1), (2, 2), (2, 3)], "1970-01-02")?,
    )
    .await?;

    let (dt, metrics) = dt.optimize().await?;
    let metrics_json = serde_json::to_value(&metrics)?;

    assert_eq!(metrics_json["plannerStrategy"], json!("preserveLocality"));
    assert_eq!(metrics_json["preservedStableOrder"], json!(true));
    assert_eq!(metrics_json["preserveInsertionOrder"], json!(true));
    assert_eq!(metrics_json["maxBinSpanFiles"], json!(2));
    assert!(metrics_json.get("maxInputDisplacement").is_none());

    let commit_info: Vec<_> = dt.history(Some(1)).await?.collect();
    let last_commit = &commit_info[0];
    assert_eq!(
        last_commit.info["operationMetrics"]["plannerStrategy"],
        json!("preserveLocality")
    );

    Ok(())
}

#[tokio::test]
async fn test_optimize_zorder_metrics_do_not_claim_stable_order() -> Result<(), Box<dyn Error>> {
    let context = setup_test(false).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 1), (1, 2), (1, 3)], "1970-01-01")?,
    )
    .await?;
    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(2, 1), (2, 2), (2, 3)], "1970-01-02")?,
    )
    .await?;

    let (_, metrics) = dt
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["x".to_string()]))
        .await?;
    let metrics_json = serde_json::to_value(&metrics)?;

    assert_eq!(metrics_json["plannerStrategy"], json!("zOrder"));
    assert_eq!(metrics_json["preservedStableOrder"], json!(false));
    assert_eq!(metrics_json["preserveInsertionOrder"], json!(false));
    assert_eq!(metrics_json["maxBinSpanFiles"], json!(2));
    assert!(metrics_json.get("maxInputDisplacement").is_none());

    Ok(())
}

#[tokio::test]
async fn test_zorder_rejects_zero_columns() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let dt = context.table;

    // Rejects zero columns
    let result = dt.optimize().with_type(OptimizeType::ZOrder(vec![])).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Z-order requires at least one column")
    );
    Ok(())
}

#[tokio::test]
async fn test_zorder_rejects_nonexistent_columns() -> Result<(), Box<dyn Error>> {
    let context = setup_test(true).await?;
    let dt = context.table;

    // Rejects non-existent columns
    let result = dt
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["non-existent".to_string()]))
        .await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("field \"non-existent\" not found in schema")
    );
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
    let result = dt
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["date".to_string()]))
        .await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Z-order columns cannot be partition columns. Found: [\"date\"]")
    );

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

    let optimize = dt.optimize().with_type(OptimizeType::ZOrder(vec![
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
    let files = dt.get_files_by_partitions(&[]).await?;
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

    let optimize = dt
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["x".to_string(), "y".to_string()]))
        .with_filters(&filter);
    let (dt, metrics) = optimize.await?;

    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);

    // Check data
    let files = dt.get_files_by_partitions(&filter).await?;
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

    let optimize = dt
        .optimize()
        .with_writer_properties(
            WriterProperties::builder()
                .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                // Easier to hit the target size with a smaller row group size
                .set_max_row_group_size(64 * 1024)
                .build(),
        )
        .with_type(OptimizeType::ZOrder(vec!["x".to_string(), "y".to_string()]))
        .with_target_size(NonZeroU64::new(10_000_000).unwrap());
    let (_, metrics) = optimize.await?;

    assert_eq!(metrics.num_files_added, 2);
    assert_eq!(metrics.num_files_removed, 3);

    // Allow going a little over the target size
    assert!(metrics.files_added.max < 11_000_000);

    Ok(())
}

#[tokio::test]
async fn test_zorder_nested_columns() -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "meta",
            ArrowDataType::Struct(vec![Field::new("field_a", ArrowDataType::Int32, false)].into()),
            false,
        ),
        Field::new("value", ArrowDataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::StructArray::from(vec![(
                Arc::new(Field::new("field_a", ArrowDataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow_array::Array>,
            )])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::StructArray::from(vec![(
                Arc::new(Field::new("field_a", ArrowDataType::Int32, false)),
                Arc::new(Int32Array::from(vec![4, 5, 6])) as Arc<dyn arrow_array::Array>,
            )])),
            Arc::new(Int32Array::from(vec![40, 50, 60])),
        ],
    )?;

    let table = DeltaTable::new_in_memory()
        .write(vec![batch1])
        .with_save_mode(deltalake_core::protocol::SaveMode::Append)
        .await?;

    let table = table
        .write(vec![batch2])
        .with_save_mode(deltalake_core::protocol::SaveMode::Append)
        .await?;

    let (_, metrics) = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["meta.field_a".to_string()]))
        .await?;

    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);

    Ok(())
}

#[tokio::test]
async fn test_zorder_rejects_invalid_nested_path() -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "meta",
            ArrowDataType::Struct(vec![Field::new("field_a", ArrowDataType::Int32, false)].into()),
            false,
        ),
        Field::new("value", ArrowDataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::StructArray::from(vec![(
                Arc::new(Field::new("field_a", ArrowDataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow_array::Array>,
            )])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )?;

    // Non-existent nested field
    let table = DeltaTable::new_in_memory()
        .write(vec![batch.clone()])
        .with_save_mode(deltalake_core::protocol::SaveMode::Append)
        .await?;

    let result = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["meta.nonexistent".to_string()]))
        .await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("field \"nonexistent\" not found in schema")
    );

    // Non-struct intermediate field
    let table = DeltaTable::new_in_memory()
        .write(vec![batch])
        .with_save_mode(deltalake_core::protocol::SaveMode::Append)
        .await?;

    let result = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["value.sub".to_string()]))
        .await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("\"value\" is not a struct type")
    );

    Ok(())
}

async fn read_parquet_file(
    path: &Path,
    object_store: ObjectStoreRef,
) -> Result<RecordBatch, Box<dyn Error>> {
    let file = object_store.head(path).await?;
    let file_reader =
        ParquetObjectReader::new(object_store, path.clone()).with_file_size(file.size);
    let batches = ParquetRecordBatchStreamBuilder::new(file_reader)
        .await?
        .build()?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(concat_batches(&batches[0].schema(), &batches)?)
}

async fn read_parquet_metadata(
    path: &Path,
    object_store: ObjectStoreRef,
) -> Result<parquet::file::metadata::ParquetMetaData, Box<dyn Error>> {
    let file = object_store.head(path).await?;
    let file_reader =
        ParquetObjectReader::new(object_store, path.clone()).with_file_size(file.size);
    let builder = ParquetRecordBatchStreamBuilder::new(file_reader).await?;
    Ok(builder.metadata().as_ref().clone())
}
