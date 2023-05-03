#![cfg(all(feature = "arrow", feature = "parquet"))]

use arrow::datatypes::Schema as ArrowSchema;
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use deltalake::action::{Action, Remove};
use deltalake::operations::optimize::{create_merge_plan, MetricDetails, Metrics};
use deltalake::operations::DeltaOps;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{DeltaTable, DeltaTableError, PartitionFilter, SchemaDataType, SchemaField};
use parquet::file::properties::WriterProperties;
use rand::prelude::*;
use serde_json::json;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::{collections::HashMap, error::Error, sync::Arc};
use tempdir::TempDir;

struct Context {
    pub tmp_dir: TempDir,
    pub table: DeltaTable,
}

async fn setup_test(partitioned: bool) -> Result<Context, Box<dyn Error>> {
    let columns = vec![
        SchemaField::new(
            "x".to_owned(),
            SchemaDataType::primitive("integer".to_owned()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "y".to_owned(),
            SchemaDataType::primitive("integer".to_owned()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "date".to_owned(),
            SchemaDataType::primitive("string".to_owned()),
            false,
            HashMap::new(),
        ),
    ];

    let partition_columns = if partitioned {
        vec!["date".to_owned()]
    } else {
        vec![]
    };

    let tmp_dir = tempdir::TempDir::new("opt_table").unwrap();
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
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("date", DataType::Utf8, false),
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
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("date", DataType::Utf8, false),
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
    assert_eq!(dt.get_state().files().len(), 5);

    let optimize = DeltaOps(dt).optimize().with_target_size(2_000_000);
    let (dt, metrics) = optimize.await?;

    assert_eq!(version + 1, dt.version());
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 4);
    assert_eq!(metrics.total_considered_files, 5);
    assert_eq!(metrics.partitions_optimized, 1);
    assert_eq!(dt.get_state().files().len(), 2);

    Ok(())
}

async fn write(
    writer: &mut RecordBatchWriter,
    table: &mut DeltaTable,
    batch: RecordBatch,
) -> Result<(), DeltaTableError> {
    writer.write(batch).await?;
    writer.flush_and_commit(table).await?;
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
    assert_eq!(dt.get_state().files().len(), 3);

    Ok(())
}

#[tokio::test]
#[ignore]
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
        &dt.state,
        &filter,
        None,
        WriterProperties::builder().build(),
    )?;

    let uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let mut other_dt = deltalake::open_table(uri).await?;
    let add = &other_dt.get_state().files()[0];
    let remove = Remove {
        path: add.path.clone(),
        deletion_timestamp: Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        ),
        data_change: true,
        extended_file_metadata: None,
        size: Some(add.size),
        partition_values: Some(add.partition_values.clone()),
        tags: Some(HashMap::new()),
    };

    #[allow(deprecated)]
    let mut transaction = other_dt.create_transaction(None);
    transaction.add_action(Action::remove(remove));
    transaction.commit(None, None).await?;

    let maybe_metrics = plan.execute(dt.object_store(), &dt.state).await;
    assert!(maybe_metrics.is_err());
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

    //create the merge plan, remove a file, and execute the plan.
    let filter = vec![PartitionFilter::try_from(("date", "=", "2022-05-22"))?];
    let plan = create_merge_plan(
        &dt.state,
        &filter,
        None,
        WriterProperties::builder().build(),
    )?;

    let uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let mut other_dt = deltalake::open_table(uri).await?;
    let mut writer = RecordBatchWriter::for_table(&other_dt)?;
    write(
        &mut writer,
        &mut other_dt,
        tuples_to_batch(vec![(3, 2), (3, 3), (3, 4)], "2022-05-22")?,
    )
    .await?;

    let metrics = plan.execute(dt.object_store(), &dt.state).await?;
    assert_eq!(metrics.num_files_added, 1);
    assert_eq!(metrics.num_files_removed, 2);

    dt.update().await.unwrap();
    assert_eq!(dt.version(), version + 2);
    Ok(())
}

#[tokio::test]
#[ignore]
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
    let (mut dt, metrics) = optimize.await?;

    let commit_info = dt.history(None).await?;
    let last_commit = &commit_info[commit_info.len() - 1];

    let commit_metrics =
        serde_json::from_value::<Metrics>(last_commit.info["operationMetrics"].clone())?;

    assert_eq!(commit_metrics, metrics);
    assert_eq!(last_commit.read_version, Some(version));
    let parameters = last_commit.operation_parameters.clone().unwrap();
    assert_eq!(parameters["targetSize"], json!("2000000"));
    // TODO: Requires a string representation for PartitionFilter
    // assert_eq!(parameters["predicate"], None);

    Ok(())
}
