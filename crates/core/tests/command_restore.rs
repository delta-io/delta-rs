use arrow::datatypes::Schema as ArrowSchema;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field};
use chrono::DateTime;
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::protocol::SaveMode;
use deltalake_core::storage::commit_uri_from_version;
use deltalake_core::{DeltaOps, DeltaTable};
use itertools::Itertools;
use rand::Rng;
use std::error::Error;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[derive(Debug)]
struct Context {
    pub tmp_dir: TempDir,
    pub table: DeltaTable,
}

async fn setup_test() -> Result<Context, Box<dyn Error>> {
    let columns = vec![
        StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "value".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            true,
        ),
    ];

    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();
    let table = DeltaOps::try_from_uri(table_uri)
        .await?
        .create()
        .with_columns(columns)
        .await?;

    let batch = get_record_batch();
    thread::sleep(Duration::from_secs(1));
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    thread::sleep(Duration::from_secs(1));
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();

    thread::sleep(Duration::from_secs(1));
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    Ok(Context { tmp_dir, table })
}

fn get_record_batch() -> RecordBatch {
    let mut id_vec: Vec<i32> = Vec::with_capacity(10);
    let mut value_vec: Vec<i32> = Vec::with_capacity(10);
    let mut rng = rand::thread_rng();

    for _ in 0..10 {
        id_vec.push(rng.gen());
        value_vec.push(rng.gen());
    }

    let schema = ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, true),
        Field::new("value", ArrowDataType::Int32, true),
    ]);

    let id_array = Int32Array::from(id_vec);
    let value_array = Int32Array::from(value_vec);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(id_array), Arc::new(value_array)],
    )
    .unwrap()
}

#[tokio::test]
async fn test_restore_by_version() -> Result<(), Box<dyn Error>> {
    let context = setup_test().await?;
    let table = context.table;
    let result = DeltaOps(table).restore().with_version_to_restore(1).await?;
    assert_eq!(result.1.num_restored_file, 1);
    assert_eq!(result.1.num_removed_file, 2);
    assert_eq!(result.0.snapshot()?.version(), 4);
    let table_uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let mut table = DeltaOps::try_from_uri(table_uri).await?;
    table.0.load_version(1).await?;
    let curr_files = table.0.snapshot()?.file_paths_iter().collect_vec();
    let result_files = result.0.snapshot()?.file_paths_iter().collect_vec();
    assert_eq!(curr_files, result_files);

    let result = DeltaOps(result.0)
        .restore()
        .with_version_to_restore(0)
        .await?;
    assert_eq!(result.0.get_files_count(), 0);
    Ok(())
}

#[tokio::test]
async fn test_restore_by_datetime() -> Result<(), Box<dyn Error>> {
    let context = setup_test().await?;
    let table = context.table;
    let version = 1;

    // The way we obtain a timestamp for a version will have to change when/if we start using CommitInfo for timestamps
    let meta = table
        .object_store()
        .head(&commit_uri_from_version(version))
        .await?;
    let timestamp = meta.last_modified.timestamp_millis();
    let datetime = DateTime::from_timestamp_millis(timestamp).unwrap();

    let result = DeltaOps(table)
        .restore()
        .with_datetime_to_restore(datetime)
        .await?;
    assert_eq!(result.1.num_restored_file, 1);
    assert_eq!(result.1.num_removed_file, 2);
    assert_eq!(result.0.snapshot()?.version(), 4);
    Ok(())
}

#[tokio::test]
async fn test_restore_with_error_params() -> Result<(), Box<dyn Error>> {
    let context = setup_test().await?;
    let table = context.table;
    let history = table.history(Some(10)).await?;
    let timestamp = history.get(1).unwrap().timestamp.unwrap();
    let datetime = DateTime::from_timestamp_millis(timestamp).unwrap();

    // datetime and version both set
    let result = DeltaOps(table)
        .restore()
        .with_version_to_restore(1)
        .with_datetime_to_restore(datetime)
        .await;
    assert!(result.is_err());

    // version too large
    let table_uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let ops = DeltaOps::try_from_uri(table_uri).await?;
    let result = ops.restore().with_version_to_restore(5).await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn test_restore_file_missing() -> Result<(), Box<dyn Error>> {
    let context = setup_test().await?;

    for file in context.table.snapshot()?.log_data() {
        let p = context.tmp_dir.path().join(file.path().as_ref());
        fs::remove_file(p).unwrap();
    }

    for file in context
        .table
        .snapshot()?
        .all_tombstones(context.table.object_store().clone())
        .await?
    {
        let p = context.tmp_dir.path().join(file.clone().path);
        fs::remove_file(p).unwrap();
    }

    let result = DeltaOps(context.table)
        .restore()
        .with_version_to_restore(1)
        .await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn test_restore_allow_file_missing() -> Result<(), Box<dyn Error>> {
    let context = setup_test().await?;

    for file in context.table.snapshot()?.log_data() {
        let p = context.tmp_dir.path().join(file.path().as_ref());
        fs::remove_file(p).unwrap();
    }

    for file in context
        .table
        .snapshot()?
        .all_tombstones(context.table.object_store().clone())
        .await?
    {
        let p = context.tmp_dir.path().join(file.clone().path);
        fs::remove_file(p).unwrap();
    }

    let result = DeltaOps(context.table)
        .restore()
        .with_ignore_missing_files(true)
        .with_version_to_restore(1)
        .await;
    assert!(result.is_ok());
    Ok(())
}

#[tokio::test]
async fn test_restore_transaction_conflict() -> Result<(), Box<dyn Error>> {
    let context = setup_test().await?;
    let mut table = context.table;
    table.load_version(2).await?;

    let result = DeltaOps(table).restore().with_version_to_restore(1).await;
    assert!(result.is_err());
    Ok(())
}
