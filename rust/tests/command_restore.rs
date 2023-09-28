#![cfg(all(feature = "arrow", feature = "parquet", feature = "datafusion"))]

use arrow::datatypes::Schema as ArrowSchema;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable, SchemaDataType, SchemaField};
use rand::Rng;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::sync::Arc;
use tempdir::TempDir;

#[derive(Debug)]
struct Context {
    pub tmp_dir: TempDir,
    pub table: DeltaTable,
}

async fn setup_test() -> Result<Context, Box<dyn Error>> {
    let columns = vec![
        SchemaField::new(
            "id".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        ),
        SchemaField::new(
            "value".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        ),
    ];

    let tmp_dir = tempdir::TempDir::new("restore_table").unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();
    let table = DeltaOps::try_from_uri(table_uri)
        .await?
        .create()
        .with_columns(columns)
        .await?;

    let batch = get_record_batch();

    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();

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
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Int32, true),
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
    assert_eq!(result.0.state.version(), 4);
    let table_uri = context.tmp_dir.path().to_str().to_owned().unwrap();
    let mut table = DeltaOps::try_from_uri(table_uri).await?;
    table.0.load_version(1).await?;
    assert_eq!(table.0.state.files(), result.0.state.files());

    let result = DeltaOps(result.0)
        .restore()
        .with_version_to_restore(0)
        .await?;
    assert_eq!(result.0.state.files().len(), 0);
    Ok(())
}

#[tokio::test]
async fn test_restore_by_datetime() -> Result<(), Box<dyn Error>> {
    let context = setup_test().await?;
    let mut table = context.table;
    let history = table.history(Some(10)).await?;
    let timestamp = history.get(1).unwrap().timestamp.unwrap();
    let naive = NaiveDateTime::from_timestamp_millis(timestamp).unwrap();
    let datetime: DateTime<Utc> = Utc.from_utc_datetime(&naive);

    let result = DeltaOps(table)
        .restore()
        .with_datetime_to_restore(datetime)
        .await?;
    assert_eq!(result.1.num_restored_file, 1);
    assert_eq!(result.1.num_removed_file, 2);
    assert_eq!(result.0.state.version(), 4);
    Ok(())
}

#[tokio::test]
async fn test_restore_with_error_params() -> Result<(), Box<dyn Error>> {
    let context = setup_test().await?;
    let mut table = context.table;
    let history = table.history(Some(10)).await?;
    let timestamp = history.get(1).unwrap().timestamp.unwrap();
    let naive = NaiveDateTime::from_timestamp_millis(timestamp).unwrap();
    let datetime: DateTime<Utc> = Utc.from_utc_datetime(&naive);

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

    for file in context.table.state.files().iter() {
        let p = context.tmp_dir.path().join(file.clone().path);
        fs::remove_file(p).unwrap();
    }

    for file in context.table.state.all_tombstones().iter() {
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

    for file in context.table.state.files().iter() {
        let p = context.tmp_dir.path().join(file.clone().path);
        fs::remove_file(p).unwrap();
    }

    for file in context.table.state.all_tombstones().iter() {
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
