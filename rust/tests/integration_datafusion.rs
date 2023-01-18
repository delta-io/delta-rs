#![cfg(all(feature = "integration_test", feature = "datafusion"))]

use arrow::array::Int64Array;
use common::datafusion::context_with_delta_table_factory;
use datafusion::prelude::SessionContext;
use deltalake::DeltaTableBuilder;
use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use serial_test::serial;
use std::sync::Arc;
use url::Url;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use deltalake::optimize::{MetricDetails, Metrics};
use deltalake::DeltaTableError;
use deltalake::{
    action,
    action::Remove,
    optimize::{create_merge_plan, Optimize},
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaTableMetaData, PartitionFilter,
};
use deltalake::{DeltaTable, Schema, SchemaDataType, SchemaField};
use rand::prelude::*;
use serde_json::{json, Map, Value};
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::{collections::HashMap, error::Error};
use tempdir::TempDir;



mod common;
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


struct Context {
    pub tmp_dir: TempDir,
    pub table: DeltaTable,
}

async fn setup_test(partitioned: bool) -> Result<Context, Box<dyn Error>> {
    let schema = Schema::new(vec![
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
    ]);

    let p = if partitioned {
        vec!["date".to_owned()]
    } else {
        vec![]
    };

    let table_meta = DeltaTableMetaData::new(
        Some("opt_table".to_owned()),
        Some("Table for optimize tests".to_owned()),
        None,
        schema.clone(),
        p,
        HashMap::new(),
    );

    let tmp_dir = tempdir::TempDir::new("opt_table").unwrap();
    let p = tmp_dir.path().to_str().to_owned().unwrap();
    let mut dt = DeltaTableBuilder::from_uri(p).build()?;

    let mut commit_info = Map::<String, Value>::new();

    let protocol = action::Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
    };

    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );
    dt.create(
        table_meta.clone(),
        protocol.clone(),
        Some(commit_info),
        None,
    )
    .await?;

    Ok(Context { tmp_dir, table: dt })
}

pub async fn write(
    writer: &mut RecordBatchWriter,
    table: &mut DeltaTable,
    batch: RecordBatch,
) -> Result<(), DeltaTableError> {
    writer.write(batch).await?;
    writer.flush_and_commit(table).await?;
    Ok(())
}


#[tokio::test]
#[serial]
async fn test_datafusion_local() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Local).await?)
}

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
#[tokio::test]
#[serial]
async fn test_datafusion_aws() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Amazon).await?)
}

#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_datafusion_azure() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Microsoft).await?)
}

#[cfg(feature = "gcs")]
#[tokio::test]
#[serial]
async fn test_datafusion_gcp() -> TestResult {
    Ok(test_datafusion(StorageIntegration::Google).await?)
}

#[tokio::test]
#[serial]
async fn test_datafusion_table_with_statistics() -> TestResult {
    let context = setup_test(true).await?;
    let mut dt = context.table;
    let mut writer = RecordBatchWriter::for_table(&dt)?;

    write(
        &mut writer,
        &mut dt,
        tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_table("demo", Arc::new(dt))?;

    let batches = ctx
        .sql("SELECT count(*) FROM demo WHERE date = '2022-05-22'")
        .await?
        .collect()
        .await?;

    Ok(())
}

async fn test_datafusion(storage: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new(storage)?;
    context.load_table(TestTables::Simple).await?;

    simple_query(&context).await?;

    Ok(())
}

async fn simple_query(context: &IntegrationContext) -> TestResult {
    let table_uri = context.uri_for_table(TestTables::Simple);

    let dynamo_lock_option = "'DYNAMO_LOCK_OWNER_NAME' 's3::deltars/simple'".to_string();
    let options = match context.integration {
        StorageIntegration::Amazon => format!("'AWS_STORAGE_ALLOW_HTTP' '1', {dynamo_lock_option}"),
        StorageIntegration::Microsoft => {
            format!("'AZURE_STORAGE_ALLOW_HTTP' '1', {dynamo_lock_option}")
        }
        _ => dynamo_lock_option,
    };

    let sql = format!(
        "CREATE EXTERNAL TABLE demo \
        STORED AS DELTATABLE \
        OPTIONS ({options}) \
        LOCATION '{table_uri}'",
    );

    let ctx = context_with_delta_table_factory();
    let _ = ctx
        .sql(sql.as_str())
        .await
        .expect("Failed to register table!");

    let batches = ctx
        .sql("SELECT id FROM demo WHERE id > 5 ORDER BY id ASC")
        .await?
        .collect()
        .await?;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    assert_eq!(
        batch.column(0).as_ref(),
        Arc::new(Int64Array::from(vec![7, 9])).as_ref(),
    );

    Ok(())
}
