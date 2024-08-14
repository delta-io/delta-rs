#![allow(dead_code)]
mod fs_common;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use datafusion_common::Column;
use datafusion_expr::{col, lit, Expr};
use deltalake_core::kernel::{DataType as DeltaDataType, PrimitiveType, StructField, StructType};
use deltalake_core::operations::merge::MergeMetrics;
use deltalake_core::operations::transaction::TransactionError;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{open_table, DeltaOps, DeltaResult, DeltaTable, DeltaTableError};
use std::sync::Arc;

async fn create_table(table_uri: &str, partition: Option<Vec<&str>>) -> DeltaTable {
    let table_schema = get_delta_schema();
    let ops = DeltaOps::try_from_uri(table_uri).await.unwrap();
    let table = ops
        .create()
        .with_columns(table_schema.fields().cloned())
        .with_partition_columns(partition.unwrap_or_default())
        .await
        .expect("Failed to create table");

    let schema = get_arrow_schema();
    write_data(table, &schema).await
}

fn get_delta_schema() -> StructType {
    StructType::new(vec![
        StructField::new(
            "id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "value".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "event_date".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ])
}

fn get_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
        Field::new("event_date", DataType::Utf8, true),
    ]))
}

async fn write_data(table: DeltaTable, schema: &Arc<ArrowSchema>) -> DeltaTable {
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-01",
                "2021-02-01",
                "2021-02-02",
                "2021-02-02",
            ])),
        ],
    )
    .unwrap();
    // write some data
    DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

fn create_test_data() -> (DataFrame, DataFrame) {
    let schema = get_arrow_schema();
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["C", "D"])),
            Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-02",
                "2021-02-02",
            ])),
        ],
    )
    .unwrap();
    let df1 = ctx.read_batch(batch).unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["E", "F"])),
            Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-03",
                "2021-02-03",
            ])),
        ],
    )
    .unwrap();
    let df2 = ctx.read_batch(batch).unwrap();
    (df1, df2)
}

async fn merge(
    table: DeltaTable,
    df: DataFrame,
    predicate: Expr,
) -> DeltaResult<(DeltaTable, MergeMetrics)> {
    DeltaOps(table)
        .merge(df, predicate)
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update
                .update("value", col("source.value"))
                .update("event_date", col("source.event_date"))
        })
        .unwrap()
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
                .set("event_date", col("source.event_date"))
        })
        .unwrap()
        .await
}

#[tokio::test]
async fn test_merge_concurrent_conflict() {
    // Overlapping id ranges -> Commit conflict
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    let table_ref1 = create_table(table_uri, Some(vec!["event_date"])).await;
    let table_ref2 = open_table(table_uri).await.unwrap();
    let (df1, _df2) = create_test_data();

    let expr = col("target.id").eq(col("source.id"));
    let (_table_ref1, _metrics) = merge(table_ref1, df1.clone(), expr.clone()).await.unwrap();
    let result = merge(table_ref2, df1, expr).await;

    assert!(matches!(
        result.as_ref().unwrap_err(),
        DeltaTableError::Transaction { .. }
    ));
    if let DeltaTableError::Transaction { source } = result.unwrap_err() {
        assert!(matches!(source, TransactionError::CommitConflict(_)));
    }
}

#[tokio::test]
async fn test_merge_different_range() {
    // No overlapping id ranges -> No conflict
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    let table_ref1 = create_table(table_uri, Some(vec!["event_date"])).await;
    let table_ref2 = open_table(table_uri).await.unwrap();
    let (df1, df2) = create_test_data();

    let expr = col("target.id").eq(col("source.id"));
    let (_table_ref1, _metrics) = merge(table_ref1, df1, expr.clone()).await.unwrap();
    let result = merge(table_ref2, df2, expr).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_merge_concurrent_different_partition() {
    // partition key in predicate -> Successful merge
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    let table_ref1 = create_table(table_uri, Some(vec!["event_date"])).await;
    let table_ref2 = open_table(table_uri).await.unwrap();
    let (df1, df2) = create_test_data();

    let expr = col("target.id")
        .eq(col("source.id"))
        .and(col("target.event_date").eq(col("source.event_date")));
    let (_table_ref1, _metrics) = merge(table_ref1, df1, expr.clone()).await.unwrap();
    let result = merge(table_ref2, df2, expr).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_merge_concurrent_with_overlapping_files() {
    // predicate contains filter and files are overlapping -> Commit conflict
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    let table_ref1 = create_table(table_uri, None).await;
    let table_ref2 = open_table(table_uri).await.unwrap();
    let (df1, _df2) = create_test_data();

    let expr = col("target.id").eq(col("source.id"));
    let (_table_ref1, _metrics) = merge(
        table_ref1,
        df1.clone(),
        expr.clone()
            .and(col(Column::from_qualified_name("target.event_date")).lt_eq(lit("2021-02-02"))),
    )
    .await
    .unwrap();
    let result = merge(
        table_ref2,
        df1,
        expr.and(col(Column::from_qualified_name("target.event_date")).eq(lit("2021-02-02"))),
    )
    .await;

    assert!(matches!(
        result.as_ref().unwrap_err(),
        DeltaTableError::Transaction { .. }
    ));
    if let DeltaTableError::Transaction { source } = result.unwrap_err() {
        assert!(matches!(source, TransactionError::CommitConflict(_)));
    }
}
