#![allow(dead_code)]

#[allow(unused_imports)]
use crate::fs_common;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::kernel::{DataType as DeltaDataType, PrimitiveType, StructField, StructType};
use deltalake_core::operations::merge::MergeMetrics;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError, open_table};
use std::sync::Arc;
use url::Url;

async fn create_table(table_uri: &str, partition: Option<Vec<&str>>) -> DeltaTable {
    let table_schema = get_delta_schema();
    let table_url = url::Url::from_directory_path(table_uri).unwrap();
    let ops = DeltaTable::try_from_url(table_url).await.unwrap();
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
    StructType::try_new(vec![
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
    .unwrap()
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
    table
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
    table
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
    let table_url = Url::from_directory_path(table_uri).unwrap();
    let table_ref2 = open_table(table_url).await.unwrap();
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
    let table_url = Url::from_directory_path(table_uri).unwrap();
    let table_ref2 = open_table(table_url).await.unwrap();
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
    let table_url = url::Url::from_directory_path(table_uri).unwrap();
    let table_ref2 = open_table(table_url).await.unwrap();
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
    let table_url = Url::from_directory_path(table_uri).unwrap();
    let table_ref2 = open_table(table_url).await.unwrap();
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

/// Verify that the merge write projection preserves the physical Arrow types
/// produced by the target scan.
///
/// When `schema_force_view_types` is enabled (the default), the scan produces
/// `Utf8View`/`BinaryView` columns. The write projection must not cast these
/// back to `Utf8`/`Binary` (which would re-introduce Arrow's 2 GB offset limit).
///
/// When disabled, the legacy `Utf8`/`Binary` types must be preserved.
#[tokio::test]
async fn test_merge_respects_schema_force_view_types() -> DeltaResult<()> {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("content", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
    ]));

    let delta_schema = StructType::try_new(vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::String), true),
        StructField::new(
            "content",
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "payload",
            DeltaDataType::Primitive(PrimitiveType::Binary),
            true,
        ),
    ])
    .unwrap();

    let initial_batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B"])),
            Arc::new(arrow::array::StringArray::from(vec!["hello", "world"])),
            Arc::new(arrow::array::BinaryArray::from(vec![
                b"bin1".as_ref(),
                b"bin2".as_ref(),
            ])),
        ],
    )
    .unwrap();

    let source_batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["B", "C"])),
            Arc::new(arrow::array::StringArray::from(vec!["updated", "new"])),
            Arc::new(arrow::array::BinaryArray::from(vec![
                b"bin2_new".as_ref(),
                b"bin3".as_ref(),
            ])),
        ],
    )
    .unwrap();

    // -- view types ENABLED (default) -----------------------------------------
    {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .await
            .unwrap();
        let table = table
            .write(vec![initial_batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let source = ctx.read_batch(source_batch.clone()).unwrap();

        let (table, metrics) = table
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("content", col("source.content"))
                    .update("payload", col("source.payload"))
            })?
            .when_not_matched_insert(|insert| {
                insert
                    .set("id", col("source.id"))
                    .set("content", col("source.content"))
                    .set("payload", col("source.payload"))
            })?
            .await?;

        assert_eq!(metrics.num_target_rows_updated, 1);
        assert_eq!(metrics.num_target_rows_inserted, 1);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_output_rows, 3);

        // Read back and assert the physical Arrow types are Utf8View / BinaryView.
        let read_ctx = SessionContext::new();
        table.update_datafusion_session(&read_ctx.state()).unwrap();
        let provider = table.table_provider().await.unwrap();
        read_ctx.register_table("t", provider).unwrap();
        let batches = read_ctx
            .sql("SELECT * FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Utf8View,
            "expected Utf8View for 'id' when view types enabled"
        );
        assert_eq!(
            schema.field_with_name("content").unwrap().data_type(),
            &DataType::Utf8View,
            "expected Utf8View for 'content' when view types enabled"
        );
        assert_eq!(
            schema.field_with_name("payload").unwrap().data_type(),
            &DataType::BinaryView,
            "expected BinaryView for 'payload' when view types enabled"
        );
    }

    // -- view types DISABLED --------------------------------------------------
    {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .await
            .unwrap();
        let table = table
            .write(vec![initial_batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let source = ctx.read_batch(source_batch.clone()).unwrap();

        // Create a session with schema_force_view_types disabled
        let config: SessionConfig =
            deltalake_core::delta_datafusion::DeltaSessionConfig::default().into();
        let config = config.set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        );
        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .build_arc()
            .unwrap();
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .build();
        let session = Arc::new(state);

        let (table, metrics) = table
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .with_session_state(session)
            .when_matched_update(|update| {
                update
                    .update("content", col("source.content"))
                    .update("payload", col("source.payload"))
            })?
            .when_not_matched_insert(|insert| {
                insert
                    .set("id", col("source.id"))
                    .set("content", col("source.content"))
                    .set("payload", col("source.payload"))
            })?
            .await?;

        assert_eq!(metrics.num_target_rows_updated, 1);
        assert_eq!(metrics.num_target_rows_inserted, 1);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_output_rows, 3);

        // Read back with view types disabled and assert legacy Utf8 / Binary.
        let read_config = SessionConfig::default().set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        );
        let read_ctx = SessionContext::new_with_config(read_config);
        table.update_datafusion_session(&read_ctx.state()).unwrap();
        let provider = table
            .table_provider()
            .with_session(Arc::new(read_ctx.state()))
            .await
            .unwrap();
        read_ctx.register_table("t", provider).unwrap();
        let batches = read_ctx
            .sql("SELECT * FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Utf8,
            "expected Utf8 for 'id' when view types disabled"
        );
        assert_eq!(
            schema.field_with_name("content").unwrap().data_type(),
            &DataType::Utf8,
            "expected Utf8 for 'content' when view types disabled"
        );
        assert_eq!(
            schema.field_with_name("payload").unwrap().data_type(),
            &DataType::Binary,
            "expected Binary for 'payload' when view types disabled"
        );
    }

    Ok(())
}
