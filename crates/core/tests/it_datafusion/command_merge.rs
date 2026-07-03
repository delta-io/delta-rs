#![allow(dead_code)]

#[allow(unused_imports)]
use crate::fs_common;

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_array::{RecordBatch, StringViewArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::kernel::{DataType as DeltaDataType, PrimitiveType, StructField, StructType};
use deltalake_core::operations::merge::MergeMetrics;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError, open_table};
use std::{collections::HashSet, sync::Arc};
use url::Url;

const SCD_TS_V1_US: i64 = 1_747_133_263_000_000;
const SCD_TS_V2_US: i64 = 1_747_133_498_000_000;
const SCD_OPEN_END_US: i64 = 9_214_646_399_000_000;

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

fn id_value_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn id_value_delta_schema() -> StructType {
    StructType::try_new(vec![
        StructField::new(
            "id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        StructField::new(
            "value".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
    ])
    .unwrap()
}

fn id_value_batch(ids: Vec<i64>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        id_value_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn int64_value(batch: &RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}

fn scd_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("metric_value", DataType::Float64, false),
        Field::new(
            "ingestion_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "valid_from_dt",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "valid_to_dt",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn scd_delta_schema() -> StructType {
    StructType::try_new(vec![
        StructField::new(
            "customer_id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "metric_value".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Double),
            false,
        ),
        StructField::new(
            "ingestion_datetime".to_string(),
            DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
        StructField::new(
            "valid_from_dt".to_string(),
            DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
        StructField::new(
            "valid_to_dt".to_string(),
            DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
    ])
    .unwrap()
}

fn scd_batch(
    row_count: i64,
    metric_values: Vec<f64>,
    ingestion_datetime: i64,
    valid_from_dt: i64,
    valid_to_dt: i64,
) -> RecordBatch {
    let row_count = usize::try_from(row_count).unwrap();
    assert_eq!(metric_values.len(), row_count);
    let customer_ids = (0..row_count)
        .map(|idx| format!("C-{idx}"))
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        scd_arrow_schema(),
        vec![
            Arc::new(StringArray::from(customer_ids)),
            Arc::new(Float64Array::from(metric_values)),
            Arc::new(TimestampMicrosecondArray::from_iter(
                (0..row_count).map(|_| Some(ingestion_datetime)),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter(
                (0..row_count).map(|_| Some(valid_from_dt)),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter(
                (0..row_count).map(|_| Some(valid_to_dt)),
            )),
        ],
    )
    .unwrap()
}

fn qualified_col(relation: &str, name: &str) -> Expr {
    col(Column::new(Some(TableReference::bare(relation)), name))
}

enum TestStringArray<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl TestStringArray<'_> {
    fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(array) => array.value(row),
            Self::Utf8View(array) => array.value(row),
        }
    }
}

fn string_column<'a>(batch: &'a RecordBatch, column: &str) -> TestStringArray<'a> {
    let array = batch.column_by_name(column).unwrap();
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        TestStringArray::Utf8(array)
    } else if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        TestStringArray::Utf8View(array)
    } else {
        panic!("unexpected {column} type: {:?}", array.data_type());
    }
}

async fn single_file_id_value_table(row_count: i64) -> DeltaTable {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .await
        .unwrap();

    let ids = (0..row_count).collect::<Vec<_>>();
    let values = vec![1_i64; row_count as usize];
    let table = table
        .write(vec![id_value_batch(ids, values)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    table
}

async fn multi_file_id_value_table(file_count: usize, rows_per_file: i64) -> DeltaTable {
    let mut table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .await
        .unwrap();

    for file_idx in 0..file_count {
        let start = i64::try_from(file_idx).unwrap() * rows_per_file;
        let end = start + rows_per_file;
        table = table
            .write(vec![id_value_batch(
                (start..end).collect::<Vec<_>>(),
                vec![1_i64; rows_per_file as usize],
            )])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    assert_eq!(table.snapshot().unwrap().log_data().num_files(), file_count);
    table
}

async fn single_file_scd_table(row_count: i64) -> DeltaTable {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(scd_delta_schema().fields().cloned())
        .await
        .unwrap();

    let table = table
        .write(vec![scd_batch(
            row_count,
            vec![100.0; row_count as usize],
            SCD_TS_V1_US,
            SCD_TS_V1_US,
            SCD_OPEN_END_US,
        )])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    table
}

async fn assert_id_value_table_complete(
    table: &DeltaTable,
    row_count: i64,
    updated_id: Option<i64>,
    expected_updated_rows: i64,
) -> DeltaResult<()> {
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state()).unwrap();
    let provider = table.table_provider().await.unwrap();
    ctx.register_table("target", provider).unwrap();

    let summary = ctx
        .sql(
            "SELECT COUNT(*) AS row_count, COUNT(DISTINCT id) AS distinct_ids, MIN(id) AS min_id, MAX(id) AS max_id, \
                    SUM(CASE WHEN value = 2 THEN 1 ELSE 0 END) AS updated_rows, \
                    SUM(CASE WHEN value = 1 THEN 1 ELSE 0 END) AS unchanged_rows \
             FROM target",
        )
        .await?
        .collect()
        .await?;
    let batch = &summary[0];
    assert_eq!(int64_value(batch, 0, 0), row_count);
    assert_eq!(int64_value(batch, 1, 0), row_count);
    assert_eq!(int64_value(batch, 2, 0), 0);
    assert_eq!(int64_value(batch, 3, 0), row_count - 1);
    assert_eq!(int64_value(batch, 4, 0), expected_updated_rows);
    assert_eq!(int64_value(batch, 5, 0), row_count - expected_updated_rows);

    if let Some(updated_id) = updated_id {
        let updated = ctx
            .sql(&format!("SELECT value FROM target WHERE id = {updated_id}"))
            .await?
            .collect()
            .await?;
        assert_eq!(int64_value(&updated[0], 0, 0), 2);
    }

    Ok(())
}

async fn assert_scd_table_complete(
    table: &DeltaTable,
    row_count: i64,
    expected_expired_rows: usize,
) -> DeltaResult<()> {
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state()).unwrap();
    let provider = table.table_provider().await.unwrap();
    ctx.register_table("target", provider).unwrap();

    let batches = ctx
        .sql("SELECT customer_id, metric_value, valid_to_dt FROM target")
        .await?
        .collect()
        .await?;

    let mut customer_ids = HashSet::new();
    let mut row_total = 0usize;
    let mut expired_rows = 0usize;
    let mut current_rows = 0usize;
    let mut unchanged_metric_rows = 0usize;

    for batch in batches {
        row_total += batch.num_rows();
        let customer_id = string_column(&batch, "customer_id");
        let metric_value = batch
            .column_by_name("metric_value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let valid_to = batch
            .column_by_name("valid_to_dt")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            customer_ids.insert(customer_id.value(row).to_string());
            assert_eq!(metric_value.value(row), 100.0);
            unchanged_metric_rows += 1;
            match valid_to.value(row) {
                SCD_TS_V2_US => expired_rows += 1,
                SCD_OPEN_END_US => current_rows += 1,
                other => panic!("unexpected valid_to_dt value: {other}"),
            }
        }
    }

    assert_eq!(row_total, row_count as usize);
    assert_eq!(customer_ids.len(), row_count as usize);
    assert_eq!(expired_rows, expected_expired_rows);
    assert_eq!(current_rows, row_count as usize - expected_expired_rows);
    assert_eq!(unchanged_metric_rows, row_count as usize);

    Ok(())
}

#[tokio::test]
async fn test_merge_large_single_file_unique_source_does_not_report_duplicate_match()
-> DeltaResult<()> {
    let table = single_file_id_value_table(25_000).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source = ctx
        .read_batch(id_value_batch(
            (0..25_000).collect::<Vec<_>>(),
            vec![2_i64; 25_000],
        ))
        .unwrap();

    let (table, metrics) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| update.update("value", col("source.value")))?
        .await?;

    assert_eq!(metrics.num_source_rows, 25_000);
    assert_eq!(metrics.num_target_rows_updated, 25_000);
    assert_eq!(metrics.num_target_rows_inserted, 0);
    assert_eq!(metrics.num_output_rows, 25_000);
    assert_eq!(metrics.num_target_files_scanned, 1);

    assert_id_value_table_complete(&table, 25_000, Some(0), 25_000).await?;
    Ok(())
}

#[tokio::test]
async fn test_merge_noop_heavy_matched_update_with_inserts_does_not_rewrite_target_file()
-> DeltaResult<()> {
    let target_rows = 25_000;
    let inserted_rows = 1_000;
    let total_source_rows = target_rows + inserted_rows;
    let table = single_file_id_value_table(target_rows).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source = ctx
        .read_batch(id_value_batch(
            (0..total_source_rows).collect::<Vec<_>>(),
            vec![1_i64; total_source_rows as usize],
        ))
        .unwrap();

    let (table, metrics) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| {
            update
                .predicate(
                    qualified_col("target", "value").not_eq(qualified_col("source", "value")),
                )
                .update("value", qualified_col("source", "value"))
        })?
        .when_not_matched_insert(|insert| {
            insert
                .set("id", qualified_col("source", "id"))
                .set("value", qualified_col("source", "value"))
        })?
        .await?;

    assert_eq!(metrics.num_source_rows, total_source_rows as usize);
    assert_eq!(metrics.num_target_rows_updated, 0);
    assert_eq!(metrics.num_target_rows_inserted, inserted_rows as usize);
    assert_eq!(metrics.num_target_rows_deleted, 0);
    assert_eq!(metrics.num_target_rows_copied, 0);
    assert_eq!(metrics.num_output_rows, inserted_rows as usize);
    assert_eq!(metrics.num_target_files_scanned, 1);
    assert_eq!(metrics.num_target_files_removed, 0);

    assert_id_value_table_complete(&table, total_source_rows, None, 0).await?;
    Ok(())
}

#[tokio::test]
async fn test_merge_batch_boundary_update_does_not_drop_target_rows() -> DeltaResult<()> {
    let table = single_file_id_value_table(8_193).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source = ctx
        .read_batch(id_value_batch(vec![8_192], vec![2]))
        .unwrap();

    let (table, metrics) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| update.update("value", col("source.value")))?
        .await?;

    assert_eq!(metrics.num_source_rows, 1);
    assert_eq!(metrics.num_target_rows_updated, 1);
    assert_eq!(metrics.num_target_rows_inserted, 0);
    assert_eq!(metrics.num_output_rows, 8_193);
    assert_eq!(metrics.num_target_files_scanned, 1);

    assert_id_value_table_complete(&table, 8_193, Some(8_192), 1).await?;
    Ok(())
}

#[tokio::test]
async fn test_merge_multi_file_batch_boundary_unique_source_preserves_target_rows()
-> DeltaResult<()> {
    let file_count = 4;
    let rows_per_file = 8_193;
    let row_count = i64::try_from(file_count).unwrap() * rows_per_file;
    let table = multi_file_id_value_table(file_count, rows_per_file).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source = ctx
        .read_batch(id_value_batch(
            (0..row_count).collect::<Vec<_>>(),
            vec![2_i64; row_count as usize],
        ))
        .unwrap();

    let (table, metrics) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| update.update("value", col("source.value")))?
        .await?;

    assert_eq!(metrics.num_source_rows, row_count as usize);
    assert_eq!(metrics.num_target_rows_updated, row_count as usize);
    assert_eq!(metrics.num_target_rows_inserted, 0);
    assert_eq!(metrics.num_output_rows, row_count as usize);
    assert_eq!(metrics.num_target_files_scanned, file_count);

    assert_id_value_table_complete(&table, row_count, Some(0), row_count).await?;
    Ok(())
}

#[tokio::test]
async fn test_merge_scd2_conditional_update_unique_source_does_not_report_duplicate_match()
-> DeltaResult<()> {
    let row_count = 25_000;
    let table = single_file_scd_table(row_count).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source_metric_values = (0..row_count)
        .map(|idx| if idx % 2 == 0 { 200.0 } else { 100.0 })
        .collect::<Vec<_>>();
    let source = ctx
        .read_batch(scd_batch(
            row_count,
            source_metric_values,
            SCD_TS_V2_US,
            SCD_TS_V2_US,
            SCD_OPEN_END_US,
        ))
        .unwrap();

    let (table, metrics) = table
        .merge(
            source,
            qualified_col("target", "customer_id")
                .eq(qualified_col("source", "customer_id"))
                .and(qualified_col("target", "valid_to_dt").eq(lit(
                    ScalarValue::TimestampMicrosecond(Some(SCD_OPEN_END_US), None),
                )))
                .and(qualified_col("target", "ingestion_datetime").lt(lit(
                    ScalarValue::TimestampMicrosecond(Some(SCD_TS_V2_US), None),
                ))),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| {
            update
                .update(
                    "valid_to_dt",
                    lit(ScalarValue::TimestampMicrosecond(Some(SCD_TS_V2_US), None)),
                )
                .predicate(
                    qualified_col("target", "metric_value")
                        .not_eq(qualified_col("source", "metric_value")),
                )
        })?
        .await?;

    assert_eq!(metrics.num_source_rows, 25_000);
    assert_eq!(metrics.num_target_rows_updated, 12_500);
    assert_eq!(metrics.num_target_rows_inserted, 0);
    assert_eq!(metrics.num_output_rows, 25_000);
    assert_eq!(metrics.num_target_files_scanned, 1);

    assert_scd_table_complete(&table, row_count, 12_500).await?;
    Ok(())
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
