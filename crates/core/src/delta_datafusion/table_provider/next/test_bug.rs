use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::session::create_session;
use crate::test_utils::{TestResult, open_fs_path};
use arrow::array::{Date32Array, TimestampMillisecondArray};
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use arrow_array::builder::{BinaryDictionaryBuilder, StringDictionaryBuilder};
use arrow_array::types::UInt16Type;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::dml::InsertOp;
use std::sync::Arc;

async fn get_table_and_provider() -> TestResult<(
    crate::DeltaTable,
    Arc<crate::delta_datafusion::table_provider::next::DeltaScan>,
)> {
    let mut table = open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
    table.load().await.unwrap();

    let logical_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "letter",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Utf8),
            ),
            true,
        ),
        ArrowField::new("date", ArrowDataType::Date32, true),
        ArrowField::new(
            "data",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Binary),
            ),
            true,
        ),
        ArrowField::new(
            "number",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]));
    let config = DeltaScanConfig::default().with_schema(logical_schema.clone());

    let provider = crate::delta_datafusion::table_provider::next::DeltaScan::new(
        table.snapshot().unwrap().snapshot().clone(),
        config,
    )
    .unwrap()
    .with_log_store(table.log_store());

    Ok((table, Arc::new(provider)))
}

#[tokio::test]
async fn test_delta_scan_config_schema_override_scan() -> TestResult {
    let (_table, provider) = get_table_and_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider).unwrap();

    let df = ctx.sql("SELECT number FROM test_table").await.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(
        batches[0].schema().fields()[0].data_type(),
        &ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
    );

    Ok(())
}

#[tokio::test]
async fn test_delta_scan_config_schema_override_filter() -> TestResult {
    let (_table, provider) = get_table_and_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider).unwrap();

    let df = ctx
        .sql("SELECT number FROM test_table WHERE number < '2020-01-01T00:00:00Z'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(
        batches[0].schema().fields()[0].data_type(),
        &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
        "Filter output schema does not match logical schema"
    );

    Ok(())
}

#[tokio::test]
async fn test_delta_scan_config_schema_override_filter_aggregate() -> TestResult {
    let (_table, provider) = get_table_and_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider).unwrap();
    let query = "SELECT count(1), max(number) fake_ts FROM test_table WHERE letter != 'a' and number < '2020-01-01T00:00:00Z'";
    let df = ctx.sql(query).await.unwrap();
    let batches = df.collect().await.unwrap();
    datafusion::assert_batches_eq!(
        [
            "+-----------------+-------------------------+",
            "| count(Int64(1)) | fake_ts                 |",
            "+-----------------+-------------------------+",
            "| 2               | 1970-01-01T00:00:00.007 |",
            "+-----------------+-------------------------+",
        ],
        &batches
    );

    Ok(())
}

#[tokio::test]
async fn test_delta_scan_config_schema_override_insert() -> TestResult {
    let (_table, provider) = get_table_and_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider.clone()).unwrap();
    let state = ctx.state();

    let logical_schema = provider.schema();

    let mut dict_builder = StringDictionaryBuilder::<UInt16Type>::new();
    dict_builder.append("a").unwrap();
    let mut bin_builder = BinaryDictionaryBuilder::<UInt16Type>::new();
    bin_builder.append(b"hello").unwrap();

    let batch = RecordBatch::try_new(
        logical_schema.clone(),
        vec![
            Arc::new(dict_builder.finish()),
            Arc::new(Date32Array::from(vec![0])),
            Arc::new(bin_builder.finish()),
            Arc::new(TimestampMillisecondArray::from(vec![2000])),
        ],
    )
    .unwrap();

    let mem_table = MemTable::try_new(logical_schema.clone(), vec![vec![batch]]).unwrap();
    let input = mem_table.scan(&state, None, &[], None).await.unwrap();

    let write_plan = provider
        .insert_into(&state, input, InsertOp::Append)
        .await
        .unwrap();

    let _ = datafusion::physical_plan::collect_partitioned(write_plan, ctx.task_ctx())
        .await
        .unwrap();

    Ok(())
}

#[tokio::test]
async fn test_delta_scan_config_file_column_projection() -> TestResult {
    let (mut table, _) = get_table_and_provider().await?;
    table.load().await.unwrap();

    let logical_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "letter",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Utf8),
            ),
            true,
        ),
        ArrowField::new("date", ArrowDataType::Date32, true),
        ArrowField::new(
            "data",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Binary),
            ),
            true,
        ),
        ArrowField::new(
            "number",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]));
    let config = DeltaScanConfig::default()
        .with_schema(logical_schema.clone())
        .with_file_column_name("_file");

    let provider = crate::delta_datafusion::table_provider::next::DeltaScan::new(
        table.snapshot().unwrap().snapshot().clone(),
        config,
    )
    .unwrap()
    .with_log_store(table.log_store());

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql("SELECT * EXCEPT (_file) FROM test_table ORDER BY date")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let all_fields = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        batches[0].schema().fields().len(),
        4,
        "Expected exactly 4 fields after projection, but got: {:?}",
        all_fields
    );
    assert!(
        !all_fields.contains(&"_file".to_string()),
        "The output must not contain _file column"
    );

    // Reproduce downstream aggregation bug where a subquery selects _file and aggregates over it
    let df_agg = ctx
        .sql("SELECT date, substr(_file, 0, 9) as _file, count FROM (SELECT date, _file, count(1) as count FROM test_table GROUP BY date, _file)")
        .await
        .unwrap();
    let batches_agg = df_agg.collect().await.unwrap();

    let agg_fields = batches_agg[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        batches_agg[0].schema().fields().len(),
        3,
        "Expected exactly 3 fields after aggregation projection, but got: {:?}",
        agg_fields
    );
    assert!(
        agg_fields.contains(&"_file".to_string()),
        "The output must contain _file column"
    );

    // Reproduce downstream bug where file_id_idx was calculated incorrectly in execution_plan
    let df_fail = ctx.sql("SELECT data, _file FROM test_table").await.unwrap();
    let batches_fail = df_fail.collect().await.unwrap();

    let fail_fields = batches_fail[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        batches_fail[0].schema().fields().len(),
        2,
        "Expected exactly 2 fields after projection, but got: {:?}",
        fail_fields
    );
    assert!(fail_fields.contains(&"_file".to_string()));

    Ok(())
}
