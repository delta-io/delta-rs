#![cfg(feature = "datafusion")]
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::types::UInt16Type;
use arrow_array::{Array, DictionaryArray, StringArray, StringViewArray};
use datafusion::assert_batches_sorted_eq;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::{ExecutionPlan, collect_partitioned};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use deltalake_core::delta_datafusion::DeltaScanConfig;
use deltalake_core::delta_datafusion::DeltaScanNext;
use deltalake_core::delta_datafusion::create_session;
use deltalake_core::delta_datafusion::engine::DataFusionEngine;
use deltalake_core::kernel::Snapshot;
use deltalake_test::TestResult;
use deltalake_test::acceptance::read_dat_case;

async fn scan_dat(case: &str) -> TestResult<(Snapshot, SessionContext)> {
    let session = create_session().into_inner();
    let snapshot = scan_dat_with_session(case, &session).await?;
    Ok((snapshot, session))
}

async fn scan_dat_with_session(case: &str, session: &SessionContext) -> TestResult<Snapshot> {
    let root_dir = format!(
        "{}/../../dat/v0.0.3/reader_tests/generated/{}/",
        env!["CARGO_MANIFEST_DIR"],
        case
    );
    let root_dir = std::fs::canonicalize(root_dir)?;
    let case = read_dat_case(root_dir)?;

    let engine = DataFusionEngine::new_from_session(&session.state());
    let snapshot =
        Snapshot::try_new_with_engine(engine.clone(), case.table_root()?, Default::default(), None)
            .await?;

    Ok(snapshot)
}

async fn collect_plan(
    plan: Arc<dyn ExecutionPlan>,
    session: &SessionContext,
) -> TestResult<Vec<RecordBatch>> {
    let batches: Vec<_> = collect_partitioned(plan, session.task_ctx())
        .await?
        .into_iter()
        .flatten()
        .collect();
    Ok(batches)
}

fn file_id_at_row(batch: &RecordBatch, row: usize) -> Option<String> {
    let file_id_idx = batch.schema().index_of("file_id").ok()?;
    let dict = batch
        .column(file_id_idx)
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()?;
    if row >= dict.len() {
        return None;
    }
    if dict.is_null(row) {
        return None;
    }

    let key = dict.keys().value(row) as usize;
    if let Some(values) = dict.values().as_any().downcast_ref::<StringArray>() {
        (key < values.len()).then(|| values.value(key).to_string())
    } else if let Some(values) = dict.values().as_any().downcast_ref::<StringViewArray>() {
        (key < values.len()).then(|| values.value(key).to_string())
    } else {
        panic!(
            "unexpected file_id dictionary value type: {:?}",
            dict.values().data_type()
        );
    }
}

#[test]
fn test_file_id_at_row_out_of_bounds_returns_none() {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{DictionaryArray, Int32Array, StringArray, UInt16Array};

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, false),
        Field::new(
            "file_id",
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ),
    ]));
    let file_ids = DictionaryArray::new(
        UInt16Array::from(vec![Some(0)]),
        Arc::new(StringArray::from(vec!["f0"])),
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![1])), Arc::new(file_ids)],
    )
    .expect("valid batch");

    assert_eq!(file_id_at_row(&batch, 1), None);
}

#[tokio::test]
async fn test_all_primitive_types() -> TestResult<()> {
    let (snapshot, session) = scan_dat("all_primitive_types").await?;
    let provider = DeltaScanNext::builder().with_snapshot(snapshot).await?;

    let plan = provider.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+------+-------+-------+-------+------+---------+---------+-------+----------+---------+------------+----------------------+",
        "| utf8 | int64 | int32 | int16 | int8 | float32 | float64 | bool  | binary   | decimal | date32     | timestamp            |",
        "+------+-------+-------+-------+------+---------+---------+-------+----------+---------+------------+----------------------+",
        "| 0    | 0     | 0     | 0     | 0    | 0.0     | 0.0     | true  |          | 10.000  | 1970-01-01 | 1970-01-01T00:00:00Z |",
        "| 1    | 1     | 1     | 1     | 1    | 1.0     | 1.0     | false | 00       | 11.000  | 1970-01-02 | 1970-01-01T01:00:00Z |",
        "| 2    | 2     | 2     | 2     | 2    | 2.0     | 2.0     | true  | 0000     | 12.000  | 1970-01-03 | 1970-01-01T02:00:00Z |",
        "| 3    | 3     | 3     | 3     | 3    | 3.0     | 3.0     | false | 000000   | 13.000  | 1970-01-04 | 1970-01-01T03:00:00Z |",
        "| 4    | 4     | 4     | 4     | 4    | 4.0     | 4.0     | true  | 00000000 | 14.000  | 1970-01-05 | 1970-01-01T04:00:00Z |",
        "+------+-------+-------+-------+------+---------+---------+-------+----------+---------+------------+----------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    let plan = provider
        .scan(&session.state(), Some(&vec![1, 3]), &[], None)
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+-------+-------+",
        "| int64 | int16 |",
        "+-------+-------+",
        "| 0     | 0     |",
        "| 1     | 1     |",
        "| 2     | 2     |",
        "| 3     | 3     |",
        "| 4     | 4     |",
        "+-------+-------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    let plan = provider
        .scan(&session.state(), Some(&vec![1, 3]), &[], Some(2))
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+-------+-------+",
        "| int64 | int16 |",
        "+-------+-------+",
        "| 0     | 0     |",
        "| 1     | 1     |",
        "+-------+-------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // Also test with a predicate
    let pred = col("float64").gt(lit(2.0_f64));
    let plan = provider
        .scan(&session.state(), Some(&vec![1, 3, 6]), &[pred], None)
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+-------+-------+---------+",
        "| int64 | int16 | float64 |",
        "+-------+-------+---------+",
        "| 3     | 3     | 3.0     |",
        "| 4     | 4     | 4.0     |",
        "+-------+-------+---------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_view_types_filter_exec_compatibility() -> TestResult<()> {
    use arrow_schema::DataType;

    let config =
        SessionConfig::new().set_bool("datafusion.execution.parquet.schema_force_view_types", true);
    let session = SessionContext::new_with_config(config);
    let snapshot = scan_dat_with_session("all_primitive_types", &session).await?;
    let provider: Arc<dyn TableProvider> = Arc::new(DeltaScanNext::new(
        snapshot,
        DeltaScanConfig::new_from_session(&session.state()),
    )?);

    let plan = provider.scan(&session.state(), None, &[], None).await?;
    let has_view_types = plan
        .schema()
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView));
    assert!(
        has_view_types,
        "view types should be present when configured"
    );

    let filter = col("utf8").eq(lit("1"));
    let batches = session
        .read_table(provider.clone())?
        .filter(filter)?
        .select(vec![col("utf8")])?
        .collect()
        .await?;
    let expected = vec!["+------+", "| utf8 |", "+------+", "| 1    |", "+------+"];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_builder_with_session_seeds_scan_config_from_session() -> TestResult<()> {
    use arrow_schema::DataType;

    let config = SessionConfig::new().set_bool(
        "datafusion.execution.parquet.schema_force_view_types",
        false,
    );
    let session = SessionContext::new_with_config(config);
    let snapshot = scan_dat_with_session("all_primitive_types", &session).await?;
    let provider = DeltaScanNext::builder()
        .with_snapshot(snapshot)
        .with_session(Arc::new(session.state()))
        .await?;

    let plan = provider.scan(&session.state(), None, &[], None).await?;
    let has_view_types = plan
        .schema()
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView));
    assert!(
        !has_view_types,
        "view types should not be present when disabled in session config"
    );

    Ok(())
}

#[tokio::test]
async fn test_multi_partitioned() -> TestResult<()> {
    let (snapshot, session) = scan_dat("multi_partitioned").await?;
    let provider = DeltaScanNext::builder().with_snapshot(snapshot).await?;

    let plan = provider.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+------------+------------+--------+",
        "| letter | date       | data       | number |",
        "+--------+------------+------------+--------+",
        "| /%20%f | 1970-01-01 | 68656c6c6f | 6      |",
        "| b      | 1970-01-01 | f09f9888   | 7      |",
        "+--------+------------+------------+--------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // While are not yet pushing down predicates to the parquet scan, the number values
    // are separated by files, so we expect only one file to be read here.
    let pred = col("number").gt(lit(6_i64));
    let plan = provider.scan(&session.state(), None, &[pred], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+------------+----------+--------+",
        "| letter | date       | data     | number |",
        "+--------+------------+----------+--------+",
        "| b      | 1970-01-01 | f09f9888 | 7      |",
        "+--------+------------+----------+--------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    let pred = col("letter").eq(lit("/%20%f"));
    let plan = provider.scan(&session.state(), None, &[pred], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+------------+------------+--------+",
        "| letter | date       | data       | number |",
        "+--------+------------+------------+--------+",
        "| /%20%f | 1970-01-01 | 68656c6c6f | 6      |",
        "+--------+------------+------------+--------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // Since we are able to do an exact filter on a partition column,
    // Datafusion will not include partition columns that are only referenced
    // in a predicate but not part of the end result in the passed projection.
    let pred = col("letter").eq(lit("/%20%f"));
    let plan = provider
        .scan(&session.state(), Some(&vec![3]), &[pred], None)
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 6      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // COUNT(*) queries may not include any columns in the projection,
    // but we still need to process predicates properly within the scan.
    let pred = col("letter").eq(lit("/%20%f"));
    let plan = provider
        .scan(&session.state(), Some(&vec![]), &[pred], None)
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec!["++", "++", "++"];
    assert_batches_sorted_eq!(&expected, &batches);
    let n_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(n_rows, 1);

    Ok(())
}

#[tokio::test]
async fn test_column_mapping() -> TestResult<()> {
    let (snapshot, session) = scan_dat("column_mapping").await?;
    let provider = DeltaScanNext::builder().with_snapshot(snapshot).await?;

    let plan = provider.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+---------+------------+",
        "| letter | new_int | date       |",
        "+--------+---------+------------+",
        "| a      | 25      | 2017-05-01 |",
        "| a      | 25      | 2017-05-01 |",
        "| a      | 604     | 1997-01-01 |",
        "| a      | 604     | 1997-01-01 |",
        "| a      | 692     | 2017-09-01 |",
        "| a      | 692     | 2017-09-01 |",
        "| a      | 95      | 1983-04-01 |",
        "| a      | 95      | 1983-04-01 |",
        "| b      | 228     | 1978-12-01 |",
        "| b      | 228     | 1978-12-01 |",
        "+--------+---------+------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_deletion_vectors() -> TestResult<()> {
    let (snapshot, session) = scan_dat("deletion_vectors").await?;
    let provider = DeltaScanNext::builder().with_snapshot(snapshot).await?;

    let plan = provider.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+-----+------------+",
        "| letter | int | date       |",
        "+--------+-----+------------+",
        "| b      | 228 | 1978-12-01 |",
        "+--------+-----+------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_deletion_vectors_multi_batch() -> TestResult<()> {
    let config = SessionConfig::new().with_batch_size(1);
    let session = SessionContext::new_with_config(config);
    let snapshot = scan_dat_with_session("deletion_vectors", &session).await?;
    let provider: Arc<dyn TableProvider> = Arc::new(DeltaScanNext::new(
        snapshot,
        DeltaScanConfig::new_from_session(&session.state()),
    )?);

    let plan = provider.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+-----+------------+",
        "| letter | int | date       |",
        "+--------+-----+------------+",
        "| b      | 228 | 1978-12-01 |",
        "+--------+-----+------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_pushdown_statuses_with_file_id_filters() -> TestResult<()> {
    let (snapshot, _) = scan_dat("multi_partitioned").await?;
    let provider = DeltaScanNext::builder()
        .with_snapshot(snapshot)
        .with_file_column("file_id")
        .await?;

    let partition_only = col("letter").eq(lit("b"));
    let data_only = col("number").gt(lit(6_i64));
    let file_id_only = col("file_id").eq(lit("ignored"));

    let statuses =
        provider.supports_filters_pushdown(&[&partition_only, &data_only, &file_id_only])?;
    assert_eq!(
        statuses,
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Inexact,
            TableProviderFilterPushDown::Unsupported,
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_file_id_filter_correctness_with_transformed_schema() -> TestResult<()> {
    let (snapshot, session) = scan_dat("column_mapping").await?;
    let provider: Arc<dyn TableProvider> = DeltaScanNext::builder()
        .with_snapshot(snapshot)
        .with_file_column("file_id")
        .await?;

    let positive_batches = session
        .read_table(provider.clone())?
        .filter(col("new_int").gt(lit(0_i64)))?
        .collect()
        .await?;
    let mut candidate_file_ids: Vec<String> = positive_batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).filter_map(|row| file_id_at_row(batch, row)))
        .collect();
    candidate_file_ids.sort();
    candidate_file_ids.dedup();
    let selected_file_id = candidate_file_ids
        .into_iter()
        .next()
        .expect("expected at least one file_id value for new_int > 0");

    let filtered_batches = session
        .read_table(provider)?
        .filter(col("new_int").gt(lit(0_i64)))?
        .filter(col("file_id").eq(lit(selected_file_id.clone())))?
        .collect()
        .await?;

    assert!(!filtered_batches.is_empty());
    for batch in &filtered_batches {
        assert!(batch.schema().column_with_name("new_int").is_some());
        for row in 0..batch.num_rows() {
            assert_eq!(
                file_id_at_row(batch, row).as_deref(),
                Some(selected_file_id.as_str())
            );
        }
    }

    Ok(())
}
