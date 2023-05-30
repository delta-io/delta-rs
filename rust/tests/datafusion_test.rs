#![cfg(feature = "datafusion")]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use common::datafusion::context_with_delta_table_factory;
use datafusion::assert_batches_sorted_eq;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{common::collect, file_format::ParquetExec, metrics::Label};
use datafusion::physical_plan::{visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::ScalarValue::*;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Expr;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use url::Url;

use deltalake::action::SaveMode;
use deltalake::delta_datafusion::{DeltaPhysicalCodec, DeltaScan};
use deltalake::operations::create::CreateBuilder;
use deltalake::storage::DeltaObjectStore;
use deltalake::{
    operations::{write::WriteBuilder, DeltaOps},
    DeltaTable, Schema,
};

mod common;

fn get_scanned_files(node: &dyn ExecutionPlan) -> HashSet<Label> {
    node.metrics()
        .unwrap()
        .iter()
        .flat_map(|m| m.labels().to_vec())
        .collect()
}

#[derive(Debug, Default)]
pub struct ExecutionMetricsCollector {
    scanned_files: HashSet<Label>,
}

impl ExecutionMetricsCollector {
    fn num_scanned_files(&self) -> usize {
        self.scanned_files.len()
    }
}

impl ExecutionPlanVisitor for ExecutionMetricsCollector {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> std::result::Result<bool, Self::Error> {
        if let Some(exec) = plan.as_any().downcast_ref::<ParquetExec>() {
            let files = get_scanned_files(exec);
            self.scanned_files.extend(files);
        }
        Ok(true)
    }
}

async fn prepare_table(
    batches: Vec<RecordBatch>,
    save_mode: SaveMode,
    partitions: Vec<String>,
) -> (tempfile::TempDir, DeltaTable) {
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();
    let table_uri = table_path.to_str().unwrap().to_string();
    let table_schema: Schema = batches[0].schema().try_into().unwrap();

    let mut table = DeltaOps::try_from_uri(table_uri)
        .await
        .unwrap()
        .create()
        .with_save_mode(SaveMode::Ignore)
        .with_columns(table_schema.get_fields().clone())
        .with_partition_columns(partitions)
        .await
        .unwrap();

    for batch in batches {
        table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(save_mode.clone())
            .await
            .unwrap();
    }

    (table_dir, table)
}

#[tokio::test]
async fn test_datafusion_sql_registration() -> Result<()> {
    let ctx = context_with_delta_table_factory();

    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("tests/data/delta-0.8.0-partitioned");
    let sql = format!(
        "CREATE EXTERNAL TABLE demo STORED AS DELTATABLE LOCATION '{}'",
        d.to_str().unwrap()
    );
    let _ = ctx
        .sql(sql.as_str())
        .await
        .expect("Failed to register table!");

    let batches = ctx
        .sql("SELECT CAST( day as int ) as my_day FROM demo WHERE CAST( year as int ) > 2020 ORDER BY CAST( day as int ) ASC")
        .await?
        .collect()
        .await?;

    let batch = &batches[0];

    assert_eq!(
        batch.column(0).as_ref(),
        Arc::new(Int32Array::from(vec![4, 5, 20, 20])).as_ref(),
    );

    Ok(())
}

#[tokio::test]
async fn test_datafusion_simple_query_partitioned() -> Result<()> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("./tests/data/delta-0.8.0-partitioned")
        .await
        .unwrap();
    ctx.register_table("demo", Arc::new(table))?;

    let batches = ctx
            .sql("SELECT CAST( day as int ) as my_day FROM demo WHERE CAST( year as int ) > 2020 ORDER BY CAST( day as int ) ASC")
            .await?
            .collect()
            .await?;

    let batch = &batches[0];

    assert_eq!(
        batch.column(0).as_ref(),
        Arc::new(Int32Array::from(vec![4, 5, 20, 20])).as_ref(),
    );

    Ok(())
}

#[tokio::test]
async fn test_datafusion_write_from_serialized_delta_scan() -> Result<()> {
    // Build an execution plan for scanning a DeltaTable and serialize it to bytes.
    // We want to emulate that this occurs on another node, so that all we have access to is the
    // plan byte serialization.
    let source_scan_bytes = {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let source_table = deltalake::open_table("./tests/data/delta-0.8.0-date").await?;
        let source_scan = source_table.scan(&state, None, &[], None).await?;
        physical_plan_to_bytes_with_extension_codec(source_scan, &DeltaPhysicalCodec {})?
    };

    // Build a new context from scratch and deserialize the plan
    let ctx = SessionContext::new();
    let state = ctx.state();
    let source_scan = physical_plan_from_bytes_with_extension_codec(
        &source_scan_bytes,
        &ctx,
        &DeltaPhysicalCodec {},
    )?;
    let fields = Schema::try_from(source_scan.schema())
        .unwrap()
        .get_fields()
        .clone();

    // Create target Delta Table
    let target_table = CreateBuilder::new()
        .with_location("memory://target")
        .with_columns(fields)
        .with_table_name("target")
        .await?;

    // Trying to execute the write from the input plan without providing Datafusion with a session
    // state containing the referenced object store in the registry results in an error.
    assert!(
        WriteBuilder::new(target_table.object_store(), target_table.state.clone())
            .with_input_execution_plan(source_scan.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("No suitable object store found for delta-rs://")
    );

    // Register the missing source table object store
    let source_uri = source_scan
        .as_any()
        .downcast_ref::<DeltaScan>()
        .unwrap()
        .table_uri
        .clone();
    let source_location = Url::parse(&source_uri).unwrap();
    let source_store = DeltaObjectStore::try_new(source_location, HashMap::new()).unwrap();
    let object_store_url = source_store.object_store_url();
    let source_store_url: &Url = object_store_url.as_ref();
    state
        .runtime_env()
        .register_object_store(source_store_url, Arc::from(source_store));

    // Execute write to the target table with the proper state
    let target_table = WriteBuilder::new(target_table.object_store(), target_table.state.clone())
        .with_input_execution_plan(source_scan)
        .with_input_session_state(state)
        .await?;
    ctx.register_table("target", Arc::new(target_table))?;

    // Check results
    let batches = ctx.sql("SELECT * FROM target").await?.collect().await?;
    let expected = vec![
        "+------------+-----------+",
        "| date       | dayOfYear |",
        "+------------+-----------+",
        "| 2021-01-01 | 1         |",
        "| 2021-01-02 | 2         |",
        "| 2021-01-03 | 3         |",
        "| 2021-01-04 | 4         |",
        "| 2021-01-05 | 5         |",
        "+------------+-----------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_datafusion_date_column() -> Result<()> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("./tests/data/delta-0.8.0-date")
        .await
        .unwrap();
    ctx.register_table("dates", Arc::new(table))?;

    let batches = ctx
        .sql("SELECT date from dates WHERE \"dayOfYear\" = 2")
        .await?
        .collect()
        .await?;

    assert_eq!(
        batches[0].column(0).as_ref(),
        Arc::new(Date32Array::from(vec![18629])).as_ref(),
    );

    Ok(())
}

#[tokio::test]
async fn test_datafusion_stats() -> Result<()> {
    let table = deltalake::open_table("./tests/data/delta-0.8.0")
        .await
        .unwrap();
    let statistics = table.state.datafusion_table_statistics();

    assert_eq!(statistics.num_rows, Some(4),);

    assert_eq!(statistics.total_byte_size, Some(440 + 440));

    assert_eq!(
        statistics
            .column_statistics
            .clone()
            .unwrap()
            .iter()
            .map(|x| x.null_count)
            .collect::<Vec<Option<usize>>>(),
        vec![Some(0)],
    );

    let ctx = SessionContext::new();
    ctx.register_table("test_table", Arc::new(table))?;

    let batches = ctx
        .sql("SELECT max(value), min(value) FROM test_table")
        .await?
        .collect()
        .await?;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(
        batch.column(0).as_ref(),
        Arc::new(Int32Array::from(vec![4])).as_ref(),
    );

    assert_eq!(
        batch.column(1).as_ref(),
        Arc::new(Int32Array::from(vec![0])).as_ref(),
    );

    assert_eq!(
        statistics
            .column_statistics
            .clone()
            .unwrap()
            .iter()
            .map(|x| x.max_value.as_ref())
            .collect::<Vec<Option<&ScalarValue>>>(),
        vec![Some(&ScalarValue::from(4_i32))],
    );

    assert_eq!(
        statistics
            .column_statistics
            .clone()
            .unwrap()
            .iter()
            .map(|x| x.min_value.as_ref())
            .collect::<Vec<Option<&ScalarValue>>>(),
        vec![Some(&ScalarValue::from(0_i32))],
    );

    Ok(())
}

async fn get_scan_metrics(
    table: &DeltaTable,
    state: &SessionState,
    e: &[Expr],
) -> Result<ExecutionMetricsCollector> {
    let mut metrics = ExecutionMetricsCollector::default();
    let scan = table.scan(state, None, e, None).await?;
    if scan.output_partitioning().partition_count() > 0 {
        let plan = CoalescePartitionsExec::new(scan);
        let task_ctx = Arc::new(TaskContext::from(state));
        let _result = collect(plan.execute(0, task_ctx)?).await?;
        visit_execution_plan(&plan, &mut metrics).unwrap();
    }

    Ok(metrics)
}

fn create_all_types_batch(not_null_rows: usize, null_rows: usize, offset: usize) -> RecordBatch {
    let mut decimal_builder = Decimal128Builder::with_capacity(not_null_rows + null_rows);
    for x in 0..not_null_rows {
        decimal_builder.append_value(((x + offset) * 100) as i128);
    }
    decimal_builder.append_nulls(null_rows);
    let decimal = decimal_builder
        .finish()
        .with_precision_and_scale(10, 2)
        .unwrap();

    let data: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset).to_string()))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(Int64Array::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset) as i64))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(Int32Array::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset) as i32))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(Int16Array::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset) as i16))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(Int8Array::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset) as i8))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(Float64Array::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset) as f64))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(Float32Array::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset) as f32))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(BooleanArray::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset) % 2 == 0))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(BinaryArray::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset).to_string().as_bytes().to_owned()))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(decimal),
        //Convert to seconds
        Arc::new(TimestampMicrosecondArray::from_iter(
            (0..not_null_rows)
                .map(|x| Some(((x + offset) * 1_000_000) as i64))
                .chain((0..null_rows).map(|_| None)),
        )),
        Arc::new(Date32Array::from_iter(
            (0..not_null_rows)
                .map(|x| Some((x + offset) as i32))
                .chain((0..null_rows).map(|_| None)),
        )),
    ];

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("utf8", ArrowDataType::Utf8, true),
        ArrowField::new("int64", ArrowDataType::Int64, true),
        ArrowField::new("int32", ArrowDataType::Int32, true),
        ArrowField::new("int16", ArrowDataType::Int16, true),
        ArrowField::new("int8", ArrowDataType::Int8, true),
        ArrowField::new("float64", ArrowDataType::Float64, true),
        ArrowField::new("float32", ArrowDataType::Float32, true),
        ArrowField::new("boolean", ArrowDataType::Boolean, true),
        ArrowField::new("binary", ArrowDataType::Binary, true),
        ArrowField::new("decimal", ArrowDataType::Decimal128(10, 2), true),
        ArrowField::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        ArrowField::new("date", ArrowDataType::Date32, true),
    ]));

    RecordBatch::try_new(schema, data).unwrap()
}

struct TestCase {
    column: &'static str,
    file1_value: Expr,
    file2_value: Expr,
    file3_value: Expr,
    non_existent_value: Expr,
}

impl TestCase {
    fn new<F>(column: &'static str, expression_builder: F) -> Self
    where
        F: Fn(i64) -> Expr,
    {
        TestCase {
            column,
            file1_value: expression_builder(1),
            file2_value: expression_builder(5),
            file3_value: expression_builder(8),
            non_existent_value: expression_builder(3),
        }
    }

    fn new_wrapped<F>(column: &'static str, expression_builder: F) -> Self
    where
        F: Fn(i64) -> Expr,
    {
        TestCase {
            column,
            file1_value: wrap_expression(expression_builder(1)),
            file2_value: wrap_expression(expression_builder(5)),
            file3_value: wrap_expression(expression_builder(8)),
            non_existent_value: wrap_expression(expression_builder(3)),
        }
    }
}

fn wrap_expression(e: Expr) -> Expr {
    let value = match e {
        Expr::Literal(lit) => lit,
        _ => unreachable!(),
    };
    Expr::Literal(ScalarValue::Dictionary(
        Box::new(ArrowDataType::UInt16),
        Box::new(value),
    ))
}

#[tokio::test]
async fn test_files_scanned() -> Result<()> {
    // Validate that datafusion prunes files based on file statistics
    // Do not scan files when we know it does not contain the requested records
    use datafusion::prelude::*;
    let ctx = SessionContext::new();
    let state = ctx.state();

    async fn append_to_table(table: DeltaTable, batch: RecordBatch) -> DeltaTable {
        DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap()
    }

    let batch = create_all_types_batch(3, 0, 0);
    let (_tmp, table) = prepare_table(vec![batch], SaveMode::Overwrite, vec![]).await;

    let batch = create_all_types_batch(3, 0, 4);
    let table = append_to_table(table, batch).await;

    let batch = create_all_types_batch(3, 0, 7);
    let table = append_to_table(table, batch).await;

    let metrics = get_scan_metrics(&table, &state, &[]).await?;
    assert_eq!(metrics.num_scanned_files(), 3);

    // (Column name, value from file 1, value from file 2, value from file 3, non existent value)
    let tests = [
        TestCase::new("utf8", |value| lit(value.to_string())),
        TestCase::new("int64", lit),
        TestCase::new("int32", |value| lit(value as i32)),
        TestCase::new("int16", |value| lit(value as i16)),
        TestCase::new("int8", |value| lit(value as i8)),
        TestCase::new("float64", |value| lit(value as f64)),
        TestCase::new("float32", |value| lit(value as f32)),
        TestCase::new("timestamp", |value| {
            lit(TimestampMicrosecond(Some(value * 1_000_000), None))
        }),
        // TODO: I think decimal statistics are being written to the log incorrectly. The underlying i128 is written
        // not the proper string representation as specified by the precision and scale
        TestCase::new("decimal", |value| {
            lit(Decimal128(Some((value * 100).into()), 10, 2))
        }),
        // TODO: The writer does not write complete statistics for date columns
        TestCase::new("date", |value| lit(Date32(Some(value as i32)))),
        // TODO: The writer does not write complete statistics for binary columns
        TestCase::new("binary", |value| lit(value.to_string().as_bytes())),
    ];

    for test in &tests {
        let TestCase {
            column,
            file1_value,
            file2_value,
            file3_value,
            non_existent_value,
        } = test.to_owned();
        let column = column.to_owned();
        // TODO: The following types don't have proper stats written.
        // See issue #1208 for decimal type
        // See issue #1209 for dates
        // Min and Max is not calculated for binary columns. This matches the Spark writer
        if column == "decimal" || column == "date" || column == "binary" {
            continue;
        }
        println!("Test Column: {} value: {}", column, file1_value);

        // Equality
        let e = col(column).eq(file1_value.clone());
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 1);

        // Value does not exist
        let e = col(column).eq(non_existent_value.clone());
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 0);

        // Conjunction
        let e = col(column)
            .gt(file1_value.clone())
            .and(col(column).lt(file2_value.clone()));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 2);

        // Disjunction
        let e = col(column)
            .lt(file1_value.clone())
            .or(col(column).gt(file3_value.clone()));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 2);
    }

    // Validate Boolean type
    let batch = create_all_types_batch(1, 0, 0);
    let (_tmp, table) = prepare_table(vec![batch], SaveMode::Overwrite, vec![]).await;
    let batch = create_all_types_batch(1, 0, 1);
    let table = append_to_table(table, batch).await;

    let e = col("boolean").eq(lit(true));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert_eq!(metrics.num_scanned_files(), 1);

    let e = col("boolean").eq(lit(false));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert_eq!(metrics.num_scanned_files(), 1);

    let tests = [
        TestCase::new_wrapped("utf8", |value| lit(value.to_string())),
        TestCase::new_wrapped("int64", lit),
        TestCase::new_wrapped("int32", |value| lit(value as i32)),
        TestCase::new_wrapped("int16", |value| lit(value as i16)),
        TestCase::new_wrapped("int8", |value| lit(value as i8)),
        TestCase::new_wrapped("float64", |value| lit(value as f64)),
        TestCase::new_wrapped("float32", |value| lit(value as f32)),
        TestCase::new_wrapped("timestamp", |value| {
            lit(TimestampMicrosecond(Some(value * 1_000_000), None))
        }),
        // TODO: I think decimal statistics are being written to the log incorrectly. The underlying i128 is written
        // not the proper string representation as specified by the precision and scale
        TestCase::new_wrapped("decimal", |value| {
            lit(Decimal128(Some((value * 100).into()), 10, 2))
        }),
        // TODO: The writer does not write complete statistics for date columns
        TestCase::new_wrapped("date", |value| lit(Date32(Some(value as i32)))),
        // TODO: The writer does not write complete statistics for binary columns
        TestCase::new_wrapped("binary", |value| lit(value.to_string().as_bytes())),
    ];

    // Ensure that tables with stats and partition columns can be pruned
    for test in tests {
        let TestCase {
            column,
            file1_value,
            file2_value,
            file3_value,
            non_existent_value,
        } = test;
        // TODO: Float and decimal partitions are not supported by the writer
        // binary fails since arrow does not implement a natural order
        // The current Datafusion pruning implementation does not work for binary columns since they do not have a natural order. See #1214
        // Timestamp and date are disabled since the hive path contains illegal Windows values. see #1215
        if column == "float32"
            || column == "float64"
            || column == "decimal"
            || column == "binary"
            || column == "timestamp"
            || column == "date"
        {
            continue;
        }

        let partitions = vec![column.to_owned()];
        let batch = create_all_types_batch(3, 0, 0);
        let (_tmp, table) = prepare_table(vec![batch], SaveMode::Overwrite, partitions).await;

        let batch = create_all_types_batch(3, 0, 4);
        let table = append_to_table(table, batch).await;

        let batch = create_all_types_batch(3, 0, 7);
        let table = append_to_table(table, batch).await;

        // Equality
        let e = col(column).eq(file1_value.clone());
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 1);

        // Value does not exist
        let e = col(column).eq(non_existent_value);
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 0);

        // Conjunction
        let e = col(column)
            .gt(file1_value.clone())
            .and(col(column).lt(file2_value));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 2);

        // Disjunction
        let e = col(column).lt(file1_value).or(col(column).gt(file3_value));
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 2);

        // TODO how to get an expression with the right datatypes eludes me ..
        // Validate null pruning
        // let batch = create_all_types_batch(5, 2, 0);
        // let partitions = vec![column.to_owned()];
        // let (_tmp, table) = prepare_table(vec![batch], SaveMode::Overwrite, partitions).await;

        // let e = col(column).is_null();
        // let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        // assert_eq!(metrics.num_scanned_files(), 1);

        /*  logically we should be able to prune the null partition but Datafusion's current implementation prevents this */
        /*
        let e = col(column).is_not_null();
        let metrics = get_scan_metrics(&table, &state, &[e]).await?;
        assert_eq!(metrics.num_scanned_files(), 5);
        */
    }

    // Validate Boolean partition
    let batch = create_all_types_batch(1, 0, 0);
    let (_tmp, table) =
        prepare_table(vec![batch], SaveMode::Overwrite, vec!["boolean".to_owned()]).await;
    let batch = create_all_types_batch(1, 0, 1);
    let table = append_to_table(table, batch).await;

    let e = col("boolean").eq(lit(true));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert_eq!(metrics.num_scanned_files(), 1);

    let e = col("boolean").eq(lit(false));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert_eq!(metrics.num_scanned_files(), 1);

    // Ensure that tables without stats and partition columns can be pruned for just partitions
    // let table = deltalake::open_table("./tests/data/delta-0.8.0-null-partition").await?;

    /*
    // Logically this should prune. See above
    let e = col("k").eq(lit("A")).and(col("k").is_not_null());
    let metrics = get_scan_metrics(&table, &state, &[e]).await.unwrap();
    println!("{:?}", metrics);
    assert!(metrics.num_scanned_files() == 1);

    let e = col("k").eq(lit("B"));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 1);
    */

    // Check pruning for null partitions
    // let e = col("k").is_null();
    // let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    // assert_eq!(metrics.num_scanned_files(), 1);

    // Check pruning for null partitions. Since there are no record count statistics pruning cannot be done
    // let e = col("k").is_not_null();
    // let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    // assert_eq!(metrics.num_scanned_files(), 2);

    Ok(())
}

#[tokio::test]
async fn test_datafusion_partitioned_types() -> Result<()> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("./tests/data/delta-2.2.0-partitioned-types")
        .await
        .unwrap();
    ctx.register_table("demo", Arc::new(table))?;

    let batches = ctx.sql("SELECT * FROM demo").await?.collect().await?;

    let expected = vec![
        "+----+----+----+",
        "| c3 | c1 | c2 |",
        "+----+----+----+",
        "| 5  | 4  | c  |",
        "| 6  | 5  | b  |",
        "| 4  | 6  | a  |",
        "+----+----+----+",
    ];

    assert_batches_sorted_eq!(&expected, &batches);

    let expected_schema = ArrowSchema::new(vec![
        ArrowField::new("c3", ArrowDataType::Int32, true),
        ArrowField::new(
            "c1",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Int32),
            ),
            false,
        ),
        ArrowField::new(
            "c2",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Utf8),
            ),
            false,
        ),
    ]);

    assert_eq!(
        Arc::new(expected_schema),
        Arc::new(
            batches[0]
                .schema()
                .as_ref()
                .clone()
                .with_metadata(Default::default())
        )
    );

    Ok(())
}

#[tokio::test]
async fn test_datafusion_scan_timestamps() -> Result<()> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("./tests/data/table_with_edge_timestamps")
        .await
        .unwrap();
    ctx.register_table("demo", Arc::new(table))?;

    let batches = ctx.sql("SELECT * FROM demo").await?.collect().await?;

    let expected = vec![
        "+-------------------------------+---------------------+------------+",
        "| BIG_DATE                      | NORMAL_DATE         | SOME_VALUE |",
        "+-------------------------------+---------------------+------------+",
        "| 1816-03-28T05:56:08.066277376 | 2022-02-01T00:00:00 | 2          |",
        "| 1816-03-29T05:56:08.066277376 | 2022-01-01T00:00:00 | 1          |",
        "+-------------------------------+---------------------+------------+",
    ];

    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_issue_1292_datafusion_sql_projection() -> Result<()> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("./tests/data/http_requests")
        .await
        .unwrap();
    ctx.register_table("http_requests", Arc::new(table))?;

    let batches = ctx
        .sql("SELECT \"ClientRequestURI\" FROM http_requests LIMIT 5")
        .await?
        .collect()
        .await?;

    let expected = vec![
        "+------------------+",
        "| ClientRequestURI |",
        "+------------------+",
        "| /                |",
        "| /                |",
        "| /                |",
        "| /                |",
        "| /                |",
        "+------------------+",
    ];

    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_issue_1291_datafusion_sql_partitioned_data() -> Result<()> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("./tests/data/http_requests")
        .await
        .unwrap();
    ctx.register_table("http_requests", Arc::new(table))?;

    let batches = ctx
        .sql(
            "SELECT \"ClientRequestURI\", date FROM http_requests WHERE date > '2023-04-13' LIMIT 5",
        )
        .await?
        .collect()
        .await?;

    let expected = vec![
        "+------------------+------------+",
        "| ClientRequestURI | date       |",
        "+------------------+------------+",
        "| /                | 2023-04-14 |",
        "| /                | 2023-04-14 |",
        "| /                | 2023-04-14 |",
        "| /                | 2023-04-14 |",
        "| /                | 2023-04-14 |",
        "+------------------+------------+",
    ];

    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_issue_1374() -> Result<()> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("./tests/data/issue_1374")
        .await
        .unwrap();
    ctx.register_table("t", Arc::new(table))?;

    let batches = ctx
        .sql(
            r#"SELECT *
        FROM t
        WHERE timestamp BETWEEN '2023-05-24T00:00:00.000Z' AND '2023-05-25T00:00:00.000Z'
        LIMIT 5
        "#,
        )
        .await?
        .collect()
        .await?;

    let expected = vec![
        "+----------------------------+-------------+------------+",
        "| timestamp                  | temperature | date       |",
        "+----------------------------+-------------+------------+",
        "| 2023-05-24T00:01:25.010301 | 8           | 2023-05-24 |",
        "| 2023-05-24T00:01:25.013902 | 21          | 2023-05-24 |",
        "| 2023-05-24T00:01:25.013972 | 58          | 2023-05-24 |",
        "| 2023-05-24T00:01:25.014025 | 24          | 2023-05-24 |",
        "| 2023-05-24T00:01:25.014072 | 90          | 2023-05-24 |",
        "+----------------------------+-------------+------------+",
    ];

    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}
