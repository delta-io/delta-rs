#![cfg(feature = "datafusion")]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use common::datafusion::context_with_delta_table_factory;
use datafusion::assert_batches_sorted_eq;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{common::collect, file_format::ParquetExec, metrics::Label};
use datafusion::physical_plan::{visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Expr;

use deltalake::action::SaveMode;
use deltalake::operations::create::CreateBuilder;
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
) -> (tempfile::TempDir, Arc<DeltaTable>) {
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
        .await
        .unwrap();

    for batch in batches {
        table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(save_mode.clone())
            .await
            .unwrap();
    }

    (table_dir, Arc::new(table))
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
async fn test_datafusion_write_from_delta_scan() -> Result<()> {
    let ctx = SessionContext::new();
    let state = ctx.state();

    // Build an execution plan for scanning a DeltaTable
    let source_table = deltalake::open_table("./tests/data/delta-0.8.0-date").await?;
    let source_scan = source_table.scan(&state, None, &[], None).await?;

    // Create target Delta Table
    let target_table = CreateBuilder::new()
        .with_location("memory://target")
        .with_columns(source_table.schema().unwrap().get_fields().clone())
        .with_table_name("target")
        .await?;

    // Trying to execute the write by providing only the Datafusion plan and not the session state
    // results in an error due to missing object store in the runtime registry.
    assert!(WriteBuilder::new()
        .with_input_execution_plan(source_scan.clone())
        .with_object_store(target_table.object_store())
        .await
        .unwrap_err()
        .to_string()
        .contains("No suitable object store found for delta-rs://"));

    // Execute write to the target table with the proper state
    let target_table = WriteBuilder::new()
        .with_input_execution_plan(source_scan)
        .with_input_session_state(state)
        .with_object_store(target_table.object_store())
        .await?;
    ctx.register_table("target", Arc::new(target_table))?;

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
    let statistics = table.datafusion_table_statistics();

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

    return Ok(metrics);
}

#[tokio::test]
async fn test_files_scanned() -> Result<()> {
    // Validate that datafusion prunes files based on file statistics
    // Do not scan files when we know it does not contain the requested records
    use datafusion::prelude::*;
    let ctx = SessionContext::new();
    let state = ctx.state();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("string", ArrowDataType::Utf8, true),
    ]));
    let columns_1: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
        Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
    ];
    let columns_2: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
        Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
    ];
    let batches = vec![
        RecordBatch::try_new(arrow_schema.clone(), columns_1)?,
        RecordBatch::try_new(arrow_schema.clone(), columns_2)?,
    ];
    let (_temp_dir, table) = prepare_table(batches, SaveMode::Append).await;
    assert_eq!(table.version(), 2);

    let metrics = get_scan_metrics(&table, &state, &[]).await?;
    assert!(metrics.num_scanned_files() == 2);

    let e = col("id").gt(lit(5));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 1);

    // Ensure that tables without stats and partition columns can be pruned for just partitions
    let table = deltalake::open_table(
        "./tests/data/delta-0.8.0-null-partition",
    )
    .await?;

    /*
    // Logically this should prune... Might require an update on datafusion
    let e = col("k").eq(lit("A")).and(col("k").is_not_null());
    let metrics = get_scan_metrics(&table, &state, &[e]).await.unwrap();
    println!("{:?}", metrics);
    assert!(metrics.num_scanned_files() == 1);

    let e = col("k").eq(lit("B"));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 1);
    */

    // Check pruning for null partitions
    let e = col("k").is_null();
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 1);

    // Check pruning for null partitions. Since there are no record count statistics pruning cannot be done
    let e = col("k").is_not_null();
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 2);

    // Ensure that tables with stats and partition columns can be pruned
    let table = deltalake::open_table(
        "./tests/data/delta-2.2.0-partitioned-types",
    )
    .await?;

    let e = col("c1").eq(lit(1));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 0);

    let e = col("c1").eq(lit(4));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 1);

    let e = col("c3").eq(lit(4));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 1);

    let e = col("c3").eq(lit(0));
    let metrics = get_scan_metrics(&table, &state, &[e]).await?;
    assert!(metrics.num_scanned_files() == 0);

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

    assert_eq!(Arc::new(expected_schema), batches[0].schema());

    Ok(())
}
