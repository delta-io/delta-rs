#![cfg(feature = "datafusion-ext")]

use arrow::{
    array::*,
    datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
        SchemaRef as ArrowSchemaRef,
    },
    record_batch::RecordBatch,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_plan::Column;
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, common, file_format::ParquetExec, metrics::Label,
    visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor,
};
use datafusion::scalar::ScalarValue;
use deltalake::{action::SaveMode, operations::DeltaCommands, DeltaTable, DeltaTableMetaData};
use std::collections::HashMap;
use std::{collections::HashSet, sync::Arc};

fn get_scanned_files(node: &dyn ExecutionPlan) -> HashSet<Label> {
    node.metrics()
        .unwrap()
        .clone()
        .iter()
        .cloned()
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
    schema: ArrowSchemaRef,
    batches: Vec<RecordBatch>,
    save_mode: SaveMode,
) -> (tempfile::TempDir, Arc<DeltaTable>) {
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();
    let table_uri = table_path.to_str().unwrap().to_string();

    let mut commands = DeltaCommands::try_from_uri(table_uri.clone())
        .await
        .unwrap();

    let table_schema = schema.clone().try_into().unwrap();
    let metadata = DeltaTableMetaData::new(None, None, None, table_schema, vec![], HashMap::new());
    let _ = commands
        .create(metadata.clone(), SaveMode::Ignore)
        .await
        .unwrap();

    for batch in batches {
        commands
            .write(vec![batch], save_mode.clone(), None)
            .await
            .unwrap();
    }

    (table_dir, Arc::new(commands.into()))
}

#[tokio::test]
async fn test_datafusion_simple_query_partitioned() -> Result<()> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("./tests/data/delta-0.8.0-partitioned")
        .await
        .unwrap();
    ctx.register_table("demo", Arc::new(table))?;

    let batches = ctx
            .sql("SELECT CAST( day as int ) FROM demo WHERE CAST( year as int ) > 2020 ORDER BY CAST( day as int ) ASC")
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
        vec![Some(&ScalarValue::from(4 as i32))],
    );

    assert_eq!(
        statistics
            .column_statistics
            .clone()
            .unwrap()
            .iter()
            .map(|x| x.min_value.as_ref())
            .collect::<Vec<Option<&ScalarValue>>>(),
        vec![Some(&ScalarValue::from(0 as i32))],
    );

    Ok(())
}

#[tokio::test]
async fn test_files_scanned() -> Result<()> {
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
    let (_temp_dir, table) = prepare_table(arrow_schema.clone(), batches, SaveMode::Append).await;
    assert_eq!(table.version(), 2);

    let ctx = SessionContext::new();
    let plan = table.scan(&ctx.state(), &None, &[], None).await?;
    let plan = CoalescePartitionsExec::new(plan.clone());

    let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
    let _ = common::collect(plan.execute(0, task_ctx)?).await?;

    let mut metrics = ExecutionMetricsCollector::default();
    visit_execution_plan(&plan, &mut metrics).unwrap();
    assert!(metrics.num_scanned_files() == 2);

    let filter = Expr::gt(
        Expr::Column(Column::from_name("id")),
        Expr::Literal(ScalarValue::Int32(Some(5))),
    );

    let plan = CoalescePartitionsExec::new(table.scan(&ctx.state(), &None, &[filter], None).await?);
    let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
    let _result = common::collect(plan.execute(0, task_ctx)?).await?;

    let mut metrics = ExecutionMetricsCollector::default();
    visit_execution_plan(&plan, &mut metrics).unwrap();
    assert!(metrics.num_scanned_files() == 1);

    Ok(())
}
