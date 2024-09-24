use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt16Type;
use arrow_array::RecordBatch;
use arrow_schema::SchemaBuilder;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::TaskContext;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchemaRef, Result, ToDFSchema};
use datafusion_expr::{col, Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::limit::LocalLimitExec;
use datafusion_physical_plan::ExecutionPlan;
use lazy_static::lazy_static;

use crate::delta_datafusion::find_files::logical::FindFilesNode;
use crate::delta_datafusion::find_files::physical::FindFilesExec;
use crate::delta_datafusion::{
    df_logical_schema, register_store, DeltaScanBuilder, DeltaScanConfigBuilder, PATH_COLUMN,
};
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;
use crate::DeltaTableError;

pub mod logical;
pub mod physical;

lazy_static! {
    static ref ONLY_FILES_SCHEMA: Arc<Schema> = {
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new(PATH_COLUMN, DataType::Utf8, false));
        Arc::new(builder.finish())
    };
    static ref ONLY_FILES_DF_SCHEMA: DFSchemaRef =
        ONLY_FILES_SCHEMA.clone().to_dfschema_ref().unwrap();
}

#[derive(Default)]
struct FindFilesPlannerExtension {}

#[derive(Default)]
struct FindFilesPlanner {}

#[async_trait]
impl ExtensionPlanner for FindFilesPlannerExtension {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(find_files_node) = node.as_any().downcast_ref::<FindFilesNode>() {
            return Ok(Some(Arc::new(FindFilesExec::new(
                find_files_node.state(),
                find_files_node.log_store(),
                find_files_node.predicate(),
            )?)));
        }
        Ok(None)
    }
}

#[async_trait]
impl QueryPlanner for FindFilesPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = Arc::new(Box::new(DefaultPhysicalPlanner::with_extension_planners(
            vec![Arc::new(FindFilesPlannerExtension {})],
        )));
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

async fn scan_table_by_partitions(batch: RecordBatch, predicate: Expr) -> Result<RecordBatch> {
    let mut arrays = Vec::new();
    let mut fields = Vec::new();

    let schema = batch.schema();

    arrays.push(
        batch
            .column_by_name("path")
            .ok_or(DeltaTableError::Generic(
                "Column with name `path` does not exist".to_owned(),
            ))?
            .to_owned(),
    );
    fields.push(Field::new(PATH_COLUMN, DataType::Utf8, false));

    for field in schema.fields() {
        if field.name().starts_with("partition.") {
            let name = field.name().strip_prefix("partition.").unwrap();

            arrays.push(batch.column_by_name(field.name()).unwrap().to_owned());
            fields.push(Field::new(
                name,
                field.data_type().to_owned(),
                field.is_nullable(),
            ));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let ctx = SessionContext::new();
    let mut df = ctx.read_table(Arc::new(mem_table))?;
    df = df
        .filter(predicate.to_owned())?
        .select(vec![col(PATH_COLUMN)])?;
    let df_schema = df.schema().clone();
    let batches = df.collect().await?;
    Ok(concat_batches(&SchemaRef::from(df_schema), &batches)?)
}

async fn scan_table_by_files(
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    state: SessionState,
    expression: Expr,
) -> Result<RecordBatch> {
    register_store(log_store.clone(), state.runtime_env().clone());
    let scan_config = DeltaScanConfigBuilder::new()
        .wrap_partition_values(true)
        .with_file_column(true)
        .build(&snapshot)?;

    let logical_schema = df_logical_schema(&snapshot, &scan_config.file_column_name, None)?;

    // Identify which columns we need to project
    let mut used_columns = expression
        .column_refs()
        .into_iter()
        .map(|column| logical_schema.index_of(&column.name))
        .collect::<std::result::Result<Vec<usize>, ArrowError>>()?;
    // Add path column
    used_columns.push(logical_schema.index_of(scan_config.file_column_name.as_ref().unwrap())?);

    let scan = DeltaScanBuilder::new(&snapshot, log_store, &state)
        .with_filter(Some(expression.clone()))
        .with_projection(Some(&used_columns))
        .with_scan_config(scan_config)
        .build()
        .await?;

    let scan = Arc::new(scan);
    let input_schema = scan.logical_schema.as_ref().to_owned();
    let input_dfschema = input_schema.clone().try_into()?;

    let predicate_expr =
        state.create_physical_expr(Expr::IsTrue(Box::new(expression.clone())), &input_dfschema)?;

    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, scan.clone())?);
    let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(filter, 1));
    let field_idx = input_schema.index_of(PATH_COLUMN)?;
    let task_ctx = Arc::new(TaskContext::from(&state));
    let path_batches: Vec<RecordBatch> = datafusion::physical_plan::collect(limit, task_ctx)
        .await?
        .into_iter()
        .map(|batch| {
            let col = batch
                .column(field_idx)
                .as_dictionary::<UInt16Type>()
                .values();
            RecordBatch::try_from_iter(vec![(PATH_COLUMN, col.clone())]).unwrap()
        })
        .collect();

    let result_batches = concat_batches(&ONLY_FILES_SCHEMA.clone(), &path_batches)?;

    Ok(result_batches)
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{DataFrame, SessionContext};
    use datafusion_common::{assert_batches_eq, assert_batches_sorted_eq};
    use datafusion_expr::{col, lit, Expr, Extension, LogicalPlan};

    use crate::delta_datafusion::find_files::logical::FindFilesNode;
    use crate::delta_datafusion::find_files::FindFilesPlanner;
    use crate::operations::collect_sendable_stream;
    use crate::{DeltaResult, DeltaTable, DeltaTableError};

    pub async fn test_plan<'a>(
        table: DeltaTable,
        expr: Expr,
    ) -> Result<Vec<arrow_array::RecordBatch>, DeltaTableError> {
        let ctx = SessionContext::new();
        let state = SessionStateBuilder::new_from_existing(ctx.state())
            .with_query_planner(Arc::new(FindFilesPlanner::default()))
            .build();
        let find_files_node = LogicalPlan::Extension(Extension {
            node: Arc::new(FindFilesNode::new(
                "my_cool_plan".into(),
                table.snapshot()?.clone(),
                table.log_store().clone(),
                expr,
            )?),
        });
        let df = DataFrame::new(state.clone(), find_files_node);

        let p = state
            .clone()
            .create_physical_plan(df.logical_plan())
            .await?;

        let e = p.execute(0, state.task_ctx())?;
        collect_sendable_stream(e).await.map_err(Into::into)
    }

    #[tokio::test]
    pub async fn test_find_files_partitioned() -> DeltaResult<()> {
        let table = crate::open_table("../test/tests/data/delta-0.8.0-partitioned").await?;
        let expr: Expr = col("year").eq(lit(2020));
        let s = test_plan(table, expr).await?;

        assert_batches_eq! {
          ["+---------------------------------------------------------------------------------------------+",
           "| __delta_rs_path                                                                             |",
           "+---------------------------------------------------------------------------------------------+",
           "| year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet |",
           "| year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet |",
           "| year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet |",
           "+---------------------------------------------------------------------------------------------+"],
            &s
        }
        Ok(())
    }

    #[tokio::test]
    pub async fn test_find_files_unpartitioned() -> DeltaResult<()> {
        let table = crate::open_table("../test/tests/data/simple_table").await?;
        let expr: Expr = col("id").in_list(vec![lit(9i64), lit(7i64)], false);
        let s = test_plan(table, expr).await?;

        assert_batches_sorted_eq! {
            ["+---------------------------------------------------------------------+",
             "| __delta_rs_path                                                     |",
             "+---------------------------------------------------------------------+",
             "| part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet |",
             "| part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet |",
             "+---------------------------------------------------------------------+"],
            &s
        }
        Ok(())
    }

    #[tokio::test]
    pub async fn test_find_files_unpartitioned2() -> DeltaResult<()> {
        let table = crate::open_table("../test/tests/data/simple_table").await?;
        let expr: Expr = col("id").is_not_null();
        let s = test_plan(table, expr).await?;

        assert_batches_sorted_eq! {
            ["+---------------------------------------------------------------------+",
             "| __delta_rs_path                                                     |",
             "+---------------------------------------------------------------------+",
             "| part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet |",
             "| part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet |",
             "| part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet |",
             "+---------------------------------------------------------------------+"],
            &s
        }
        Ok(())
    }
}
