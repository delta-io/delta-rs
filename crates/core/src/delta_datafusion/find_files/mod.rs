use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::SchemaBuilder;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::{DFSchemaRef, Result, ToDFSchema};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use lazy_static::lazy_static;

use crate::delta_datafusion::find_files::logical::FindFilesNode;
use crate::delta_datafusion::find_files::physical::FindFilesExec;
use crate::delta_datafusion::PATH_COLUMN;

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

struct FindFilesPlannerExtension {}

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
                find_files_node.files(),
                find_files_node.predicate().clone(),
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

async fn scan_memory_table_batch(batch: RecordBatch, predicate: Expr) -> Result<RecordBatch> {
    let ctx = SessionContext::new();
    let mut batches = vec![];
    let columns = predicate
        .to_columns()?
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();

    if let Some(column) = batch.column_by_name(PATH_COLUMN) {
        let mut column_iter = column.as_string::<i32>().into_iter();
        while let Some(Some(row)) = column_iter.next() {
            let df = ctx
                .read_parquet(row, ParquetReadOptions::default())
                .await?
                .select(columns.clone())?
                .filter(predicate.clone())?
                .limit(0, Some(1))?;
            if df.count().await? > 0 {
                batches.push(row);
            }
        }
    }
    let str_array = Arc::new(StringArray::from(batches));
    RecordBatch::try_new(ONLY_FILES_SCHEMA.clone(), vec![str_array]).map_err(Into::into)
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use arrow_cast::pretty::print_batches;
    use datafusion::prelude::{DataFrame, SessionContext};
    use datafusion_expr::{col, lit, Extension, LogicalPlan};

    use crate::delta_datafusion::find_files::logical::FindFilesNode;
    use crate::delta_datafusion::find_files::FindFilesPlanner;
    use crate::operations::collect_sendable_stream;
    use crate::writer::test_utils::{create_bare_table, get_record_batch};
    use crate::{DeltaOps, DeltaResult};

    async fn make_table() -> DeltaOps {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await
            .unwrap();
        DeltaOps(write)
    }

    #[tokio::test]
    pub async fn test_find_files() -> DeltaResult<()> {
        let ctx = SessionContext::new();
        let state = ctx
            .state()
            .with_query_planner(Arc::new(FindFilesPlanner {}));
        let table = make_table().await;
        let find_files_node = LogicalPlan::Extension(Extension {
            node: Arc::new(FindFilesNode::new(
                "my_cool_plan".into(),
                table.0.snapshot()?.clone(),
                table.0.log_store.clone(),
                col("id").eq(lit("A")),
            )?),
        });
        let df = DataFrame::new(state.clone(), find_files_node);
        let p = state
            .clone()
            .create_physical_plan(df.logical_plan())
            .await
            .unwrap();

        let e = p.execute(0, state.task_ctx())?;
        let s = collect_sendable_stream(e).await.unwrap();
        print_batches(&s)?;
        Ok(())
    }
}
