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
use datafusion_common::Result;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};

use crate::delta_datafusion::find_files::logical::FindFilesNode;
use crate::delta_datafusion::find_files::physical::FindFilesExec;
use crate::delta_datafusion::PATH_COLUMN;

pub mod logical;
pub mod physical;

#[inline]
fn only_file_path_schema() -> Arc<Schema> {
    let mut builder = SchemaBuilder::new();
    builder.push(Field::new(PATH_COLUMN, DataType::Utf8, false));
    Arc::new(builder.finish())
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

    if let Some(column) = batch.column_by_name(PATH_COLUMN) {
        let mut column_iter = column.as_string::<i32>().into_iter();
        while let Some(Some(row)) = column_iter.next() {
            let df = ctx
                .read_parquet(row, ParquetReadOptions::default())
                .await?
                .filter(predicate.to_owned())?;
            if df.count().await? > 0 {
                batches.push(row);
            }
        }
    }
    let str_array = Arc::new(StringArray::from(batches));
    RecordBatch::try_new(only_file_path_schema(), vec![str_array]).map_err(Into::into)
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use arrow_cast::pretty::print_batches;
    use arrow_schema::{DataType, Field, Fields, Schema, SchemaBuilder};
    use datafusion::prelude::{DataFrame, SessionContext};
    use datafusion_expr::{col, lit, Extension, LogicalPlan};

    use crate::delta_datafusion::find_files::logical::FindFilesNode;
    use crate::delta_datafusion::find_files::FindFilesPlanner;
    use crate::delta_datafusion::PATH_COLUMN;
    use crate::operations::collect_sendable_stream;
    use crate::writer::test_utils::{create_bare_table, get_record_batch};
    use crate::{DeltaOps, DeltaResult};

    #[inline]
    fn find_files_schema(fields: &Fields) -> Arc<Schema> {
        let mut builder = SchemaBuilder::from(fields);
        builder.reverse();
        builder.push(Field::new(PATH_COLUMN, DataType::Utf8, false));
        builder.reverse();
        Arc::new(builder.finish())
    }

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
        table.0.get_file_uris()?.for_each(|f| println!("{:?}", f));
        let find_files_node = LogicalPlan::Extension(Extension {
            node: Arc::new(FindFilesNode::new(
                "my_cool_plan".into(),
                table.0.snapshot()?.snapshot.clone(),
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
