use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use futures::future::BoxFuture;
use crate::delta_datafusion::DeltaScanBuilder;

use crate::DeltaTable;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;

use super::transaction::PROTOCOL;

#[derive(Debug, Clone)]
pub struct CdfLoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// A sub-selection of columns to be loaded
    columns: Option<Vec<String>>,
    /// Version to read from
    starting_version: i64,
}

impl CdfLoadBuilder {
    /// Create a new [`LoadBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            columns: None,
            starting_version: 0,
        }
    }

    /// Specify column selection to load
    pub fn with_columns(mut self, columns: impl IntoIterator<Item=impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    pub fn with_starting_version(mut self, starting_version: i64) -> Self {
        self.starting_version = starting_version;
        self
    }
}

impl std::future::IntoFuture for CdfLoadBuilder {
    type Output = DeltaResult<(DeltaTable, SendableRecordBatchStream)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            PROTOCOL.can_read_from(&this.snapshot)?;

            let table = DeltaTable::new_with_state(this.log_store, this.snapshot);
            let schema = this.snapshot.arrow_schema()?;
            let projection = this
                .columns
                .map(|cols| {
                    cols.iter()
                        .map(|col| {
                            schema.column_with_name(col).map(|(idx, _)| idx).ok_or(
                                DeltaTableError::SchemaMismatch {
                                    msg: format!("Column '{col}' does not exist in table schema."),
                                },
                            )
                        })
                        .collect::<Result<_, _>>()
                })
                .transpose()?;

            // let commits = this.snapshot.commit_infos();


            let ctx = SessionContext::new();
            let scan = DeltaScanBuilder::new(&this.snapshot, this.log_store, session)
                .with_projection(projection)
                .with_files()
                .build()
                .await?;
            let plan = CoalescePartitionsExec::new(scan_plan);
            let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
            let stream = plan.execute(0, task_ctx)?;

            Ok((table, stream))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::DeltaTableBuilder;
    use crate::writer::test_utils::TestResult;

    #[tokio::test]
    async fn test_load_local() -> TestResult {
        let mut table = DeltaTableBuilder::from_uri("C:\\Users\\shcar\\IdeaProjects\\lido\\cdf-table")
            .load()
            .await
            .unwrap();
        table.load().await?;

        for cdc_file in table.state.cdc_files() {
            println!("{:?}", cdc_file);
        }
        Ok(())
    }
}