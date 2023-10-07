use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::future::BoxFuture;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::storage::DeltaObjectStore;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;

#[derive(Debug, Clone)]
pub struct LoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    store: Arc<DeltaObjectStore>,
    /// A sub-selection of columns to be loaded
    columns: Option<Vec<String>>,
}

impl LoadBuilder {
    /// Create a new [`LoadBuilder`]
    pub fn new(store: Arc<DeltaObjectStore>, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            store,
            columns: None,
        }
    }

    /// Specify column selection to load
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }
}

impl std::future::IntoFuture for LoadBuilder {
    type Output = DeltaResult<(DeltaTable, SendableRecordBatchStream)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let table = DeltaTable::new_with_state(this.store, this.snapshot);
            let schema = table.state.arrow_schema()?;
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

            let ctx = SessionContext::new();
            let scan_plan = table
                .scan(&ctx.state(), projection.as_ref(), &[], None)
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
    use crate::operations::{collect_sendable_stream, DeltaOps};
    use crate::writer::test_utils::{get_record_batch, TestResult};
    use crate::DeltaTableBuilder;
    use datafusion::assert_batches_sorted_eq;

    #[tokio::test]
    async fn test_load_local() -> TestResult {
        let table = DeltaTableBuilder::from_uri("./tests/data/delta-0.8.0")
            .load()
            .await
            .unwrap();

        let (_table, stream) = DeltaOps(table).load().await?;
        let data = collect_sendable_stream(stream).await?;

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "| 0     |",
            "| 1     |",
            "| 2     |",
            "| 4     |",
            "+-------+",
        ];

        assert_batches_sorted_eq!(&expected, &data);
        Ok(())
    }

    #[tokio::test]
    async fn test_write_load() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = DeltaOps::new_in_memory().write(vec![batch.clone()]).await?;

        let (_table, stream) = DeltaOps(table).load().await?;
        let data = collect_sendable_stream(stream).await?;

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 1     | 2021-02-02 |",
            "| B  | 2     | 2021-02-02 |",
            "| A  | 3     | 2021-02-02 |",
            "| B  | 4     | 2021-02-01 |",
            "| A  | 5     | 2021-02-01 |",
            "| A  | 6     | 2021-02-01 |",
            "| A  | 7     | 2021-02-01 |",
            "| B  | 8     | 2021-02-01 |",
            "| B  | 9     | 2021-02-01 |",
            "| A  | 10    | 2021-02-01 |",
            "| A  | 11    | 2021-02-01 |",
            "+----+-------+------------+",
        ];

        assert_batches_sorted_eq!(&expected, &data);
        assert_eq!(batch.schema(), data[0].schema());
        Ok(())
    }

    #[tokio::test]
    async fn test_load_with_columns() -> TestResult {
        let batch = get_record_batch(None, false);
        let table = DeltaOps::new_in_memory().write(vec![batch.clone()]).await?;

        let (_table, stream) = DeltaOps(table).load().with_columns(["id", "value"]).await?;
        let data = collect_sendable_stream(stream).await?;

        let expected = vec![
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| A  | 1     |",
            "| B  | 2     |",
            "| A  | 3     |",
            "| B  | 4     |",
            "| A  | 5     |",
            "| A  | 6     |",
            "| A  | 7     |",
            "| B  | 8     |",
            "| B  | 9     |",
            "| A  | 10    |",
            "| A  | 11    |",
            "+----+-------+",
        ];

        assert_batches_sorted_eq!(&expected, &data);
        Ok(())
    }
}
