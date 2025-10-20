use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::future::BoxFuture;

use super::CustomExecuteHandler;
use crate::delta_datafusion::{create_session, DataFusionMixins as _};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::transaction::PROTOCOL;
use crate::kernel::{resolve_snapshot, EagerSnapshot};
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;

#[derive(Clone)]
pub struct LoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// A sub-selection of columns to be loaded
    columns: Option<Vec<String>>,
    /// Datafusion session state relevant for executing the input plan
    session: Option<Arc<dyn Session>>,
}

impl std::fmt::Debug for LoadBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadBuilder")
            .field("snapshot", &self.snapshot)
            .field("log_store", &self.log_store)
            .finish()
    }
}

impl super::Operation for LoadBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        unimplemented!("Not required in loadBuilder for now.")
    }
}

impl LoadBuilder {
    /// Create a new [`LoadBuilder`]
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            columns: None,
            session: None,
        }
    }

    /// Specify column selection to load
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, session: Arc<dyn Session>) -> Self {
        self.session = Some(session);
        self
    }
}

impl std::future::IntoFuture for LoadBuilder {
    type Output = DeltaResult<(DeltaTable, SendableRecordBatchStream)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot = resolve_snapshot(&this.log_store, this.snapshot, true).await?;
            PROTOCOL.can_read_from(&snapshot)?;

            let schema = snapshot.read_schema();
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

            let session = this
                .session
                .unwrap_or_else(|| Arc::new(create_session().into_inner().state()));

            let table = DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot));
            let scan_plan = table
                .scan(session.as_ref(), projection.as_ref(), &[], None)
                .await?;

            let plan = CoalescePartitionsExec::new(scan_plan);
            let stream = plan.execute(0, session.task_ctx())?;

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
    use std::path::Path;
    use url::Url;

    #[tokio::test]
    async fn test_load_local() -> TestResult {
        let table_path = Path::new("../test/tests/data/delta-0.8.0");
        let table_uri =
            Url::from_directory_path(std::fs::canonicalize(table_path).unwrap()).unwrap();
        let table = DeltaTableBuilder::from_uri(table_uri)?
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
