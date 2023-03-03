use std::collections::HashMap;
use std::sync::Arc;

use crate::storage::DeltaObjectStore;
use crate::{builder::ensure_table_uri, DeltaResult, DeltaTable};

use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::future::BoxFuture;

#[derive(Debug, Clone)]
pub struct LoadBuilder {
    location: Option<String>,
    columns: Option<Vec<String>>,
    storage_options: Option<HashMap<String, String>>,
    object_store: Option<Arc<DeltaObjectStore>>,
}

impl Default for LoadBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBuilder {
    /// Create a new [`LoadBuilder`]
    pub fn new() -> Self {
        Self {
            location: None,
            columns: None,
            storage_options: None,
            object_store: None,
        }
    }

    /// Specify the path to the location where table data is stored,
    /// which could be a path on distributed storage.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Specify column selection to load
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Set options used to initialize storage backend
    ///
    /// Options may be passed in the HashMap or set as environment variables.
    ///
    /// [crate::builder::s3_storage_options] describes the available options for the AWS or S3-compliant backend.
    /// [dynamodb_lock::DynamoDbLockClient] describes additional options for the AWS atomic rename client.
    /// [crate::builder::azure_storage_options] describes the available options for the Azure backend.
    /// [crate::builder::gcp_storage_options] describes the available options for the Google Cloud Platform backend.
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    /// Provide a [`DeltaObjectStore`] instance, that points at table location
    pub fn with_object_store(mut self, object_store: Arc<DeltaObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }
}

impl std::future::IntoFuture for LoadBuilder {
    type Output = DeltaResult<(DeltaTable, SendableRecordBatchStream)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let object_store = this.object_store.unwrap();
            let url = ensure_table_uri(object_store.root_uri())?;
            let store = object_store.storage_backend().clone();
            let mut table = DeltaTable::new(object_store, Default::default());
            table.load().await?;

            let ctx = SessionContext::new();
            ctx.state()
                .runtime_env()
                .register_object_store(url.scheme(), "", store);
            let scan_plan = table.scan(&ctx.state(), None, &[], None).await?;
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
}
