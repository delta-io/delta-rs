use datafusion::physical_plan::SendableRecordBatchStream;
use futures::future::BoxFuture;

use crate::delta_datafusion::cdf::scan::DeltaCdfScan;
use crate::DeltaTableError;
use crate::errors::DeltaResult;
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;

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
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    pub fn with_starting_version(mut self, starting_version: i64) -> Self {
        self.starting_version = starting_version;
        self
    }
}

impl std::future::IntoFuture for CdfLoadBuilder {
    type Output = DeltaResult<SendableRecordBatchStream>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let partition_cols = this.snapshot.metadata()
                .ok_or(DeltaTableError::NoMetadata)?
                .clone()
                .partition_columns
                .clone();
            let scan = DeltaCdfScan::new(
                this.log_store.clone(),
                this.starting_version,
                this.snapshot.arrow_schema()?,
                partition_cols,
            );
            scan.scan().await
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use arrow_cast::pretty::print_batches;

    use crate::operations::collect_sendable_stream;
    use crate::writer::test_utils::TestResult;
    use crate::DeltaOps;

    #[tokio::test]
    async fn test_load_local() -> TestResult {
        let _table = DeltaOps::try_from_uri("./tests/data/cdf-table/")
            .await?
            .load_cdf()
            .await?;

        let data: Vec<RecordBatch> = collect_sendable_stream(_table).await?;
        print_batches(&data)?;
        Ok(())
    }
}
