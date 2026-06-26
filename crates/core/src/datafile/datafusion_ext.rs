//! DataFusion-backed extensions to the basic data-file traits: write an
//! `ExecutionPlan`'s output, and read through `DeltaScanNext` (pushdown,
//! deletion vectors, transforms). The writer extension late-materializes a plan
//! into the basic record-batch stream and delegates to [`DeltaDataWriter`].

use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{
    ExecutionPlan, SendableRecordBatchStream, execute_stream_partitioned,
};
use futures::stream::{StreamExt as _, select_all};

use super::writer::DeltaWriter;
use super::{
    BatchFuture, DeltaDataReader, DeltaDataWriter, ReadOptions, RecordBatchFutureStream,
    results_to_future_stream,
};
use crate::DeltaTable;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;

/// Adapt several DataFusion partition streams into one basic
/// [`RecordBatchFutureStream`], polling all of them concurrently.
fn sendable_streams_to_future_stream(
    streams: Vec<SendableRecordBatchStream>,
) -> RecordBatchFutureStream {
    // `select_all` panics on an empty iterator; an empty input is just an empty stream.
    if streams.is_empty() {
        return futures::stream::empty::<BatchFuture>().boxed();
    }
    results_to_future_stream(select_all(streams))
}

/// Options controlling a DataFusion-backed scan.
#[derive(Debug, Default, Clone)]
pub struct ScanOptions {
    /// Project to this subset of (logical) column names. `None` reads all columns.
    pub projection: Option<Vec<String>>,
    /// Stop after returning at least this many rows. `None` reads the whole table.
    pub limit: Option<usize>,
}

impl From<ReadOptions> for ScanOptions {
    fn from(value: ReadOptions) -> Self {
        Self {
            projection: value.projection,
            limit: value.limit,
        }
    }
}

/// DataFusion extension to [`DeltaDataWriter`]: write the output of an execution plan.
#[async_trait::async_trait]
pub trait DeltaDataWriterExt {
    /// Execute `plan` (already containing any validation/repartition/CDC nodes)
    /// against `session` and write its output through the basic writer.
    async fn write_plan(
        self: Box<Self>,
        session: &dyn Session,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DeltaResult<Vec<Add>>;
}

#[async_trait::async_trait]
impl DeltaDataWriterExt for DeltaWriter {
    async fn write_plan(
        self: Box<Self>,
        session: &dyn Session,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DeltaResult<Vec<Add>> {
        let streams = execute_stream_partitioned(plan, session.task_ctx())?;
        self.write_all(sendable_streams_to_future_stream(streams))
            .await
    }
}

/// DataFusion extension to [`DeltaDataReader`]: a full scan with pushdown.
#[async_trait::async_trait]
pub trait DeltaDataReaderExt: DeltaDataReader {
    /// Scan the table through the DataFusion `DeltaScanNext` provider, returning
    /// a coalesced single-partition stream.
    async fn scan(
        &self,
        session: &dyn Session,
        options: ScanOptions,
    ) -> DeltaResult<SendableRecordBatchStream>;
}

/// A DataFusion-backed reader wrapping the existing `DeltaScanNext` provider.
/// It carries its own session so it can also satisfy [`DeltaDataReader`].
pub struct DataFusionDataReader {
    provider: Arc<dyn TableProvider>,
    session: Arc<dyn Session>,
}

impl DataFusionDataReader {
    /// Create a reader from an already-built table provider and session.
    pub fn new(provider: Arc<dyn TableProvider>, session: Arc<dyn Session>) -> Self {
        Self { provider, session }
    }

    /// Build a reader for `table`, registering the table's object store with
    /// `session` (idempotent) and resolving the `DeltaScanNext` provider.
    pub async fn try_new(table: &DeltaTable, session: Arc<dyn Session>) -> DeltaResult<Self> {
        table.update_datafusion_session(session.as_ref())?;
        let provider = table.table_provider().await?;
        Ok(Self::new(provider, session))
    }

    /// Resolve logical projection column names against the provider schema.
    fn projection_indices(&self, options: &ScanOptions) -> DeltaResult<Option<Vec<usize>>> {
        let schema = self.provider.schema();
        options
            .projection
            .as_ref()
            .map(|cols| {
                cols.iter()
                    .map(|col| {
                        schema
                            .column_with_name(col)
                            .map(|(idx, _)| idx)
                            .ok_or_else(|| DeltaTableError::SchemaMismatch {
                                msg: format!("Column '{col}' does not exist in table schema."),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
    }
}

#[async_trait::async_trait]
impl DeltaDataReaderExt for DataFusionDataReader {
    async fn scan(
        &self,
        session: &dyn Session,
        options: ScanOptions,
    ) -> DeltaResult<SendableRecordBatchStream> {
        let projection = self.projection_indices(&options)?;
        let scan_plan = self
            .provider
            .scan(session, projection.as_ref(), &[], options.limit)
            .await?;
        let plan = CoalescePartitionsExec::new(scan_plan);
        Ok(plan.execute(0, session.task_ctx())?)
    }
}

#[async_trait::async_trait]
impl DeltaDataReader for DataFusionDataReader {
    async fn read(&self, options: ReadOptions) -> DeltaResult<RecordBatchFutureStream> {
        let stream = self.scan(self.session.as_ref(), options.into()).await?;
        Ok(results_to_future_stream(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sendable_streams_to_future_stream_empty_is_empty() {
        // Empty input must not panic in `select_all`; it yields an empty stream.
        let mut stream = sendable_streams_to_future_stream(vec![]);
        assert!(stream.next().await.is_none());
    }
}
