//! The write-window abstraction shared by the legacy [`RecordBatchWriter`] and
//! [`JsonWriter`].
//!
//! [`WriteWindow`] is the single owner of the mutable state between flushes (open
//! sink, sealed rotations, current schema, batch count), so the writers' core
//! invariants hold by construction rather than by error-path discipline:
//!
//! 1. **A flush window commits all-or-nothing.** Any IO error aborts the whole
//!    window ([`WriteWindow::abort`]), so a later flush can't commit a partial
//!    subset of a failed write.
//! 2. **Schema advances only with its data.** The committed `baseline` advances
//!    ([`WriteWindow::committed`]) only after the log commit succeeds, and an abort
//!    reverts to it — so a failed write can't evolve the table schema.
//!
//! [`RecordBatchWriter`]: super::record_batch::RecordBatchWriter
//! [`JsonWriter`]: super::json::JsonWriter

use std::num::NonZeroU64;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use indexmap::IndexMap;
use object_store::ObjectStore;
use parquet::file::properties::WriterProperties;

use crate::datafile::writer::{DeltaWriter as DatasetSink, WriterConfig};
use crate::errors::DeltaResult;
use crate::kernel::Add;

/// Everything needed to open a streaming sink for a given schema — exactly the
/// per-writer configuration, minus the schema (which varies as a window widens).
#[derive(Clone)]
pub(crate) struct SinkFactory {
    pub(crate) storage: Arc<dyn ObjectStore>,
    pub(crate) partition_columns: Vec<String>,
    pub(crate) writer_properties: WriterProperties,
    pub(crate) target_file_size: Option<NonZeroU64>,
    pub(crate) num_indexed_cols: DataSkippingNumIndexedCols,
    pub(crate) stats_columns: Option<Vec<String>>,
}

impl SinkFactory {
    /// Open a fresh streaming sink encoding under `schema`.
    fn build(&self, schema: ArrowSchemaRef) -> DatasetSink {
        let config = WriterConfig::new(
            schema,
            self.partition_columns.clone(),
            Some(self.writer_properties.clone()),
            self.target_file_size,
            None,
            self.num_indexed_cols,
            self.stats_columns.clone(),
        );
        DatasetSink::new(self.storage.clone(), config)
    }
}

/// The abortable flush-window state; see the module docs for the invariants it
/// upholds. The writers drive it and never touch the inner state directly.
pub(crate) struct WriteWindow {
    factory: SinkFactory,
    /// Schema the open sink encodes under; widens on a `MergeSchema` write.
    schema: ArrowSchemaRef,
    /// The last committed schema; the window reverts to it on `abort`.
    baseline: ArrowSchemaRef,
    /// Open streaming sink, created lazily on the first write of a window.
    sink: Option<DatasetSink>,
    /// `Add` actions from sinks already sealed in this window (each `MergeSchema`
    /// widening seals the current sink and rotates to a fresh one).
    sealed: Vec<Add>,
    /// Batches written since the last drain/commit.
    count: usize,
}

impl WriteWindow {
    /// Create a window over `factory`, starting (and baselined) at `schema`.
    pub(crate) fn new(factory: SinkFactory, schema: ArrowSchemaRef) -> Self {
        Self {
            factory,
            baseline: schema.clone(),
            schema,
            sink: None,
            sealed: Vec::new(),
            count: 0,
        }
    }

    /// The current (possibly widened) schema.
    pub(crate) fn schema(&self) -> &ArrowSchemaRef {
        &self.schema
    }

    /// The writer's partition columns.
    pub(crate) fn partition_columns(&self) -> &[String] {
        &self.factory.partition_columns
    }

    /// Batches written since the last drain/commit.
    pub(crate) fn count(&self) -> usize {
        self.count
    }

    /// True iff a `MergeSchema` write has widened the schema since the last commit.
    pub(crate) fn schema_evolved(&self) -> bool {
        self.schema != self.baseline
    }

    /// Approximate encoded (parquet) size of all uncommitted data in the window:
    /// sealed rotations plus the open sink. Monotonic within a flush window, so
    /// usable as a flush threshold.
    pub(crate) fn buffered_size(&self) -> usize {
        let sealed: usize = self
            .sealed
            .iter()
            .map(|add| add.size.max(0) as usize)
            .sum();
        sealed + self.sink.as_ref().map_or(0, DatasetSink::buffered_size)
    }

    /// The writer properties new sinks are opened with (used by tests asserting
    /// on the default `created_by` metadata).
    #[cfg(test)]
    pub(crate) fn writer_properties(&self) -> &WriterProperties {
        &self.factory.writer_properties
    }

    /// Set the target file size used for sinks opened from now on.
    pub(crate) fn set_target_file_size(&mut self, target_file_size: Option<NonZeroU64>) {
        self.factory.target_file_size = target_file_size;
    }

    /// Set the writer properties used for sinks opened from now on.
    pub(crate) fn set_writer_properties(&mut self, writer_properties: WriterProperties) {
        self.factory.writer_properties = writer_properties;
    }

    /// Stream `batch` into the open sink. It must conform to `schema()`, or to the
    /// merged schema when `widen_to` is `Some` — in which case the current sink is
    /// first sealed (its files predate the new column, so they read it back as null)
    /// and the wider schema adopted.
    ///
    /// All-or-nothing: any IO error [`abort`]s the window.
    ///
    /// [`abort`]: WriteWindow::abort
    pub(crate) async fn write(
        &mut self,
        batch: &RecordBatch,
        widen_to: Option<ArrowSchemaRef>,
    ) -> DeltaResult<()> {
        if let Err(e) = self.try_write(batch, widen_to).await {
            self.abort();
            return Err(e);
        }
        Ok(())
    }

    async fn try_write(
        &mut self,
        batch: &RecordBatch,
        widen_to: Option<ArrowSchemaRef>,
    ) -> DeltaResult<()> {
        if let Some(merged) = widen_to {
            self.seal().await?;
            self.schema = merged;
        }
        if self.sink.is_none() {
            self.sink = Some(self.factory.build(self.schema.clone()));
        }
        self.sink
            .as_mut()
            .expect("sink was just created")
            .write(batch)
            .await?;
        self.count += 1;
        Ok(())
    }

    /// Stream a pre-partitioned `batch` into the partition identified by
    /// `partition_values`. As with [`write`](Self::write), a `Some` `widen_to`
    /// seals the current sink and adopts the wider schema. All-or-nothing.
    pub(crate) async fn write_partition(
        &mut self,
        batch: RecordBatch,
        partition_values: &IndexMap<String, Scalar>,
        widen_to: Option<ArrowSchemaRef>,
    ) -> DeltaResult<()> {
        if let Err(e) = self
            .try_write_partition(batch, partition_values, widen_to)
            .await
        {
            self.abort();
            return Err(e);
        }
        Ok(())
    }

    async fn try_write_partition(
        &mut self,
        batch: RecordBatch,
        partition_values: &IndexMap<String, Scalar>,
        widen_to: Option<ArrowSchemaRef>,
    ) -> DeltaResult<()> {
        if let Some(merged) = widen_to {
            self.seal().await?;
            self.schema = merged;
        }
        if self.sink.is_none() {
            self.sink = Some(self.factory.build(self.schema.clone()));
        }
        self.sink
            .as_mut()
            .expect("sink was just created")
            .write_partition(batch, partition_values)
            .await?;
        self.count += 1;
        Ok(())
    }

    /// Seal the open sink, moving its `Add`s into `sealed`.
    async fn seal(&mut self) -> DeltaResult<()> {
        if let Some(sink) = self.sink.take() {
            self.sealed.extend(sink.close().await?);
        }
        Ok(())
    }

    /// `flush()`: seal the open sink and take every `Add`, emptying the window.
    /// On a seal error the window is aborted (nothing is left committable).
    pub(crate) async fn drain(&mut self) -> DeltaResult<Vec<Add>> {
        if let Err(e) = self.seal().await {
            self.abort();
            return Err(e);
        }
        self.count = 0;
        Ok(std::mem::take(&mut self.sealed))
    }

    /// `flush_and_commit()` step 1: seal the open sink but **do not** clear the
    /// window, so a failed log commit can be retried without re-uploading. Returns
    /// the staged `Add`s. On a seal error the window is aborted.
    pub(crate) async fn stage(&mut self) -> DeltaResult<&[Add]> {
        if let Err(e) = self.seal().await {
            self.abort();
            return Err(e);
        }
        Ok(&self.sealed)
    }

    /// `flush_and_commit()` step 2: the log commit landed. Advance the committed
    /// baseline to the (possibly widened) schema and empty the window.
    pub(crate) fn committed(&mut self) {
        self.baseline = self.schema.clone();
        self.sealed.clear();
        self.count = 0;
    }

    /// Discard everything accumulated since the last commit — open sink, sealed
    /// rotations, batch count, and any uncommitted schema widening (schema reverts
    /// to the committed baseline) — so a later flush commits nothing.
    ///
    /// The open sink's in-progress multipart uploads are aborted in the background
    /// (their parts are invisible to vacuum); already-finalized files — size rolls
    /// and sealed rotations — are left unreferenced for a later vacuum.
    pub(crate) fn abort(&mut self) {
        if let Some(sink) = self.sink.take() {
            sink.abort_detached();
        }
        self.sealed.clear();
        self.count = 0;
        self.schema = self.baseline.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use object_store::memory::InMemory;

    fn schema_one() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "a",
            DataType::Int32,
            true,
        )]))
    }

    fn schema_two() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]))
    }

    fn batch_one() -> RecordBatch {
        RecordBatch::try_new(schema_one(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap()
    }

    fn batch_two() -> RecordBatch {
        RecordBatch::try_new(
            schema_two(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap()
    }

    fn window(schema: ArrowSchemaRef) -> WriteWindow {
        let factory = SinkFactory {
            storage: Arc::new(InMemory::new()),
            partition_columns: vec![],
            writer_properties: WriterProperties::builder().build(),
            target_file_size: None,
            num_indexed_cols: DataSkippingNumIndexedCols::AllColumns,
            stats_columns: None,
        };
        WriteWindow::new(factory, schema)
    }

    #[tokio::test]
    async fn write_then_drain_yields_one_file() {
        let mut w = window(schema_one());
        w.write(&batch_one(), None).await.unwrap();
        assert_eq!(w.count(), 1);
        let adds = w.drain().await.unwrap();
        assert_eq!(adds.len(), 1);
        assert_eq!(w.count(), 0);
        assert!(!w.schema_evolved());
    }

    #[tokio::test]
    async fn widening_seals_old_sink_and_drains_both() {
        let mut w = window(schema_one());
        w.write(&batch_one(), None).await.unwrap();
        // A widening write seals the narrow sink and rotates to the wide schema.
        w.write(&batch_two(), Some(schema_two())).await.unwrap();
        assert_eq!(w.schema(), &schema_two());
        assert!(w.schema_evolved());
        let adds = w.drain().await.unwrap();
        assert_eq!(adds.len(), 2, "narrow + wide files");
    }

    #[tokio::test]
    async fn abort_reverts_schema_and_empties_window() {
        let mut w = window(schema_one());
        w.write(&batch_one(), None).await.unwrap();
        w.write(&batch_two(), Some(schema_two())).await.unwrap();
        assert!(w.schema_evolved());

        w.abort();

        assert_eq!(w.schema(), &schema_one(), "reverted to baseline");
        assert_eq!(w.count(), 0);
        assert!(!w.schema_evolved());
        assert!(
            w.drain().await.unwrap().is_empty(),
            "nothing committable after abort"
        );
    }

    #[tokio::test]
    async fn failed_write_aborts_window() {
        let mut w = window(schema_one());
        w.write(&batch_one(), None).await.unwrap();
        // A batch that does not conform to the sink schema must fail and abort.
        let mismatched =
            RecordBatch::try_new(schema_two(), batch_two().columns().to_vec()).unwrap();
        let result = w.write(&mismatched, None).await;
        assert!(result.is_err(), "mismatched batch must error");
        assert_eq!(w.count(), 0);
        assert_eq!(w.schema(), &schema_one());
        assert!(w.drain().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn stage_then_committed_advances_baseline() {
        let mut w = window(schema_one());
        w.write(&batch_one(), None).await.unwrap();
        w.write(&batch_two(), Some(schema_two())).await.unwrap();

        let staged = w.stage().await.unwrap();
        assert_eq!(staged.len(), 2);
        assert!(w.schema_evolved());

        w.committed();
        assert!(
            !w.schema_evolved(),
            "baseline advanced to the widened schema"
        );
        // A subsequent stage (e.g. a retried commit) has nothing left to commit.
        assert!(w.stage().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn stage_then_abort_reverts() {
        let mut w = window(schema_one());
        w.write(&batch_two(), Some(schema_two())).await.unwrap();
        let _ = w.stage().await.unwrap();
        w.abort();
        assert!(!w.schema_evolved());
        assert_eq!(w.schema(), &schema_one());
        assert!(w.drain().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn write_partition_unpartitioned_roundtrips() {
        // With no partition columns the sink writes to the empty-path partition.
        let mut w = window(schema_one());
        w.write_partition(batch_one(), &IndexMap::new(), None)
            .await
            .unwrap();
        assert_eq!(w.count(), 1);
        let adds = w.drain().await.unwrap();
        assert_eq!(adds.len(), 1);
    }
}
