use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::scan::{Scan as KernelScan, ScanBuilder as KernelScanBuilder, ScanMetadata};
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::{Engine, EngineData, PredicateRef, SnapshotRef, Version};
use futures::future::ready;
use futures::stream::once;
use futures::Stream;
use url::Url;

use crate::kernel::{scan_row_in_eval, ReceiverStreamBuilder};
use crate::DeltaResult;

pub type SendableScanMetadataStream = Pin<Box<dyn Stream<Item = DeltaResult<ScanMetadata>> + Send>>;

/// Builder to scan a snapshot of a table.
#[derive(Debug)]
pub struct ScanBuilder {
    inner: KernelScanBuilder,
}

impl ScanBuilder {
    /// Create a new [`ScanBuilder`] instance.
    pub fn new(snapshot: impl Into<Arc<KernelSnapshot>>) -> Self {
        Self {
            inner: KernelScanBuilder::new(snapshot.into()),
        }
    }

    /// Provide [`Schema`] for columns to select from the [`Snapshot`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    /// [`Snapshot`]: crate::snapshot::Snapshot
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.inner = self.inner.with_schema(schema);
        self
    }

    /// Optionally provide a [`SchemaRef`] for columns to select from the [`Snapshot`]. See
    /// [`ScanBuilder::with_schema`] for details. If `schema_opt` is `None` this is a no-op.
    ///
    /// [`Snapshot`]: crate::Snapshot
    pub fn with_schema_opt(mut self, schema_opt: Option<SchemaRef>) -> Self {
        self.inner = self.inner.with_schema_opt(schema_opt);
        self
    }

    /// Optionally provide an expression to filter rows. For example, using the predicate `x <
    /// 4` to return a subset of the rows in the scan which satisfy the filter. If `predicate_opt`
    /// is `None`, this is a no-op.
    ///
    /// NOTE: The filtering is best-effort and can produce false positives (rows that should should
    /// have been filtered out but were kept).
    pub fn with_predicate(mut self, predicate: impl Into<Option<PredicateRef>>) -> Self {
        self.inner = self.inner.with_predicate(predicate);
        self
    }

    pub fn build(self) -> DeltaResult<Scan> {
        Ok(Scan::from(self.inner.build()?))
    }
}

#[derive(Debug)]
pub struct Scan {
    inner: Arc<KernelScan>,
}

impl From<KernelScan> for Scan {
    fn from(inner: KernelScan) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl From<Arc<KernelScan>> for Scan {
    fn from(inner: Arc<KernelScan>) -> Self {
        Self { inner }
    }
}

impl Scan {
    /// The table's root URL. Any relative paths returned from `scan_data` (or in a callback from
    /// [`ScanMetadata::visit_scan_files`]) must be resolved against this root to get the actual path to
    /// the file.
    ///
    /// [`ScanMetadata::visit_scan_files`]: crate::scan::ScanMetadata::visit_scan_files
    // NOTE: this is obviously included in the snapshot, just re-exposed here for convenience.
    pub fn table_root(&self) -> &Url {
        self.inner.table_root()
    }

    /// Get a shared reference to the [`Snapshot`] of this scan.
    ///
    /// [`Snapshot`]: crate::Snapshot
    pub fn snapshot(&self) -> &SnapshotRef {
        self.inner.snapshot()
    }

    /// Get a shared reference to the logical [`Schema`] of the scan (i.e. the output schema of the
    /// scan). Note that the logical schema can differ from the physical schema due to e.g.
    /// partition columns which are present in the logical schema but not in the physical schema.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn logical_schema(&self) -> &SchemaRef {
        self.inner.logical_schema()
    }

    /// Get a shared reference to the physical [`Schema`] of the scan. This represents the schema
    /// of the underlying data files which must be read from storage.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn physical_schema(&self) -> &SchemaRef {
        self.inner.physical_schema()
    }

    /// Get the predicate [`PredicateRef`] of the scan.
    pub fn physical_predicate(&self) -> Option<PredicateRef> {
        self.inner.physical_predicate()
    }

    pub fn scan_metadata(&self, engine: Arc<dyn Engine>) -> SendableScanMetadataStream {
        // TODO: which capacity to choose?
        let mut builder = ReceiverStreamBuilder::<ScanMetadata>::new(100);
        let tx = builder.tx();

        let inner = self.inner.clone();
        let blocking_iter = move || {
            for res in inner.scan_metadata(engine.as_ref())? {
                if tx.blocking_send(Ok(res?)).is_err() {
                    break;
                }
            }
            Ok(())
        };

        builder.spawn_blocking(blocking_iter);
        builder.build()
    }

    pub fn scan_metadata_from<T: Iterator<Item = RecordBatch> + Send + 'static>(
        &self,
        engine: Arc<dyn Engine>,
        existing_version: Version,
        existing_data: Box<T>,
        existing_predicate: Option<PredicateRef>,
    ) -> SendableScanMetadataStream {
        let inner = self.inner.clone();
        let snapshot = self.inner.snapshot().clone();

        // process our stored / cached data to conform to the expected input for log replay
        let evaluator = match scan_row_in_eval(&snapshot) {
            Ok(scan_row_in_eval) => scan_row_in_eval,
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };
        let scan_row_iter = existing_data
            .map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
            .map(move |b| {
                evaluator
                    .evaluate(b.as_ref())
                    .expect("malformed cached log data")
            });

        // TODO: which capacity to choose?
        let mut builder = ReceiverStreamBuilder::<ScanMetadata>::new(100);
        let tx = builder.tx();
        let scan_inner = move || {
            for res in inner.scan_metadata_from(
                engine.as_ref(),
                existing_version,
                Box::new(scan_row_iter),
                existing_predicate,
            )? {
                if tx.blocking_send(Ok(res?)).is_err() {
                    break;
                }
            }
            Ok(())
        };

        builder.spawn_blocking(scan_inner);
        builder.build()
    }
}
