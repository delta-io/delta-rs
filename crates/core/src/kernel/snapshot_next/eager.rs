use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::Schema;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{PredicateRef, Table, Version};
use futures::TryStreamExt;
use itertools::Itertools;
use object_store::ObjectStore;
use url::Url;
use uuid::Uuid;

use super::arrow_ext::ScanExt;
use super::lazy::LazySnapshot;
use super::stream::SendableRBStream;
use super::{LogStoreHandler, Snapshot};
use crate::kernel::CommitInfo;
use crate::logstore::LogStore;
use crate::{DeltaResult, DeltaTableConfig};

/// An eager snapshot of a Delta Table at a specific version.
///
/// This snapshot loads some log data eagerly and keeps it in memory.
#[derive(Clone)]
pub struct EagerSnapshot {
    snapshot: LazySnapshot,
    files: RecordBatch,
    predicate: Option<PredicateRef>,
}

#[async_trait::async_trait]
impl Snapshot for EagerSnapshot {
    fn version(&self) -> Version {
        self.snapshot.version()
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.snapshot.schema()
    }

    fn table_properties(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }

    fn logical_files(
        &self,
        log_store: &LogStoreHandler,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        if let Some(predicate) = predicate {
            self.snapshot.logical_files(log_store, Some(predicate))
        } else {
            let batch = self.files.clone();
            return Box::pin(futures::stream::once(async move { Ok(batch) }));
        }
    }

    async fn application_transaction_version(
        &self,
        log_store: &LogStoreHandler,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        self.snapshot
            .application_transaction_version(log_store, app_id)
            .await
    }

    async fn commit_infos(
        &self,
        log_store: &LogStoreHandler,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Vec<(Version, CommitInfo)>> {
        self.snapshot
            .commit_infos(log_store, start_version, limit)
            .await
    }

    async fn update(
        &mut self,
        log_store: &LogStoreHandler,
        target_version: Option<Version>,
    ) -> DeltaResult<bool> {
        self.update_impl(log_store, target_version).await
    }
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub(crate) async fn try_new(
        log_store: &LogStoreHandler,
        config: DeltaTableConfig,
        version: impl Into<Option<Version>>,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<Self> {
        let snapshot = LazySnapshot::try_new(log_store, version).await?;
        let all: Vec<_> = snapshot
            .logical_files(log_store, predicate.clone())
            .try_collect()
            .await?;
        let files = if all.is_empty() {
            RecordBatch::new_empty(Arc::new((scan_row_schema().as_ref()).try_into_arrow()?))
        } else {
            concat_batches(&all[0].schema(), &all)?
        };
        Ok(Self {
            snapshot,
            files,
            predicate,
        })
    }

    /// Get the number of files in the current snapshot
    pub fn files_count(&self) -> usize {
        self.files.num_rows()
    }

    async fn update_impl(
        &mut self,
        log_store: &LogStoreHandler,
        target_version: Option<Version>,
    ) -> DeltaResult<bool> {
        let mut snapshot = self.snapshot.clone();
        let current = snapshot.version();
        if !snapshot.update(log_store, target_version.clone()).await? {
            return Ok(false);
        }

        let scan = snapshot.inner.clone().scan_builder().build()?;
        let engine = log_store.engine();
        // TODO: process blocking iterator
        let files: Vec<_> = scan
            .scan_metadata_from_arrow(
                engine.as_ref(),
                current,
                Box::new(std::iter::once(self.files.clone())),
                self.predicate.clone(),
            )?
            .map_ok(|s| s.scan_files)
            .try_collect()?;

        self.files = concat_batches(&files[0].schema(), &files)?;
        self.snapshot = snapshot;

        Ok(true)
    }
}
