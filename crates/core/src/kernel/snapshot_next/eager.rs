use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use delta_kernel::actions::visitors::SetTransactionMap;
use delta_kernel::actions::{Add, Metadata, Protocol, SetTransaction};
use delta_kernel::engine::arrow_extensions::ScanExt;
use delta_kernel::schema::Schema;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{ExpressionRef, Table, Version};
use itertools::Itertools;
use object_store::ObjectStore;
use url::Url;

use super::iterators::AddIterator;
use super::lazy::LazySnapshot;
use super::{Snapshot, SnapshotError};
use crate::kernel::CommitInfo;
use crate::{DeltaResult, DeltaTableConfig};

/// An eager snapshot of a Delta Table at a specific version.
///
/// This snapshot loads some log data eagerly and keeps it in memory.
#[derive(Clone)]
pub struct EagerSnapshot {
    snapshot: LazySnapshot,
    files: Option<RecordBatch>,
}

impl Snapshot for EagerSnapshot {
    fn table_root(&self) -> &Url {
        self.snapshot.table_root()
    }

    fn version(&self) -> Version {
        self.snapshot.version()
    }

    fn schema(&self) -> Arc<Schema> {
        self.snapshot.schema()
    }

    fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    fn table_properties(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }

    fn logical_files(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>> + '_>> {
        let scan = self.snapshot.inner.as_ref().clone();
        let builder = scan.into_scan_builder().with_predicate(predicate).build()?;
        let iter: Vec<_> = builder
            .scan_metadata_from_existing_arrow(
                self.snapshot.engine_ref().as_ref(),
                self.version(),
                Some(self.file_data()?.clone()),
            )?
            .collect();
        Ok(Box::new(iter.into_iter().map(|sc| Ok(sc?.scan_files))))
    }

    fn tombstones(&self) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        self.snapshot.tombstones()
    }

    fn application_transactions(&self) -> DeltaResult<SetTransactionMap> {
        self.snapshot.application_transactions()
    }

    fn application_transaction(&self, app_id: &str) -> DeltaResult<Option<SetTransaction>> {
        self.snapshot.application_transaction(app_id)
    }

    fn commit_infos(
        &self,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Box<dyn Iterator<Item = (Version, CommitInfo)>>> {
        self.snapshot.commit_infos(start_version, limit)
    }

    fn update(&mut self, target_version: Option<Version>) -> DeltaResult<bool> {
        self.update_impl(target_version)
    }
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        table_root: impl AsRef<str>,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: impl Into<Option<Version>>,
    ) -> DeltaResult<Self> {
        let snapshot =
            LazySnapshot::try_new(Table::try_from_uri(table_root)?, store, version).await?;
        let files = config
            .require_files
            .then(|| -> DeltaResult<_> {
                let all: Vec<RecordBatch> = snapshot.logical_files(None)?.try_collect()?;
                Ok(concat_batches(&all[0].schema(), &all)?)
            })
            .transpose()?;
        Ok(Self { snapshot, files })
    }

    pub fn file_data(&self) -> DeltaResult<&RecordBatch> {
        Ok(self
            .files
            .as_ref()
            .ok_or(SnapshotError::FilesNotInitialized)?)
    }

    pub fn file_actions(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<Add>> + '_> {
        AddIterator::try_new(self.file_data()?)
    }

    /// Get the number of files in the current snapshot
    pub fn files_count(&self) -> DeltaResult<usize> {
        Ok(self
            .files
            .as_ref()
            .map(|f| f.num_rows())
            .ok_or(SnapshotError::FilesNotInitialized)?)
    }

    fn update_impl(&mut self, target_version: Option<Version>) -> DeltaResult<bool> {
        let mut snapshot = self.snapshot.clone();
        let current = snapshot.version();
        if !snapshot.update(target_version.clone())? {
            return Ok(false);
        }

        let scan = snapshot.inner.clone().scan_builder().build()?;
        let engine = snapshot.engine_ref().clone();
        let files: Vec<_> = scan
            .scan_metadata_from_existing_arrow(
                engine.as_ref(),
                current,
                Some(self.file_data()?.clone()),
            )?
            .map_ok(|s| s.scan_files)
            .try_collect()?;

        self.files = Some(concat_batches(&files[0].schema(), &files)?);
        self.snapshot = snapshot;

        Ok(true)
    }
}
