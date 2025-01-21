use std::sync::Arc;

use arrow_array::RecordBatch;
use delta_kernel::actions::set_transaction::SetTransactionMap;
use delta_kernel::actions::{get_log_add_schema, get_log_schema, ADD_NAME, REMOVE_NAME};
use delta_kernel::actions::{Add, Metadata, Protocol, SetTransaction};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::log_segment::LogSegment;
use delta_kernel::schema::Schema;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{ExpressionRef, Table, Version};
use itertools::Itertools;
use object_store::ObjectStore;
use url::Url;

use super::iterators::AddIterator;
use super::lazy::LazySnapshot;
use super::{replay_file_actions, scan_as_log_data, Snapshot, SnapshotError};
use crate::kernel::CommitInfo;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

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

    fn schema(&self) -> &Schema {
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
        _predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        todo!()
    }

    fn files(
        &self,
        predicate: Option<ExpressionRef>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<RecordBatch>>>> {
        Ok(Box::new(std::iter::once(scan_as_log_data(
            &self.snapshot,
            vec![(self.file_data()?.clone(), false)],
            predicate,
        ))))
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
            .then(|| -> DeltaResult<_> { replay_file_actions(&snapshot, None) })
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
        if !snapshot.update(target_version.clone())? {
            return Ok(false);
        }

        let log_root = snapshot
            .table_root()
            .join("_delta_log/")
            .map_err(|e| DeltaTableError::generic(e))?;
        let fs_client = snapshot.engine_ref().get_file_system_client();
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_add_schema().clone();

        let segment = LogSegment::for_table_changes(
            fs_client.as_ref(),
            log_root,
            self.snapshot.version() + 1,
            snapshot.version(),
        )?;
        let mut slice_iter = segment
            .replay(
                self.snapshot.engine_ref().as_ref(),
                commit_read_schema,
                checkpoint_read_schema,
                None,
            )?
            .map_ok(
                |(data, flag)| -> Result<(RecordBatch, bool), delta_kernel::Error> {
                    Ok((ArrowEngineData::try_from_engine_data(data)?.into(), flag))
                },
            )
            .flatten()
            .collect::<Result<Vec<_>, _>>()?;

        slice_iter.push((
            self.files
                .as_ref()
                .ok_or(SnapshotError::FilesNotInitialized)?
                .clone(),
            false,
        ));

        self.files = Some(scan_as_log_data(&self.snapshot, slice_iter, None)?);
        self.snapshot = snapshot;

        Ok(true)
    }
}
