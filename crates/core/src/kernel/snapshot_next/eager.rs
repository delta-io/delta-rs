use std::sync::Arc;

use arrow::compute::{concat_batches, filter_record_batch};
use arrow_array::{BooleanArray, RecordBatch};
use chrono::format::Item;
use delta_kernel::actions::set_transaction::SetTransactionMap;
use delta_kernel::actions::{get_log_add_schema, get_log_schema, ADD_NAME, REMOVE_NAME};
use delta_kernel::actions::{Add, Metadata, Protocol, SetTransaction};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::log_segment::LogSegment;
use delta_kernel::scan::log_replay::scan_action_iter;
use delta_kernel::schema::Schema;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{EngineData, Expression, Table, Version};
use itertools::Itertools;
use object_store::ObjectStore;
use url::Url;

use super::iterators::{AddIterator, AddView, AddViewItem};
use super::lazy::LazySnapshot;
use super::{Snapshot, SnapshotError};
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

/// An eager snapshot of a Delta Table at a specific version.
///
/// This snapshot loads some log data eagerly and keeps it in memory.
#[derive(Clone)]
pub struct EagerSnapshot {
    snapshot: LazySnapshot,
    files: Option<RecordBatch>,
    predicate: Option<Arc<Expression>>,
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

    fn files(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
        Ok(std::iter::once(Ok(self
            .files
            .clone()
            .ok_or(SnapshotError::FilesNotInitialized)?)))
    }

    fn tombstones(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
        self.snapshot.tombstones()
    }

    fn application_transactions(&self) -> DeltaResult<SetTransactionMap> {
        self.snapshot.application_transactions()
    }

    fn application_transaction(
        &self,
        app_id: impl AsRef<str>,
    ) -> DeltaResult<Option<SetTransaction>> {
        self.snapshot.application_transaction(app_id)
    }
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        table_root: impl AsRef<str>,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: impl Into<Option<Version>>,
        predicate: impl Into<Option<Arc<Expression>>>,
    ) -> DeltaResult<Self> {
        let snapshot =
            LazySnapshot::try_new(Table::try_from_uri(table_root)?, store, version).await?;
        let files = config
            .require_files
            .then(|| -> DeltaResult<_> { Ok(replay_file_actions(&snapshot)?) })
            .transpose()?;
        Ok(Self {
            snapshot,
            files,
            predicate: predicate.into(),
        })
    }

    pub fn file_data(&self) -> DeltaResult<&RecordBatch> {
        Ok(self
            .files
            .as_ref()
            .ok_or(SnapshotError::FilesNotInitialized)?)
    }

    pub fn files(&self) -> DeltaResult<impl Iterator<Item = AddViewItem>> {
        AddView::try_new(self.file_data()?.clone())
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
            .ok_or_else(|| SnapshotError::FilesNotInitialized)?)
    }

    pub(crate) fn update(&mut self) -> DeltaResult<()> {
        let log_root = self.snapshot.table_root().join("_delta_log/").unwrap();
        let fs_client = self.snapshot.engine_ref().get_file_system_client();
        let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
        let checkpoint_read_schema = get_log_add_schema().clone();

        let segment =
            LogSegment::for_table_changes(fs_client.as_ref(), log_root, self.version() + 1, None)?;
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

        self.files = Some(scan_as_log_data(&self.snapshot, slice_iter)?);

        Ok(())
    }
}

fn replay_file_actions(snapshot: &LazySnapshot) -> DeltaResult<RecordBatch> {
    let commit_read_schema = get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?;
    let checkpoint_read_schema = get_log_add_schema().clone();

    let curr_data = snapshot
        .inner
        ._log_segment()
        .replay(
            snapshot.engine_ref().as_ref(),
            commit_read_schema.clone(),
            checkpoint_read_schema.clone(),
            None,
        )?
        .map_ok(
            |(data, flag)| -> Result<(RecordBatch, bool), delta_kernel::Error> {
                Ok((ArrowEngineData::try_from_engine_data(data)?.into(), flag))
            },
        )
        .flatten()
        .collect::<Result<Vec<_>, _>>()?;

    scan_as_log_data(snapshot, curr_data)
}

fn scan_as_log_data(
    snapshot: &LazySnapshot,
    curr_data: Vec<(RecordBatch, bool)>,
) -> Result<RecordBatch, DeltaTableError> {
    let scan_iter = curr_data.clone().into_iter().map(|(data, flag)| {
        Ok((
            Box::new(ArrowEngineData::new(data.clone())) as Box<dyn EngineData>,
            flag,
        ))
    });

    let res = scan_action_iter(snapshot.engine_ref().as_ref(), scan_iter, None)
        .map(|res| {
            res.and_then(|(d, selection)| {
                Ok((
                    RecordBatch::from(ArrowEngineData::try_from_engine_data(d)?),
                    selection,
                ))
            })
        })
        .zip(curr_data.into_iter())
        .map(|(scan_res, (data_raw, _))| match scan_res {
            Ok((_, selection)) => {
                let data = filter_record_batch(&data_raw, &BooleanArray::from(selection))?;
                Ok(data.project(&[0])?)
            }
            Err(e) => Err(e),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(concat_batches(res[0].schema_ref(), &res)?)
}

#[cfg(test)]
mod tests {
    use deltalake_test::acceptance::{read_dat_case, TestCaseInfo};
    use deltalake_test::TestResult;

    use super::super::tests::get_dat_dir;
    use super::*;

    #[tokio::test]
    async fn load_eager_snapshot() -> TestResult<()> {
        let mut dat_dir = get_dat_dir();
        dat_dir.push("multi_partitioned");

        let dat_info: TestCaseInfo = read_dat_case(dat_dir)?;
        let table_info = dat_info.table_summary()?;

        let table = Table::try_from_uri(dat_info.table_root()?)?;

        let mut snapshot = EagerSnapshot::try_new(
            table.location(),
            Arc::new(object_store::local::LocalFileSystem::default()),
            Default::default(),
            Some(1),
            None,
        )
        .await?;

        // assert_eq!(snapshot.version(), table_info.version);
        // assert_eq!(
        //     snapshot.protocol().min_reader_version(),
        //     table_info.min_reader_version
        // );

        snapshot.update()?;

        for file in snapshot.file_actions()? {
            println!("file: {:#?}", file.unwrap());
        }

        Ok(())
    }
}
