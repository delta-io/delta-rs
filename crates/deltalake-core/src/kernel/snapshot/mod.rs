//! Delta table snapshots
//!
//! A snapshot represents the state of a Delta Table at a given version.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::Value;

use self::log_segment::LogSegment;
use self::parse::extract_adds;
use self::replay::{LogReplayScanner, ReplayStream};
use super::{Action, Add, CommitInfo, Metadata, Protocol};
use crate::kernel::StructType;
use crate::protocol::DeltaOperation;
use crate::table::config::TableConfig;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

mod extract;
mod log_segment;
mod parse;
mod replay;

/// A snapshot of a Delta table
pub struct Snapshot {
    log_segment: LogSegment,
    store: Arc<dyn ObjectStore>,
    config: DeltaTableConfig,
    protocol: Protocol,
    metadata: Metadata,
    schema: StructType,
    // TODO make this an URL
    /// path of the table root within the object store
    table_url: Path,
}

impl Snapshot {
    /// Create a new [`Snapshot`] instance
    pub async fn try_new(
        table_root: &Path,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let log_segment = LogSegment::try_new(table_root, version, store.as_ref()).await?;
        let (protocol, metadata) = log_segment.read_metadata(store.clone(), &config).await?;
        let schema = serde_json::from_str(&metadata.schema_string)?;
        Ok(Self {
            log_segment,
            store,
            config,
            protocol,
            metadata,
            schema,
            table_url: table_root.to_owned(),
        })
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.log_segment.version()
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> &StructType {
        &self.schema
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    /// Get the table root of the snapshot
    pub fn table_root(&self) -> &Path {
        &self.table_url
    }

    /// Well known table configuration
    pub fn table_config(&self) -> TableConfig<'_> {
        TableConfig(&self.metadata.configuration)
    }

    /// Get the files in the snapshot
    pub fn files(&self) -> DeltaResult<ReplayStream<BoxStream<'_, DeltaResult<RecordBatch>>>> {
        let log_stream = self.log_segment.commit_stream(
            self.store.clone(),
            &log_segment::COMMIT_SCHEMA,
            &self.config,
        )?;
        let checkpoint_stream = self.log_segment.checkpoint_stream(
            self.store.clone(),
            &log_segment::CHECKPOINT_SCHEMA,
            &self.config,
        );
        Ok(ReplayStream::new(log_stream, checkpoint_stream))
    }

    /// Get the commit infos in the snapshot
    pub(crate) fn commit_infos(&self) -> BoxStream<'_, DeltaResult<Option<CommitInfo>>> {
        futures::stream::iter(self.log_segment.commit_files.clone())
            .map(move |meta| {
                let store = self.store.clone();
                async move {
                    let commit_log_bytes = store.get(&meta.location).await?.bytes().await?;
                    let reader = BufReader::new(Cursor::new(commit_log_bytes));
                    for line in reader.lines() {
                        let action: Action = serde_json::from_str(line?.as_str())?;
                        match action {
                            Action::CommitInfo(commit_info) => {
                                return Ok::<_, DeltaTableError>(Some(commit_info))
                            }
                            _ => (),
                        };
                    }
                    Ok(None)
                }
            })
            .buffered(self.config.log_buffer_size)
            .boxed()
    }
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
pub struct EagerSnapshot {
    snapshot: Snapshot,
    files: Vec<RecordBatch>,
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        table_root: &Path,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(table_root, store, config, version).await?;
        let files = snapshot.files()?.try_collect().await?;
        Ok(Self { snapshot, files })
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> &StructType {
        self.snapshot.schema()
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    /// Get the table root of the snapshot
    pub fn table_root(&self) -> &Path {
        self.snapshot.table_root()
    }

    /// Well known table configuration
    pub fn table_config(&self) -> TableConfig<'_> {
        self.snapshot.table_config()
    }

    /// Get the files in the snapshot
    pub fn files(&self) -> DeltaResult<impl Iterator<Item = Add> + '_> {
        Ok(self.files.iter().flat_map(|b| extract_adds(b)).flatten())
    }

    /// Advance the snapshot based on the given commit actions
    pub fn advance<'a>(
        &mut self,
        commits: impl IntoIterator<
            Item = &'a (Vec<Action>, DeltaOperation, Option<HashMap<String, Value>>),
        >,
    ) -> DeltaResult<i64> {
        let actions = self.snapshot.log_segment.advance(
            commits,
            &self.snapshot.table_url,
            &log_segment::COMMIT_SCHEMA,
            &self.snapshot.config,
        )?;

        let mut files = Vec::new();
        let mut scanner = LogReplayScanner::new();

        for batch in actions {
            files.push(scanner.process_files_batch(&batch?, true)?);
        }
        self.files = files
            .into_iter()
            .chain(
                self.files
                    .iter()
                    .flat_map(|batch| scanner.process_files_batch(batch, false)),
            )
            .collect();

        Ok(self.snapshot.version())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use deltalake_test::utils::*;
    use futures::TryStreamExt;
    use itertools::Itertools;

    use super::log_segment::tests::test_log_segment;
    use super::replay::tests::test_log_replay;
    use super::*;
    use crate::kernel::Remove;
    use crate::protocol::SaveMode;

    #[tokio::test]
    async fn test_snapshots() -> TestResult {
        let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;
        context.load_table(TestTables::Checkpoints).await?;
        context.load_table(TestTables::Simple).await?;
        context.load_table(TestTables::WithDvSmall).await?;

        test_log_segment(&context).await?;
        test_log_replay(&context).await?;
        test_snapshot(&context).await?;
        test_eager_snapshot(&context).await?;

        Ok(())
    }

    async fn test_snapshot(context: &IntegrationContext) -> TestResult {
        let store = context
            .table_builder(TestTables::Simple)
            .build_storage()?
            .object_store();

        let snapshot =
            Snapshot::try_new(&Path::default(), store.clone(), Default::default(), None).await?;

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema(), &expected);

        let infos = snapshot.commit_infos().try_collect::<Vec<_>>().await?;
        let infos = infos.into_iter().flatten().collect_vec();
        assert_eq!(infos.len(), 5);

        let batches = snapshot.files()?.try_collect::<Vec<_>>().await?;
        let expected = [
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| add                                                                                                                                                                                                                                                             |",
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {path: part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet, partitionValues: {}, size: 262, modificationTime: 1587968626000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet, partitionValues: {}, size: 262, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        let store = context
            .table_builder(TestTables::Checkpoints)
            .build_storage()?
            .object_store();

        for version in 0..=12 {
            let snapshot = Snapshot::try_new(
                &Path::default(),
                store.clone(),
                Default::default(),
                Some(version),
            )
            .await?;
            let batches = snapshot.files()?.try_collect::<Vec<_>>().await?;
            let num_files = batches.iter().map(|b| b.num_rows() as i64).sum::<i64>();
            assert_eq!(num_files, version);
        }

        Ok(())
    }

    async fn test_eager_snapshot(context: &IntegrationContext) -> TestResult {
        let store = context
            .table_builder(TestTables::Simple)
            .build_storage()?
            .object_store();

        let snapshot =
            EagerSnapshot::try_new(&Path::default(), store.clone(), Default::default(), None)
                .await?;

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema(), &expected);

        let store = context
            .table_builder(TestTables::Checkpoints)
            .build_storage()?
            .object_store();

        for version in 0..=12 {
            let snapshot = EagerSnapshot::try_new(
                &Path::default(),
                store.clone(),
                Default::default(),
                Some(version),
            )
            .await?;
            let batches = snapshot.files()?.collect::<Vec<_>>();
            assert_eq!(batches.len(), version as usize);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_snapshot_advance() -> TestResult {
        let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;
        context.load_table(TestTables::Simple).await?;

        let store = context
            .table_builder(TestTables::Simple)
            .build_storage()?
            .object_store();

        let mut snapshot =
            EagerSnapshot::try_new(&Path::default(), store.clone(), Default::default(), None)
                .await?;

        let version = snapshot.version();

        let files = snapshot.files()?.enumerate().collect_vec();
        let num_files = files.len();

        let split = files.split(|(idx, _)| *idx == num_files / 2).collect_vec();
        assert!(split.len() == 2 && !split[0].is_empty() && !split[1].is_empty());
        let (first, second) = split.into_iter().next_tuple().unwrap();

        let removes = first
            .iter()
            .map(|(_, add)| {
                Action::Remove(Remove {
                    path: add.path.clone(),
                    size: Some(add.size),
                    data_change: add.data_change,
                    deletion_timestamp: Some(Utc::now().timestamp_millis()),
                    extended_file_metadata: Some(true),
                    partition_values: Some(add.partition_values.clone()),
                    tags: add.tags.clone(),
                    deletion_vector: add.deletion_vector.clone(),
                    base_row_id: add.base_row_id,
                    default_row_commit_version: add.default_row_commit_version,
                })
            })
            .collect_vec();

        let actions = vec![(
            removes,
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
            None,
        )];

        let new_version = snapshot.advance(&actions)?;
        assert_eq!(new_version, version + 1);

        let new_files = snapshot.files()?.map(|f| f.path).collect::<Vec<_>>();
        assert_eq!(new_files.len(), num_files - first.len());
        assert!(first
            .iter()
            .all(|(_, add)| { !new_files.contains(&add.path) }));
        assert!(second
            .iter()
            .all(|(_, add)| { new_files.contains(&add.path) }));

        Ok(())
    }
}
