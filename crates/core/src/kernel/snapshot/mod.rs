//! Delta table snapshots
//!
//! A snapshot represents the state of a Delta Table at a given version.
//!
//! There are two types of snapshots:
//!
//! - [`Snapshot`] is a snapshot where most data is loaded on demand and only the
//!   bare minimum - [`Protocol`] and [`Metadata`] - is cached in memory.
//! - [`EagerSnapshot`] is a snapshot where much more log data is eagerly loaded into memory.
//!
//! The sub modules provide structures and methods that aid in generating
//! and consuming snapshots.
//!
//! ## Reading the log
//!
//!

use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;

use ::serde::{Deserialize, Serialize};
use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;

use self::log_segment::{CommitData, LogSegment, PathExt};
use self::parse::{read_adds, read_removes};
use self::replay::{LogMapper, LogReplayScanner, ReplayStream};
use super::{Action, Add, CommitInfo, DataType, Metadata, Protocol, Remove, StructField};
use crate::kernel::StructType;
use crate::logstore::LogStore;
use crate::table::config::TableConfig;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

mod log_data;
mod log_segment;
pub(crate) mod parse;
mod replay;
mod serde;

pub use log_data::*;

/// A snapshot of a Delta table
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Snapshot {
    log_segment: LogSegment,
    config: DeltaTableConfig,
    protocol: Protocol,
    metadata: Metadata,
    schema: StructType,
    // TODO make this an URL
    /// path of the table root within the object store
    table_url: String,
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
        if metadata.is_none() || protocol.is_none() {
            return Err(DeltaTableError::Generic(
                "Cannot read metadata from log segment".into(),
            ));
        };
        let (metadata, protocol) = (metadata.unwrap(), protocol.unwrap());
        let schema = serde_json::from_str(&metadata.schema_string)?;
        Ok(Self {
            log_segment,
            config,
            protocol,
            metadata,
            schema,
            table_url: table_root.to_string(),
        })
    }

    #[cfg(test)]
    pub fn new_test<'a>(
        commits: impl IntoIterator<Item = &'a CommitData>,
    ) -> DeltaResult<(Self, RecordBatch)> {
        use arrow_select::concat::concat_batches;
        let (log_segment, batches) = LogSegment::new_test(commits)?;
        let batch = batches.into_iter().collect::<Result<Vec<_>, _>>()?;
        let batch = concat_batches(&batch[0].schema(), &batch)?;
        let protocol = parse::read_protocol(&batch)?.unwrap();
        let metadata = parse::read_metadata(&batch)?.unwrap();
        let schema = serde_json::from_str(&metadata.schema_string)?;
        Ok((
            Self {
                log_segment,
                config: Default::default(),
                protocol,
                metadata,
                schema,
                table_url: Path::default().to_string(),
            },
            batch,
        ))
    }

    /// Update the snapshot to the given version
    pub async fn update(
        &mut self,
        log_store: Arc<dyn LogStore>,
        target_version: Option<i64>,
    ) -> DeltaResult<()> {
        self.update_inner(log_store, target_version).await?;
        Ok(())
    }

    async fn update_inner(
        &mut self,
        log_store: Arc<dyn LogStore>,
        target_version: Option<i64>,
    ) -> DeltaResult<Option<LogSegment>> {
        if let Some(version) = target_version {
            if version == self.version() {
                return Ok(None);
            }
            if version < self.version() {
                return Err(DeltaTableError::Generic("Cannot downgrade snapshot".into()));
            }
        }
        let log_segment = LogSegment::try_new_slice(
            &Path::default(),
            self.version() + 1,
            target_version,
            log_store.as_ref(),
        )
        .await?;
        if log_segment.commit_files.is_empty() && log_segment.checkpoint_files.is_empty() {
            return Ok(None);
        }

        let (protocol, metadata) = log_segment
            .read_metadata(log_store.object_store().clone(), &self.config)
            .await?;
        if let Some(protocol) = protocol {
            self.protocol = protocol;
        }
        if let Some(metadata) = metadata {
            self.metadata = metadata;
            self.schema = serde_json::from_str(&self.metadata.schema_string)?;
        }

        if !log_segment.checkpoint_files.is_empty() {
            self.log_segment.checkpoint_files = log_segment.checkpoint_files.clone();
            self.log_segment.commit_files = log_segment.commit_files.clone();
        } else {
            for file in &log_segment.commit_files {
                self.log_segment.commit_files.push_front(file.clone());
            }
        }

        self.log_segment.version = log_segment.version;

        Ok(Some(log_segment))
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
    pub fn table_root(&self) -> Path {
        Path::from(self.table_url.clone())
    }

    /// Well known table configuration
    pub fn table_config(&self) -> TableConfig<'_> {
        TableConfig(&self.metadata.configuration)
    }

    /// Get the files in the snapshot
    pub fn files(
        &self,
        store: Arc<dyn ObjectStore>,
    ) -> DeltaResult<ReplayStream<BoxStream<'_, DeltaResult<RecordBatch>>>> {
        let log_stream = self.log_segment.commit_stream(
            store.clone(),
            &log_segment::COMMIT_SCHEMA,
            &self.config,
        )?;
        let checkpoint_stream = self.log_segment.checkpoint_stream(
            store,
            &log_segment::CHECKPOINT_SCHEMA,
            &self.config,
        );
        ReplayStream::try_new(log_stream, checkpoint_stream, self)
    }

    /// Get the commit infos in the snapshot
    pub(crate) async fn commit_infos(
        &self,
        store: Arc<dyn ObjectStore>,
        limit: Option<usize>,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<Option<CommitInfo>>>> {
        let log_root = self.table_root().child("_delta_log");
        let start_from = log_root.child(
            format!(
                "{:020}",
                limit
                    .map(|l| (self.version() - l as i64 + 1).max(0))
                    .unwrap_or(0)
            )
            .as_str(),
        );

        let mut commit_files = Vec::new();
        for meta in store
            .list_with_offset(Some(&log_root), &start_from)
            .try_collect::<Vec<_>>()
            .await?
        {
            if meta.location.is_commit_file() {
                commit_files.push(meta);
            }
        }
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        Ok(futures::stream::iter(commit_files)
            .map(move |meta| {
                let store = store.clone();
                async move {
                    let commit_log_bytes = store.get(&meta.location).await?.bytes().await?;
                    let reader = BufReader::new(Cursor::new(commit_log_bytes));
                    for line in reader.lines() {
                        let action: Action = serde_json::from_str(line?.as_str())?;
                        if let Action::CommitInfo(commit_info) = action {
                            return Ok::<_, DeltaTableError>(Some(commit_info));
                        }
                    }
                    Ok(None)
                }
            })
            .buffered(self.config.log_buffer_size)
            .boxed())
    }

    pub(crate) fn tombstones(
        &self,
        store: Arc<dyn ObjectStore>,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<Vec<Remove>>>> {
        let log_stream = self.log_segment.commit_stream(
            store.clone(),
            &log_segment::TOMBSTONE_SCHEMA,
            &self.config,
        )?;
        let checkpoint_stream =
            self.log_segment
                .checkpoint_stream(store, &log_segment::TOMBSTONE_SCHEMA, &self.config);

        Ok(log_stream
            .chain(checkpoint_stream)
            .map(|batch| match batch {
                Ok(batch) => read_removes(&batch),
                Err(e) => Err(e),
            })
            .boxed())
    }

    /// Get the statistics schema of the snapshot
    pub fn stats_schema(&self) -> DeltaResult<StructType> {
        let num_indexed_cols = self.table_config().num_indexed_cols();
        let stats_fields: Vec<_> = self.schema()
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, f)| match f.data_type() {
                DataType::Map(_) | DataType::Array(_) | &DataType::BINARY => None,
                _ if num_indexed_cols < 0 || (idx as i32) < num_indexed_cols => {
                    Some(StructField::new(f.name(), f.data_type().clone(), true))
                }
                _ => None,
            })
            .collect();

        Ok(StructType::new(vec![
            StructField::new("numRecords", DataType::LONG, true),
            StructField::new("minValues", StructType::new(stats_fields.clone()), true),
            StructField::new("maxValues", StructType::new(stats_fields.clone()), true),
            StructField::new(
                "nullCount",
                StructType::new(stats_fields.iter().filter_map(to_count_field).collect()),
                true,
            ),
        ]))
    }
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
#[derive(Debug, Clone, PartialEq)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    // NOTE: this is a Vec of RecordBatch instead of a single RecordBatch because
    //       we do not yet enforce a consistent schema across all batches we read from the log.
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
        let snapshot = Snapshot::try_new(table_root, store.clone(), config, version).await?;
        let files = snapshot.files(store)?.try_collect().await?;
        Ok(Self { snapshot, files })
    }

    #[cfg(test)]
    pub fn new_test<'a>(commits: impl IntoIterator<Item = &'a CommitData>) -> DeltaResult<Self> {
        let (snapshot, batch) = Snapshot::new_test(commits)?;
        let mut files = Vec::new();
        let mut scanner = LogReplayScanner::new();
        files.push(scanner.process_files_batch(&batch, true)?);
        let mapper = LogMapper::try_new(&snapshot)?;
        files = files
            .into_iter()
            .map(|b| mapper.map_batch(b))
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Self { snapshot, files })
    }

    /// Update the snapshot to the given version
    pub async fn update(
        &mut self,
        log_store: Arc<dyn LogStore>,
        target_version: Option<i64>,
    ) -> DeltaResult<()> {
        if Some(self.version()) == target_version {
            return Ok(());
        }
        let new_slice = self
            .snapshot
            .update_inner(log_store.clone(), target_version)
            .await?;
        if let Some(new_slice) = new_slice {
            let files = std::mem::take(&mut self.files);
            let log_stream = new_slice.commit_stream(
                log_store.object_store().clone(),
                &log_segment::COMMIT_SCHEMA,
                &self.snapshot.config,
            )?;
            let checkpoint_stream = if new_slice.checkpoint_files.is_empty() {
                futures::stream::iter(files.into_iter().map(Ok)).boxed()
            } else {
                new_slice
                    .checkpoint_stream(
                        log_store.object_store(),
                        &log_segment::CHECKPOINT_SCHEMA,
                        &self.snapshot.config,
                    )
                    .boxed()
            };
            let mapper = LogMapper::try_new(&self.snapshot)?;
            let files = ReplayStream::try_new(log_stream, checkpoint_stream, &self.snapshot)?
                .map(|batch| batch.and_then(|b| mapper.map_batch(b)))
                .try_collect()
                .await?;

            self.files = files;
        }
        Ok(())
    }

    /// Get the underlying snapshot
    pub(crate) fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// Get the timestamp of the given version
    pub fn version_timestamp(&self, version: i64) -> Option<i64> {
        self.snapshot
            .log_segment
            .version_timestamp(version)
            .map(|ts| ts.timestamp_millis())
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
    pub fn table_root(&self) -> Path {
        self.snapshot.table_root()
    }

    /// Well known table configuration
    pub fn table_config(&self) -> TableConfig<'_> {
        self.snapshot.table_config()
    }

    /// Get a [`LogDataHandler`] for the snapshot to inspect the currently loaded state of the log.
    pub fn log_data(&self) -> LogDataHandler<'_> {
        LogDataHandler::new(&self.files, self.metadata(), self.schema())
    }

    /// Get the number of files in the snapshot
    pub fn files_count(&self) -> usize {
        self.files.iter().map(|f| f.num_rows()).sum()
    }

    /// Get the files in the snapshot
    pub fn file_actions(&self) -> DeltaResult<impl Iterator<Item = Add> + '_> {
        Ok(self.files.iter().flat_map(|b| read_adds(b)).flatten())
    }

    /// Get a file action iterator for the given version
    pub fn files(&self) -> impl Iterator<Item = LogicalFile<'_>> {
        self.log_data().into_iter()
    }

    /// Advance the snapshot based on the given commit actions
    pub fn advance<'a>(
        &mut self,
        commits: impl IntoIterator<Item = &'a CommitData>,
    ) -> DeltaResult<i64> {
        let mut metadata = None;
        let mut protocol = None;
        let mut send = Vec::new();
        for commit in commits {
            if metadata.is_none() {
                metadata = commit.0.iter().find_map(|a| match a {
                    Action::Metadata(metadata) => Some(metadata.clone()),
                    _ => None,
                });
            }
            if protocol.is_none() {
                protocol = commit.0.iter().find_map(|a| match a {
                    Action::Protocol(protocol) => Some(protocol.clone()),
                    _ => None,
                });
            }
            send.push(commit);
        }
        let actions = self.snapshot.log_segment.advance(
            send,
            &self.table_root(),
            &log_segment::COMMIT_SCHEMA,
            &self.snapshot.config,
        )?;

        let mut files = Vec::new();
        let mut scanner = LogReplayScanner::new();

        for batch in actions {
            files.push(scanner.process_files_batch(&batch?, true)?);
        }

        let mapper = LogMapper::try_new(&self.snapshot)?;
        self.files = files
            .into_iter()
            .chain(
                self.files
                    .iter()
                    .flat_map(|batch| scanner.process_files_batch(batch, false)),
            )
            .map(|b| mapper.map_batch(b))
            .collect::<DeltaResult<Vec<_>>>()?;

        if let Some(metadata) = metadata {
            self.snapshot.metadata = metadata;
            self.snapshot.schema = serde_json::from_str(&self.snapshot.metadata.schema_string)?;
        }
        if let Some(protocol) = protocol {
            self.snapshot.protocol = protocol;
        }

        Ok(self.snapshot.version())
    }
}

fn to_count_field(field: &StructField) -> Option<StructField> {
    match field.data_type() {
        DataType::Map(_) | DataType::Array(_) | &DataType::BINARY => None,
        DataType::Struct(s) => Some(StructField::new(
            field.name(),
            StructType::new(
                s.fields()
                    .iter()
                    .filter_map(to_count_field)
                    .collect::<Vec<_>>(),
            ),
            true,
        )),
        _ => Some(StructField::new(field.name(), DataType::LONG, true)),
    }
}

#[cfg(feature = "datafusion")]
mod datafusion {
    use datafusion_common::stats::Statistics;

    use super::*;

    impl EagerSnapshot {
        /// Provide table level statistics to Datafusion
        pub fn datafusion_table_statistics(&self) -> Option<Statistics> {
            self.log_data().statistics()
        }
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
    use crate::protocol::{DeltaOperation, SaveMode};

    #[tokio::test]
    async fn test_snapshots() -> TestResult {
        let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;
        context.load_table(TestTables::Checkpoints).await?;
        context.load_table(TestTables::Simple).await?;
        context.load_table(TestTables::SimpleWithCheckpoint).await?;
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

        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let actual: Snapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual, snapshot);

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema(), &expected);

        let infos = snapshot
            .commit_infos(store.clone(), None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let infos = infos.into_iter().flatten().collect_vec();
        assert_eq!(infos.len(), 5);

        let tombstones = snapshot
            .tombstones(store.clone())?
            .try_collect::<Vec<_>>()
            .await?;
        let tombstones = tombstones.into_iter().flatten().collect_vec();
        assert_eq!(tombstones.len(), 31);

        let batches = snapshot
            .files(store.clone())?
            .try_collect::<Vec<_>>()
            .await?;
        let expected = [
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| add                                                                                                                                                                                                                                                                                                                                  |",
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {path: part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet, partitionValues: {}, size: 262, modificationTime: 1587968626000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , minValues: , maxValues: , nullCount: }} |",
            "| {path: part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet, partitionValues: {}, size: 262, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , minValues: , maxValues: , nullCount: }} |",
            "| {path: part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , minValues: , maxValues: , nullCount: }} |",
            "| {path: part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , minValues: , maxValues: , nullCount: }} |",
            "| {path: part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , minValues: , maxValues: , nullCount: }} |",
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
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
            let batches = snapshot
                .files(store.clone())?
                .try_collect::<Vec<_>>()
                .await?;
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

        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let actual: EagerSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual, snapshot);

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
            let batches = snapshot.file_actions()?.collect::<Vec<_>>();
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

        let files = snapshot.file_actions()?.enumerate().collect_vec();
        let num_files = files.len();

        let split = files.split(|(idx, _)| *idx == num_files / 2).collect_vec();
        assert!(split.len() == 2 && !split[0].is_empty() && !split[1].is_empty());
        let (first, second) = split.into_iter().next_tuple().unwrap();

        let removes = first
            .iter()
            .map(|(_, add)| {
                Remove {
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
                }
                .into()
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

        let new_files = snapshot.file_actions()?.map(|f| f.path).collect::<Vec<_>>();
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
