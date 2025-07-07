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
//! The submodules provide structures and methods that aid in generating
//! and consuming snapshots.
//!
//! ## Reading the log
//!
//!

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Cursor};

use ::serde::{Deserialize, Serialize};
use arrow_array::RecordBatch;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use self::log_segment::LogSegment;
use self::parse::{read_adds, read_removes};
use self::replay::{LogMapper, LogReplayScanner, ReplayStream};
use self::visitors::*;
use super::arrow::engine_ext::stats_schema_from_config;
use super::{Action, Add, AddCDCFile, CommitInfo, Metadata, Protocol, Remove, Transaction};
use crate::kernel::parse::read_cdf_adds;
use crate::kernel::transaction::{CommitData, PROTOCOL};
use crate::kernel::{ActionType, StructType};
use crate::logstore::LogStore;
use crate::table::config::TableConfig;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

pub use self::log_data::*;

mod log_data;
pub(crate) mod log_segment;
pub(crate) mod parse;
mod replay;
mod serde;
pub mod visitors;

/// A snapshot of a Delta table
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Snapshot {
    /// Log segment containing all log files in the snapshot
    log_segment: LogSegment,
    /// Configuration for the current session
    config: DeltaTableConfig,
    /// Protocol of the Delta table
    protocol: Protocol,
    /// Metadata of the Delta table
    metadata: Metadata,
    /// Logical table schema
    schema: StructType,
    /// Fully qualified URL of the table
    table_url: Url,
}

impl Snapshot {
    /// Create a new [`Snapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let log_segment = LogSegment::try_new(log_store, version).await?;
        let (protocol, metadata) = log_segment.read_metadata(log_store, &config).await?;
        if metadata.is_none() || protocol.is_none() {
            return Err(DeltaTableError::Generic(
                "Cannot read metadata from log segment".into(),
            ));
        };
        let (metadata, protocol) = (metadata.unwrap(), protocol.unwrap());
        let schema = metadata.parse_schema()?;

        PROTOCOL.can_read_from_protocol(&protocol)?;

        Ok(Self {
            log_segment,
            config,
            protocol,
            metadata,
            schema,
            table_url: log_store.config().location.clone(),
        })
    }

    #[cfg(test)]
    pub fn new_test<'a>(
        commits: impl IntoIterator<Item = &'a CommitData>,
        table_root: &Path,
    ) -> DeltaResult<(Self, RecordBatch)> {
        use arrow_select::concat::concat_batches;
        let (log_segment, batches) = LogSegment::new_test(commits, table_root)?;
        let batch = batches.into_iter().collect::<Result<Vec<_>, _>>()?;
        let batch = concat_batches(&batch[0].schema(), &batch)?;
        let protocol = parse::read_protocol(&batch)?.unwrap();
        let metadata = parse::read_metadata(&batch)?.unwrap();
        let schema = metadata.parse_schema()?;
        Ok((
            Self {
                log_segment,
                config: Default::default(),
                protocol,
                metadata,
                schema,
                table_url: Url::parse("dummy:///").unwrap(),
            },
            batch,
        ))
    }

    /// Update the snapshot to the given version
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<i64>,
    ) -> DeltaResult<()> {
        self.update_inner(log_store, target_version).await?;
        Ok(())
    }

    async fn update_inner(
        &mut self,
        log_store: &dyn LogStore,
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
        let log_segment =
            LogSegment::try_new_slice(self.version() + 1, target_version, log_store).await?;
        if log_segment.commit_files.is_empty() && log_segment.checkpoint_files.is_empty() {
            return Ok(None);
        }

        let (protocol, metadata) = log_segment.read_metadata(log_store, &self.config).await?;
        if let Some(protocol) = protocol {
            self.protocol = protocol;
        }
        if let Some(metadata) = metadata {
            self.metadata = metadata;
            self.schema = self.metadata.parse_schema()?;
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

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        &self.config
    }

    /// Get the table root of the snapshot
    pub(crate) fn table_root_path(&self) -> DeltaResult<Path> {
        Ok(Path::from_url_path(self.table_url.path())?)
    }

    /// Well known table configuration
    pub fn table_config(&self) -> TableConfig<'_> {
        TableConfig(self.metadata.configuration())
    }

    /// Get the files in the snapshot
    pub fn files<'a>(
        &self,
        log_store: &dyn LogStore,
        visitors: &'a mut Vec<Box<dyn ReplayVisitor>>,
    ) -> DeltaResult<ReplayStream<'a, BoxStream<'_, DeltaResult<RecordBatch>>>> {
        let mut schema_actions: HashSet<_> =
            visitors.iter().flat_map(|v| v.required_actions()).collect();

        schema_actions.insert(ActionType::Add);
        let checkpoint_stream = self.log_segment.checkpoint_stream(
            log_store,
            &StructType::new(schema_actions.iter().map(|a| a.schema_field().clone())),
            &self.config,
        );

        schema_actions.insert(ActionType::Remove);
        let log_stream = self.log_segment.commit_stream(
            log_store,
            &StructType::new(schema_actions.iter().map(|a| a.schema_field().clone())),
            &self.config,
        )?;

        ReplayStream::try_new(log_stream, checkpoint_stream, self, visitors)
    }

    /// Get the commit infos in the snapshot
    pub(crate) async fn commit_infos(
        &self,
        log_store: &dyn LogStore,
        limit: Option<usize>,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<Option<CommitInfo>>>> {
        let store = log_store.root_object_store(None);

        let log_root = self.table_root_path()?.child("_delta_log");
        let start_from = log_root.child(
            format!(
                "{:020}",
                limit
                    .map(|l| (self.version() - l as i64 + 1).max(0))
                    .unwrap_or(0)
            )
            .as_str(),
        );

        let dummy_url = url::Url::parse("memory:///").unwrap();
        let mut commit_files = Vec::new();
        for meta in store
            .list_with_offset(Some(&log_root), &start_from)
            .try_collect::<Vec<_>>()
            .await?
        {
            // safety: object store path are always valid urls paths.
            let dummy_path = dummy_url.join(meta.location.as_ref()).unwrap();
            if let Some(parsed_path) = ParsedLogPath::try_from(dummy_path)? {
                if matches!(parsed_path.file_type, LogPathFileType::Commit) {
                    commit_files.push(meta);
                }
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
        log_store: &dyn LogStore,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<Vec<Remove>>>> {
        let log_stream = self.log_segment.commit_stream(
            log_store,
            &log_segment::TOMBSTONE_SCHEMA,
            &self.config,
        )?;
        let checkpoint_stream = self.log_segment.checkpoint_stream(
            log_store,
            &log_segment::TOMBSTONE_SCHEMA,
            &self.config,
        );

        Ok(log_stream
            .chain(checkpoint_stream)
            .map(|batch| match batch {
                Ok(batch) => read_removes(&batch),
                Err(e) => Err(e),
            })
            .boxed())
    }

    /// Get the statistics schema of the snapshot
    pub fn stats_schema(&self, table_schema: Option<&StructType>) -> DeltaResult<StructType> {
        let schema = table_schema.unwrap_or_else(|| self.schema());
        Ok(stats_schema_from_config(schema, self.table_config())?
            .as_ref()
            .clone())
    }

    /// Get the partition values schema of the snapshot
    pub fn partitions_schema(
        &self,
        table_schema: Option<&StructType>,
    ) -> DeltaResult<Option<StructType>> {
        if self.metadata().partition_columns().is_empty() {
            return Ok(None);
        }
        let schema = table_schema.unwrap_or_else(|| self.schema());
        partitions_schema(schema, &self.metadata().partition_columns())
    }
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
#[derive(Debug, Clone, PartialEq)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    // additional actions that should be tracked during log replay.
    tracked_actions: HashSet<ActionType>,

    pub(crate) transactions: Option<HashMap<String, Transaction>>,

    // NOTE: this is a Vec of RecordBatch instead of a single RecordBatch because
    //       we do not yet enforce a consistent schema across all batches we read from the log.
    pub(crate) files: Vec<RecordBatch>,
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        Self::try_new_with_visitor(log_store, config, version, Default::default()).await
    }

    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new_with_visitor(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
        tracked_actions: HashSet<ActionType>,
    ) -> DeltaResult<Self> {
        let mut visitors = tracked_actions
            .iter()
            .flat_map(get_visitor)
            .collect::<Vec<_>>();
        let snapshot = Snapshot::try_new(log_store, config.clone(), version).await?;

        let files = match config.require_files {
            true => {
                snapshot
                    .files(log_store, &mut visitors)?
                    .try_collect()
                    .await?
            }
            false => vec![],
        };

        let mut sn = Self {
            snapshot,
            files,
            tracked_actions,
            transactions: None,
        };

        sn.process_visitors(visitors)?;

        Ok(sn)
    }

    fn process_visitors(&mut self, visitors: Vec<Box<dyn ReplayVisitor>>) -> DeltaResult<()> {
        for visitor in visitors {
            if let Some(tv) = visitor
                .as_ref()
                .as_any()
                .downcast_ref::<AppTransactionVisitor>()
            {
                if self.transactions.is_none() {
                    self.transactions = Some(tv.app_transaction_version.clone());
                } else {
                    self.transactions = Some(tv.merge(self.transactions.as_ref().unwrap()));
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn new_test<'a>(
        commits: impl IntoIterator<Item = &'a CommitData>,
        table_root: &Path,
    ) -> DeltaResult<Self> {
        let (snapshot, batch) = Snapshot::new_test(commits, table_root)?;
        let mut files = Vec::new();
        let mut scanner = LogReplayScanner::new();
        files.push(scanner.process_files_batch(&batch, true)?);
        let mapper = LogMapper::try_new(&snapshot, None)?;
        files = files
            .into_iter()
            .map(|b| mapper.map_batch(b))
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Self {
            snapshot,
            files,
            tracked_actions: Default::default(),
            transactions: None,
        })
    }

    /// Update the snapshot to the given version
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<i64>,
    ) -> DeltaResult<()> {
        if Some(self.version()) == target_version {
            return Ok(());
        }

        let new_slice = self
            .snapshot
            .update_inner(log_store, target_version)
            .await?;

        if new_slice.is_none() {
            return Ok(());
        }
        let new_slice = new_slice.unwrap();

        let mut visitors = self
            .tracked_actions
            .iter()
            .flat_map(get_visitor)
            .collect::<Vec<_>>();

        let mut schema_actions: HashSet<_> =
            visitors.iter().flat_map(|v| v.required_actions()).collect();
        let files = std::mem::take(&mut self.files);

        schema_actions.insert(ActionType::Add);
        let checkpoint_stream = if new_slice.checkpoint_files.is_empty() {
            // NOTE: we don't need to add the visitor relevant data here, as it is repüresented in the state already
            futures::stream::iter(files.into_iter().map(Ok)).boxed()
        } else {
            let read_schema =
                StructType::new(schema_actions.iter().map(|a| a.schema_field().clone()));
            new_slice
                .checkpoint_stream(log_store, &read_schema, &self.snapshot.config)
                .boxed()
        };

        schema_actions.insert(ActionType::Remove);
        let read_schema = StructType::new(schema_actions.iter().map(|a| a.schema_field().clone()));
        let log_stream = new_slice.commit_stream(log_store, &read_schema, &self.snapshot.config)?;

        let mapper = LogMapper::try_new(&self.snapshot, None)?;

        let files =
            ReplayStream::try_new(log_stream, checkpoint_stream, &self.snapshot, &mut visitors)?
                .map(|batch| batch.and_then(|b| mapper.map_batch(b)))
                .try_collect()
                .await?;

        self.files = files;
        self.process_visitors(visitors)?;

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
    pub(crate) fn table_root_path(&self) -> DeltaResult<Path> {
        self.snapshot.table_root_path()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        self.snapshot.load_config()
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

    /// Get an iterator for the CDC files added in this version
    pub fn cdc_files(&self) -> DeltaResult<impl Iterator<Item = AddCDCFile> + '_> {
        Ok(self.files.iter().flat_map(|b| read_cdf_adds(b)).flatten())
    }

    /// Iterate over all latest app transactions
    pub async fn transaction_version(&self, app_id: impl AsRef<str>) -> DeltaResult<Option<i64>> {
        Ok(self
            .transactions
            .as_ref()
            .ok_or(DeltaTableError::Generic(
                "Transactions are not available. Please enable tracking of transactions."
                    .to_string(),
            ))?
            .get(app_id.as_ref())
            .map(|txn| txn.version))
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
                metadata = commit.actions.iter().find_map(|a| match a {
                    Action::Metadata(metadata) => Some(metadata.clone()),
                    _ => None,
                });
            }
            if protocol.is_none() {
                protocol = commit.actions.iter().find_map(|a| match a {
                    Action::Protocol(protocol) => Some(protocol.clone()),
                    _ => None,
                });
            }
            send.push(commit);
        }

        let mut visitors = self
            .tracked_actions
            .iter()
            .flat_map(get_visitor)
            .collect::<Vec<_>>();
        let mut schema_actions: HashSet<_> =
            visitors.iter().flat_map(|v| v.required_actions()).collect();
        schema_actions.extend([ActionType::Add, ActionType::Remove]);
        let read_schema = StructType::new(schema_actions.iter().map(|a| a.schema_field().clone()));
        let actions = self.snapshot.log_segment.advance(
            send,
            &self.table_root_path()?,
            &read_schema,
            &self.snapshot.config,
        )?;

        let mut files = Vec::new();
        let mut scanner = LogReplayScanner::new();

        for batch in actions {
            let batch = batch?;
            files.push(scanner.process_files_batch(&batch, true)?);
            for visitor in &mut visitors {
                visitor.visit_batch(&batch)?;
            }
        }

        let mapper = if let Some(metadata) = &metadata {
            let new_schema: StructType = metadata.parse_schema()?;
            LogMapper::try_new(&self.snapshot, Some(&new_schema))?
        } else {
            LogMapper::try_new(&self.snapshot, None)?
        };

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
            self.snapshot.schema = self.snapshot.metadata.parse_schema()?;
        }
        if let Some(protocol) = protocol {
            self.snapshot.protocol = protocol;
        }
        self.process_visitors(visitors)?;

        Ok(self.snapshot.version())
    }
}

pub(crate) fn partitions_schema(
    schema: &StructType,
    partition_columns: &[String],
) -> DeltaResult<Option<StructType>> {
    if partition_columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(StructType::new(
        partition_columns
            .iter()
            .map(|col| {
                schema.field(col).cloned().ok_or_else(|| {
                    DeltaTableError::Generic(format!("Partition column {col} not found in schema"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    )))
}

#[cfg(feature = "datafusion")]
mod datafusion {
    use ::datafusion::common::stats::Statistics;

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
    use std::collections::HashMap;

    use delta_kernel::schema::{DataType, StructField};
    use futures::TryStreamExt;
    use itertools::Itertools;

    use super::log_segment::tests::{concurrent_checkpoint, test_log_segment};
    use super::replay::tests::test_log_replay;
    use super::*;
    use crate::protocol::{DeltaOperation, SaveMode};
    use crate::test_utils::{assert_batches_sorted_eq, ActionFactory, TestResult, TestTables};

    #[tokio::test]
    async fn test_snapshots() -> TestResult {
        test_log_segment().await?;
        test_log_replay().await?;
        test_snapshot().await?;
        test_eager_snapshot().await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_checkpoint() -> TestResult {
        concurrent_checkpoint().await?;
        Ok(())
    }

    async fn test_snapshot() -> TestResult {
        let log_store = TestTables::Simple.table_builder().build_storage()?;

        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;

        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let actual: Snapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual, snapshot);

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema(), &expected);

        let infos = snapshot
            .commit_infos(&log_store, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let infos = infos.into_iter().flatten().collect_vec();
        assert_eq!(infos.len(), 5);

        let tombstones = snapshot
            .tombstones(&log_store)?
            .try_collect::<Vec<_>>()
            .await?;
        let tombstones = tombstones.into_iter().flatten().collect_vec();
        assert_eq!(tombstones.len(), 31);

        let batches = snapshot
            .files(&log_store, &mut vec![])?
            .try_collect::<Vec<_>>()
            .await?;
        let expected = [
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| add                                                                                                                                                                                                                                                                                                                                  |",
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {path: part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet, partitionValues: {}, size: 262, modificationTime: 1587968626000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , nullCount: , minValues: , maxValues: }} |",
            "| {path: part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet, partitionValues: {}, size: 262, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , nullCount: , minValues: , maxValues: }} |",
            "| {path: part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , nullCount: , minValues: , maxValues: }} |",
            "| {path: part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , nullCount: , minValues: , maxValues: }} |",
            "| {path: part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: , stats_parsed: {numRecords: , nullCount: , minValues: , maxValues: }} |",
            "+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        let log_store = TestTables::Checkpoints.table_builder().build_storage()?;

        for version in 0..=12 {
            let snapshot = Snapshot::try_new(&log_store, Default::default(), Some(version)).await?;
            let batches = snapshot
                .files(&log_store, &mut vec![])?
                .try_collect::<Vec<_>>()
                .await?;
            let num_files = batches.iter().map(|b| b.num_rows() as i64).sum::<i64>();
            assert_eq!(num_files, version);
        }

        Ok(())
    }

    async fn test_eager_snapshot() -> TestResult {
        let log_store = TestTables::Simple.table_builder().build_storage()?;

        let snapshot = EagerSnapshot::try_new(&log_store, Default::default(), None).await?;

        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let actual: EagerSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual, snapshot);

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema(), &expected);

        let log_store = TestTables::Checkpoints.table_builder().build_storage()?;

        for version in 0..=12 {
            let snapshot =
                EagerSnapshot::try_new(&log_store, Default::default(), Some(version)).await?;
            let batches = snapshot.file_actions()?.collect::<Vec<_>>();
            assert_eq!(batches.len(), version as usize);
        }

        Ok(())
    }

    #[test]
    fn test_partition_schema() {
        let schema = StructType::new(vec![
            StructField::new("id", DataType::LONG, true),
            StructField::new("name", DataType::STRING, true),
            StructField::new("date", DataType::DATE, true),
        ]);

        let partition_columns = vec!["date".to_string()];
        let metadata = ActionFactory::metadata(&schema, Some(&partition_columns), None);
        let protocol = ActionFactory::protocol(None, None, None::<Vec<_>>, None::<Vec<_>>);

        let commit_data = CommitData::new(
            vec![
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
            ],
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: Some(partition_columns),
                predicate: None,
            },
            HashMap::new(),
            vec![],
        );

        let (log_segment, _) = LogSegment::new_test(vec![&commit_data], &Path::default()).unwrap();

        let snapshot = Snapshot {
            log_segment: log_segment.clone(),
            protocol: protocol.clone(),
            metadata,
            schema: schema.clone(),
            table_url: Url::parse("dummy:///").unwrap(),
            config: Default::default(),
        };

        let expected = StructType::new(vec![StructField::new("date", DataType::DATE, true)]);
        assert_eq!(snapshot.partitions_schema(None).unwrap(), Some(expected));

        let metadata = ActionFactory::metadata(&schema, None::<Vec<&str>>, None);
        let commit_data = CommitData::new(
            vec![
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
            ],
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
            HashMap::new(),
            vec![],
        );
        let (log_segment, _) = LogSegment::new_test(vec![&commit_data], &Path::default()).unwrap();

        let snapshot = Snapshot {
            log_segment,
            config: Default::default(),
            protocol: protocol.clone(),
            metadata,
            schema: schema.clone(),
            table_url: Url::parse("dummy:///").unwrap(),
        };

        assert_eq!(snapshot.partitions_schema(None).unwrap(), None);
    }
}
