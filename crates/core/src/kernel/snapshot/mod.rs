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

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;

use ::serde::{Deserialize, Serialize};
use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;

use self::log_segment::{LogSegment, PathExt};
use self::parse::{read_adds, read_removes};
use self::replay::{LogMapper, LogReplayScanner, ReplayStream};
use self::visitors::*;
use super::{
    Action, Add, AddCDCFile, CommitInfo, DataType, Metadata, Protocol, Remove, StructField,
    Transaction,
};
use crate::kernel::parse::read_cdf_adds;
use crate::kernel::{ActionType, StructType};
use crate::logstore::LogStore;
use crate::operations::transaction::CommitData;
use crate::table::config::TableConfig;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

pub use self::log_data::*;

mod log_data;
mod log_segment;
pub(crate) mod parse;
mod replay;
mod serde;
mod visitors;

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

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        &self.config
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
    pub fn files<'a>(
        &self,
        store: Arc<dyn ObjectStore>,
        visitors: &'a mut Vec<Box<dyn ReplayVisitor>>,
    ) -> DeltaResult<ReplayStream<'a, BoxStream<'_, DeltaResult<RecordBatch>>>> {
        let mut schema_actions: HashSet<_> =
            visitors.iter().flat_map(|v| v.required_actions()).collect();

        schema_actions.insert(ActionType::Add);
        let checkpoint_stream = self.log_segment.checkpoint_stream(
            store.clone(),
            &StructType::new(
                schema_actions
                    .iter()
                    .map(|a| a.schema_field().clone())
                    .collect(),
            ),
            &self.config,
        );

        schema_actions.insert(ActionType::Remove);
        let log_stream = self.log_segment.commit_stream(
            store.clone(),
            &StructType::new(
                schema_actions
                    .iter()
                    .map(|a| a.schema_field().clone())
                    .collect(),
            ),
            &self.config,
        )?;

        ReplayStream::try_new(log_stream, checkpoint_stream, self, visitors)
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
    pub fn stats_schema(&self, table_schema: Option<&StructType>) -> DeltaResult<StructType> {
        let schema = table_schema.unwrap_or_else(|| self.schema());
        stats_schema(schema, self.table_config())
    }

    /// Get the partition values schema of the snapshot
    pub fn partitions_schema(
        &self,
        table_schema: Option<&StructType>,
    ) -> DeltaResult<Option<StructType>> {
        if self.metadata().partition_columns.is_empty() {
            return Ok(None);
        }
        let schema = table_schema.unwrap_or_else(|| self.schema());
        partitions_schema(schema, &self.metadata().partition_columns)
    }
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
#[derive(Debug, Clone, PartialEq)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    // additional actions that should be tracked during log replay.
    tracked_actions: HashSet<ActionType>,

    transactions: Option<HashMap<String, Transaction>>,

    // NOTE: this is a Vec of RecordBatch instead of a single RecordBatch because
    //       we do not yet enforce a consistent schema across all batches we read from the log.
    pub(crate) files: Vec<RecordBatch>,
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        table_root: &Path,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        Self::try_new_with_visitor(table_root, store, config, version, Default::default()).await
    }

    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new_with_visitor(
        table_root: &Path,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: Option<i64>,
        tracked_actions: HashSet<ActionType>,
    ) -> DeltaResult<Self> {
        let mut visitors = tracked_actions
            .iter()
            .flat_map(get_visitor)
            .collect::<Vec<_>>();
        let snapshot =
            Snapshot::try_new(table_root, store.clone(), config.clone(), version).await?;

        let files = match config.require_files {
            true => snapshot.files(store, &mut visitors)?.try_collect().await?,
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
    pub fn new_test<'a>(commits: impl IntoIterator<Item = &'a CommitData>) -> DeltaResult<Self> {
        let (snapshot, batch) = Snapshot::new_test(commits)?;
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
    pub async fn update<'a>(
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
            // NOTE: we don't need to add the visitor relevant data here, as it is repüresented in teh state already
            futures::stream::iter(files.into_iter().map(Ok)).boxed()
        } else {
            let read_schema = StructType::new(
                schema_actions
                    .iter()
                    .map(|a| a.schema_field().clone())
                    .collect(),
            );
            new_slice
                .checkpoint_stream(
                    log_store.object_store(),
                    &read_schema,
                    &self.snapshot.config,
                )
                .boxed()
        };

        schema_actions.insert(ActionType::Remove);
        let read_schema = StructType::new(
            schema_actions
                .iter()
                .map(|a| a.schema_field().clone())
                .collect(),
        );
        let log_stream = new_slice.commit_stream(
            log_store.object_store().clone(),
            &read_schema,
            &self.snapshot.config,
        )?;

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
    pub fn table_root(&self) -> Path {
        self.snapshot.table_root()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        &self.snapshot.load_config()
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
    pub fn transactions(&self) -> DeltaResult<impl Iterator<Item = Transaction> + '_> {
        self.transactions
            .as_ref()
            .map(|t| t.values().cloned())
            .ok_or(DeltaTableError::Generic(
                "Transactions are not available. Please enable tracking of transactions."
                    .to_string(),
            ))
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
        let read_schema = StructType::new(
            schema_actions
                .iter()
                .map(|a| a.schema_field().clone())
                .collect(),
        );
        let actions = self.snapshot.log_segment.advance(
            send,
            &self.table_root(),
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
            let new_schema: StructType = serde_json::from_str(&metadata.schema_string)?;
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
            self.snapshot.schema = serde_json::from_str(&self.snapshot.metadata.schema_string)?;
        }
        if let Some(protocol) = protocol {
            self.snapshot.protocol = protocol;
        }
        self.process_visitors(visitors)?;

        Ok(self.snapshot.version())
    }
}

fn stats_schema(schema: &StructType, config: TableConfig<'_>) -> DeltaResult<StructType> {
    let stats_fields = if let Some(stats_cols) = config.stats_columns() {
        stats_cols
            .iter()
            .map(|col| match get_stats_field(schema, col) {
                Some(field) => match field.data_type() {
                    DataType::Map(_) | DataType::Array(_) | &DataType::BINARY => {
                        Err(DeltaTableError::Generic(format!(
                            "Stats column {} has unsupported type {}",
                            col,
                            field.data_type()
                        )))
                    }
                    _ => Ok(StructField::new(
                        field.name(),
                        field.data_type().clone(),
                        true,
                    )),
                },
                _ => Err(DeltaTableError::Generic(format!(
                    "Stats column {} not found in schema",
                    col
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let num_indexed_cols = config.num_indexed_cols();
        schema
            .fields
            .values()
            .enumerate()
            .filter_map(|(idx, f)| stats_field(idx, num_indexed_cols, f))
            .collect()
    };
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

pub(crate) fn partitions_schema(
    schema: &StructType,
    partition_columns: &Vec<String>,
) -> DeltaResult<Option<StructType>> {
    if partition_columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(StructType::new(
        partition_columns
            .iter()
            .map(|col| {
                schema.field(col).map(|field| field.clone()).ok_or_else(|| {
                    DeltaTableError::Generic(format!(
                        "Partition column {} not found in schema",
                        col
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    )))
}

fn stats_field(idx: usize, num_indexed_cols: i32, field: &StructField) -> Option<StructField> {
    if !(num_indexed_cols < 0 || (idx as i32) < num_indexed_cols) {
        return None;
    }
    match field.data_type() {
        DataType::Map(_) | DataType::Array(_) | &DataType::BINARY => None,
        DataType::Struct(dt_struct) => Some(StructField::new(
            field.name(),
            StructType::new(
                dt_struct
                    .fields()
                    .flat_map(|f| stats_field(idx, num_indexed_cols, f))
                    .collect(),
            ),
            true,
        )),
        DataType::Primitive(_) => Some(StructField::new(
            field.name(),
            field.data_type.clone(),
            true,
        )),
    }
}

fn to_count_field(field: &StructField) -> Option<StructField> {
    match field.data_type() {
        DataType::Map(_) | DataType::Array(_) | &DataType::BINARY => None,
        DataType::Struct(s) => Some(StructField::new(
            field.name(),
            StructType::new(s.fields().filter_map(to_count_field).collect::<Vec<_>>()),
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

/// Retrieves a specific field from the schema based on the provided field name.
/// It handles cases where the field name is nested or enclosed in backticks.
fn get_stats_field<'a>(schema: &'a StructType, stats_field_name: &str) -> Option<&'a StructField> {
    let dialect = sqlparser::dialect::GenericDialect {};
    match sqlparser::parser::Parser::new(&dialect).try_with_sql(stats_field_name) {
        Ok(mut parser) => match parser.parse_multipart_identifier() {
            Ok(parts) => find_nested_field(schema, &parts),
            Err(_) => schema.field(stats_field_name),
        },
        Err(_) => schema.field(stats_field_name),
    }
}

fn find_nested_field<'a>(
    schema: &'a StructType,
    parts: &[sqlparser::ast::Ident],
) -> Option<&'a StructField> {
    if parts.is_empty() {
        return None;
    }
    let part_name = &parts[0].value;
    match schema.field(part_name) {
        Some(field) => {
            if parts.len() == 1 {
                Some(field)
            } else {
                match field.data_type() {
                    DataType::Struct(struct_schema) => {
                        find_nested_field(struct_schema, &parts[1..])
                    }
                    // Any part before the end must be a struct
                    _ => None,
                }
            }
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Utc;
    use deltalake_test::utils::*;
    use futures::TryStreamExt;
    use itertools::Itertools;

    use super::log_segment::tests::{concurrent_checkpoint, test_log_segment};
    use super::replay::tests::test_log_replay;
    use super::*;
    use crate::kernel::Remove;
    use crate::protocol::{DeltaOperation, SaveMode};
    use crate::test_utils::ActionFactory;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_checkpoint() -> TestResult {
        let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;
        concurrent_checkpoint(&context).await?;
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
            .files(store.clone(), &mut vec![])?
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
                .files(store.clone(), &mut vec![])?
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

        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };

        let actions = vec![CommitData::new(
            removes,
            operation,
            HashMap::new(),
            Vec::new(),
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
        let (log_segment, _) = LogSegment::new_test(vec![&commit_data]).unwrap();

        let snapshot = Snapshot {
            log_segment: log_segment.clone(),
            protocol: protocol.clone(),
            metadata,
            schema: schema.clone(),
            table_url: "table".to_string(),
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
        let (log_segment, _) = LogSegment::new_test(vec![&commit_data]).unwrap();

        let snapshot = Snapshot {
            log_segment,
            config: Default::default(),
            protocol: protocol.clone(),
            metadata,
            schema: schema.clone(),
            table_url: "table".to_string(),
        };

        assert_eq!(snapshot.partitions_schema(None).unwrap(), None);
    }

    #[test]
    fn test_field_with_name() {
        let schema = StructType::new(vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b", DataType::INTEGER, true),
        ]);
        let field = get_stats_field(&schema, "b").unwrap();
        assert_eq!(*field, StructField::new("b", DataType::INTEGER, true));
    }

    #[test]
    fn test_field_with_name_escaped() {
        let schema = StructType::new(vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b.b", DataType::INTEGER, true),
        ]);
        let field = get_stats_field(&schema, "`b.b`").unwrap();
        assert_eq!(*field, StructField::new("b.b", DataType::INTEGER, true));
    }

    #[test]
    fn test_field_does_not_exist() {
        let schema = StructType::new(vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b", DataType::INTEGER, true),
        ]);
        let field = get_stats_field(&schema, "c");
        assert!(field.is_none());
    }

    #[test]
    fn test_field_part_is_not_struct() {
        let schema = StructType::new(vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b", DataType::INTEGER, true),
        ]);
        let field = get_stats_field(&schema, "b.c");
        assert!(field.is_none());
    }

    #[test]
    fn test_field_name_does_not_parse() {
        let schema = StructType::new(vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b", DataType::INTEGER, true),
        ]);
        let field = get_stats_field(&schema, "b.");
        assert!(field.is_none());
    }

    #[test]
    fn test_field_with_name_nested() {
        let nested = StructType::new(vec![StructField::new(
            "nested_struct",
            DataType::BOOLEAN,
            true,
        )]);
        let schema = StructType::new(vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b", DataType::Struct(Box::new(nested)), true),
        ]);

        let field = get_stats_field(&schema, "b.nested_struct").unwrap();

        assert_eq!(
            *field,
            StructField::new("nested_struct", DataType::BOOLEAN, true)
        );
    }

    #[test]
    fn test_field_with_last_name_nested_backticks() {
        let nested = StructType::new(vec![StructField::new("pr!me", DataType::BOOLEAN, true)]);
        let schema = StructType::new(vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b", DataType::Struct(Box::new(nested)), true),
        ]);

        let field = get_stats_field(&schema, "b.`pr!me`").unwrap();

        assert_eq!(*field, StructField::new("pr!me", DataType::BOOLEAN, true));
    }

    #[test]
    fn test_field_with_name_nested_backticks() {
        let nested = StructType::new(vec![StructField::new("pr", DataType::BOOLEAN, true)]);
        let schema = StructType::new(vec![
            StructField::new("a", DataType::STRING, true),
            StructField::new("b&b", DataType::Struct(Box::new(nested)), true),
        ]);

        let field = get_stats_field(&schema, "`b&b`.pr").unwrap();

        assert_eq!(*field, StructField::new("pr", DataType::BOOLEAN, true));
    }
}
