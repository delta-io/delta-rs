use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use chrono::Utc;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};
use tracing::debug;
use url::Url;

use super::parse;
use crate::kernel::transaction::CommitData;
use crate::kernel::{arrow::json, ActionType, Metadata, Protocol, Schema, StructType};
use crate::logstore::{LogStore, LogStoreExt};
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";
pub(super) static TOMBSTONE_SCHEMA: LazyLock<StructType> =
    LazyLock::new(|| StructType::new(vec![ActionType::Remove.schema_field().clone()]));

#[derive(Debug, Clone, PartialEq)]
pub(super) struct LogSegment {
    pub(super) version: i64,
    pub(super) commit_files: VecDeque<ObjectMeta>,
    pub(super) checkpoint_files: Vec<ObjectMeta>,
}

impl LogSegment {
    /// Try to create a new [`LogSegment`]
    ///
    /// This will list the entire log directory and find all relevant files for the given table version.
    pub async fn try_new(log_store: &dyn LogStore, version: Option<i64>) -> DeltaResult<Self> {
        let root_store = log_store.root_object_store(None);

        let root_url = log_store.table_root_url();
        let mut store_root = root_url.clone();
        store_root.set_path("");

        let log_url = log_store.log_root_url();
        let log_path = crate::logstore::object_store_path(&log_url)?;

        let maybe_cp = read_last_checkpoint(&root_store, &log_path).await?;

        // List relevant files from log
        let (mut commit_files, checkpoint_files) = match (maybe_cp, version) {
            (Some(cp), None) => {
                list_log_files_with_checkpoint(&cp, &root_store, &log_path, &store_root).await?
            }
            (Some(cp), Some(v)) if cp.version <= v => {
                list_log_files_with_checkpoint(&cp, &root_store, &log_path, &store_root).await?
            }
            _ => list_log_files(&root_store, &log_path, version, None, &store_root).await?,
        };

        // remove all files above requested version
        if let Some(version) = version {
            commit_files.retain(|meta| meta.1.version <= version as u64);
        }

        validate(&commit_files, &checkpoint_files)?;

        let mut segment = Self {
            version: 0,
            commit_files: commit_files.into_iter().map(|(meta, _)| meta).collect(),
            checkpoint_files: checkpoint_files.into_iter().map(|(meta, _)| meta).collect(),
        };
        if segment.commit_files.is_empty() && segment.checkpoint_files.is_empty() {
            return Err(DeltaTableError::NotATable("no log files".into()));
        }
        // get the effective version from chosen files
        let version_eff = segment.file_version().ok_or(DeltaTableError::Generic(
            "failed to get effective version".into(),
        ))?;
        segment.version = version_eff;

        if let Some(v) = version {
            if version_eff != v {
                return Err(DeltaTableError::Generic("missing version".into()));
            }
        }

        Ok(segment)
    }

    /// Try to create a new [`LogSegment`] from a slice of the log.
    ///
    /// This will create a new [`LogSegment`] from the log with all relevant log files
    /// starting at `start_version` and ending at `end_version`.
    pub async fn try_new_slice(
        start_version: i64,
        end_version: Option<i64>,
        log_store: &dyn LogStore,
    ) -> DeltaResult<Self> {
        debug!("try_new_slice: start_version: {start_version}, end_version: {end_version:?}",);
        log_store.refresh().await?;
        let log_url = log_store.log_root_url();
        let mut store_root = log_url.clone();
        store_root.set_path("");
        let log_path = crate::logstore::object_store_path(&log_url)?;

        let (mut commit_files, checkpoint_files) = list_log_files(
            &log_store.root_object_store(None),
            &log_path,
            end_version,
            Some(start_version),
            &store_root,
        )
        .await?;

        // remove all files above requested version
        if let Some(version) = end_version {
            commit_files.retain(|meta| meta.1.version <= version as u64);
        }

        validate(&commit_files, &checkpoint_files)?;

        let mut segment = Self {
            version: start_version,
            commit_files: commit_files.into_iter().map(|(meta, _)| meta).collect(),
            checkpoint_files: checkpoint_files.into_iter().map(|(meta, _)| meta).collect(),
        };
        segment.version = segment
            .file_version()
            .unwrap_or(end_version.unwrap_or(start_version));

        Ok(segment)
    }

    /// Returns the highest commit version number in the log segment
    pub fn file_version(&self) -> Option<i64> {
        let dummy_url = Url::parse("dummy:///").unwrap();
        self.commit_files
            .front()
            .and_then(|f| {
                let file_url = dummy_url.join(f.location.as_ref()).ok()?;
                let parsed = ParsedLogPath::try_from(file_url).ok()?;
                parsed.map(|p| p.version as i64)
            })
            .or(self.checkpoint_files.first().and_then(|f| {
                let file_url = dummy_url.join(f.location.as_ref()).ok()?;
                let parsed = ParsedLogPath::try_from(file_url).ok()?;
                parsed.map(|p| p.version as i64)
            }))
    }

    #[cfg(test)]
    pub(super) fn new_test<'a>(
        commits: impl IntoIterator<Item = &'a CommitData>,
        table_root: &Path,
    ) -> DeltaResult<(Self, Vec<Result<RecordBatch, DeltaTableError>>)> {
        let mut log = Self {
            version: -1,
            commit_files: Default::default(),
            checkpoint_files: Default::default(),
        };
        let iter = log
            .advance(
                commits,
                table_root,
                crate::kernel::models::fields::log_schema(),
                &Default::default(),
            )?
            .collect_vec();
        Ok((log, iter))
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    /// Returns the last modified timestamp for a commit file with the given version
    pub fn version_timestamp(&self, version: i64) -> Option<chrono::DateTime<Utc>> {
        let dummy_url = Url::parse("dummy:///").unwrap();
        self.commit_files
            .iter()
            .find(|f| {
                let parsed = dummy_url
                    .join(f.location.as_ref())
                    .ok()
                    .and_then(|p| ParsedLogPath::try_from(p).ok())
                    .flatten();
                parsed.map(|p| p.version == version as u64).unwrap_or(false)
            })
            .map(|f| f.last_modified)
    }

    pub(super) fn commit_stream(
        &self,
        log_store: &dyn LogStore,
        read_schema: &Schema,
        config: &DeltaTableConfig,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<RecordBatch>>> {
        let root_store = log_store.root_object_store(None);

        let decoder = json::get_decoder(Arc::new(read_schema.try_into_arrow()?), config)?;
        let stream = futures::stream::iter(self.commit_files.iter())
            .map(move |meta| {
                let root_store = root_store.clone();
                async move { root_store.get(&meta.location).await?.bytes().await }
            })
            .buffered(config.log_buffer_size);
        Ok(json::decode_stream(decoder, stream).boxed())
    }

    pub(super) fn checkpoint_stream(
        &self,
        log_store: &dyn LogStore,
        read_schema: &Schema,
        config: &DeltaTableConfig,
    ) -> BoxStream<'_, DeltaResult<RecordBatch>> {
        let root_store = log_store.root_object_store(None);

        let batch_size = config.log_batch_size;
        let read_schema = Arc::new(read_schema.clone());
        futures::stream::iter(self.checkpoint_files.clone())
            .map(move |meta| {
                let root_store = root_store.clone();
                let read_schema = read_schema.clone();
                async move {
                    let mut reader = ParquetObjectReader::new(root_store, meta.location)
                        .with_file_size(meta.size);
                    let options = ArrowReaderOptions::new();
                    let reader_meta = ArrowReaderMetadata::load_async(&mut reader, options).await?;

                    // Create projection selecting read_schema fields from parquet file's arrow schema
                    let projection = reader_meta
                        .schema()
                        .fields
                        .iter()
                        .enumerate()
                        .filter_map(|(i, f)| {
                            if read_schema.fields.contains_key(f.name()) {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    let projection =
                        ProjectionMask::roots(reader_meta.parquet_schema(), projection);

                    // Note: the output batch stream batches have all null value rows for action types not
                    // present in the projection. When a RowFilter was used to remove null rows, the performance
                    // got worse when projecting all fields, and was no better when projecting a subset.
                    // The all null rows are filtered out anyway when the batch stream is consumed.
                    ParquetRecordBatchStreamBuilder::new_with_metadata(reader, reader_meta)
                        .with_projection(projection.clone())
                        .with_batch_size(batch_size)
                        .build()
                }
            })
            .buffered(config.log_buffer_size)
            .try_flatten()
            .map_err(Into::into)
            .boxed()
    }

    /// Read [`Protocol`] and [`Metadata`] actions
    pub(super) async fn read_metadata(
        &self,
        log_store: &dyn LogStore,
        config: &DeltaTableConfig,
    ) -> DeltaResult<(Option<Protocol>, Option<Metadata>)> {
        static READ_SCHEMA: LazyLock<StructType> = LazyLock::new(|| {
            StructType::new(vec![
                ActionType::Protocol.schema_field().clone(),
                ActionType::Metadata.schema_field().clone(),
            ])
        });

        let mut maybe_protocol = None;
        let mut maybe_metadata = None;

        let mut commit_stream = self.commit_stream(log_store, &READ_SCHEMA, config)?;
        while let Some(batch) = commit_stream.next().await {
            let batch = batch?;
            if maybe_protocol.is_none() {
                if let Some(p) = parse::read_protocol(&batch)? {
                    maybe_protocol.replace(p);
                };
            }
            if maybe_metadata.is_none() {
                if let Some(m) = parse::read_metadata(&batch)? {
                    maybe_metadata.replace(m);
                };
            }
            if maybe_protocol.is_some() && maybe_metadata.is_some() {
                return Ok((maybe_protocol, maybe_metadata));
            }
        }

        let mut checkpoint_stream = self.checkpoint_stream(log_store, &READ_SCHEMA, config);
        while let Some(batch) = checkpoint_stream.next().await {
            let batch = batch?;
            if maybe_protocol.is_none() {
                if let Some(p) = parse::read_protocol(&batch)? {
                    maybe_protocol.replace(p);
                };
            }
            if maybe_metadata.is_none() {
                if let Some(m) = parse::read_metadata(&batch)? {
                    maybe_metadata.replace(m);
                };
            }
            if maybe_protocol.is_some() && maybe_metadata.is_some() {
                return Ok((maybe_protocol, maybe_metadata));
            }
        }

        Ok((maybe_protocol, maybe_metadata))
    }

    /// Advance the log segment with new commits
    ///
    /// Returns an iterator over record batches, as if the commits were read from the log.
    /// The input commits should be in order in which they would be committed to the table.
    pub(super) fn advance<'a>(
        &mut self,
        commits: impl IntoIterator<Item = &'a CommitData>,
        table_root: &Path,
        read_schema: &Schema,
        config: &DeltaTableConfig,
    ) -> DeltaResult<impl Iterator<Item = Result<RecordBatch, DeltaTableError>> + '_> {
        let log_path = table_root.child("_delta_log");
        let mut decoder = json::get_decoder(Arc::new(read_schema.try_into_arrow()?), config)?;

        let mut commit_data = Vec::new();
        for commit in commits {
            self.version += 1;
            let path = log_path.child(format!("{:020}.json", self.version));
            let bytes = commit.get_bytes()?;
            let meta = ObjectMeta {
                location: path,
                size: bytes.len() as u64,
                last_modified: Utc::now(),
                e_tag: None,
                version: None,
            };
            // NOTE: We always assume the commit files are sorted in reverse order
            self.commit_files.push_front(meta);
            let reader = json::get_reader(&bytes);
            let batches =
                json::decode_reader(&mut decoder, reader).collect::<Result<Vec<_>, _>>()?;
            commit_data.push(batches);
        }

        // NOTE: Most recent commits need to be processed first
        commit_data.reverse();
        Ok(commit_data.into_iter().flatten().map(Ok))
    }
}

fn validate(
    commit_files: &[(ObjectMeta, ParsedLogPath<Url>)],
    checkpoint_files: &[(ObjectMeta, ParsedLogPath<Url>)],
) -> DeltaResult<()> {
    let is_contiguous = commit_files
        .iter()
        .collect_vec()
        .windows(2)
        .all(|cfs| cfs[0].1.version - 1 == cfs[1].1.version);
    if !is_contiguous {
        return Err(DeltaTableError::Generic(
            "non-contiguous log segment".into(),
        ));
    }

    let checkpoint_version = checkpoint_files.iter().map(|f| f.1.version).max();
    if let Some(v) = checkpoint_version {
        if !commit_files.iter().all(|f| f.1.version > v) {
            return Err(DeltaTableError::Generic("inconsistent log segment".into()));
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointMetadata {
    /// The version of the table when the last checkpoint was made.
    #[allow(unreachable_pub)] // used by acceptance tests (TODO make an fn accessor?)
    pub version: i64,
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i64,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub(crate) parts: Option<i32>,
    /// The number of bytes of the checkpoint.
    pub(crate) size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint.
    pub(crate) num_of_add_files: Option<i64>,
    /// The schema of the checkpoint file.
    pub(crate) checkpoint_schema: Option<Schema>,
    /// The checksum of the last checkpoint JSON.
    pub(crate) checksum: Option<String>,
}

/// Try reading the `_last_checkpoint` file.
///
/// In case the file is not found, `None` is returned.
async fn read_last_checkpoint(
    fs_client: &dyn ObjectStore,
    log_root: &Path,
) -> DeltaResult<Option<CheckpointMetadata>> {
    let file_path = log_root.child(LAST_CHECKPOINT_FILE_NAME);
    match fs_client.get(&file_path).await {
        Ok(data) => {
            let data = data.bytes().await?;
            Ok(Some(serde_json::from_slice(&data)?))
        }
        Err(ObjectStoreError::NotFound { .. }) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// List all log files after a given checkpoint.
async fn list_log_files_with_checkpoint(
    cp: &CheckpointMetadata,
    root_store: &dyn ObjectStore,
    log_root: &Path,
    store_root: &Url,
) -> DeltaResult<(
    Vec<(ObjectMeta, ParsedLogPath<Url>)>,
    Vec<(ObjectMeta, ParsedLogPath<Url>)>,
)> {
    let version_prefix = format!("{:020}", cp.version);
    let start_from = log_root.child(version_prefix.as_str());

    let files = root_store
        .list_with_offset(Some(log_root), &start_from)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|f| {
            let file_url = store_root.join(f.location.as_ref()).ok()?;
            let path = ParsedLogPath::try_from(file_url).ok()??;
            Some((f, path))
        })
        .collect::<Vec<_>>();

    let mut commit_files = files
        .iter()
        .filter_map(|f| {
            if matches!(f.1.file_type, LogPathFileType::Commit) && f.1.version > cp.version as u64 {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();

    // NOTE: this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.0.location.cmp(&a.0.location));

    let checkpoint_files = files
        .iter()
        .filter_map(|f| {
            if matches!(
                f.1.file_type,
                // UUID named checkpoints are part of the v2 spec and can currently not be parsed.
                // This will be supported once we do kernel log replay.
                // | LogPathFileType::UuidCheckpoint(_)
                LogPathFileType::SinglePartCheckpoint | LogPathFileType::MultiPartCheckpoint { .. }
            ) && f.1.version == cp.version as u64
            {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();

    if checkpoint_files.len() != cp.parts.unwrap_or(1) as usize {
        let msg = format!(
            "Number of checkpoint files '{}' is not equal to number of checkpoint metadata parts '{:?}'",
            checkpoint_files.len(),
            cp.parts
        );
        Err(DeltaTableError::MetadataError(msg))
    } else {
        Ok((commit_files, checkpoint_files))
    }
}

/// List relevant log files.
///
/// Relevant files are the max checkpoint found and all subsequent commits.
pub(super) async fn list_log_files(
    root_store: &dyn ObjectStore,
    log_root: &Path,
    max_version: Option<i64>,
    start_version: Option<i64>,
    store_root: &Url,
) -> DeltaResult<(
    Vec<(ObjectMeta, ParsedLogPath<Url>)>,
    Vec<(ObjectMeta, ParsedLogPath<Url>)>,
)> {
    let max_version = max_version.unwrap_or(i64::MAX - 1);
    let start_from = log_root.child(format!("{:020}", start_version.unwrap_or(0)).as_str());

    let mut max_checkpoint_version = -1_i64;
    let mut commit_files = Vec::with_capacity(25);
    let mut checkpoint_files = Vec::with_capacity(10);

    for meta in root_store
        .list_with_offset(Some(log_root), &start_from)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|f| {
            let file_url = store_root.join(f.location.as_ref()).ok()?;
            let path = ParsedLogPath::try_from(file_url).ok()??;
            Some((f, path))
        })
    {
        if meta.1.version <= max_version as u64 && Some(meta.1.version as i64) >= start_version {
            if matches!(
                meta.1.file_type,
                // UUID named checkpoints are part of the v2 spec and can currently not be parsed.
                // This will be supported once we do kernel log replay.
                // | LogPathFileType::UuidCheckpoint(_)
                LogPathFileType::SinglePartCheckpoint | LogPathFileType::MultiPartCheckpoint { .. }
            ) {
                match (meta.1.version as i64).cmp(&max_checkpoint_version) {
                    Ordering::Greater => {
                        max_checkpoint_version = meta.1.version as i64;
                        checkpoint_files.clear();
                        checkpoint_files.push(meta);
                    }
                    Ordering::Equal => {
                        checkpoint_files.push(meta);
                    }
                    _ => {}
                }
            } else if matches!(meta.1.file_type, LogPathFileType::Commit) {
                commit_files.push(meta);
            }
        }
    }

    commit_files.retain(|f| f.1.version as i64 > max_checkpoint_version);
    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.0.location.cmp(&a.0.location));

    Ok((commit_files, checkpoint_files))
}

#[cfg(test)]
pub(super) mod tests {
    use delta_kernel::table_features::{ReaderFeature, WriterFeature};
    use maplit::hashset;
    use tokio::task::JoinHandle;

    use crate::{
        checkpoints::create_checkpoint_from_table_uri_and_cleanup,
        kernel::{
            transaction::{CommitBuilder, TableReference},
            Action, Add, ProtocolInner, Remove,
        },
        protocol::{create_checkpoint_for, DeltaOperation, SaveMode},
        test_utils::{TestResult, TestTables},
        DeltaTableBuilder,
    };

    use super::*;

    pub(crate) async fn test_log_segment() -> TestResult {
        read_log_files().await?;
        read_metadata().await?;
        log_segment_serde().await?;

        Ok(())
    }

    async fn log_segment_serde() -> TestResult {
        let log_store = TestTables::Simple.table_builder().build_storage()?;

        let segment = LogSegment::try_new(&log_store, None).await?;
        let bytes = serde_json::to_vec(&segment).unwrap();
        let actual: LogSegment = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual.version(), segment.version());
        assert_eq!(actual.commit_files.len(), segment.commit_files.len());
        assert_eq!(
            actual.checkpoint_files.len(),
            segment.checkpoint_files.len()
        );

        Ok(())
    }

    async fn read_log_files() -> TestResult {
        let log_store = TestTables::SimpleWithCheckpoint
            .table_builder()
            .build_storage()?;
        let root_store = log_store.root_object_store(None);
        let log_url = log_store.log_root_url();
        let log_path = Path::from_url_path(log_url.path())?;
        let mut store_url = log_url.clone();
        store_url.set_path("");

        let cp = read_last_checkpoint(&root_store, &log_path).await?.unwrap();
        assert_eq!(cp.version, 10);

        let (log, check) =
            list_log_files_with_checkpoint(&cp, &root_store, &log_path, &store_url).await?;
        assert_eq!(log.len(), 0);
        assert_eq!(check.len(), 1);

        let (log, check) = list_log_files(&root_store, &log_path, None, None, &store_url).await?;
        assert_eq!(log.len(), 0);
        assert_eq!(check.len(), 1);

        let (log, check) =
            list_log_files(&root_store, &log_path, Some(8), None, &store_url).await?;
        assert_eq!(log.len(), 9);
        assert_eq!(check.len(), 0);

        let segment = LogSegment::try_new(&log_store, None).await?;
        assert_eq!(segment.version, 10);
        assert_eq!(segment.commit_files.len(), 0);
        assert_eq!(segment.checkpoint_files.len(), 1);

        let segment = LogSegment::try_new(&log_store, Some(8)).await?;
        assert_eq!(segment.version, 8);
        assert_eq!(segment.commit_files.len(), 9);
        assert_eq!(segment.checkpoint_files.len(), 0);

        let log_store = TestTables::Simple.table_builder().build_storage()?;
        let root_store = log_store.root_object_store(None);
        let log_url = log_store.log_root_url();
        let mut store_url = log_url.clone();
        store_url.set_path("");
        let log_path = Path::from_url_path(log_url.path())?;

        let (log, check) = list_log_files(&root_store, &log_path, None, None, &store_url).await?;
        assert_eq!(log.len(), 5);
        assert_eq!(check.len(), 0);

        let (log, check) =
            list_log_files(&root_store, &log_path, Some(2), None, &store_url).await?;
        assert_eq!(log.len(), 3);
        assert_eq!(check.len(), 0);

        Ok(())
    }

    async fn read_metadata() -> TestResult {
        let log_store = TestTables::WithDvSmall.table_builder().build_storage()?;
        let segment = LogSegment::try_new(&log_store, None).await?;
        let (protocol, _metadata) = segment
            .read_metadata(&log_store, &Default::default())
            .await?;
        let protocol = protocol.unwrap();

        let expected = ProtocolInner {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(hashset! {ReaderFeature::DeletionVectors}),
            writer_features: Some(hashset! {WriterFeature::DeletionVectors}),
        }
        .as_kernel();
        assert_eq!(protocol, expected);

        Ok(())
    }

    pub(crate) async fn concurrent_checkpoint() -> TestResult {
        let table_to_checkpoint = TestTables::LatestNotCheckpointed
            .table_builder()
            .load()
            .await?;

        let base_store = table_to_checkpoint.log_store().root_object_store(None);
        let slow_list_store = Arc::new(slow_store::SlowListStore { store: base_store });
        let slow_log_store = TestTables::LatestNotCheckpointed
            .table_builder()
            .with_storage_backend(slow_list_store, url::Url::parse("dummy:///").unwrap())
            .build_storage()?;

        let version = table_to_checkpoint.version().unwrap();
        let load_task: JoinHandle<Result<LogSegment, DeltaTableError>> = tokio::spawn(async move {
            let segment = LogSegment::try_new(&slow_log_store, Some(version)).await?;
            Ok(segment)
        });

        create_checkpoint_from_table_uri_and_cleanup(
            &table_to_checkpoint.table_uri(),
            version,
            Some(false),
            None,
        )
        .await?;

        let segment = load_task.await??;
        assert_eq!(segment.version, version);

        Ok(())
    }

    mod slow_store {
        use std::sync::Arc;

        use futures::stream::BoxStream;
        use object_store::{
            path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
            ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
        };

        #[derive(Debug)]
        pub(super) struct SlowListStore {
            pub store: Arc<dyn ObjectStore>,
        }

        impl std::fmt::Display for SlowListStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "SlowListStore {{ store: {} }}", self.store)
            }
        }

        #[async_trait::async_trait]
        impl object_store::ObjectStore for SlowListStore {
            async fn put_opts(
                &self,
                location: &Path,
                bytes: PutPayload,
                opts: PutOptions,
            ) -> Result<PutResult> {
                self.store.put_opts(location, bytes, opts).await
            }
            async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
                self.store.put_multipart(location).await
            }

            async fn put_multipart_opts(
                &self,
                location: &Path,
                opts: PutMultipartOpts,
            ) -> Result<Box<dyn MultipartUpload>> {
                self.store.put_multipart_opts(location, opts).await
            }

            async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
                self.store.get_opts(location, options).await
            }

            async fn delete(&self, location: &Path) -> Result<()> {
                self.store.delete(location).await
            }

            fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
                std::thread::sleep(std::time::Duration::from_secs(1));
                self.store.list(prefix)
            }

            async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
                self.store.list_with_delimiter(prefix).await
            }

            async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
                self.store.copy(from, to).await
            }

            async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
                self.store.copy_if_not_exists(from, to).await
            }
        }
    }

    #[tokio::test]
    async fn test_checkpoint_stream_parquet_read() {
        let value = serde_json::json!({
            "id": "test".to_string(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": r#"{"type":"struct",  "fields": []}"#.to_string(),
            "partitionColumns": Vec::<String>::new(),
            "createdTime": Some(Utc::now().timestamp_millis()),
            "name": "test",
            "description": "test",
            "configuration": {}
        });
        let metadata: Metadata = serde_json::from_value(value).unwrap();
        let protocol = ProtocolInner::default().as_kernel();

        let mut actions = vec![Action::Metadata(metadata), Action::Protocol(protocol)];
        for i in 0..10 {
            actions.push(Action::Add(Add {
                path: format!("part-{i}.parquet"),
                modification_time: chrono::Utc::now().timestamp_millis(),
                ..Default::default()
            }));
        }

        let log_store = DeltaTableBuilder::from_uri("memory:///")
            .build_storage()
            .unwrap();
        let op = DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            partition_by: None,
            predicate: None,
        };
        let commit = CommitBuilder::default()
            .with_actions(actions)
            .build(None, log_store.clone(), op)
            .await
            .unwrap();

        let mut actions = Vec::new();
        // remove all but one file
        for i in 0..9 {
            actions.push(Action::Remove(Remove {
                path: format!("part-{i}.parquet"),
                deletion_timestamp: Some(chrono::Utc::now().timestamp_millis()),
                ..Default::default()
            }))
        }

        let op = DeltaOperation::Delete { predicate: None };
        let table_data = &commit.snapshot as &dyn TableReference;
        let commit = CommitBuilder::default()
            .with_actions(actions)
            .build(Some(table_data), log_store.clone(), op)
            .await
            .unwrap();

        create_checkpoint_for(commit.version as u64, log_store.as_ref(), None)
            .await
            .unwrap();

        assert_eq!(commit.metrics.num_retries, 0);
        assert_eq!(commit.metrics.num_log_files_cleaned_up, 0);
        assert!(!commit.metrics.new_checkpoint_created);

        let batches = LogSegment::try_new(&log_store, Some(commit.version))
            .await
            .unwrap()
            .checkpoint_stream(
                &log_store,
                &StructType::new(vec![
                    ActionType::Metadata.schema_field().clone(),
                    ActionType::Protocol.schema_field().clone(),
                    ActionType::Add.schema_field().clone(),
                ]),
                &Default::default(),
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();

        // there are 9 remove action rows but all columns are null
        // because the removes are not projected in the schema
        // these get filtered out upstream and there was no perf
        // benefit when applying a row filter
        // in addition there is 1 add, 1 metadata, and 1 protocol row
        assert_eq!(batch.num_rows(), 12);

        assert_eq!(batch.schema().fields().len(), 3);
        assert!(batch.schema().field_with_name("metaData").is_ok());
        assert!(batch.schema().field_with_name("protocol").is_ok());
        assert!(batch.schema().field_with_name("add").is_ok());
    }
}
