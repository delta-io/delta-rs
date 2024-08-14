use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::RecordBatch;
use chrono::Utc;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::parse;
use crate::kernel::{arrow::json, ActionType, Metadata, Protocol, Schema, StructType};
use crate::logstore::LogStore;
use crate::operations::transaction::CommitData;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

lazy_static! {
    static ref CHECKPOINT_FILE_PATTERN: Regex =
        Regex::new(r"\d+\.checkpoint(\.\d+\.\d+)?\.parquet").unwrap();
    static ref DELTA_FILE_PATTERN: Regex = Regex::new(r"^\d+\.json$").unwrap();
    pub(super) static ref TOMBSTONE_SCHEMA: StructType =
        StructType::new(vec![ActionType::Remove.schema_field().clone(),]);
}

/// Trait to extend a file path representation with delta specific functionality
///
/// specifically, this trait adds the ability to recognize valid log files and
/// parse the version number from a log file path
// TODO handle compaction files
pub(super) trait PathExt {
    fn child(&self, path: impl AsRef<str>) -> DeltaResult<Path>;
    /// Returns the last path segment if not terminated with a "/"
    fn filename(&self) -> Option<&str>;

    /// Parse the version number assuming a commit json or checkpoint parquet file
    fn commit_version(&self) -> Option<i64> {
        self.filename()
            .and_then(|f| f.split_once('.'))
            .and_then(|(name, _)| name.parse().ok())
    }

    /// Returns true if the file is a checkpoint parquet file
    fn is_checkpoint_file(&self) -> bool {
        self.filename()
            .map(|name| CHECKPOINT_FILE_PATTERN.captures(name).is_some())
            .unwrap_or(false)
    }

    /// Returns true if the file is a commit json file
    fn is_commit_file(&self) -> bool {
        self.filename()
            .map(|name| DELTA_FILE_PATTERN.captures(name).is_some())
            .unwrap_or(false)
    }
}

impl PathExt for Path {
    fn child(&self, path: impl AsRef<str>) -> DeltaResult<Path> {
        Ok(self.child(path.as_ref()))
    }

    fn filename(&self) -> Option<&str> {
        self.filename()
    }
}

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
    pub async fn try_new(
        table_root: &Path,
        version: Option<i64>,
        store: &dyn ObjectStore,
    ) -> DeltaResult<Self> {
        let log_url = table_root.child("_delta_log");
        let maybe_cp = read_last_checkpoint(store, &log_url).await?;

        // List relevant files from log
        let (mut commit_files, checkpoint_files) = match (maybe_cp, version) {
            (Some(cp), None) => list_log_files_with_checkpoint(&cp, store, &log_url).await?,
            (Some(cp), Some(v)) if cp.version <= v => {
                list_log_files_with_checkpoint(&cp, store, &log_url).await?
            }
            _ => list_log_files(store, &log_url, version, None).await?,
        };

        // remove all files above requested version
        if let Some(version) = version {
            commit_files.retain(|meta| meta.location.commit_version() <= Some(version));
        }

        let mut segment = Self {
            version: 0,
            commit_files: commit_files.into(),
            checkpoint_files,
        };
        if segment.commit_files.is_empty() && segment.checkpoint_files.is_empty() {
            return Err(DeltaTableError::NotATable("no log files".into()));
        }
        // get the effective version from chosen files
        let version_eff = segment.file_version().ok_or(DeltaTableError::Generic(
            "failed to get effective version".into(),
        ))?; // TODO: A more descriptive error
        segment.version = version_eff;
        segment.validate()?;

        if let Some(v) = version {
            if version_eff != v {
                // TODO more descriptive error
                return Err(DeltaTableError::Generic("missing version".into()));
            }
        }

        Ok(segment)
    }

    /// Try to create a new [`LogSegment`] from a slice of the log.
    ///
    /// Ths will create a new [`LogSegment`] from the log with all relevant log files
    /// starting at `start_version` and ending at `end_version`.
    pub async fn try_new_slice(
        table_root: &Path,
        start_version: i64,
        end_version: Option<i64>,
        log_store: &dyn LogStore,
    ) -> DeltaResult<Self> {
        debug!(
            "try_new_slice: start_version: {}, end_version: {:?}",
            start_version, end_version
        );
        log_store.refresh().await?;
        let log_url = table_root.child("_delta_log");
        let (mut commit_files, checkpoint_files) = list_log_files(
            log_store.object_store().as_ref(),
            &log_url,
            end_version,
            Some(start_version),
        )
        .await?;
        // remove all files above requested version
        if let Some(version) = end_version {
            commit_files.retain(|meta| meta.location.commit_version() <= Some(version));
        }
        let mut segment = Self {
            version: start_version,
            commit_files: commit_files.into(),
            checkpoint_files,
        };
        segment.version = segment
            .file_version()
            .unwrap_or(end_version.unwrap_or(start_version));
        Ok(segment)
    }

    pub fn validate(&self) -> DeltaResult<()> {
        let checkpoint_version = self
            .checkpoint_files
            .iter()
            .filter_map(|f| f.location.commit_version())
            .max();
        if let Some(v) = checkpoint_version {
            if !self
                .commit_files
                .iter()
                .all(|f| f.location.commit_version() > Some(v))
            {
                return Err(DeltaTableError::Generic("inconsistent log segment".into()));
            }
        }
        Ok(())
    }

    /// Returns the highes commit version number in the log segment
    pub fn file_version(&self) -> Option<i64> {
        self.commit_files
            .iter()
            .filter_map(|f| f.location.commit_version())
            .max()
            .or(self
                .checkpoint_files
                .first()
                .and_then(|f| f.location.commit_version()))
    }

    #[cfg(test)]
    pub(super) fn new_test<'a>(
        commits: impl IntoIterator<Item = &'a CommitData>,
    ) -> DeltaResult<(Self, Vec<Result<RecordBatch, DeltaTableError>>)> {
        let mut log = Self {
            version: -1,
            commit_files: Default::default(),
            checkpoint_files: Default::default(),
        };
        let iter = log
            .advance(
                commits,
                &Path::default(),
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
        self.commit_files
            .iter()
            .find(|f| f.location.commit_version() == Some(version))
            .map(|f| f.last_modified)
    }

    pub(super) fn commit_stream(
        &self,
        store: Arc<dyn ObjectStore>,
        read_schema: &Schema,
        config: &DeltaTableConfig,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<RecordBatch>>> {
        let decoder = json::get_decoder(Arc::new(read_schema.try_into()?), config)?;
        let stream = futures::stream::iter(self.commit_files.iter())
            .map(move |meta| {
                let store = store.clone();
                async move { store.get(&meta.location).await?.bytes().await }
            })
            .buffered(config.log_buffer_size);
        Ok(json::decode_stream(decoder, stream).boxed())
    }

    pub(super) fn checkpoint_stream(
        &self,
        store: Arc<dyn ObjectStore>,
        read_schema: &Schema,
        config: &DeltaTableConfig,
    ) -> BoxStream<'_, DeltaResult<RecordBatch>> {
        let batch_size = config.log_batch_size;
        let read_schema = Arc::new(read_schema.clone());
        futures::stream::iter(self.checkpoint_files.clone())
            .map(move |meta| {
                let store = store.clone();
                let read_schema = read_schema.clone();
                async move {
                    let mut reader = ParquetObjectReader::new(store, meta);
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
        store: Arc<dyn ObjectStore>,
        config: &DeltaTableConfig,
    ) -> DeltaResult<(Option<Protocol>, Option<Metadata>)> {
        lazy_static::lazy_static! {
            static ref READ_SCHEMA: StructType = StructType::new(vec![
                ActionType::Protocol.schema_field().clone(),
                ActionType::Metadata.schema_field().clone(),
            ]);
        }

        let mut maybe_protocol = None;
        let mut maybe_metadata = None;

        let mut commit_stream = self.commit_stream(store.clone(), &READ_SCHEMA, config)?;
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

        let mut checkpoint_stream = self.checkpoint_stream(store.clone(), &READ_SCHEMA, config);
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
    /// The input commits should be in order in which they would be commited to the table.
    pub(super) fn advance<'a>(
        &mut self,
        commits: impl IntoIterator<Item = &'a CommitData>,
        table_root: &Path,
        read_schema: &Schema,
        config: &DeltaTableConfig,
    ) -> DeltaResult<impl Iterator<Item = Result<RecordBatch, DeltaTableError>> + '_> {
        let log_path = table_root.child("_delta_log");
        let mut decoder = json::get_decoder(Arc::new(read_schema.try_into()?), config)?;

        let mut commit_data = Vec::new();
        for commit in commits {
            self.version += 1;
            let path = log_path.child(format!("{:020}.json", self.version));
            let bytes = commit.get_bytes()?;
            let meta = ObjectMeta {
                location: path,
                size: bytes.len(),
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
    fs_client: &dyn ObjectStore,
    log_root: &Path,
) -> DeltaResult<(Vec<ObjectMeta>, Vec<ObjectMeta>)> {
    let version_prefix = format!("{:020}", cp.version);
    let start_from = log_root.child(version_prefix.as_str());

    let files = fs_client
        .list_with_offset(Some(log_root), &start_from)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        // TODO this filters out .crc files etc which start with "." - how do we want to use these kind of files?
        .filter(|f| f.location.commit_version().is_some())
        .collect::<Vec<_>>();

    let mut commit_files = files
        .iter()
        .filter_map(|f| {
            if f.location.is_commit_file() && f.location.commit_version() > Some(cp.version) {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();

    // NOTE: this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));

    let checkpoint_files = files
        .iter()
        .filter_map(|f| {
            if f.location.is_checkpoint_file() && f.location.commit_version() == Some(cp.version) {
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
    fs_client: &dyn ObjectStore,
    log_root: &Path,
    max_version: Option<i64>,
    start_version: Option<i64>,
) -> DeltaResult<(Vec<ObjectMeta>, Vec<ObjectMeta>)> {
    let max_version = max_version.unwrap_or(i64::MAX - 1);
    let start_from = log_root.child(format!("{:020}", start_version.unwrap_or(0)).as_str());

    let mut max_checkpoint_version = -1_i64;
    let mut commit_files = Vec::with_capacity(25);
    let mut checkpoint_files = Vec::with_capacity(10);

    for meta in fs_client
        .list_with_offset(Some(log_root), &start_from)
        .try_collect::<Vec<_>>()
        .await?
    {
        if meta.location.commit_version().unwrap_or(i64::MAX) <= max_version
            && meta.location.commit_version() >= start_version
        {
            if meta.location.is_checkpoint_file() {
                let version = meta.location.commit_version().unwrap_or(0);
                match version.cmp(&max_checkpoint_version) {
                    Ordering::Greater => {
                        max_checkpoint_version = version;
                        checkpoint_files.clear();
                        checkpoint_files.push(meta);
                    }
                    Ordering::Equal => {
                        checkpoint_files.push(meta);
                    }
                    _ => {}
                }
            } else if meta.location.is_commit_file() {
                commit_files.push(meta);
            }
        }
    }

    commit_files.retain(|f| f.location.commit_version().unwrap_or(0) > max_checkpoint_version);
    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));

    Ok((commit_files, checkpoint_files))
}

#[cfg(test)]
pub(super) mod tests {
    use deltalake_test::utils::*;
    use tokio::task::JoinHandle;

    use crate::{
        checkpoints::{create_checkpoint_for, create_checkpoint_from_table_uri_and_cleanup},
        kernel::{Action, Add, Format, Remove},
        operations::transaction::{CommitBuilder, TableReference},
        protocol::{DeltaOperation, SaveMode},
        DeltaTableBuilder,
    };

    use super::*;

    pub(crate) async fn test_log_segment(context: &IntegrationContext) -> TestResult {
        read_log_files(context).await?;
        read_metadata(context).await?;
        log_segment_serde(context).await?;

        Ok(())
    }

    async fn log_segment_serde(context: &IntegrationContext) -> TestResult {
        let store = context
            .table_builder(TestTables::Simple)
            .build_storage()?
            .object_store();

        let segment = LogSegment::try_new(&Path::default(), None, store.as_ref()).await?;
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

    async fn read_log_files(context: &IntegrationContext) -> TestResult {
        let store = context
            .table_builder(TestTables::SimpleWithCheckpoint)
            .build_storage()?
            .object_store();

        let log_path = Path::from("_delta_log");
        let cp = read_last_checkpoint(store.as_ref(), &log_path)
            .await?
            .unwrap();
        assert_eq!(cp.version, 10);

        let (log, check) = list_log_files_with_checkpoint(&cp, store.as_ref(), &log_path).await?;
        assert_eq!(log.len(), 0);
        assert_eq!(check.len(), 1);

        let (log, check) = list_log_files(store.as_ref(), &log_path, None, None).await?;
        assert_eq!(log.len(), 0);
        assert_eq!(check.len(), 1);

        let (log, check) = list_log_files(store.as_ref(), &log_path, Some(8), None).await?;
        assert_eq!(log.len(), 9);
        assert_eq!(check.len(), 0);

        let segment = LogSegment::try_new(&Path::default(), None, store.as_ref()).await?;
        assert_eq!(segment.version, 10);
        assert_eq!(segment.commit_files.len(), 0);
        assert_eq!(segment.checkpoint_files.len(), 1);

        let segment = LogSegment::try_new(&Path::default(), Some(8), store.as_ref()).await?;
        assert_eq!(segment.version, 8);
        assert_eq!(segment.commit_files.len(), 9);
        assert_eq!(segment.checkpoint_files.len(), 0);

        let store = context
            .table_builder(TestTables::Simple)
            .build_storage()?
            .object_store();

        let (log, check) = list_log_files(store.as_ref(), &log_path, None, None).await?;
        assert_eq!(log.len(), 5);
        assert_eq!(check.len(), 0);

        let (log, check) = list_log_files(store.as_ref(), &log_path, Some(2), None).await?;
        assert_eq!(log.len(), 3);
        assert_eq!(check.len(), 0);

        Ok(())
    }

    async fn read_metadata(context: &IntegrationContext) -> TestResult {
        let store = context
            .table_builder(TestTables::WithDvSmall)
            .build_storage()?
            .object_store();
        let segment = LogSegment::try_new(&Path::default(), None, store.as_ref()).await?;
        let (protocol, _metadata) = segment
            .read_metadata(store.clone(), &Default::default())
            .await?;
        let protocol = protocol.unwrap();

        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()].into_iter().collect()),
            writer_features: Some(vec!["deletionVectors".into()].into_iter().collect()),
        };
        assert_eq!(protocol, expected);

        Ok(())
    }

    pub(crate) async fn concurrent_checkpoint(context: &IntegrationContext) -> TestResult {
        context
            .load_table(TestTables::LatestNotCheckpointed)
            .await?;
        let table_to_checkpoint = context
            .table_builder(TestTables::LatestNotCheckpointed)
            .load()
            .await?;
        let store = context
            .table_builder(TestTables::LatestNotCheckpointed)
            .build_storage()?
            .object_store();
        let slow_list_store = Arc::new(slow_store::SlowListStore { store });

        let version = table_to_checkpoint.version();
        let load_task: JoinHandle<Result<LogSegment, DeltaTableError>> = tokio::spawn(async move {
            let segment =
                LogSegment::try_new(&Path::default(), Some(version), slow_list_store.as_ref())
                    .await?;
            Ok(segment)
        });

        create_checkpoint_from_table_uri_and_cleanup(
            &table_to_checkpoint.table_uri(),
            version,
            Some(false),
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

            fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
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

    #[test]
    pub fn is_commit_file_only_matches_commits() {
        for path in [0, 1, 5, 10, 100, i64::MAX]
            .into_iter()
            .map(crate::storage::commit_uri_from_version)
        {
            assert!(path.is_commit_file());
        }

        let not_commits = ["_delta_log/_commit_2132c4fe-4077-476c-b8f5-e77fea04f170.json.tmp"];

        for not_commit in not_commits {
            let path = Path::from(not_commit);
            assert!(!path.is_commit_file());
        }
    }

    #[tokio::test]
    async fn test_checkpoint_stream_parquet_read() {
        let metadata = Metadata {
            id: "test".to_string(),
            format: Format::new("parquet".to_string(), None),
            schema_string: r#"{"type":"struct",  "fields": []}"#.to_string(),
            ..Default::default()
        };
        let protocol = Protocol::default();

        let mut actions = vec![Action::Metadata(metadata), Action::Protocol(protocol)];
        for i in 0..10 {
            actions.push(Action::Add(Add {
                path: format!("part-{}.parquet", i),
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
                path: format!("part-{}.parquet", i),
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

        create_checkpoint_for(commit.version, &commit.snapshot, log_store.as_ref())
            .await
            .unwrap();

        let batches = LogSegment::try_new(
            &Path::default(),
            Some(commit.version),
            log_store.object_store().as_ref(),
        )
        .await
        .unwrap()
        .checkpoint_stream(
            log_store.object_store(),
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
