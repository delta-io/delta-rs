use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;
use std::task::{ready, Poll};

use arrow_array::{Array, Int32Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow_json::{reader::Decoder, ReaderBuilder};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use bytes::{Buf, Bytes};
use chrono::Utc;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, ObjectMeta, ObjectStore, Result as ObjectStoreResult,
};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::debug;

use crate::kernel::schema::Schema;
use crate::kernel::{Action, ActionType, Metadata, Protocol, StructType};
use crate::operations::transaction::get_commit_bytes;
use crate::protocol::DeltaOperation;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

use super::extract::{extract_and_cast, extract_and_cast_opt, read_primitive};
use super::parse::read_metadata;

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";
const BATCH_SIZE: usize = 1024;

pub type CommitData = (Vec<Action>, DeltaOperation, Option<HashMap<String, Value>>);

lazy_static! {
    static ref CHECKPOINT_FILE_PATTERN: Regex =
        Regex::new(r"\d+\.checkpoint(\.\d+\.\d+)?\.parquet").unwrap();
    static ref DELTA_FILE_PATTERN: Regex = Regex::new(r"\d+\.json").unwrap();
    pub(super) static ref COMMIT_SCHEMA: StructType = StructType::new(vec![
        ActionType::Add.schema_field().clone(),
        ActionType::Remove.schema_field().clone(),
    ]);
    pub(super) static ref CHECKPOINT_SCHEMA: StructType =
        StructType::new(vec![ActionType::Add.schema_field().clone(),]);
    pub(super) static ref TOMBSTONE_SCHEMA: StructType =
        StructType::new(vec![ActionType::Remove.schema_field().clone(),]);
}

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

#[inline]
fn get_decoder(schema: ArrowSchemaRef, config: &DeltaTableConfig) -> DeltaResult<Decoder> {
    Ok(ReaderBuilder::new(schema)
        .with_batch_size(config.log_batch_size)
        .build_decoder()?)
}

#[inline]
fn get_reader(data: &[u8]) -> BufReader<Cursor<&[u8]>> {
    BufReader::new(Cursor::new(data))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileInfo {
    location: String,
    size: usize,
    last_modified: chrono::DateTime<Utc>,
    e_tag: Option<String>,
    version: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct LogSegment {
    pub(super) version: i64,
    pub(super) commit_files: VecDeque<ObjectMeta>,
    pub(super) checkpoint_files: Vec<ObjectMeta>,
}

impl LogSegment {
    /// Try to create a new [`LogSegment`]
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

    pub async fn try_new_slice(
        table_root: &Path,
        start_version: i64,
        end_version: Option<i64>,
        store: &dyn ObjectStore,
    ) -> DeltaResult<Self> {
        debug!(
            "try_new_slice: start_version: {}, end_version: {:?}",
            start_version, end_version
        );
        let log_url = table_root.child("_delta_log");
        let (mut commit_files, checkpoint_files) =
            list_log_files(store, &log_url, end_version, Some(start_version)).await?;
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
                crate::kernel::actions::schemas::log_schema(),
                &Default::default(),
            )?
            .collect_vec();
        Ok((log, iter))
    }

    pub fn version(&self) -> i64 {
        self.version
    }

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
        let decoder = get_decoder(Arc::new(read_schema.try_into()?), config)?;
        let stream = futures::stream::iter(self.commit_files.iter())
            .map(move |meta| {
                let store = store.clone();
                async move { store.get(&meta.location).await?.bytes().await }
            })
            .buffered(config.log_buffer_size);
        Ok(decode_stream(decoder, stream).boxed())
    }

    pub(super) fn checkpoint_stream(
        &self,
        store: Arc<dyn ObjectStore>,
        _read_schema: &Schema,
        config: &DeltaTableConfig,
    ) -> BoxStream<'_, DeltaResult<RecordBatch>> {
        futures::stream::iter(self.checkpoint_files.clone())
            .map(move |meta| {
                let store = store.clone();
                async move {
                    let reader = ParquetObjectReader::new(store, meta);
                    let options = ArrowReaderOptions::new(); //.with_page_index(enable_page_index);
                    let builder =
                        ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;
                    builder.with_batch_size(BATCH_SIZE).build()
                }
            })
            .buffered(config.log_buffer_size)
            .try_flatten()
            .map_err(Into::into)
            .boxed()
    }

    /// Read Protocol and Metadata actions
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
                if let Some(p) = read_protocol(&batch)? {
                    maybe_protocol.replace(p);
                };
            }
            if maybe_metadata.is_none() {
                if let Some(m) = read_metadata(&batch)? {
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
                if let Some(p) = read_protocol(&batch)? {
                    maybe_protocol.replace(p);
                };
            }
            if maybe_metadata.is_none() {
                if let Some(m) = read_metadata(&batch)? {
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
        let mut decoder = get_decoder(Arc::new(read_schema.try_into()?), config)?;

        let mut commit_data = Vec::new();
        for (actions, operation, app_metadata) in commits {
            self.version += 1;
            let path = log_path.child(format!("{:020}.json", self.version));
            let bytes = get_commit_bytes(operation, actions, app_metadata.clone())?;
            let meta = ObjectMeta {
                location: path,
                size: bytes.len(),
                last_modified: Utc::now(),
                e_tag: None,
                version: None,
            };
            // NOTE: We always assume the commit files are sorted in reverse order
            self.commit_files.push_front(meta);
            let reader = get_reader(&bytes);
            let batches = read_from_json(reader, &mut decoder).collect::<Result<Vec<_>, _>>()?;
            commit_data.push(batches);
        }

        // NOTE: Most recent commits need to be processed first
        commit_data.reverse();
        Ok(commit_data.into_iter().flatten().map(Ok))
    }
}

pub(super) fn read_protocol(batch: &RecordBatch) -> DeltaResult<Option<Protocol>> {
    if let Some(arr) = extract_and_cast_opt::<StructArray>(batch, "protocol") {
        let min_reader_version = extract_and_cast::<Int32Array>(arr, "minReaderVersion")?;
        let min_writer_version = extract_and_cast::<Int32Array>(arr, "minWriterVersion")?;
        let maybe_reader_features = extract_and_cast_opt::<ListArray>(arr, "readerFeatures");
        let maybe_writer_features = extract_and_cast_opt::<ListArray>(arr, "writerFeatures");

        for idx in 0..arr.len() {
            if arr.is_valid(idx) {
                let reader_features = maybe_reader_features.map(|arr| {
                    let value = arr.value(idx);
                    let val = value.as_any().downcast_ref::<StringArray>();
                    match val {
                        Some(val) => val
                            .iter()
                            .filter_map(|s| s.map(|i| i.into()))
                            .collect::<std::collections::HashSet<_>>(),
                        None => std::collections::HashSet::new(),
                    }
                });

                let writer_features = maybe_writer_features.map(|arr| {
                    let value = arr.value(idx);
                    let val = value.as_any().downcast_ref::<StringArray>();
                    match val {
                        Some(val) => val
                            .iter()
                            .filter_map(|s| s.map(|i| i.into()))
                            .collect::<std::collections::HashSet<_>>(),
                        None => std::collections::HashSet::new(),
                    }
                });

                return Ok(Some(Protocol {
                    min_reader_version: read_primitive(min_reader_version, idx)?,
                    min_writer_version: read_primitive(min_writer_version, idx)?,
                    reader_features,
                    writer_features,
                }));
            }
        }
    }
    Ok(None)
}

fn decode_stream<S: Stream<Item = ObjectStoreResult<Bytes>> + Unpin>(
    mut decoder: Decoder,
    mut input: S,
) -> impl Stream<Item = Result<RecordBatch, DeltaTableError>> {
    let mut buffered = Bytes::new();
    futures::stream::poll_fn(move |cx| {
        loop {
            if buffered.is_empty() {
                buffered = match ready!(input.poll_next_unpin(cx)) {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                    None => break,
                };
            }
            let decoded = match decoder.decode(buffered.as_ref()) {
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            };
            let read = buffered.len();
            buffered.advance(decoded);
            if decoded != read {
                break;
            }
        }

        Poll::Ready(decoder.flush().map_err(DeltaTableError::from).transpose())
    })
}

fn read_from_json<'a, R: BufRead + 'a>(
    mut reader: R,
    decoder: &'a mut Decoder,
) -> impl Iterator<Item = Result<RecordBatch, DeltaTableError>> + '_ {
    let mut next = move || {
        loop {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break; // Input exhausted
            }
            let read = buf.len();
            let decoded = decoder.decode(buf)?;

            reader.consume(decoded);
            if decoded != read {
                break; // Read batch size
            }
        }
        decoder.flush()
    };
    std::iter::from_fn(move || next().map_err(DeltaTableError::from).transpose())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointMetadata {
    /// The version of the table when the last checkpoint was made.
    #[allow(unreachable_pub)] // used by acceptance tests (TODO make an fn accessor?)
    pub version: i64,
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i32,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub(crate) parts: Option<i32>,
    /// The number of bytes of the checkpoint.
    pub(crate) size_in_bytes: Option<i32>,
    /// The number of AddFile actions in the checkpoint.
    pub(crate) num_of_add_files: Option<i32>,
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
            if f.location.is_checkpoint_file() {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();

    // TODO raise a proper error
    assert_eq!(checkpoint_files.len(), cp.parts.unwrap_or(1) as usize);

    Ok((commit_files, checkpoint_files))
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
            .table_builder(TestTables::Checkpoints)
            .build_storage()?
            .object_store();

        let log_path = Path::from("_delta_log");
        let cp = read_last_checkpoint(store.as_ref(), &log_path)
            .await?
            .unwrap();
        assert_eq!(cp.version, 10);

        let (log, check) = list_log_files_with_checkpoint(&cp, store.as_ref(), &log_path).await?;
        assert_eq!(log.len(), 2);
        assert_eq!(check.len(), 1);

        let (log, check) = list_log_files(store.as_ref(), &log_path, None, None).await?;
        assert_eq!(log.len(), 2);
        assert_eq!(check.len(), 1);

        let (log, check) = list_log_files(store.as_ref(), &log_path, Some(8), None).await?;
        assert_eq!(log.len(), 3);
        assert_eq!(check.len(), 1);

        let segment = LogSegment::try_new(&Path::default(), None, store.as_ref()).await?;
        assert_eq!(segment.version, 12);
        assert_eq!(segment.commit_files.len(), 2);
        assert_eq!(segment.checkpoint_files.len(), 1);

        let segment = LogSegment::try_new(&Path::default(), Some(8), store.as_ref()).await?;
        assert_eq!(segment.version, 8);
        assert_eq!(segment.commit_files.len(), 3);
        assert_eq!(segment.checkpoint_files.len(), 1);

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
}
