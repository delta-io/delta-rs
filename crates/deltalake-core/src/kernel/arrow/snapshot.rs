#![allow(dead_code)]
//! Snapshot of a Delta table.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::reader::{Decoder, ReaderBuilder};
use arrow_schema::ArrowError;
use arrow_select::concat::concat_batches;
use bytes::{Buf, Bytes};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
// use parquet::arrow::async_reader::ParquetObjectReader;
// use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use serde::{Deserialize, Serialize};
use std::task::{ready, Poll};

use super::checkpoint::{parse_action, parse_actions};
use super::schemas::get_log_schema;
use crate::kernel::error::{DeltaResult, Error};
use crate::kernel::snapshot::Snapshot;
use crate::kernel::{Action, ActionType, Add, Metadata, Protocol, StructType};
use crate::storage::path::{commit_version, is_checkpoint_file, is_commit_file, FileMeta, LogPath};
use crate::table::config::TableConfig;

/// A [`Snapshot`] that is dynamically typed.
pub type DynSnapshot = dyn Snapshot;

#[derive(Debug)]
/// A [`Snapshot`] that is backed by an Arrow [`RecordBatch`].
pub struct TableStateArrow {
    version: i64,
    actions: RecordBatch,
    metadata: Metadata,
    schema: StructType,
    protocol: Protocol,
}

impl TableStateArrow {
    /// Create a new [`Snapshot`] from a [`RecordBatch`].
    pub fn try_new(version: i64, actions: RecordBatch) -> DeltaResult<Self> {
        let metadata = parse_action(&actions, &ActionType::Metadata)?
            .next()
            .and_then(|a| match a {
                Action::Metadata(m) => Some(m),
                _ => None,
            })
            .ok_or(Error::Generic("expected metadata".into()))?;
        let protocol = parse_action(&actions, &ActionType::Protocol)?
            .next()
            .and_then(|a| match a {
                Action::Protocol(p) => Some(p),
                _ => None,
            })
            .ok_or(Error::Generic("expected protocol".into()))?;
        let schema = serde_json::from_str(&metadata.schema_string)?;
        Ok(Self {
            version,
            actions,
            metadata,
            protocol,
            schema,
        })
    }

    /// Load a [`Snapshot`] from a given [`LogPath`].
    pub async fn load(
        table_root: LogPath,
        object_store: Arc<dyn ObjectStore>,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let (log_segment, version) =
            LogSegment::create(&table_root, object_store.as_ref(), version).await?;
        Self::try_new(version, log_segment.load(object_store, None).await?)
    }
}

impl std::fmt::Display for TableStateArrow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.actions.schema())
    }
}

impl Snapshot for TableStateArrow {
    fn version(&self) -> i64 {
        self.version
    }

    /// Table [`Metadata`] at this [`Snapshot`]'s version.
    fn metadata(&self) -> DeltaResult<Metadata> {
        Ok(self.metadata.clone())
    }

    /// Table [`Schema`](crate::kernel::schema::StructType) at this [`Snapshot`]'s version.
    fn schema(&self) -> Option<&StructType> {
        Some(&self.schema)
    }

    /// Table [`Protocol`] at this [`Snapshot`]'s version.
    fn protocol(&self) -> DeltaResult<Protocol> {
        Ok(self.protocol.clone())
    }

    fn files(&self) -> DeltaResult<Box<dyn Iterator<Item = Add> + '_>> {
        Ok(Box::new(
            parse_actions(&self.actions, &[ActionType::Add])?.filter_map(|it| match it {
                Action::Add(add) => Some(add),
                _ => None,
            }),
        ))
    }

    /// Well known table [configuration](crate::table::config::TableConfig).
    fn table_config(&self) -> TableConfig<'_> {
        TableConfig(&self.metadata.configuration)
    }
}

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";
const LOG_FOLDER_NAME: &str = "_delta_log";
// const SIDECAR_FOLDER_NAME: &str = "_sidecars";
// const CDC_FOLDER_NAME: &str = "_change_data";

#[derive(Debug)]
pub(crate) struct LogSegment {
    log_root: LogPath,
    /// Reverse order sorted commit files in the log segment
    pub(crate) commit_files: Vec<FileMeta>,
    /// checkpoint files in the log segment.
    pub(crate) checkpoint_files: Vec<FileMeta>,
}

impl LogSegment {
    pub(crate) async fn create(
        table_root: &LogPath,
        object_store: &dyn ObjectStore,
        version: Option<i64>,
    ) -> DeltaResult<(Self, i64)> {
        let log_url = table_root.child(LOG_FOLDER_NAME).unwrap();
        let log_path = match log_url {
            LogPath::ObjectStore(log_path) => log_path,
            LogPath::Url(_) => return Err(Error::Generic("Url handling not yet supported".into())),
        };

        // List relevant files from log
        let (mut commit_files, checkpoint_files) = match (
            read_last_checkpoint(object_store, &log_path).await?,
            version,
        ) {
            (Some(cp), None) => {
                list_log_files_with_checkpoint(&cp, object_store, &log_path).await?
            }
            (Some(cp), Some(version)) if cp.version >= version => {
                list_log_files_with_checkpoint(&cp, object_store, &log_path).await?
            }
            _ => list_log_files(object_store, &log_path, version).await?,
        };

        // remove all files above requested version
        if let Some(version) = version {
            commit_files.retain(|meta| {
                if let Some(v) = meta.location.commit_version() {
                    v <= version
                } else {
                    false
                }
            });
        }

        // get the effective version from chosen files
        let version_eff = commit_files
            .first()
            .or(checkpoint_files.first())
            .and_then(|f| f.location.commit_version())
            .ok_or(Error::MissingVersion)?; // TODO: A more descriptive error

        if let Some(v) = version {
            if version_eff != v {
                // TODO more descriptive error
                return Err(Error::MissingVersion);
            }
        }

        Ok((
            Self {
                log_root: LogPath::ObjectStore(log_path),
                commit_files,
                checkpoint_files,
            },
            version_eff,
        ))
    }

    pub(crate) fn commit_files(&self) -> impl Iterator<Item = &FileMeta> {
        self.commit_files.iter()
    }

    pub async fn load(
        &self,
        object_store: Arc<dyn ObjectStore>,
        buffer_size: Option<usize>,
    ) -> DeltaResult<RecordBatch> {
        let buffer_size = buffer_size.unwrap_or_else(num_cpus::get);
        let byte_stream = futures::stream::iter(self.commit_files())
            .map(|file| {
                let store = object_store.clone();
                async move {
                    let path = match file.location {
                        LogPath::ObjectStore(ref path) => path.clone(),
                        LogPath::Url(_) => {
                            return Err(Error::Generic("Url handling not yet supported".into()))
                        }
                    };
                    let data = store
                        .get(&path)
                        .await
                        .map_err(Error::from)?
                        .bytes()
                        .await
                        .map_err(Error::from)?;

                    Ok(data)
                }
            })
            .buffered(buffer_size);

        let log_schema = Arc::new(get_log_schema());
        let decoder = ReaderBuilder::new(log_schema.clone()).build_decoder()?;

        let commit_batch = decode_commit_file_stream(decoder, byte_stream)?
            .try_collect::<Vec<_>>()
            .await?;
        let batch = concat_batches(&log_schema, &commit_batch)?;

        // let checkpoint_stream = ParquetRecordBatchStreamBuilder::new(input);

        Ok(batch)
    }

    // Read a stream of log data from this log segment.
    //
    // The log files will be read from most recent to oldest.
    //
    // `read_schema` is the schema to read the log files with. This can be used
    // to project the log files to a subset of the columns.
    //
    // `predicate` is an optional expression to filter the log files with.
    // pub fn replay(
    //     &self,
    //     table_client: &dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>,
    //     read_schema: Arc<ArrowSchema>,
    //     predicate: Option<Expression>,
    // ) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
    //     let mut commit_files: Vec<_> = self.commit_files().cloned().collect();
    //
    //     // NOTE this will already sort in reverse order
    //     commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
    //     let json_client = table_client.get_json_handler();
    //     let read_contexts =
    //         json_client.contextualize_file_reads(commit_files, predicate.clone())?;
    //     let commit_stream = json_client
    //         .read_json_files(read_contexts, Arc::new(read_schema.as_ref().try_into()?))?;
    //
    //     let parquet_client = table_client.get_parquet_handler();
    //     let read_contexts =
    //         parquet_client.contextualize_file_reads(self.checkpoint_files.clone(), predicate)?;
    //     let checkpoint_stream = parquet_client
    //         .read_parquet_files(read_contexts, Arc::new(read_schema.as_ref().try_into()?))?;
    //
    //     let batches = commit_stream.chain(checkpoint_stream);
    //
    //     Ok(batches)
    // }
}

fn decode_commit_file_stream<S: Stream<Item = DeltaResult<Bytes>> + Unpin>(
    mut decoder: Decoder,
    mut input: S,
) -> DeltaResult<impl Stream<Item = Result<RecordBatch, ArrowError>>> {
    let mut buffered = Bytes::new();
    Ok(futures::stream::poll_fn(move |cx| {
        loop {
            if buffered.is_empty() {
                buffered = match ready!(input.poll_next_unpin(cx)) {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => {
                        return Poll::Ready(Some(Err(ArrowError::ExternalError(Box::new(e)))))
                    }
                    None => break,
                };
            }
            let decoded = match decoder.decode(buffered.as_ref()) {
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            let read = buffered.len();
            buffered.advance(decoded);
            if decoded != read {
                break;
            }
        }
        Poll::Ready(decoder.flush().transpose())
    }))
}

/// The last checkpoint file.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCheckpoint {
    /// The version of the table when the last checkpoint was made.
    pub version: i64,
    /// The number of actions that are stored in the checkpoint.
    pub size: i32,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub parts: Option<i32>,
    /// The number of bytes of the checkpoint.
    pub size_in_bytes: Option<i32>,
    /// The number of AddFile actions in the checkpoint.
    pub num_of_add_files: Option<i32>,
    /// The schema of the checkpoint file.
    pub checkpoint_schema: Option<StructType>,
    /// The checksum of the last checkpoint JSON.
    pub checksum: Option<String>,
}

/// Try reading the `_last_checkpoint` file.
///
/// In case the file is not found, `None` is returned.
async fn read_last_checkpoint(
    object_store: &dyn ObjectStore,
    log_root: &Path,
) -> DeltaResult<Option<LastCheckpoint>> {
    let file_path = log_root.child(LAST_CHECKPOINT_FILE_NAME);
    match object_store.get(&file_path).await {
        Ok(data) => Ok(Some(serde_json::from_slice(&data.bytes().await?)?)),
        Err(ObjectStoreError::NotFound { .. }) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// List all log files after a given checkpoint.
async fn list_log_files_with_checkpoint(
    cp: &LastCheckpoint,
    fs_client: &dyn ObjectStore,
    log_root: &Path,
) -> DeltaResult<(Vec<FileMeta>, Vec<FileMeta>)> {
    let version_prefix = format!("{:020}", cp.version);
    let start_from = log_root.child(version_prefix);

    let files = fs_client
        .list_with_offset(Some(log_root), &start_from)
        .await?
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|m| commit_version(m.location.filename().unwrap_or_default()).map(|_| m))
        .collect::<Vec<_>>();

    let mut commit_files = files
        .iter()
        .filter_map(|f| {
            if is_commit_file(f.location.filename().unwrap_or_default()) {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
    let commit_files = commit_files
        .into_iter()
        .map(|f| FileMeta {
            location: LogPath::ObjectStore(f.location),
            size: f.size,
            last_modified: f.last_modified.timestamp(),
        })
        .collect::<Vec<_>>();

    let checkpoint_files = files
        .iter()
        .filter_map(|f| {
            if is_checkpoint_file(f.location.filename().unwrap_or_default()) {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // TODO raise a proper error
    assert_eq!(checkpoint_files.len(), cp.parts.unwrap_or(1) as usize);
    let checkpoint_files = checkpoint_files
        .into_iter()
        .map(|f| FileMeta {
            location: LogPath::ObjectStore(f.location),
            size: f.size,
            last_modified: f.last_modified.timestamp(),
        })
        .collect::<Vec<_>>();

    Ok((commit_files, checkpoint_files))
}

/// List relevant log files.
///
/// Relevant files are the max checkpoint found and all subsequent commits.
async fn list_log_files(
    fs_client: &dyn ObjectStore,
    log_root: &Path,
    max_version: Option<i64>,
) -> DeltaResult<(Vec<FileMeta>, Vec<FileMeta>)> {
    let start_from = Path::from(format!("{:020}", 0));

    let max_version = max_version.unwrap_or(i64::MAX);
    let mut max_checkpoint_version = -1_i64;
    let mut commit_files = Vec::new();
    let mut checkpoint_files = Vec::with_capacity(10);
    let mut files = fs_client
        .list_with_offset(Some(log_root), &start_from)
        .await?;

    while let Some(maybe_meta) = files.next().await {
        let meta = maybe_meta?;
        let filename = meta.location.filename().unwrap_or_default();
        let version = commit_version(filename).unwrap_or(0) as i64;
        if version <= max_version {
            if is_commit_file(filename) {
                commit_files.push(meta);
            } else if is_checkpoint_file(filename) {
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
            }
        }
    }

    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
    let commit_files = commit_files
        .into_iter()
        .map(|f| FileMeta {
            location: LogPath::ObjectStore(f.location),
            size: f.size,
            last_modified: f.last_modified.timestamp(),
        })
        .collect::<Vec<_>>();

    let commit_files = commit_files
        .into_iter()
        .filter(|f| f.location.commit_version().unwrap_or(0) > max_checkpoint_version)
        .collect::<Vec<_>>();

    let checkpoint_files = checkpoint_files
        .into_iter()
        .map(|f| FileMeta {
            location: LogPath::ObjectStore(f.location),
            size: f.size,
            last_modified: f.last_modified.timestamp(),
        })
        .collect::<Vec<_>>();

    Ok((commit_files, checkpoint_files))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use super::*;
    use crate::kernel::schema::StructType;

    #[tokio::test]
    async fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = Path::from_filesystem_path(path).unwrap();
        let store = Arc::new(LocalFileSystem::new());

        let snapshot = TableStateArrow::load(LogPath::ObjectStore(url), store.clone(), Some(1))
            .await
            .unwrap();

        let protocol = snapshot.protocol().unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()].into_iter().collect()),
            writer_features: Some(vec!["deletionVectors".into()].into_iter().collect()),
        };
        assert_eq!(protocol, expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string).unwrap();
        let schema = snapshot.schema().unwrap();
        assert_eq!(schema, &expected);
    }

    #[tokio::test]
    async fn test_read_table_with_last_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let path = Path::from_filesystem_path(path).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let cp = read_last_checkpoint(store.as_ref(), &path).await.unwrap();
        assert!(cp.is_none())
    }

    #[tokio::test]
    async fn test_read_table_with_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))
        .unwrap();
        let location = Path::from_filesystem_path(path).unwrap();
        let store = Arc::new(LocalFileSystem::new());

        let (log_segment, version) = LogSegment::create(
            &LogPath::ObjectStore(location.clone()),
            store.as_ref(),
            Some(3),
        )
        .await
        .unwrap();

        assert_eq!(version, 3);
        assert_eq!(log_segment.checkpoint_files.len(), 1);
        assert_eq!(
            log_segment.checkpoint_files[0].location.commit_version(),
            Some(2)
        );
        assert_eq!(log_segment.commit_files.len(), 1);
        assert_eq!(
            log_segment.commit_files[0].location.commit_version(),
            Some(3)
        );

        let (log_segment, version) =
            LogSegment::create(&LogPath::ObjectStore(location), store.as_ref(), Some(1))
                .await
                .unwrap();

        assert_eq!(version, 1);
        assert_eq!(log_segment.checkpoint_files.len(), 0);
        assert_eq!(log_segment.commit_files.len(), 2);
        assert_eq!(
            log_segment.commit_files[0].location.commit_version(),
            Some(1)
        );
    }
}
