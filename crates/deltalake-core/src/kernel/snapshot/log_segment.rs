use std::cmp::Ordering;
use std::sync::Arc;
use std::task::{ready, Poll};

use arrow_array::RecordBatch;
use arrow_json::{reader::Decoder, ReaderBuilder};
use bytes::{Buf, Bytes};
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, ObjectMeta, ObjectStore, Result as ObjectStoreResult,
};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::kernel::schema::Schema;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

lazy_static! {
    static ref CHECKPOINT_FILE_PATTERN: Regex =
        Regex::new(r"\d+\.checkpoint(\.\d+\.\d+)?\.parquet").unwrap();
    static ref DELTA_FILE_PATTERN: Regex = Regex::new(r"\d+\.json").unwrap();
}

trait PathExt {
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

pub(crate) struct LogSegment {
    pub version: i64,
    pub log_root: Path,
    pub commit_files: Vec<ObjectMeta>,
    pub checkpoint_files: Vec<ObjectMeta>,
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
            _ => list_log_files(store, &log_url, version).await?,
        };

        // remove all files above requested version
        if let Some(version) = version {
            commit_files.retain(|meta| meta.location.commit_version() <= Some(version));
        }

        // get the effective version from chosen files
        let version_eff = commit_files
            .first()
            .or(checkpoint_files.first())
            .and_then(|f| f.location.commit_version())
            .ok_or(DeltaTableError::Generic(
                "failed to get effective version".into(),
            ))?; // TODO: A more descriptive error

        if let Some(v) = version {
            if version_eff != v {
                // TODO more descriptive error
                return Err(DeltaTableError::Generic("missing version".into()));
            }
        }

        Ok(Self {
            version: version_eff,
            log_root: log_url,
            commit_files,
            checkpoint_files,
        })
    }

    pub(super) fn commit_stream(
        &self,
        store: Arc<dyn ObjectStore>,
        read_schema: &Schema,
        config: &DeltaTableConfig,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<RecordBatch>>> {
        let decoder = ReaderBuilder::new(Arc::new(read_schema.try_into()?))
            .with_batch_size(1024)
            .build_decoder()?;

        let stream = futures::stream::iter(self.commit_files.iter())
            .map(move |meta| {
                let store = store.clone();
                async move { store.get(&meta.location).await?.bytes().await }
            })
            .buffered(config.log_buffer_size);

        Ok(decode_stream(decoder, stream).boxed())
    }
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
async fn list_log_files(
    fs_client: &dyn ObjectStore,
    log_root: &Path,
    max_version: Option<i64>,
) -> DeltaResult<(Vec<ObjectMeta>, Vec<ObjectMeta>)> {
    let max_version = max_version.unwrap_or(i64::MAX);
    let start_from = log_root.child(format!("{:020}", 0).as_str());

    let mut max_checkpoint_version = -1_i64;
    let mut commit_files = Vec::with_capacity(25);
    let mut checkpoint_files = Vec::with_capacity(10);

    for meta in fs_client
        .list_with_offset(Some(log_root), &start_from)
        .try_collect::<Vec<_>>()
        .await?
    {
        if meta.location.commit_version() <= Some(max_version) {
            if meta.location.is_checkpoint_file() {
                let version = meta.location.commit_version().unwrap_or(0) as i64;
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

    commit_files
        .retain(|f| f.location.commit_version().unwrap_or(0) as i64 > max_checkpoint_version);
    // NOTE this will sort in reverse order
    commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));

    Ok((commit_files, checkpoint_files))
}

#[cfg(test)]
mod tests {
    use deltalake_test::utils::*;

    use super::*;

    #[tokio::test]
    async fn test_read_log_files() -> TestResult {
        let context = IntegrationContext::new(Box::new(LocalStorageIntegration::default()))?;
        context.load_table(TestTables::Checkpoints).await?;
        context.load_table(TestTables::Simple).await?;

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

        let (log, check) = list_log_files(store.as_ref(), &log_path, None).await?;
        assert_eq!(log.len(), 2);
        assert_eq!(check.len(), 1);

        let (log, check) = list_log_files(store.as_ref(), &log_path, Some(8)).await?;
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

        let (log, check) = list_log_files(store.as_ref(), &log_path, None).await?;
        assert_eq!(log.len(), 5);
        assert_eq!(check.len(), 0);

        let (log, check) = list_log_files(store.as_ref(), &log_path, Some(2)).await?;
        assert_eq!(log.len(), 3);
        assert_eq!(check.len(), 0);

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     use std::collections::HashMap;
//     use std::path::PathBuf;
//
//     use object_store::local::LocalFileSystem;
//     use object_store::path::Path;
//
//     use crate::kernel::executor::tokio::TokioBackgroundExecutor;
//     use crate::kernel::filesystem::ObjectStoreFileSystemClient;
//     use crate::kernel::schema::StructType;
//
//     #[test]
//     fn test_snapshot_read_metadata() {
//         let path =
//             std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
//         let url = url::Url::from_directory_path(path).unwrap();
//
//         let client = default_table_client(&url);
//         let snapshot = Snapshot::try_new(url, &client, Some(1)).unwrap();
//
//         let expected = Protocol {
//             min_reader_version: 3,
//             min_writer_version: 7,
//             reader_features: Some(vec!["deletionVectors".into()]),
//             writer_features: Some(vec!["deletionVectors".into()]),
//         };
//         assert_eq!(snapshot.protocol(), &expected);
//
//         let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
//         let expected: StructType = serde_json::from_str(schema_string).unwrap();
//         assert_eq!(snapshot.schema(), &expected);
//     }
//
//     #[test]
//     fn test_new_snapshot() {
//         let path =
//             std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
//         let url = url::Url::from_directory_path(path).unwrap();
//
//         let client = default_table_client(&url);
//         let snapshot = Snapshot::try_new(url, &client, None).unwrap();
//
//         let expected = Protocol {
//             min_reader_version: 3,
//             min_writer_version: 7,
//             reader_features: Some(vec!["deletionVectors".into()]),
//             writer_features: Some(vec!["deletionVectors".into()]),
//         };
//         assert_eq!(snapshot.protocol(), &expected);
//
//         let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
//         let expected: StructType = serde_json::from_str(schema_string).unwrap();
//         assert_eq!(snapshot.schema(), &expected);
//     }
//
//     #[test]
//     fn test_read_table_with_last_checkpoint() {
//         let path = std::fs::canonicalize(PathBuf::from(
//             "./tests/data/table-with-dv-small/_delta_log/",
//         ))
//         .unwrap();
//         let url = url::Url::from_directory_path(path).unwrap();
//
//         let store = Arc::new(LocalFileSystem::new());
//         let prefix = Path::from(url.path());
//         let client = ObjectStoreFileSystemClient::new(
//             store,
//             prefix,
//             Arc::new(TokioBackgroundExecutor::new()),
//         );
//         let cp = read_last_checkpoint(&client, &url).unwrap();
//         assert!(cp.is_none())
//     }
//
//     #[test]
//     fn test_read_table_with_checkpoint() {
//         let path = std::fs::canonicalize(PathBuf::from(
//             "./tests/data/with_checkpoint_no_last_checkpoint/",
//         ))
//         .unwrap();
//         let location = url::Url::from_directory_path(path).unwrap();
//         let table_client = default_table_client(&location);
//         let snapshot = Snapshot::try_new(location, &table_client, None).unwrap();
//
//         assert_eq!(snapshot.log_segment.checkpoint_files.len(), 1);
//         assert_eq!(
//             LogPath(&snapshot.log_segment.checkpoint_files[0].location).commit_version(),
//             Some(2)
//         );
//         assert_eq!(snapshot.log_segment.commit_files.len(), 1);
//         assert_eq!(
//             LogPath(&snapshot.log_segment.commit_files[0].location).commit_version(),
//             Some(3)
//         );
//     }
// }
