use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use futures::stream::BoxStream;
use object_store::ObjectMeta;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, path::Path,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use url::Url;

use crate::logstore::default_logstore::DefaultLogStore;
use crate::logstore::{LogStoreRef, ObjectStoreRef};

#[derive(Debug, PartialEq)]
pub(crate) enum RecordedObjectStoreOperation {
    Get(RecordedPathKind),
    GetOpts(RecordedPathKind),
    GetRange(RecordedPathKind, Range<u64>),
    GetRanges(RecordedPathKind, Vec<Range<u64>>),
    Head(RecordedPathKind),
    List(RecordedPathKind),
    ListWithOffset(RecordedPathKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordedPathKind {
    Commit,
    Checkpoint,
    Data,
    Other,
}

impl RecordedObjectStoreOperation {
    pub(crate) fn is_log_replay_read(&self) -> bool {
        matches!(
            self,
            Self::Get(RecordedPathKind::Commit | RecordedPathKind::Checkpoint)
                | Self::GetOpts(RecordedPathKind::Commit | RecordedPathKind::Checkpoint)
                | Self::GetRange(RecordedPathKind::Commit | RecordedPathKind::Checkpoint, _)
                | Self::GetRanges(RecordedPathKind::Commit | RecordedPathKind::Checkpoint, _)
                | Self::Head(RecordedPathKind::Commit | RecordedPathKind::Checkpoint)
                | Self::List(RecordedPathKind::Commit | RecordedPathKind::Checkpoint)
                | Self::ListWithOffset(RecordedPathKind::Commit | RecordedPathKind::Checkpoint)
        )
    }
}

pub(crate) struct RecordingObjectStore {
    inner: ObjectStoreRef,
    operations: UnboundedSender<RecordedObjectStoreOperation>,
}

impl RecordingObjectStore {
    pub(crate) fn with_operations(
        inner: ObjectStoreRef,
        operations: UnboundedSender<RecordedObjectStoreOperation>,
    ) -> Self {
        Self { inner, operations }
    }
}

impl Display for RecordingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Debug for RecordingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}

pub(crate) async fn drain_recorded_object_store_operations(
    operations: &mut UnboundedReceiver<RecordedObjectStoreOperation>,
) -> Vec<RecordedObjectStoreOperation> {
    tokio::task::yield_now().await;
    let mut recorded = Vec::new();
    while let Ok(op) = operations.try_recv() {
        recorded.push(op);
    }
    recorded
}

pub(crate) fn recording_log_store(
    base: LogStoreRef,
) -> (LogStoreRef, UnboundedReceiver<RecordedObjectStoreOperation>) {
    let (operations_sender, operations) = unbounded_channel();
    let wrapped_prefixed_object_store = Arc::new(RecordingObjectStore::with_operations(
        base.object_store(None),
        operations_sender.clone(),
    ));
    let wrapped_root_object_store = Arc::new(RecordingObjectStore::with_operations(
        base.root_object_store(None),
        operations_sender,
    ));
    let log_store = Arc::new(DefaultLogStore::new(
        wrapped_prefixed_object_store,
        wrapped_root_object_store,
        base.config().clone(),
    ));
    (log_store, operations)
}

fn classify_path(path: &Path) -> RecordedPathKind {
    let dummy_url = Url::parse("dummy:///").unwrap();
    if let Some(parsed) = dummy_url
        .join(path.as_ref())
        .ok()
        .and_then(|url| ParsedLogPath::try_from(url).ok())
        .flatten()
    {
        return match parsed.file_type {
            LogPathFileType::Commit
            | LogPathFileType::StagedCommit
            | LogPathFileType::CompactedCommit { .. }
            | LogPathFileType::Crc
            | LogPathFileType::Unknown => RecordedPathKind::Commit,
            LogPathFileType::SinglePartCheckpoint
            | LogPathFileType::UuidCheckpoint
            | LogPathFileType::MultiPartCheckpoint { .. } => RecordedPathKind::Checkpoint,
        };
    }

    let path_str = path.as_ref();
    if path_str == "_delta_log" || path_str.starts_with("_delta_log/") {
        RecordedPathKind::Commit
    } else if path_str.starts_with("part-") {
        RecordedPathKind::Data
    } else {
        RecordedPathKind::Other
    }
}

fn classify_optional_path(path: Option<&Path>) -> RecordedPathKind {
    path.map(classify_path).unwrap_or(RecordedPathKind::Other)
}

#[async_trait::async_trait]
impl ObjectStore for RecordingObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.operations
            .send(RecordedObjectStoreOperation::Get(classify_path(location)))
            .unwrap();
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.operations
            .send(RecordedObjectStoreOperation::GetOpts(classify_path(
                location,
            )))
            .unwrap();
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        self.operations
            .send(RecordedObjectStoreOperation::GetRange(
                classify_path(location),
                range.clone(),
            ))
            .unwrap();
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.operations
            .send(RecordedObjectStoreOperation::GetRanges(
                classify_path(location),
                ranges.to_vec(),
            ))
            .unwrap();
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.operations
            .send(RecordedObjectStoreOperation::Head(classify_path(location)))
            .unwrap();
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.operations
            .send(RecordedObjectStoreOperation::List(classify_optional_path(
                prefix,
            )))
            .unwrap();
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.operations
            .send(RecordedObjectStoreOperation::ListWithOffset(
                classify_optional_path(prefix),
            ))
            .unwrap();
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_checkpoint_paths() {
        let path = Path::from("_delta_log/00000000000000000010.checkpoint.parquet");
        assert_eq!(classify_path(&path), RecordedPathKind::Checkpoint);
    }

    #[test]
    fn classifies_crc_paths_as_log_replay_reads() {
        let path = Path::from("_delta_log/00000000000000000010.crc");
        assert!(RecordedObjectStoreOperation::Get(classify_path(&path)).is_log_replay_read(),);
    }

    #[test]
    fn classifies_staged_commit_paths_as_log_replay_reads() {
        let path = Path::from(
            "_delta_log/_staged_commits/00000000000000000010.12345678-1234-1234-1234-123456789abc.json",
        );
        assert!(RecordedObjectStoreOperation::Get(classify_path(&path)).is_log_replay_read(),);
    }
}
