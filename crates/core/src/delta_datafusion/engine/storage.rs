use std::sync::Arc;

use bytes::Bytes;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use delta_kernel::{DeltaResult, Error, FileMeta, FileSlice, StorageHandler};
use futures::TryStreamExt as _;
use futures::stream::{self, BoxStream, StreamExt};
use itertools::Itertools;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore, PutMode};
use tracing::instrument;
use url::Url;

use crate::delta_datafusion::engine::{TracedHandle, UrlExt as _};

#[derive(Debug, Clone)]
pub struct DataFusionStorageHandler {
    ctx: Arc<TaskContext>,
    task_executor: TracedHandle,
    readahead: usize,
}

impl DataFusionStorageHandler {
    pub(crate) fn new(ctx: Arc<TaskContext>, task_executor: TracedHandle) -> Self {
        Self {
            ctx,
            task_executor,
            readahead: 10,
        }
    }

    /// Set the maximum number of files to read in parallel.
    pub fn with_readahead(mut self, readahead: usize) -> Self {
        self.readahead = readahead;
        self
    }
}

/// Native async implementation for list_from
async fn list_from_impl(
    store: Arc<DynObjectStore>,
    path: Url,
) -> DeltaResult<BoxStream<'static, DeltaResult<FileMeta>>> {
    // The offset is used for list-after; the prefix is used to restrict the listing to a specific directory.
    // Unfortunately, `Path` provides no easy way to check whether a name is directory-like,
    // because it strips trailing /, so we're reduced to manually checking the original URL.
    let offset = Path::from_url_path(path.path())?;
    let prefix = if path.path().ends_with('/') {
        offset.clone()
    } else {
        let mut parts = offset.parts().collect_vec();
        if parts.pop().is_none() {
            return Err(Error::Generic(format!(
                "Offset path must not be a root directory. Got: '{path}'",
            )));
        }
        Path::from_iter(parts)
    };

    // HACK to check if we're using a LocalFileSystem from ObjectStore. We need this because
    // local filesystem doesn't return a sorted list by default. Although the `object_store`
    // crate explicitly says it _does not_ return a sorted listing, in practice all the cloud
    // implementations actually do:
    // - AWS:
    //   [`ListObjectsV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
    //   states: "For general purpose buckets, ListObjectsV2 returns objects in lexicographical
    //   order based on their key names." (Directory buckets are out of scope for now)
    // - Azure: Docs state
    //   [here](https://learn.microsoft.com/en-us/rest/api/storageservices/enumerating-blob-resources):
    //   "A listing operation returns an XML response that contains all or part of the requested
    //   list. The operation returns entities in alphabetical order."
    // - GCP: The [main](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) doc
    //   doesn't indicate order, but [this
    //   page](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) does say: "This page
    //   shows you how to list the [objects](https://cloud.google.com/storage/docs/objects) stored
    //   in your Cloud Storage buckets, which are ordered in the list lexicographically by name."
    // So we just need to know if we're local and then if so, we sort the returned file list
    let has_ordered_listing = path.scheme() != "file";

    let stream = store
        .list_with_offset(Some(&prefix), &offset)
        .map(move |meta| {
            let meta = meta?;
            let mut location = path.clone();
            location.set_path(&format!("/{}", meta.location.as_ref()));
            Ok::<_, Error>(FileMeta {
                location,
                last_modified: meta.last_modified.timestamp_millis(),
                size: meta.size,
            })
        });

    if !has_ordered_listing {
        // Local filesystem doesn't return sorted list - need to collect and sort
        let mut items: Vec<_> = stream.try_collect().await?;
        items.sort_unstable();
        Ok(Box::pin(stream::iter(items.into_iter().map(Ok))) as _)
    } else {
        Ok(Box::pin(stream))
    }
}

/// Native async implementation for read_files
async fn read_files_impl(
    store: Arc<DynObjectStore>,
    files: Vec<FileSlice>,
    readahead: usize,
) -> DeltaResult<BoxStream<'static, DeltaResult<Bytes>>> {
    let files = stream::iter(files).map(move |(url, range)| {
        let store = store.clone();
        async move {
            // Wasn't checking the scheme before calling to_file_path causing the url path to
            // be eaten in a strange way. Now, if not a file scheme, just blindly convert to a path.
            // https://docs.rs/url/latest/url/struct.Url.html#method.to_file_path has more
            // details about why this check is necessary
            let path = if url.scheme() == "file" {
                let file_path = url
                    .to_file_path()
                    .map_err(|_| Error::InvalidTableLocation(format!("Invalid file URL: {url}")))?;
                Path::from_absolute_path(file_path)
                    .map_err(|e| Error::InvalidTableLocation(format!("Invalid file path: {e}")))?
            } else {
                Path::from(url.path())
            };
            if url.is_presigned() {
                // have to annotate type here or rustc can't figure it out
                Ok::<bytes::Bytes, Error>(reqwest::get(url).await?.bytes().await?)
            } else if let Some(rng) = range {
                Ok(store.get_range(&path, rng).await?)
            } else {
                let result = store.get(&path).await?;
                Ok(result.bytes().await?)
            }
        }
    });

    // We allow executing up to `readahead` futures concurrently and
    // buffer the results. This allows us to achieve async concurrency.
    Ok(Box::pin(files.buffered(readahead)))
}

/// Native async implementation for copy_atomic
async fn copy_atomic_impl(
    store: Arc<DynObjectStore>,
    src_path: Path,
    dest_path: Path,
) -> DeltaResult<()> {
    // Read source file then write atomically with PutMode::Create. Note that a GET/PUT is not
    // necessarily atomic, but since the source file is immutable, we aren't exposed to the
    // possibility of source file changing while we do the PUT.
    let data = store.get(&src_path).await?.bytes().await?;
    let result = store
        .put_opts(&dest_path, data.into(), PutMode::Create.into())
        .await;

    result.map_err(|e| match e {
        object_store::Error::AlreadyExists { .. } => Error::FileAlreadyExists(dest_path.into()),
        e => e.into(),
    })?;
    Ok(())
}

/// Native async implementation for head
async fn head_impl(store: Arc<DynObjectStore>, url: Url) -> DeltaResult<FileMeta> {
    let meta = store.head(&Path::from_url_path(url.path())?).await?;
    Ok(FileMeta {
        location: url,
        last_modified: meta.last_modified.timestamp_millis(),
        size: meta.size,
    })
}

impl StorageHandler for DataFusionStorageHandler {
    #[instrument(skip(self))]
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        let store_url = path.as_object_store_url();
        let store = self
            .ctx
            .runtime_env()
            .object_store(store_url)
            .map_err(Error::generic_err)?;
        let future = list_from_impl(store, path.clone());
        let iter = super::stream_future_to_iter(self.task_executor.clone(), future)?;
        Ok(iter) // type coercion drops the unneeded Send bound
    }

    /// Read data specified by the start and end offset from the file.
    ///
    /// This will return the data in the same order as the provided file slices.
    ///
    /// Multiple reads may occur in parallel, depending on the configured readahead.
    /// See [`Self::with_readahead`].
    #[instrument(skip(self))]
    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let store_url = files
            .first()
            .expect("files is not empty")
            .0
            .as_object_store_url();
        let store = self
            .ctx
            .runtime_env()
            .object_store(store_url)
            .map_err(Error::generic_err)?;

        let future = read_files_impl(store, files, self.readahead);
        let iter = super::stream_future_to_iter(self.task_executor.clone(), future)?;
        Ok(iter) // type coercion drops the unneeded Send bound
    }

    #[instrument(skip(self))]
    fn copy_atomic(&self, src: &Url, dest: &Url) -> DeltaResult<()> {
        let store_url = src.as_object_store_url();
        let store = self
            .ctx
            .runtime_env()
            .object_store(store_url)
            .map_err(Error::generic_err)?;

        let src_path = Path::from_url_path(src.path())?;
        let dest_path = Path::from_url_path(dest.path())?;
        let future = copy_atomic_impl(store, src_path, dest_path);

        self.task_executor.block_on(future)
    }

    #[instrument(skip(self))]
    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        let store_url = path.as_object_store_url();
        let store = self
            .ctx
            .runtime_env()
            .object_store(store_url)
            .map_err(Error::generic_err)?;

        let future = head_impl(store, path.clone());

        self.task_executor.block_on(future)
    }
}

pub trait AsObjectStoreUrl {
    fn as_object_store_url(&self) -> ObjectStoreUrl;
}

impl AsObjectStoreUrl for Url {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        get_store_url(self)
    }
}

impl AsObjectStoreUrl for FileMeta {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        self.location.as_object_store_url()
    }
}

impl AsObjectStoreUrl for &FileMeta {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        self.location.as_object_store_url()
    }
}

impl AsObjectStoreUrl for FileSlice {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        self.0.as_object_store_url()
    }
}

fn get_store_url(url: &url::Url) -> ObjectStoreUrl {
    ObjectStoreUrl::parse(format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    ))
    // Safety: The url is guaranteed to be valid as we construct it
    // from a valid url and pass only the parts that object store url
    // expects.
    .unwrap()
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use datafusion::prelude::SessionContext;
    use object_store::{ObjectStore, local::LocalFileSystem, path::Path};
    use rstest::*;
    use tokio::runtime::Handle;

    use crate::test_utils::TestResult;

    use super::*;

    fn get_handler() -> DataFusionStorageHandler {
        let handle = Handle::current();
        let session = SessionContext::new();
        let ctx = session.task_ctx();

        DataFusionStorageHandler::new(ctx, TracedHandle(handle))
    }

    pub fn delta_path_for_version(version: u64, suffix: &str) -> Path {
        let path = format!("_delta_log/{version:020}.{suffix}");
        Path::from(path.as_str())
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_files() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let tmp_store = LocalFileSystem::new_with_prefix(tmp.path())?;

        let data = Bytes::from("kernel-data");
        tmp_store.put(&Path::from("a"), data.clone().into()).await?;
        tmp_store.put(&Path::from("b"), data.clone().into()).await?;
        tmp_store.put(&Path::from("c"), data.clone().into()).await?;

        let mut url = Url::from_directory_path(tmp.path()).unwrap();

        let storage = get_handler();

        let mut slices: Vec<FileSlice> = Vec::new();

        let mut url1 = url.clone();
        url1.set_path(&format!("{}/b", url.path()));
        slices.push((url1.clone(), Some(Range { start: 0, end: 6 })));
        slices.push((url1, Some(Range { start: 7, end: 11 })));

        url.set_path(&format!("{}/c", url.path()));
        slices.push((url, Some(Range { start: 4, end: 9 })));
        let data: Vec<Bytes> = storage.read_files(slices)?.try_collect()?;

        assert_eq!(data.len(), 3);
        assert_eq!(data[0], Bytes::from("kernel"));
        assert_eq!(data[1], Bytes::from("data"));
        assert_eq!(data[2], Bytes::from("el-da"));

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_default_engine_listing() {
        let tmp = tempfile::tempdir().unwrap();
        let tmp_store = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
        let data = Bytes::from("kernel-data");

        let expected_names: Vec<Path> =
            (0..10).map(|i| delta_path_for_version(i, "json")).collect();

        // put them in in reverse order
        for name in expected_names.iter().rev() {
            tmp_store.put(name, data.clone().into()).await.unwrap();
        }

        let url = Url::from_directory_path(tmp.path()).unwrap();
        let storage = get_handler();
        let files = storage
            .list_from(&url.join("_delta_log").unwrap().join("0").unwrap())
            .unwrap();
        let mut len = 0;
        for (file, expected) in files.zip(expected_names.iter()) {
            assert!(
                file.as_ref()
                    .unwrap()
                    .location
                    .path()
                    .ends_with(expected.as_ref()),
                "{} does not end with {}",
                file.unwrap().location.path(),
                expected
            );
            len += 1;
        }
        assert_eq!(len, 10, "list_from should have returned 10 files");
    }
}
