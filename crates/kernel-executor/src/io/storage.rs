use std::sync::Arc;

use bytes::Bytes;
use chrono::DateTime;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use delta_kernel::plans::ir::nodes::ScanFile;
use delta_kernel::{DeltaResult, Error as DeltaError, FileMeta, FileSlice, StorageHandler};
use delta_kernel_default_engine::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel_default_engine::filesystem::ObjectStoreStorageHandler;
use indexmap::IndexMap;
use itertools::Itertools;
use object_store::ObjectMeta;
use object_store::path::Path;
use tokio::runtime::{Handle, RuntimeFlavor};
use url::Url;

#[derive(Clone)]
pub struct DataFusionStorageHandler {
    /// Object store registry shared with datafusion session
    ctx: Arc<TaskContext>,
    /// Registry of object store handlers
    registry: Arc<DashMap<ObjectStoreUrl, Arc<dyn StorageHandler>>>,
    /// The executor to run async tasks on
    handle: Handle,
}

impl DataFusionStorageHandler {
    /// Create a new [`DataFusionStorageHandler`] instance.
    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        Self {
            ctx,
            registry: DashMap::new().into(),
            handle,
        }
    }

    fn registry(&self) -> Arc<dyn ObjectStoreRegistry> {
        self.ctx.runtime_env().object_store_registry.clone()
    }

    fn handler_for(
        &self,
        url: ObjectStoreUrl,
    ) -> DeltaResult<Ref<'_, ObjectStoreUrl, Arc<dyn StorageHandler>>> {
        if let Some(handler) = self.registry.get(&url) {
            return Ok(handler);
        }
        // The entry holds the shard's write lock, so concurrent first calls build one handler.
        let handler = self.registry.entry(url.clone()).or_try_insert_with(|| {
            let store = self
                .registry()
                .get_store(url.as_ref())
                .map_err(DeltaError::generic_err)?;

            let handler: Arc<dyn StorageHandler> = match self.handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => Arc::new(ObjectStoreStorageHandler::new(
                    store,
                    Arc::new(TokioMultiThreadExecutor::new(self.handle.clone())),
                )),
                RuntimeFlavor::CurrentThread => Arc::new(ObjectStoreStorageHandler::new(
                    store,
                    Arc::new(TokioBackgroundExecutor::new()),
                )),
                _ => panic!("unsupported runtime flavor"),
            };
            Ok::<_, DeltaError>(handler)
        })?;
        Ok(handler.downgrade())
    }
}

impl StorageHandler for DataFusionStorageHandler {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        self.handler_for(path.as_object_store_url())?
            .list_from(path)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let grouped_files = group_by_store(files);
        Ok(Box::new(
            grouped_files
                .into_iter()
                .map(|(url, files)| self.handler_for(url)?.read_files(files))
                // TODO: this should not do any blocking operations, since this should
                // happen when the iterators are polled and we are just creating a vec of iterators.
                // Is this correct?
                .try_collect::<_, Vec<_>, _>()?
                .into_iter()
                .flatten(),
        ))
    }

    fn copy_atomic(&self, from: &Url, to: &Url) -> DeltaResult<()> {
        self.handler_for(from.as_object_store_url())?
            .copy_atomic(from, to)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        self.handler_for(path.as_object_store_url())?
            .put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> DeltaResult<FileMeta> {
        self.handler_for(path.as_object_store_url())?.head(path)
    }

    fn delete(&self, path: &Url) -> DeltaResult<()> {
        self.handler_for(path.as_object_store_url())?.delete(path)
    }
}

/// Conversion to a DataFusion [`ObjectStoreUrl`], the key used to register and look up an
/// object store in a session's runtime environment.
pub trait AsObjectStoreUrl {
    /// Return the [`ObjectStoreUrl`] that identifies the object store backing `self`.
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

impl AsObjectStoreUrl for &ScanFile {
    fn as_object_store_url(&self) -> ObjectStoreUrl {
        self.meta.as_object_store_url()
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

/// The file at `meta` as a [`PartitionedFile`], without the double encoding of `PartitionedFile::new`.
pub fn to_partitioned_file(meta: &FileMeta) -> Result<PartitionedFile, object_store::path::Error> {
    Ok(ObjectMeta {
        location: Path::from_url_path(meta.location.path())?,
        last_modified: DateTime::from_timestamp_millis(meta.last_modified).unwrap_or_default(),
        size: meta.size,
        e_tag: None,
        version: None,
    }
    .into())
}

/// `files` grouped by the object store that serves them, in first-seen order.
pub(crate) fn group_by_store<T: IntoIterator<Item = impl AsObjectStoreUrl>>(
    files: T,
) -> IndexMap<ObjectStoreUrl, Vec<T::Item>> {
    let mut groups: IndexMap<ObjectStoreUrl, Vec<T::Item>> = IndexMap::new();
    for file in files {
        groups
            .entry(file.as_object_store_url())
            .or_default()
            .push(file);
    }
    groups
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use datafusion::prelude::SessionContext;
    use object_store::{ObjectStoreExt as _, local::LocalFileSystem, path::Path};
    use rstest::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    use super::*;

    fn get_handler() -> DataFusionStorageHandler {
        let handle = Handle::current();
        let session = SessionContext::new();
        let ctx = session.task_ctx();

        DataFusionStorageHandler::new(ctx, handle)
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
    async fn put_head_copy_delete_round_trip() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let dir = Url::from_directory_path(tmp.path()).unwrap();
        let source = dir.join("a.bin")?;
        let copy = dir.join("b.bin")?;
        let storage = get_handler();

        storage.put(&source, Bytes::from("kernel"), false)?;
        let meta = storage.head(&source)?;
        assert_eq!(meta.location, source);
        assert_eq!(meta.size, 6);

        storage.copy_atomic(&source, &copy)?;
        assert_eq!(storage.head(&copy)?.size, 6);
        // The copy is atomic: a second copy onto an existing file is refused.
        assert!(storage.copy_atomic(&source, &copy).is_err());

        storage.delete(&copy)?;
        assert!(matches!(
            storage.head(&copy),
            Err(DeltaError::FileNotFound(_))
        ));
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
