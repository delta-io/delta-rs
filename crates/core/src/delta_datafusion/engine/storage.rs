use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::filesystem::ObjectStoreStorageHandler;
use delta_kernel::{DeltaResult, Error as DeltaError, FileMeta, FileSlice, StorageHandler};
use itertools::Itertools;
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

    fn get_or_create(
        &self,
        url: ObjectStoreUrl,
    ) -> DeltaResult<Ref<'_, ObjectStoreUrl, Arc<dyn StorageHandler>>> {
        if let Some(handler) = self.registry.get(&url) {
            return Ok(handler);
        }
        let store = self
            .registry()
            .get_store(url.as_ref())
            .map_err(DeltaError::generic_err)?;

        let handler: Arc<dyn StorageHandler> = match self.handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => Arc::new(ObjectStoreStorageHandler::new(
                store,
                Arc::new(TokioMultiThreadExecutor::new(self.handle.clone())),
                None,
            )),
            RuntimeFlavor::CurrentThread => Arc::new(ObjectStoreStorageHandler::new(
                store,
                Arc::new(TokioBackgroundExecutor::new()),
                None,
            )),
            _ => panic!("unsupported runtime flavor"),
        };

        self.registry.insert(url.clone(), handler);
        Ok(self.registry.get(&url).unwrap())
    }
}

impl StorageHandler for DataFusionStorageHandler {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        self.get_or_create(path.as_object_store_url())?
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
                .map(|(url, files)| self.get_or_create(url)?.read_files(files))
                // TODO: this should not do any blocking operations, since this should
                // happen when the iterators are polled and we are just creating a vec of iterators.
                // Is this correct?
                .try_collect::<_, Vec<_>, _>()?
                .into_iter()
                .flatten(),
        ))
    }

    fn copy_atomic(&self, _from: &Url, _to: &Url) -> DeltaResult<()> {
        // TODO: Implement atomic copy operation
        Err(delta_kernel::Error::generic("copy_atomic not implemented"))
    }

    fn head(&self, _path: &Url) -> DeltaResult<FileMeta> {
        // TODO: Implement atomic copy operation
        Err(delta_kernel::Error::generic("head not implemented"))
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

pub(crate) fn group_by_store<T: IntoIterator<Item = impl AsObjectStoreUrl>>(
    files: T,
) -> std::collections::HashMap<ObjectStoreUrl, Vec<T::Item>> {
    files
        .into_iter()
        .map(|item| (item.as_object_store_url(), item))
        .into_group_map()
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use datafusion::prelude::SessionContext;
    use object_store::{ObjectStore, local::LocalFileSystem, path::Path};
    use rstest::*;

    use crate::test_utils::TestResult;

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
