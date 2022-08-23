//! Object storage backend abstraction layer for Delta Table transaction logs and data

pub mod file;
#[cfg(any(feature = "s3", feature = "s3-rustls"))]
pub mod s3;

use bytes::Bytes;
#[cfg(feature = "datafusion-ext")]
use datafusion::datasource::object_store::ObjectStoreUrl;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use object_store::{
    path::{Path, DELIMITER},
    DynObjectStore, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result as ObjectStoreResult,
};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

/// Configuration for a DeltaObjectStore
#[derive(Debug, Clone)]
struct DeltaObjectStoreConfig {
    table_root: Path,
}

impl DeltaObjectStoreConfig {
    /// Create a new [DeltaObjectStoreConfig]
    pub fn new(table_root: impl Into<Path>) -> Self {
        Self {
            table_root: table_root.into(),
        }
    }

    /// Prefix a path with the table root path
    fn full_path(&self, location: &Path) -> Path {
        Path::from_iter(self.table_root.parts().chain(location.parts()))
    }

    fn strip_prefix(&self, path: &Path) -> Option<Path> {
        let path: &str = path.as_ref();
        let stripped = match self.table_root.as_ref() {
            "" => path,
            p => path.strip_prefix(p)?.strip_prefix(DELIMITER)?,
        };
        Some(Path::from_iter(stripped.split(DELIMITER)))
    }
}

/// Object Store implementation for DeltaTable.
///
/// The [DeltaObjectStore] implements the [object_store::ObjectStore] trait to facilitate
/// interoperability with the larger rust / arrow ecosystem. Specifically it can directly
/// be registered as store within datafusion.
///
/// The table root is treated as the root of the object store.
/// All [Path] are reported relative to the table root.
#[derive(Debug, Clone)]
pub struct DeltaObjectStore {
    scheme: String,
    root: Path,
    storage: Arc<DynObjectStore>,
    config: DeltaObjectStoreConfig,
}

impl std::fmt::Display for DeltaObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaObjectStore({}://{})", self.scheme, self.root)
    }
}

impl DeltaObjectStore {
    /// Create new DeltaObjectStore
    pub fn new(table_root: &Path, storage: Arc<DynObjectStore>) -> Self {
        let config = DeltaObjectStoreConfig::new(table_root.clone());
        Self {
            scheme: String::from("file"),
            root: table_root.clone(),
            storage,
            config,
        }
    }

    /// Get a reference to the underlying storage backend
    pub fn storage_backend(&self) -> Arc<DynObjectStore> {
        self.storage.clone()
    }

    /// Get fully qualified uri for table root
    pub fn root_uri(&self) -> String {
        self.to_uri(&Path::from(""))
    }

    /// convert a table [Path] to a fully qualified uri
    pub fn to_uri(&self, location: &Path) -> String {
        let uri = match self.scheme.as_ref() {
            "file" | "" => {
                // On windows the drive (e.g. 'c:') is part of root and must not be prefixed.
                #[cfg(windows)]
                let os_uri = format!("{}/{}", self.root, location.as_ref());
                #[cfg(unix)]
                let os_uri = format!("/{}/{}", self.root, location.as_ref());
                os_uri
            }
            _ => format!("{}://{}/{}", self.scheme, self.root, location.as_ref()),
        };
        uri.trim_end_matches('/').to_string()
    }

    #[cfg(feature = "datafusion-ext")]
    /// generate a unique enough url to identify the store in datafusion.
    pub(crate) fn object_store_url(&self) -> ObjectStoreUrl {
        // we are certain, that the URL can be parsed, since
        // we make sure when we are parsing the table uri
        ObjectStoreUrl::parse(format!(
            "delta-rs://{}",
            // NOTE We need to also replace colons, but its fine, since it just needs
            // to be a unique-ish identifier for the object store in datafusion
            self.root.as_ref().replace(DELIMITER, "-").replace(':', "-")
        ))
        .expect("Invalid object store url.")
    }

    /// [Path] to Delta log
    pub fn log_path(&self) -> &Path {
        &DELTA_LOG_PATH
    }

    /// Deletes object by `paths`.
    pub async fn delete_batch(&self, paths: &[Path]) -> ObjectStoreResult<()> {
        for path in paths {
            match self.delete(path).await {
                Ok(_) => continue,
                Err(ObjectStoreError::NotFound { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for DeltaObjectStore {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        let full_path = self.config.full_path(location);
        self.storage.put(&full_path, bytes).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let full_path = self.config.full_path(location);
        self.storage.get(&full_path).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let full_path = self.config.full_path(location);
        object_store::ObjectStore::get_range(self.storage.as_ref(), &full_path, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let full_path = self.config.full_path(location);
        self.storage.head(&full_path).await.map(|meta| ObjectMeta {
            last_modified: meta.last_modified,
            size: meta.size,
            location: self
                .config
                .strip_prefix(&meta.location)
                .unwrap_or(meta.location),
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let full_path = self.config.full_path(location);
        self.storage.delete(&full_path).await
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        let prefix = prefix.map(|p| self.config.full_path(p));
        Ok(self
            .storage
            .list(Some(&prefix.unwrap_or_else(|| self.root.clone())))
            .await?
            .map_ok(|meta| ObjectMeta {
                last_modified: meta.last_modified,
                size: meta.size,
                location: self
                    .config
                    .strip_prefix(&meta.location)
                    .unwrap_or(meta.location),
            })
            .boxed())
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let prefix = prefix.map(|p| self.config.full_path(p));
        self.storage
            .list_with_delimiter(Some(&prefix.unwrap_or_else(|| self.root.clone())))
            .await
            .map(|lst| ListResult {
                common_prefixes: lst
                    .common_prefixes
                    .iter()
                    .map(|p| self.config.strip_prefix(p).unwrap_or_else(|| p.clone()))
                    .collect(),
                objects: lst
                    .objects
                    .iter()
                    .map(|meta| ObjectMeta {
                        last_modified: meta.last_modified,
                        size: meta.size,
                        location: self
                            .config
                            .strip_prefix(&meta.location)
                            .unwrap_or_else(|| meta.location.clone()),
                    })
                    .collect(),
            })
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.config.full_path(from);
        let full_to = self.config.full_path(to);
        self.storage.copy(&full_from, &full_to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.config.full_path(from);
        let full_to = self.config.full_path(to);
        self.storage.copy_if_not_exists(&full_from, &full_to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let full_from = self.config.full_path(from);
        let full_to = self.config.full_path(to);
        self.storage
            .rename_if_not_exists(&full_from, &full_to)
            .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let full_path = self.config.full_path(location);
        self.storage.put_multipart(&full_path).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        let full_path = self.config.full_path(location);
        self.storage.abort_multipart(&full_path, multipart_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::test_utils::{
        copy_if_not_exists, list_with_delimiter, put_get_delete_list, rename_and_copy,
        rename_if_not_exists,
    };
    use crate::test_utils::{IntegrationContext, StorageIntegration, TestResult};
    use object_store::DynObjectStore;

    #[cfg(feature = "azure", feature = "integration_test")]
    #[tokio::test]
    async fn test_object_store_azure() -> TestResult {
        let integration = IntegrationContext::new(StorageIntegration::Microsoft)?;
        test_object_store(integration.object_store().as_ref()).await?;
        Ok(())
    }

    #[cfg(feature = "s3", feature = "integration_test")]
    #[tokio::test]
    async fn test_object_store_aws() -> TestResult {
        let integration = IntegrationContext::new(StorageIntegration::Amazon)?;
        test_object_store(integration.object_store().as_ref()).await?;
        Ok(())
    }

    async fn test_object_store(storage: &DynObjectStore) -> TestResult {
        put_get_delete_list(storage).await?;
        list_with_delimiter(storage).await?;
        rename_and_copy(storage).await?;
        copy_if_not_exists(storage).await?;
        rename_if_not_exists(storage).await?;
        // get_nonexistent_object(storage, None).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use crate::test_utils::TestResult;
    use object_store::{path::Path, Error as ObjectStoreError, Result as ObjectStoreResult};

    pub(crate) async fn put_get_delete_list(storage: &DynObjectStore) -> TestResult {
        let store_str = storage.to_string();

        delete_fixtures(storage).await?;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let location = Path::from("test_dir/test_file.json");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();
        storage.put(&location, data).await?;

        let root = Path::from("/");

        // List everything
        let content_list = flatten_list_stream(storage, None).await?;
        assert_eq!(content_list, &[location.clone()]);

        // Should behave the same as no prefix
        let content_list = flatten_list_stream(storage, Some(&root)).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List with delimiter
        let result = storage.list_with_delimiter(None).await?;
        assert_eq!(&result.objects, &[]);
        assert_eq!(result.common_prefixes.len(), 1);
        assert_eq!(result.common_prefixes[0], Path::from("test_dir"));

        // Should behave the same as no prefix
        let result = storage.list_with_delimiter(Some(&root)).await?;
        assert!(result.objects.is_empty());
        assert_eq!(result.common_prefixes.len(), 1);
        assert_eq!(result.common_prefixes[0], Path::from("test_dir"));

        // List everything starting with a prefix that should return results
        let prefix = Path::from("test_dir");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that shouldn't return results
        let prefix = Path::from("something");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert!(content_list.is_empty());

        let read_data = storage.get(&location).await?.bytes().await?;
        assert_eq!(&*read_data, expected_data);

        // Test range request
        let range = 3..7;
        let range_result = storage.get_range(&location, range.clone()).await;

        let out_of_range = 200..300;
        let out_of_range_result = storage.get_range(&location, out_of_range).await;

        if store_str.starts_with("MicrosoftAzureEmulator") {
            // Azurite doesn't support x-ms-range-get-content-crc64 set by Azure SDK
            // https://github.com/Azure/Azurite/issues/444
            let err = range_result.unwrap_err().to_string();
            assert!(err.contains("x-ms-range-get-content-crc64 header or parameter is not supported in Azurite strict mode"), "{}", err);

            let err = out_of_range_result.unwrap_err().to_string();
            assert!(err.contains("x-ms-range-get-content-crc64 header or parameter is not supported in Azurite strict mode"), "{}", err);
        } else {
            let bytes = range_result?;
            assert_eq!(bytes, expected_data.slice(range));

            // Should be a non-fatal error
            out_of_range_result.unwrap_err();

            let ranges = vec![0..1, 2..3, 0..5];
            let bytes = storage.get_ranges(&location, &ranges).await?;
            for (range, bytes) in ranges.iter().zip(bytes) {
                assert_eq!(bytes, expected_data.slice(range.clone()))
            }
        }

        let head = storage.head(&location).await?;
        assert_eq!(head.size, expected_data.len());

        storage.delete(&location).await?;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        let err = storage.get(&location).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound { .. }), "{}", err);

        let err = storage.head(&location).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound { .. }), "{}", err);

        // Test handling of paths containing an encoded delimiter

        let file_with_delimiter = Path::from_iter(["a", "b/c", "foo.file"]);
        storage
            .put(&file_with_delimiter, Bytes::from("arbitrary"))
            .await?;

        let files = flatten_list_stream(storage, None).await?;
        assert_eq!(files, vec![file_with_delimiter.clone()]);

        let files = flatten_list_stream(storage, Some(&Path::from("a/b"))).await?;
        assert!(files.is_empty());

        let files = storage
            .list_with_delimiter(Some(&Path::from("a/b")))
            .await?;
        assert!(files.common_prefixes.is_empty());
        assert!(files.objects.is_empty());

        let files = storage.list_with_delimiter(Some(&Path::from("a"))).await?;
        assert_eq!(files.common_prefixes, vec![Path::from_iter(["a", "b/c"])]);
        assert!(files.objects.is_empty());

        let files = storage
            .list_with_delimiter(Some(&Path::from_iter(["a", "b/c"])))
            .await?;
        assert!(files.common_prefixes.is_empty());
        assert_eq!(files.objects.len(), 1);
        assert_eq!(files.objects[0].location, file_with_delimiter);

        storage.delete(&file_with_delimiter).await?;

        // Test handling of paths containing non-ASCII characters, e.g. emoji

        let emoji_prefix = Path::from("🙀");
        let emoji_file = Path::from("🙀/😀.parquet");
        storage.put(&emoji_file, Bytes::from("arbitrary")).await?;

        storage.head(&emoji_file).await?;
        storage.get(&emoji_file).await?.bytes().await?;

        let files = flatten_list_stream(storage, Some(&emoji_prefix)).await?;

        assert_eq!(files, vec![emoji_file.clone()]);

        let dst = Path::from("foo.parquet");
        storage.copy(&emoji_file, &dst).await?;
        let mut files = flatten_list_stream(storage, None).await?;
        files.sort_unstable();
        assert_eq!(files, vec![emoji_file.clone(), dst.clone()]);

        storage.delete(&emoji_file).await?;
        storage.delete(&dst).await?;
        let files = flatten_list_stream(storage, Some(&emoji_prefix)).await?;
        assert!(files.is_empty());

        // Test handling of paths containing percent-encoded sequences

        // "HELLO" percent encoded
        let hello_prefix = Path::parse("%48%45%4C%4C%4F")?;
        let path = hello_prefix.child("foo.parquet");

        storage.put(&path, Bytes::from(vec![0, 1])).await?;
        let files = flatten_list_stream(storage, Some(&hello_prefix)).await?;
        assert_eq!(files, vec![path.clone()]);

        // Cannot list by decoded representation
        let files = flatten_list_stream(storage, Some(&Path::from("HELLO"))).await?;
        assert!(files.is_empty());

        // Cannot access by decoded representation
        let err = storage
            .head(&Path::from("HELLO/foo.parquet"))
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound { .. }), "{}", err);

        storage.delete(&path).await?;

        // Can also write non-percent encoded sequences
        let path = Path::parse("%Q.parquet")?;
        storage.put(&path, Bytes::from(vec![0, 1])).await?;

        let files = flatten_list_stream(storage, None).await?;
        assert_eq!(files, vec![path.clone()]);

        storage.delete(&path).await?;
        Ok(())
    }

    pub(crate) async fn list_with_delimiter(storage: &DynObjectStore) -> TestResult {
        delete_fixtures(storage).await?;

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        // ==================== do: create files ====================
        let data = Bytes::from("arbitrary data");

        let files: Vec<_> = [
            "test_file",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/000/000/001.segment",
            "mydb/wb/000/000/002.segment",
            "mydb/wb/001/001/000.segment",
            "mydb/wb/foo.json",
            "mydb/wbwbwb/111/222/333.segment",
            "mydb/data/whatevs",
        ]
        .iter()
        .map(|&s| Path::from(s))
        .collect();

        for f in &files {
            let data = data.clone();
            storage.put(f, data).await?;
        }

        // ==================== check: prefix-list `mydb/wb` (directory) ====================
        let prefix = Path::from("mydb/wb");

        let expected_000 = Path::from("mydb/wb/000");
        let expected_001 = Path::from("mydb/wb/001");
        let expected_location = Path::from("mydb/wb/foo.json");

        let result = storage.list_with_delimiter(Some(&prefix)).await?;

        assert_eq!(result.common_prefixes, vec![expected_000, expected_001]);
        assert_eq!(result.objects.len(), 1);

        let object = &result.objects[0];

        assert_eq!(object.location, expected_location);
        assert_eq!(object.size, data.len());

        // ==================== check: prefix-list `mydb/wb/000/000/001` (partial filename doesn't match) ====================
        let prefix = Path::from("mydb/wb/000/000/001");

        let result = storage.list_with_delimiter(Some(&prefix)).await?;
        assert!(result.common_prefixes.is_empty());
        assert_eq!(result.objects.len(), 0);

        // ==================== check: prefix-list `not_there` (non-existing prefix) ====================
        let prefix = Path::from("not_there");

        let result = storage.list_with_delimiter(Some(&prefix)).await?;
        assert!(result.common_prefixes.is_empty());
        assert!(result.objects.is_empty());

        // ==================== do: remove all files ====================
        for f in &files {
            storage.delete(f).await?;
        }

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());
        Ok(())
    }

    pub(crate) async fn rename_and_copy(storage: &DynObjectStore) -> TestResult {
        // Create two objects
        let path1 = Path::from("test1");
        let path2 = Path::from("test2");
        let contents1 = Bytes::from("cats");
        let contents2 = Bytes::from("dogs");

        // copy() make both objects identical
        storage.put(&path1, contents1.clone()).await?;
        storage.put(&path2, contents2.clone()).await?;
        storage.copy(&path1, &path2).await?;
        let new_contents = storage.get(&path2).await?.bytes().await?;
        assert_eq!(&new_contents, &contents1);

        // rename() copies contents and deletes original
        storage.put(&path1, contents1.clone()).await?;
        storage.put(&path2, contents2.clone()).await?;
        storage.rename(&path1, &path2).await?;
        let new_contents = storage.get(&path2).await?.bytes().await?;
        assert_eq!(&new_contents, &contents1);
        let result = storage.get(&path1).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        ));

        // Clean up
        storage.delete(&path2).await?;
        Ok(())
    }

    pub(crate) async fn copy_if_not_exists(storage: &DynObjectStore) -> TestResult {
        // Create two objects
        let path1 = Path::from("test1");
        let path2 = Path::from("test2");
        let contents1 = Bytes::from("cats");
        let contents2 = Bytes::from("dogs");

        // copy_if_not_exists() errors if destination already exists
        storage.put(&path1, contents1.clone()).await?;
        storage.put(&path2, contents2.clone()).await?;
        let result = storage.copy_if_not_exists(&path1, &path2).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ObjectStoreError::AlreadyExists { .. }
        ));

        // copy_if_not_exists() copies contents and allows deleting original
        storage.delete(&path2).await?;
        storage.copy_if_not_exists(&path1, &path2).await?;
        storage.delete(&path1).await?;
        let new_contents = storage.get(&path2).await?.bytes().await?;
        assert_eq!(&new_contents, &contents1);
        let result = storage.get(&path1).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        ));

        // Clean up
        storage.delete(&path2).await?;
        Ok(())
    }

    pub(crate) async fn rename_if_not_exists(storage: &DynObjectStore) -> TestResult {
        let path1 = Path::from("tmp_file1");
        let path2 = Path::from("tmp_file2");
        storage.put(&path1, bytes::Bytes::new()).await?;

        // delete objects
        let result = storage.rename_if_not_exists(&path1, &path2).await;
        assert!(result.is_ok());
        assert!(storage.head(&path1).await.is_err());
        assert!(storage.head(&path2).await.is_ok());

        storage.put(&path1, bytes::Bytes::new()).await?;
        let result = storage.rename_if_not_exists(&path1, &path2).await;
        assert!(result.is_err());
        assert!(storage.head(&path1).await.is_ok());
        assert!(storage.head(&path2).await.is_ok());
        Ok(())
    }

    // pub(crate) async fn get_nonexistent_object(
    //     storage: &DynObjectStore,
    //     location: Option<Path>,
    // ) -> ObjectStoreResult<Bytes> {
    //     let location = location.unwrap_or_else(|| Path::from("this_file_should_not_exist"));

    //     let err = storage.head(&location).await.unwrap_err();
    //     assert!(matches!(err, ObjectStoreError::NotFound { .. }));

    //     storage.get(&location).await?.bytes().await
    // }

    async fn delete_fixtures(storage: &DynObjectStore) -> TestResult {
        let paths = flatten_list_stream(storage, None).await?;

        for f in &paths {
            let _ = storage.delete(f).await?;
        }
        Ok(())
    }

    async fn flatten_list_stream(
        storage: &DynObjectStore,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<Vec<Path>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
    }
}
