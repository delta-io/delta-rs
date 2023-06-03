//! Object storage backend abstraction layer for Delta Table transaction logs and data

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use lazy_static::lazy_static;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::io::AsyncWrite;
use url::Url;

use self::config::StorageOptions;
use crate::errors::DeltaResult;

pub mod config;
pub mod file;
pub mod utils;

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
pub mod s3;

#[cfg(feature = "datafusion")]
use datafusion::datasource::object_store::ObjectStoreUrl;

pub use object_store::path::{Path, DELIMITER};
pub use object_store::{
    DynObjectStore, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result as ObjectStoreResult,
};
pub use utils::*;

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

/// Return the uri of commit version.
pub(crate) fn commit_uri_from_version(version: i64) -> Path {
    let version = format!("{version:020}.json");
    DELTA_LOG_PATH.child(version.as_str())
}

/// Sharable reference to [`DeltaObjectStore`]
pub type ObjectStoreRef = Arc<DeltaObjectStore>;

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
    storage: Arc<dyn ObjectStore>,
    location: Url,
    options: StorageOptions,
}

impl std::fmt::Display for DeltaObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaObjectStore({})", self.location.as_ref())
    }
}

impl DeltaObjectStore {
    /// Create a new instance of [`DeltaObjectStore`]
    ///
    /// # Arguments
    ///
    /// * `storage` - A shared reference to an [`ObjectStore`](object_store::ObjectStore) with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `location` - A url corresponding to the storage location of `storage`.
    pub fn new(storage: Arc<DynObjectStore>, location: Url) -> Self {
        Self {
            storage,
            location,
            options: HashMap::new().into(),
        }
    }

    /// Try creating a new instance of [`DeltaObjectStore`]
    ///
    /// # Arguments
    ///
    /// * `location` - A url pointing to the root of the delta table.
    /// * `options` - Options passed to underlying builders. See [`with_storage_options`](crate::builder::DeltaTableBuilder::with_storage_options)
    pub fn try_new(location: Url, options: impl Into<StorageOptions> + Clone) -> DeltaResult<Self> {
        let options = options.into();
        let storage = config::configure_store(&location, &options)?;
        Ok(Self {
            storage,
            location,
            options,
        })
    }

    /// Get a reference to the underlying storage backend
    pub fn storage_backend(&self) -> Arc<DynObjectStore> {
        self.storage.clone()
    }

    /// Storage options used to initialize storage backend
    pub fn storage_options(&self) -> &StorageOptions {
        &self.options
    }

    /// Get fully qualified uri for table root
    pub fn root_uri(&self) -> String {
        self.to_uri(&Path::from(""))
    }

    #[cfg(feature = "datafusion")]
    /// Generate a unique enough url to identify the store in datafusion.
    /// The DF object store registry only cares about the scheme and the host of the url for
    /// registering/fetching. In our case the scheme is hard-coded to "delta-rs", so to get a unique
    /// host we convert the location from this `DeltaObjectStore` to a valid name, combining the
    /// original scheme, host and path with invalid characters replaced.
    pub fn object_store_url(&self) -> ObjectStoreUrl {
        // we are certain, that the URL can be parsed, since
        // we make sure when we are parsing the table uri
        ObjectStoreUrl::parse(format!(
            "delta-rs://{}-{}{}",
            self.location.scheme(),
            self.location.host_str().unwrap_or("-"),
            self.location
                .path()
                .replace(DELIMITER, "-")
                .replace(':', "-")
        ))
        .expect("Invalid object store url.")
    }

    /// [Path] to Delta log
    pub fn log_path(&self) -> &Path {
        &DELTA_LOG_PATH
    }

    /// [Path] to Delta log
    pub fn to_uri(&self, location: &Path) -> String {
        match self.location.scheme() {
            "file" => {
                #[cfg(windows)]
                let uri = format!(
                    "{}/{}",
                    self.location.as_ref().trim_end_matches('/'),
                    location.as_ref()
                )
                .replace("file:///", "");
                #[cfg(unix)]
                let uri = format!(
                    "{}/{}",
                    self.location.as_ref().trim_end_matches('/'),
                    location.as_ref()
                )
                .replace("file://", "");
                uri
            }
            _ => {
                if location.as_ref().is_empty() || location.as_ref() == "/" {
                    self.location.as_ref().to_string()
                } else {
                    format!("{}/{}", self.location.as_ref(), location.as_ref())
                }
            }
        }
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

    /// Check if the location is a delta table location
    pub async fn is_delta_table_location(&self) -> ObjectStoreResult<bool> {
        // TODO We should really be using HEAD here, but this fails in windows tests
        let mut stream = self.list(Some(self.log_path())).await?;
        if let Some(res) = stream.next().await {
            match res {
                Ok(_) => Ok(true),
                Err(ObjectStoreError::NotFound { .. }) => Ok(false),
                Err(err) => Err(err),
            }
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for DeltaObjectStore {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        self.storage.put(location, bytes).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.storage.get(location).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        self.storage.get_range(location, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.storage.head(location).await
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.storage.delete(location).await
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.storage.list(prefix).await
    }

    /// List all the objects with the given prefix and a location greater than `offset`
    ///
    /// Some stores, such as S3 and GCS, may be able to push `offset` down to reduce
    /// the number of network requests required
    async fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.storage.list_with_offset(prefix, offset).await
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.storage.list_with_delimiter(prefix).await
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.storage.copy(from, to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.storage.copy_if_not_exists(from, to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.storage.rename_if_not_exists(from, to).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.storage.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        self.storage.abort_multipart(location, multipart_id).await
    }
}

impl Serialize for DeltaObjectStore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.location.to_string())?;
        seq.serialize_element(&self.options.0)?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for DeltaObjectStore {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DeltaObjectStoreVisitor {}

        impl<'de> Visitor<'de> for DeltaObjectStoreVisitor {
            type Value = DeltaObjectStore;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct DeltaObjectStore")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let location_str: String = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let options: HashMap<String, String> = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let location = Url::parse(&location_str).unwrap();
                let table = DeltaObjectStore::try_new(location, options)
                    .map_err(|_| A::Error::custom("Failed deserializing DeltaObjectStore"))?;
                Ok(table)
            }
        }

        deserializer.deserialize_seq(DeltaObjectStoreVisitor {})
    }
}

#[cfg(feature = "datafusion")]
#[cfg(test)]
mod tests {
    use crate::storage::DeltaObjectStore;
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use url::Url;

    #[tokio::test]
    async fn test_unique_object_store_url() {
        // Just a dummy store to be passed for initialization
        let inner_store = Arc::from(InMemory::new());

        for (location_1, location_2) in [
            // Same scheme, no host, different path
            ("file:///path/to/table_1", "file:///path/to/table_2"),
            // Different scheme/host, same path
            ("s3://my_bucket/path/to/table_1", "file:///path/to/table_1"),
            // Same scheme, different host, same path
            ("s3://bucket_1/table_1", "s3://bucket_2/table_1"),
        ] {
            let url_1 = Url::parse(location_1).unwrap();
            let url_2 = Url::parse(location_2).unwrap();
            let store_1 = DeltaObjectStore::new(inner_store.clone(), url_1);
            let store_2 = DeltaObjectStore::new(inner_store.clone(), url_2);

            assert_ne!(
                store_1.object_store_url().as_str(),
                store_2.object_store_url().as_str(),
            );
        }
    }
}
