//! Object storage backend abstraction layer for Delta Table transaction logs and data

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use lazy_static::lazy_static;
use object_store::GetOptions;
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
    /// * `storage` - A shared reference to an [`object_store::ObjectStore`] with "/" pointing at delta table root (i.e. where `_delta_log` is located).
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
    /// * `options` - Options passed to underlying builders. See [`with_storage_options`](crate::table::builder::DeltaTableBuilder::with_storage_options)
    pub fn try_new(location: Url, options: impl Into<StorageOptions> + Clone) -> DeltaResult<Self> {
        let mut options = options.into();
        let storage = config::configure_store(&location, &mut options)?;
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

    pub(crate) fn location(&self) -> Url {
        self.location.clone()
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

    /// Perform a get request with options
    ///
    /// Note: options.range will be ignored if [`object_store::GetResultPayload::File`]
    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.storage.get_opts(location, options).await
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
