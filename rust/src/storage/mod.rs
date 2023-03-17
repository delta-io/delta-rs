//! Object storage backend abstraction layer for Delta Table transaction logs and data

pub mod config;
pub mod file;
pub mod utils;

use self::config::{ObjectStoreKind, StorageOptions};
use crate::{DeltaDataTypeVersion, DeltaResult};

use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use lazy_static::lazy_static;
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use url::Url;

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
pub(crate) fn commit_uri_from_version(version: DeltaDataTypeVersion) -> Path {
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
    #[allow(unused)]
    prefix: Path,
}

impl std::fmt::Display for DeltaObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaObjectStore({})", self.location.as_ref())
    }
}

impl DeltaObjectStore {
    /// Create a new instance of [`DeltaObjectStore`]
    ///
    /// # Arguemnts
    ///
    /// * `storage` - A shared reference to an [`ObjectStore`](object_store::ObjectStore) with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `location` - A url corresponding to the storagle location of `storage`.
    pub fn new(storage: Arc<DynObjectStore>, location: Url) -> Self {
        Self {
            storage,
            location,
            prefix: Path::from("/"),
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
        let prefix = Path::from(location.path());
        let root_store =
            ObjectStoreKind::parse_url(&location)?.into_impl(location.as_ref(), options.clone())?;
        let storage = if prefix != Path::from("/") {
            root_store.into_prefix(prefix.clone())
        } else {
            root_store.into_store()
        };
        Ok(Self {
            storage,
            location,
            prefix,
            options: options.into(),
        })
    }

    /// Get a reference to the underlying storage backend
    pub fn storage_backend(&self) -> Arc<DynObjectStore> {
        self.storage.clone()
    }

    /// Storage options used to intialize storage backend
    pub fn storage_options(&self) -> &StorageOptions {
        &self.options
    }

    /// Get fully qualified uri for table root
    pub fn root_uri(&self) -> String {
        self.to_uri(&Path::from(""))
    }

    #[cfg(feature = "datafusion")]
    /// generate a unique enough url to identify the store in datafusion.
    pub(crate) fn object_store_url(&self) -> ObjectStoreUrl {
        // we are certain, that the URL can be parsed, since
        // we make sure when we are parsing the table uri
        ObjectStoreUrl::parse(format!(
            "delta-rs://{}",
            // NOTE We need to also replace colons, but its fine, since it just needs
            // to be a unique-ish identifier for the object store in datafusion
            self.prefix
                .as_ref()
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
