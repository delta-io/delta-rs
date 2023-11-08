//! Delta log store.
use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{
    de::{Error, SeqAccess, Visitor},
    ser::SerializeSeq,
    Deserialize, Serialize,
};
use std::{cmp::max, collections::HashMap, sync::Arc};
use url::Url;

use crate::{
    errors::DeltaResult,
    operations::transaction::TransactionError,
    protocol::{get_last_checkpoint, ProtocolError},
    storage::{commit_uri_from_version, config::StorageOptions},
    DeltaTableError,
};
use bytes::Bytes;
use log::debug;
use object_store::{
    path::Path, Error as ObjectStoreError, ObjectStore, Result as ObjectStoreResult,
};

#[cfg(feature = "datafusion")]
use datafusion::datasource::object_store::ObjectStoreUrl;

pub mod default_logstore;

/// Sharable reference to [`LogStore`]
pub type LogStoreRef = Arc<dyn LogStore>;

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
}

/// Configuration parameters for a log store
#[derive(Debug, Clone)]
pub struct LogStoreConfig {
    /// url corresponding to the storage location.
    pub location: Url,
    /// Options used for configuring backend storage
    pub options: StorageOptions,
}

/// Trait for critical operations required to read and write commit entries in Delta logs.
///
/// The correctness is predicated on the atomicity and durability guarantees of
/// the implementation of this interface. Specifically,
///
/// - Atomic visibility: Any commit created via `write_commit_entry` must become visible atomically.
/// - Mutual exclusion: Only one writer must be able to create a commit for a specific version.
/// - Consistent listing: Once a commit entry for version `v` has been written, any future call to
///   `get_latest_version` must return a version >= `v`, i.e. the underlying file system entry must
///   become visible immediately.
#[async_trait::async_trait]
pub trait LogStore: Sync + Send {
    /// Read data for commit entry with the given version.
    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Bytes>;

    /// Write list of actions as delta commit entry for given version.
    ///
    /// This operation can be retried with a higher version in case the write
    /// fails with [`TransactionError::VersionAlreadyExists`].
    async fn write_commit_entry(
        &self,
        version: i64,
        tmp_commit: &Path,
    ) -> Result<(), TransactionError>;

    /// Find latest version currently stored in the delta log.
    async fn get_latest_version(&self, start_version: i64) -> DeltaResult<i64>;

    /// Get underlying object store.
    fn object_store(&self) -> Arc<dyn ObjectStore>;

    /// [Path] to Delta log
    fn to_uri(&self, location: &Path) -> String;

    /// Get fully qualified uri for table root
    fn root_uri(&self) -> String {
        self.to_uri(&Path::from(""))
    }

    /// [Path] to Delta log
    fn log_path(&self) -> &Path {
        &DELTA_LOG_PATH
    }

    /// Check if the location is a delta table location
    async fn is_delta_table_location(&self) -> ObjectStoreResult<bool> {
        // TODO We should really be using HEAD here, but this fails in windows tests
        let object_store = self.object_store();
        let mut stream = object_store.list(Some(self.log_path())).await?;
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

    #[cfg(feature = "datafusion")]
    /// Generate a unique enough url to identify the store in datafusion.
    /// The DF object store registry only cares about the scheme and the host of the url for
    /// registering/fetching. In our case the scheme is hard-coded to "delta-rs", so to get a unique
    /// host we convert the location from this `LogStore` to a valid name, combining the
    /// original scheme, host and path with invalid characters replaced.
    fn object_store_url(&self) -> ObjectStoreUrl;

    /// Get configuration representing configured log store.
    fn config(&self) -> &LogStoreConfig;
}

// TODO: maybe a bit of a hack, required to `#[derive(Debug)]` for the operation builders
impl std::fmt::Debug for dyn LogStore + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogStore({})", self.root_uri())
    }
}

impl Serialize for LogStoreConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.location.to_string())?;
        seq.serialize_element(&self.options.0)?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for LogStoreConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LogStoreConfigVisitor {}

        impl<'de> Visitor<'de> for LogStoreConfigVisitor {
            type Value = LogStoreConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct LogStoreConfig")
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
                Ok(LogStoreConfig {
                    location,
                    options: options.into(),
                })
            }
        }

        deserializer.deserialize_seq(LogStoreConfigVisitor {})
    }
}

lazy_static! {
    static ref DELTA_LOG_REGEX: Regex = Regex::new(r"(\d{20})\.(json|checkpoint).*$").unwrap();
}

fn to_uri(root: &Url, location: &Path) -> String {
    match root.scheme() {
        "file" => {
            #[cfg(windows)]
            let uri = format!(
                "{}/{}",
                root.as_ref().trim_end_matches('/'),
                location.as_ref()
            )
            .replace("file:///", "");
            #[cfg(unix)]
            let uri = format!(
                "{}/{}",
                root.as_ref().trim_end_matches('/'),
                location.as_ref()
            )
            .replace("file://", "");
            uri
        }
        _ => {
            if location.as_ref().is_empty() || location.as_ref() == "/" {
                root.as_ref().to_string()
            } else {
                format!("{}/{}", root.as_ref(), location.as_ref())
            }
        }
    }
}

#[cfg(feature = "datafusion")]
fn object_store_url(location: &Url) -> ObjectStoreUrl {
    // we are certain, that the URL can be parsed, since
    // we make sure when we are parsing the table uri

    use object_store::path::DELIMITER;
    ObjectStoreUrl::parse(format!(
        "delta-rs://{}-{}{}",
        location.scheme(),
        location.host_str().unwrap_or("-"),
        location.path().replace(DELIMITER, "-").replace(':', "-")
    ))
    .expect("Invalid object store url.")
}

/// Extract version from a file name in the delta log
pub fn extract_version_from_filename(name: &str) -> Option<i64> {
    DELTA_LOG_REGEX
        .captures(name)
        .map(|captures| captures.get(1).unwrap().as_str().parse().unwrap())
}

async fn get_latest_version(log_store: &dyn LogStore, current_version: i64) -> DeltaResult<i64> {
    let version_start = match get_last_checkpoint(log_store).await {
        Ok(last_check_point) => last_check_point.version,
        Err(ProtocolError::CheckpointNotFound) => {
            // no checkpoint
            -1
        }
        Err(e) => {
            return Err(DeltaTableError::from(e));
        }
    };

    debug!("latest checkpoint version: {version_start}");

    let version_start = max(current_version, version_start);

    // list files to find max version
    let version = async {
        let mut max_version: i64 = version_start;
        let prefix = Some(log_store.log_path());
        let offset_path = commit_uri_from_version(max_version);
        let object_store = log_store.object_store();
        let mut files = object_store.list_with_offset(prefix, &offset_path).await?;

        while let Some(obj_meta) = files.next().await {
            let obj_meta = obj_meta?;
            if let Some(log_version) = extract_version_from_filename(obj_meta.location.as_ref()) {
                max_version = max(max_version, log_version);
                // also cache timestamp for version, for faster time-travel
                // TODO: temporarily disabled because `version_timestamp` is not available in the [`LogStore`]
                // self.version_timestamp
                //     .insert(log_version, obj_meta.last_modified.timestamp());
            }
        }

        if max_version < 0 {
            return Err(DeltaTableError::not_a_table(log_store.root_uri()));
        }

        Ok::<i64, DeltaTableError>(max_version)
    }
    .await?;
    Ok(version)
}

async fn read_commit_entry(storage: &dyn ObjectStore, version: i64) -> DeltaResult<Bytes> {
    let commit_uri = commit_uri_from_version(version);
    let data = storage.get(&commit_uri).await?.bytes().await?;
    Ok(data)
}

async fn write_commit_entry(
    storage: &dyn ObjectStore,
    version: i64,
    tmp_commit: &Path,
) -> Result<(), TransactionError> {
    // move temporary commit file to delta log directory
    // rely on storage to fail if the file already exists -
    storage
        .rename_if_not_exists(tmp_commit, &commit_uri_from_version(version))
        .await
        .map_err(|err| -> TransactionError {
            match err {
                ObjectStoreError::AlreadyExists { .. } => {
                    TransactionError::VersionAlreadyExists(version)
                }
                _ => TransactionError::from(err),
            }
        })?;
    Ok(())
}

#[cfg(feature = "datafusion")]
#[cfg(test)]
mod tests {
    use url::Url;

    #[tokio::test]
    async fn test_unique_object_store_url() {
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

            assert_ne!(
                super::object_store_url(&url_1).as_str(),
                super::object_store_url(&url_2).as_str(),
            );
        }
    }
}
