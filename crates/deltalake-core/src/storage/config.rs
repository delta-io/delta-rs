//! Configurltion handling for defining Storage backends for DeltaTables.
use std::collections::HashMap;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{parse_url_opts, DynObjectStore, Error as ObjectStoreError, ObjectStore};
use serde::{Deserialize, Serialize};
use url::Url;

use super::file::FileStorageBackend;
use super::utils::str_is_truthy;
use super::ObjectStoreRef;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::logstore::default_logstore::DefaultLogStore;
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use crate::logstore::s3::S3DynamoDbLogStore;
use crate::logstore::{LogStoreConfig, LogStoreRef};
use crate::table::builder::ensure_table_uri;

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use super::s3::{S3StorageBackend, S3StorageOptions};
#[cfg(feature = "hdfs")]
use datafusion_objectstore_hdfs::object_store::hdfs::HadoopFileSystem;
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use object_store::aws::AmazonS3ConfigKey;
#[cfg(feature = "azure")]
use object_store::azure::AzureConfigKey;
#[cfg(feature = "gcs")]
use object_store::gcp::GoogleConfigKey;
#[cfg(any(
    feature = "s3",
    feature = "s3-native-tls",
    feature = "gcs",
    feature = "azure"
))]
use std::str::FromStr;

#[cfg(feature = "azure")]
mod azure;

/// Recognises various URL formats, identifying the relevant [`ObjectStore`](crate::ObjectStore)
#[derive(Debug, Eq, PartialEq)]
enum ObjectStoreScheme {
    /// Url corresponding to LocalFileSystem
    Local,
    /// Url corresponding to InMemory
    Memory,
    /// Url corresponding to S3
    AmazonS3,
    /// Url corresponding to GoogleCloudStorage
    GoogleCloudStorage,
    /// Url corresponding to MicrosoftAzure
    MicrosoftAzure,
    /// Url corresponding to HttpStore
    Http,
    /// Url corresponding to Hdfs
    Hdfs,
}

impl ObjectStoreScheme {
    /// Create an [`ObjectStoreScheme`] from the provided [`Url`]
    ///
    /// Returns the [`ObjectStoreScheme`] and the remaining [`Path`]
    fn parse(
        url: &Url,
        #[allow(unused)] options: &mut StorageOptions,
    ) -> Result<(Self, Path), ObjectStoreError> {
        let strip_bucket = || Some(url.path().strip_prefix('/')?.split_once('/')?.1);

        let (scheme, path) = match (url.scheme(), url.host_str()) {
            ("file", None) => (Self::Local, url.path()),
            ("memory", None) => (Self::Memory, url.path()),
            ("s3" | "s3a", Some(_)) => (Self::AmazonS3, url.path()),
            ("gs", Some(_)) => (Self::GoogleCloudStorage, url.path()),
            ("az" | "adl" | "azure" | "abfs" | "abfss", Some(_)) => {
                (Self::MicrosoftAzure, url.path())
            }
            ("http", Some(_)) => (Self::Http, url.path()),
            ("hdfs", Some(_)) => (Self::Hdfs, url.path()),
            ("https", Some(host)) => {
                if host.ends_with("dfs.core.windows.net") || host.ends_with("blob.core.windows.net")
                {
                    (Self::MicrosoftAzure, url.path())
                } else if host.contains("dfs.fabric.microsoft.com")
                    || host.contains("blob.fabric.microsoft.com")
                {
                    #[cfg(feature = "azure")]
                    if !options
                        .as_azure_options()
                        .contains_key(&AzureConfigKey::UseFabricEndpoint)
                    {
                        options.0.insert(
                            AzureConfigKey::UseFabricEndpoint.as_ref().to_string(),
                            "true".to_string(),
                        );
                    }
                    (Self::MicrosoftAzure, url.path())
                } else if host.ends_with("amazonaws.com") {
                    match host.starts_with("s3") {
                        true => (Self::AmazonS3, strip_bucket().unwrap_or_default()),
                        false => (Self::AmazonS3, url.path()),
                    }
                } else if host.ends_with("r2.cloudflarestorage.com") {
                    (Self::AmazonS3, strip_bucket().unwrap_or_default())
                } else {
                    (Self::Http, url.path())
                }
            }
            _ => return Err(ObjectStoreError::NotImplemented),
        };

        let path = Path::parse(path)?;
        Ok((scheme, path))
    }
}

/// Options used for configuring backend storage
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct StorageOptions(pub HashMap<String, String>);

impl StorageOptions {
    /// Create a new instance of [`StorageOptions`]
    pub fn new(options: HashMap<String, String>) -> Self {
        let mut options = options;
        if let Ok(value) = std::env::var("AZURE_STORAGE_ALLOW_HTTP") {
            options.insert("allow_http".into(), value);
        }
        if let Ok(value) = std::env::var("AZURE_STORAGE_USE_HTTP") {
            options.insert("allow_http".into(), value);
        }
        if let Ok(value) = std::env::var("AWS_ALLOW_HTTP") {
            options.insert("allow_http".into(), value);
        }
        Self(options)
    }

    /// Add values from the environment to storage options
    #[cfg(feature = "azure")]
    pub fn with_env_azure(&mut self) {
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = AzureConfigKey::from_str(&key.to_ascii_lowercase()) {
                    if !self.0.contains_key(config_key.as_ref()) {
                        self.0
                            .insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
            }
        }
    }

    /// Add values from the environment to storage options
    #[cfg(feature = "gcs")]
    pub fn with_env_gcs(&mut self) {
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = GoogleConfigKey::from_str(&key.to_ascii_lowercase()) {
                    if !self.0.contains_key(config_key.as_ref()) {
                        self.0
                            .insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
            }
        }
    }

    /// Add values from the environment to storage options
    #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
    pub fn with_env_s3(&mut self) {
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                    if !self.0.contains_key(config_key.as_ref()) {
                        self.0
                            .insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
            }
        }
    }

    /// Denotes if unsecure connections via http are allowed
    pub fn allow_http(&self) -> bool {
        self.0.iter().any(|(key, value)| {
            key.to_ascii_lowercase().contains("allow_http") & str_is_truthy(value)
        })
    }

    /// Subset of options relevant for azure storage
    #[cfg(feature = "azure")]
    pub fn as_azure_options(&self) -> HashMap<AzureConfigKey, String> {
        self.0
            .iter()
            .filter_map(|(key, value)| {
                let az_key = AzureConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((az_key, value.clone()))
            })
            .collect()
    }

    /// Subset of options relevant for s3 storage
    #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
    pub fn as_s3_options(&self) -> HashMap<AmazonS3ConfigKey, String> {
        self.0
            .iter()
            .filter_map(|(key, value)| {
                let s3_key = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((s3_key, value.clone()))
            })
            .collect()
    }

    /// Subset of options relevant for gcs storage
    #[cfg(feature = "gcs")]
    pub fn as_gcs_options(&self) -> HashMap<GoogleConfigKey, String> {
        self.0
            .iter()
            .filter_map(|(key, value)| {
                let gcs_key = GoogleConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((gcs_key, value.clone()))
            })
            .collect()
    }
}

impl From<HashMap<String, String>> for StorageOptions {
    fn from(value: HashMap<String, String>) -> Self {
        Self::new(value)
    }
}

/// Configure a [`LogStoreRef`] for the given url and configuration
pub fn configure_log_store(
    location: &str,
    options: impl Into<StorageOptions> + Clone,
    storage_backend: Option<(ObjectStoreRef, Url)>,
) -> DeltaResult<LogStoreRef> {
    let mut options = options.into();
    let (object_store, location) = match storage_backend {
        Some((object_store, url)) => (object_store, url),
        None => {
            let url = ensure_table_uri(location)?;
            let object_store = crate::storage::config::configure_store(&url, &mut options)?;
            (object_store, url)
        }
    };

    let (scheme, _prefix) = ObjectStoreScheme::parse(&location, &mut options)?;
    match scheme {
        #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
        ObjectStoreScheme::AmazonS3 => {
            let s3_options = S3StorageOptions::from_map(&options.0);
            if Some("dynamodb".to_owned())
                == s3_options
                    .locking_provider
                    .as_ref()
                    .map(|v| v.to_lowercase())
            {
                Ok(Arc::new(S3DynamoDbLogStore::try_new(
                    location,
                    options,
                    &s3_options,
                    object_store,
                )?))
            } else {
                Ok(Arc::new(DefaultLogStore::new(
                    object_store,
                    LogStoreConfig { location, options },
                )))
            }
        }
        _ => Ok(Arc::new(DefaultLogStore::new(
            object_store,
            LogStoreConfig { location, options },
        ))),
    }
}

/// Configure an instance of an [`ObjectStore`] for the given url and configuration
pub fn configure_store(
    url: &Url,
    options: &mut StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    let (scheme, _prefix) = ObjectStoreScheme::parse(url, options)?;
    match scheme {
        ObjectStoreScheme::Local => {
            let path = url
                .to_file_path()
                .map_err(|_| DeltaTableError::InvalidTableLocation(url.to_string()))?;
            Ok(Arc::new(FileStorageBackend::try_new(path)?))
        }
        ObjectStoreScheme::Memory => url_prefix_handler(InMemory::new(), Path::parse(url.path())?),
        #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
        ObjectStoreScheme::AmazonS3 => {
            options.with_env_s3();
            let (store, prefix) = parse_url_opts(url, options.as_s3_options())?;
            let s3_options = S3StorageOptions::from_map(&options.0);
            if options
                .as_s3_options()
                .contains_key(&AmazonS3ConfigKey::CopyIfNotExists)
            {
                url_prefix_handler(store, prefix)
            } else if Some("dynamodb".to_owned())
                == s3_options
                    .locking_provider
                    .as_ref()
                    .map(|v| v.to_lowercase())
            {
                // if a lock client is requested, unsafe rename is always safe
                let store = S3StorageBackend::try_new(Arc::new(store), true)?;
                url_prefix_handler(store, prefix)
            } else {
                let store =
                    S3StorageBackend::try_new(Arc::new(store), s3_options.allow_unsafe_rename)?;
                url_prefix_handler(store, prefix)
            }
        }
        #[cfg(feature = "azure")]
        ObjectStoreScheme::MicrosoftAzure => {
            let config = azure::AzureConfigHelper::try_new(options.as_azure_options())?.build()?;
            let (store, prefix) = parse_url_opts(url, config)?;
            url_prefix_handler(store, prefix)
        }
        #[cfg(feature = "gcs")]
        ObjectStoreScheme::GoogleCloudStorage => {
            options.with_env_gcs();
            let (store, prefix) = parse_url_opts(url, options.as_gcs_options())?;
            url_prefix_handler(store, prefix)
        }
        #[cfg(feature = "hdfs")]
        ObjectStoreScheme::Hdfs => {
            let store = HadoopFileSystem::new(url.as_ref()).ok_or_else(|| {
                DeltaTableError::Generic(format!(
                    "failed to create HadoopFileSystem for {}",
                    url.as_ref()
                ))
            })?;
            url_prefix_handler(store, _prefix)
        }
        #[cfg(not(feature = "hdfs"))]
        ObjectStoreScheme::Hdfs => Err(DeltaTableError::MissingFeature {
            feature: "hdfs",
            url: url.as_ref().into(),
        }),
        _ => {
            let (store, prefix) = parse_url_opts(url, options.0.clone())?;
            url_prefix_handler(store, prefix)
        }
    }
}

fn url_prefix_handler<T: ObjectStore>(store: T, prefix: Path) -> DeltaResult<Arc<DynObjectStore>> {
    if prefix != Path::from("/") {
        Ok(Arc::new(PrefixStore::new(store, prefix)))
    } else {
        Ok(Arc::new(store))
    }
}

#[cfg(test)]
mod test {
    use crate::table::builder::ensure_table_uri;

    use super::*;

    #[tokio::test]
    async fn test_configure_store_local() -> Result<(), Box<dyn std::error::Error + 'static>> {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path();
        let path = temp_dir_path.join("test space 😁");

        let table_uri = ensure_table_uri(path.as_os_str().to_str().unwrap()).unwrap();

        let store = configure_store(&table_uri, &mut StorageOptions::default()).unwrap();

        let contents = b"test";
        let key = "test.txt";
        let file_path = path.join(key);
        std::fs::write(&file_path, contents).unwrap();

        let res = store
            .get(&object_store::path::Path::from(key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(res.as_ref(), contents);

        Ok(())
    }
}
