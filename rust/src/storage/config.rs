//! Configuration handling for defining Storage backends for DeltaTables.
use std::collections::HashMap;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{DynObjectStore, ObjectStore};
use serde::{Deserialize, Serialize};
use url::Url;

use super::file::FileStorageBackend;
use super::utils::str_is_truthy;
use crate::errors::{DeltaResult, DeltaTableError};

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use super::s3::{S3StorageBackend, S3StorageOptions};
#[cfg(feature = "hdfs")]
use datafusion_objectstore_hdfs::object_store::hdfs::HadoopFileSystem;
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
#[cfg(feature = "azure")]
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
#[cfg(feature = "gcs")]
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
#[cfg(any(
    feature = "s3",
    feature = "s3-native-tls",
    feature = "gcs",
    feature = "azure"
))]
use std::str::FromStr;

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
        if let Ok(value) = std::env::var("AWS_STORAGE_ALLOW_HTTP") {
            options.insert("allow_http".into(), value);
        }
        Self(options)
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
pub(crate) fn configure_store(
    url: &Url,
    options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    match url.scheme() {
        "file" => try_configure_local(
            url.to_file_path()
                .map_err(|_| DeltaTableError::InvalidTableLocation(url.to_string()))?
                .to_str()
                .ok_or_else(|| DeltaTableError::InvalidTableLocation(url.to_string()))?,
        ),
        "memory" => try_configure_memory(url),
        "az" | "abfs" | "abfss" | "azure" | "wasb" | "wasbs" | "adl" => {
            try_configure_azure(url, options)
        }
        "s3" | "s3a" => try_configure_s3(url, options),
        "gs" => try_configure_gcs(url, options),
        "hdfs" => try_configure_hdfs(url, options),
        "https" => {
            let host = url.host_str().unwrap_or_default();
            if host.contains("amazonaws.com") {
                try_configure_s3(url, options)
            } else if host.contains("dfs.core.windows.net")
                || host.contains("blob.core.windows.net")
            {
                try_configure_azure(url, options)
            } else {
                Err(DeltaTableError::Generic(format!(
                    "unsupported url: {}",
                    url.as_str()
                )))
            }
        }
        _ => Err(DeltaTableError::Generic(format!(
            "unsupported url: {}",
            url.as_str()
        ))),
    }
}

fn try_configure_local<P: AsRef<str>>(path: P) -> DeltaResult<Arc<DynObjectStore>> {
    Ok(Arc::new(FileStorageBackend::try_new(path.as_ref())?))
}

fn try_configure_memory(storage_url: &Url) -> DeltaResult<Arc<DynObjectStore>> {
    url_prefix_handler(InMemory::new(), storage_url)
}

#[cfg(feature = "gcs")]
fn try_configure_gcs(
    storage_url: &Url,
    options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    let store = GoogleCloudStorageBuilder::from_env()
        .with_url(storage_url.as_ref())
        .try_with_options(&options.as_gcs_options())?
        .build()?;
    url_prefix_handler(store, storage_url)
}

#[cfg(not(feature = "gcs"))]
fn try_configure_gcs(
    storage_url: &Url,
    _options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    Err(DeltaTableError::MissingFeature {
        feature: "gcs",
        url: storage_url.as_ref().into(),
    })
}

#[cfg(feature = "azure")]
fn try_configure_azure(
    storage_url: &Url,
    options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    let store = MicrosoftAzureBuilder::from_env()
        .with_url(storage_url.as_ref())
        .try_with_options(&options.as_azure_options())?
        .with_allow_http(options.allow_http())
        .build()?;
    url_prefix_handler(store, storage_url)
}

#[cfg(not(feature = "azure"))]
fn try_configure_azure(
    storage_url: &Url,
    _options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    Err(DeltaTableError::MissingFeature {
        feature: "azure",
        url: storage_url.as_ref().into(),
    })
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
fn try_configure_s3(
    storage_url: &Url,
    options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    let amazon_s3 = AmazonS3Builder::from_env()
        .with_url(storage_url.as_ref())
        .try_with_options(&options.as_s3_options())?
        .with_allow_http(options.allow_http())
        .build()?;
    let store =
        S3StorageBackend::try_new(Arc::new(amazon_s3), S3StorageOptions::from_map(&options.0))?;
    url_prefix_handler(store, storage_url)
}

#[cfg(not(any(feature = "s3", feature = "s3-native-tls")))]
fn try_configure_s3(
    storage_url: &Url,
    _options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    Err(DeltaTableError::MissingFeature {
        feature: "s3",
        url: storage_url.as_ref().into(),
    })
}

#[cfg(feature = "hdfs")]
fn try_configure_hdfs(
    storage_url: &Url,
    _options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    let store = HadoopFileSystem::new(storage_url.as_ref()).ok_or_else(|| {
        DeltaTableError::Generic(format!(
            "failed to create HadoopFileSystem for {}",
            storage_url.as_ref()
        ))
    })?;
    url_prefix_handler(store, storage_url)
}

#[cfg(not(feature = "hdfs"))]
fn try_configure_hdfs(
    storage_url: &Url,
    _options: &StorageOptions,
) -> DeltaResult<Arc<DynObjectStore>> {
    Err(DeltaTableError::MissingFeature {
        feature: "hdfs",
        url: storage_url.as_ref().into(),
    })
}

fn url_prefix_handler<T: ObjectStore>(
    store: T,
    storage_url: &Url,
) -> DeltaResult<Arc<DynObjectStore>> {
    let prefix = Path::parse(storage_url.path())?;
    if prefix != Path::from("/") {
        Ok(Arc::new(PrefixStore::new(store, prefix)))
    } else {
        Ok(Arc::new(store))
    }
}

#[cfg(test)]
mod test {
    use crate::ensure_table_uri;

    use super::*;

    #[tokio::test]
    async fn test_configure_store_local() -> Result<(), Box<dyn std::error::Error + 'static>> {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path();
        let path = temp_dir_path.join("test space 😁");

        let table_uri = ensure_table_uri(path.as_os_str().to_str().unwrap()).unwrap();

        let store = configure_store(&table_uri, &StorageOptions::default()).unwrap();

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
