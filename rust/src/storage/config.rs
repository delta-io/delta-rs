//! Configuration handling for defining Storage backends for DeltaTables.
use super::file::FileStorageBackend;
use super::utils::str_is_truthy;
use crate::{DeltaResult, DeltaTableError};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixObjectStore;
use object_store::DynObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use super::s3::{S3StorageBackend, S3StorageOptions};
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
#[cfg(feature = "azure")]
use object_store::azure::{AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
#[cfg(feature = "gcs")]
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder, GoogleConfigKey};
#[cfg(any(
    feature = "s3",
    feature = "s3-native-tls",
    feature = "gcs",
    feature = "azure"
))]
use std::str::FromStr;

/// Options used for configuring backend storage
#[derive(Clone, Debug, Serialize, Deserialize)]
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

pub(crate) enum ObjectStoreImpl {
    Local(FileStorageBackend),
    InMemory(InMemory),
    #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
    S3(S3StorageBackend),
    #[cfg(feature = "gcs")]
    Google(GoogleCloudStorage),
    #[cfg(feature = "azure")]
    Azure(MicrosoftAzure),
}

impl ObjectStoreImpl {
    pub(crate) fn into_prefix(self, prefix: Path) -> Arc<DynObjectStore> {
        match self {
            ObjectStoreImpl::Local(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
            ObjectStoreImpl::InMemory(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
            #[cfg(feature = "azure")]
            ObjectStoreImpl::Azure(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
            #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
            ObjectStoreImpl::S3(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
            #[cfg(feature = "gcs")]
            ObjectStoreImpl::Google(store) => Arc::new(PrefixObjectStore::new(store, prefix)),
        }
    }

    pub(crate) fn into_store(self) -> Arc<DynObjectStore> {
        match self {
            ObjectStoreImpl::Local(store) => Arc::new(store),
            ObjectStoreImpl::InMemory(store) => Arc::new(store),
            #[cfg(feature = "azure")]
            ObjectStoreImpl::Azure(store) => Arc::new(store),
            #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
            ObjectStoreImpl::S3(store) => Arc::new(store),
            #[cfg(feature = "gcs")]
            ObjectStoreImpl::Google(store) => Arc::new(store),
        }
    }
}

pub(crate) enum ObjectStoreKind {
    Local,
    InMemory,
    S3,
    Google,
    Azure,
}

impl ObjectStoreKind {
    pub fn parse_url(url: &Url) -> DeltaResult<Self> {
        match url.scheme() {
            "file" => Ok(ObjectStoreKind::Local),
            "memory" => Ok(ObjectStoreKind::InMemory),
            "az" | "abfs" | "abfss" | "azure" | "wasb" | "adl" => Ok(ObjectStoreKind::Azure),
            "s3" | "s3a" => Ok(ObjectStoreKind::S3),
            "gs" => Ok(ObjectStoreKind::Google),
            "https" => {
                let host = url.host_str().unwrap_or_default();
                if host.contains("amazonaws.com") {
                    Ok(ObjectStoreKind::S3)
                } else if host.contains("dfs.core.windows.net")
                    || host.contains("blob.core.windows.net")
                {
                    Ok(ObjectStoreKind::Azure)
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

    pub fn into_impl(
        self,
        storage_url: impl AsRef<str>,
        options: impl Into<StorageOptions>,
    ) -> DeltaResult<ObjectStoreImpl> {
        let _options = options.into();
        match self {
            ObjectStoreKind::Local => Ok(ObjectStoreImpl::Local(FileStorageBackend::new())),
            ObjectStoreKind::InMemory => Ok(ObjectStoreImpl::InMemory(InMemory::new())),
            #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
            ObjectStoreKind::S3 => {
                let store = AmazonS3Builder::from_env()
                    .with_url(storage_url.as_ref())
                    .try_with_options(&_options.as_s3_options())?
                    .with_allow_http(_options.allow_http())
                    .build()?;
                Ok(ObjectStoreImpl::S3(S3StorageBackend::try_new(
                    Arc::new(store),
                    S3StorageOptions::from_map(&_options.0),
                )?))
            }
            #[cfg(not(any(feature = "s3", feature = "s3-native-tls")))]
            ObjectStoreKind::S3 => Err(DeltaTableError::MissingFeature {
                feature: "s3",
                url: storage_url.as_ref().into(),
            }),
            #[cfg(feature = "azure")]
            ObjectStoreKind::Azure => {
                let store = MicrosoftAzureBuilder::from_env()
                    .with_url(storage_url.as_ref())
                    .try_with_options(&_options.as_azure_options())?
                    .with_allow_http(_options.allow_http())
                    .build()?;
                Ok(ObjectStoreImpl::Azure(store))
            }
            #[cfg(not(feature = "azure"))]
            ObjectStoreKind::Azure => Err(DeltaTableError::MissingFeature {
                feature: "azure",
                url: storage_url.as_ref().into(),
            }),
            #[cfg(feature = "gcs")]
            ObjectStoreKind::Google => {
                let store = GoogleCloudStorageBuilder::from_env()
                    .with_url(storage_url.as_ref())
                    .try_with_options(&_options.as_gcs_options())?
                    .build()?;
                Ok(ObjectStoreImpl::Google(store))
            }
            #[cfg(not(feature = "gcs"))]
            ObjectStoreKind::Google => Err(DeltaTableError::MissingFeature {
                feature: "gcs",
                url: storage_url.as_ref().into(),
            }),
        }
    }
}
