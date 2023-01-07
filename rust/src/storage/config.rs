//! Configuration handling for defining Storage backends for DeltaTables.
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, Result as ObjectStoreResult};
use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use url::Url;

#[cfg(any(feature = "s3", feature = "s3-rustls"))]
use object_store::aws::AmazonS3ConfigKey;
#[cfg(feature = "azure")]
use object_store::azure::AzureConfigKey;
#[cfg(feature = "gcs")]
use object_store::gcp::GoogleConfigKey;
#[cfg(any(
    feature = "s3",
    feature = "s3-rustls",
    feature = "gcs",
    feature = "azure"
))]
use std::str::FromStr;

/// Options used for configuring backend storage
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

    /// Denotes if unsecure connections are configures to be allowed
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
    #[cfg(any(feature = "s3", feature = "s3-rustls"))]
    pub fn as_s3_options(&self) -> HashMap<AmazonS3ConfigKey, String> {
        let mut config = self
            .0
            .iter()
            .filter_map(|(key, value)| {
                let s3_key = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((s3_key, value.clone()))
            })
            .collect::<HashMap<_, _>>();
        if let Ok(region) = std::env::var("AWS_REGION") {
            if !config.contains_key(&AmazonS3ConfigKey::Region) {
                config.insert(AmazonS3ConfigKey::Region, region);
            }
        }
        if let Ok(region) = std::env::var("AWS_DEFAULT_REGION") {
            if !config.contains_key(&AmazonS3ConfigKey::DefaultRegion) {
                config.insert(AmazonS3ConfigKey::DefaultRegion, region);
            }
        }
        if !(config.contains_key(&AmazonS3ConfigKey::DefaultRegion)
            | config.contains_key(&AmazonS3ConfigKey::Region))
        {
            config.insert(AmazonS3ConfigKey::DefaultRegion, "us-east-1".into());
        }
        config
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

/// A parsed URL identifying a storage location
/// for more information on the supported expressions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLocation {
    /// A URL that identifies a file or directory to list files from
    pub(crate) url: Url,
    /// The path prefix
    pub(crate) prefix: Path,
}

impl StorageLocation {
    /// Parse a provided string as a `StorageUrl`
    ///
    /// # Paths without a Scheme
    ///
    /// If no scheme is provided, or the string is an absolute filesystem path
    /// as determined [`std::path::Path::is_absolute`], the string will be
    /// interpreted as a path on the local filesystem using the operating
    /// system's standard path delimiter, i.e. `\` on Windows, `/` on Unix.
    ///
    /// Otherwise, the path will be resolved to an absolute path, returning
    /// an error if it does not exist, and converted to a [file URI]
    ///
    /// If you wish to specify a path that does not exist on the local
    /// machine you must provide it as a fully-qualified [file URI]
    /// e.g. `file:///myfile.txt`
    ///
    /// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
    ///
    /// # Well-known formats
    ///
    /// The lists below enumerates some well known uris, that are understood by the
    /// parse function. We parse uris to refer to a specific storage location, which
    /// is accessed using the internal storage backends.
    ///
    /// ## Azure
    ///
    /// URIs according to <https://github.com/fsspec/adlfs#filesystem-interface-to-azure-datalake-gen1-and-gen2-storage>:
    ///
    ///   * az://<container>/<path>
    ///   * adl://<container>/<path>
    ///   * abfs(s)://<container>/<path>
    ///
    /// URIs according to <https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri>:
    ///   
    ///   * abfs(s)://<file_system>@<account_name>.dfs.core.windows.net/<path>
    ///
    /// and a custom one
    ///
    ///   * azure://<container>/<path>
    ///
    /// ## S3
    ///   * s3://<bucket>/<path>
    ///   * s3a://<bucket>/<path>
    ///
    /// ## GCS
    ///   * gs://<bucket>/<path>
    pub fn parse(s: impl AsRef<str>) -> ObjectStoreResult<Self> {
        let s = s.as_ref();

        // This is necessary to handle the case of a path starting with a drive letter
        if std::path::Path::new(s).is_absolute() {
            return Self::parse_path(s);
        }

        match Url::parse(s) {
            Ok(url) => Ok(Self::new(url)),
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::parse_path(s),
            Err(e) => Err(ObjectStoreError::Generic {
                store: "DeltaObjectStore",
                source: Box::new(e),
            }),
        }
    }

    /// Creates a new [`StorageLocation`] from an url
    pub fn new(url: Url) -> Self {
        let prefix = Path::parse(url.path()).expect("should be URL safe");
        Self { url, prefix }
    }

    /// Creates a new [`StorageUrl`] interpreting `s` as a filesystem path
    fn parse_path(s: &str) -> ObjectStoreResult<Self> {
        let path =
            std::path::Path::new(s)
                .canonicalize()
                .map_err(|e| ObjectStoreError::Generic {
                    store: "DeltaObjectStore",
                    source: Box::new(e),
                })?;
        let url = match path.is_file() {
            true => Url::from_file_path(path).unwrap(),
            false => Url::from_directory_path(path).unwrap(),
        };

        Ok(Self::new(url))
    }

    /// Returns the URL scheme
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Create the full path from a path relative to prefix
    pub fn full_path(&self, location: &Path) -> Path {
        self.prefix.parts().chain(location.parts()).collect()
    }

    /// Strip the constant prefix from a given path
    pub fn strip_prefix(&self, path: &Path) -> Option<Path> {
        Some(path.prefix_match(&self.prefix)?.collect())
    }

    /// convert a table [Path] to a fully qualified uri
    pub fn to_uri(&self, location: &Path) -> String {
        let uri = match self.scheme() {
            "file" | "" => {
                // On windows the drive (e.g. 'c:') is part of root and must not be prefixed.
                #[cfg(windows)]
                let os_uri = format!("{}/{}", self.prefix, location.as_ref());
                #[cfg(unix)]
                let os_uri = format!("/{}/{}", self.prefix, location.as_ref());
                os_uri
            }
            _ => format!("{}/{}", self.as_ref(), location.as_ref()),
        };
        uri.trim_end_matches('/').into()
    }
}

impl AsRef<str> for StorageLocation {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl std::fmt::Display for StorageLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl Serialize for StorageLocation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(self.url.as_str())?;
        seq.serialize_element(&self.prefix.to_string())?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for StorageLocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StorageLocationVisitor {}
        impl<'de> Visitor<'de> for StorageLocationVisitor {
            type Value = StorageLocation;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct StorageUrl")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<StorageLocation, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let url = seq
                    .next_element()?
                    .ok_or_else(|| V::Error::invalid_length(0, &self))?;
                let prefix: &str = seq
                    .next_element()?
                    .ok_or_else(|| V::Error::invalid_length(1, &self))?;
                let url = Url::parse(url).map_err(|_| V::Error::missing_field("url"))?;
                let prefix = Path::parse(prefix).map_err(|_| V::Error::missing_field("prefix"))?;
                let url = StorageLocation { url, prefix };
                Ok(url)
            }
        }
        deserializer.deserialize_seq(StorageLocationVisitor {})
    }
}

pub(crate) fn str_is_truthy(val: &str) -> bool {
    val.eq_ignore_ascii_case("1")
        | val.eq_ignore_ascii_case("true")
        | val.eq_ignore_ascii_case("on")
        | val.eq_ignore_ascii_case("yes")
        | val.eq_ignore_ascii_case("y")
}
