use std::collections::HashMap;

use ::object_store::RetryConfig;
use object_store::{path::Path, prefix::PrefixStore, ObjectStore, ObjectStoreScheme};
use tokio::runtime::Handle;

#[cfg(feature = "delta-cache")]
use super::storage::cache::LogCacheConfig;
use super::storage::runtime::RuntimeConfig;
use super::storage::LimitConfig;
use crate::{DeltaResult, DeltaTableError};

pub(super) trait TryUpdateKey {
    /// Update an internal field in the configuration.
    ///
    /// ## Returns
    /// - `Ok(Some(()))` if the key was updated.
    /// - `Ok(None)` if the key was not found and no internal field was updated.
    /// - `Err(_)` if the update failed. Failed updates may include finding a known key,
    ///   but failing to parse the value into the expected type.
    fn try_update_key(&mut self, key: &str, value: &str) -> DeltaResult<Option<()>>;
}

impl<K, V> FromIterator<(K, V)> for RuntimeConfig
where
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        ParseResult::from_iter(iter).config
    }
}

#[cfg(feature = "delta-cache")]
impl<K, V> FromIterator<(K, V)> for LogCacheConfig
where
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        ParseResult::from_iter(iter).config
    }
}

pub(super) struct ParseResult<T> {
    pub config: T,
    pub unparsed: HashMap<String, String>,
    pub errors: Vec<(String, String)>,
    pub is_default: bool,
}

impl<T> ParseResult<T> {
    pub fn raise_errors(&self) -> DeltaResult<()> {
        if !self.errors.is_empty() {
            return Err(DeltaTableError::Generic(format!(
                "Failed to parse config: {:?}",
                self.errors
            )));
        }
        Ok(())
    }
}

impl<T, K, V> FromIterator<(K, V)> for ParseResult<T>
where
    T: TryUpdateKey + Default,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut config = T::default();
        let mut unparsed = HashMap::new();
        let mut errors = Vec::new();
        let mut is_default = true;
        for (k, v) in iter {
            match config.try_update_key(k.as_ref(), v.as_ref()) {
                Ok(None) => {
                    unparsed.insert(k.into(), v.into());
                }
                Ok(Some(_)) => is_default = false,
                Err(e) => errors.push((k.into(), e.to_string())),
            }
        }
        ParseResult {
            config,
            unparsed,
            errors,
            is_default,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct StorageConfig {
    #[cfg(feature = "delta-cache")]
    /// Log cache configuration.
    ///
    /// The log cache will selectively cache reads from the storage layer
    /// that target known log files. Specifically JSON commit files
    /// (including compacted commits) and deletion vectors.
    pub cache: Option<LogCacheConfig>,

    /// Runtime configuration.
    ///
    /// Configuration to set up a dedicated IO runtime to execute IO related operations.
    pub runtime: Option<RuntimeConfig>,

    pub retry: ::object_store::RetryConfig,

    /// Limit configuration.
    ///
    /// Configuration to limit the number of concurrent requests to the object store.
    pub limit: Option<LimitConfig>,

    pub unknown_properties: HashMap<String, String>,

    pub raw: HashMap<String, String>,
}

impl StorageConfig {
    /// Wrap an object store with additional layers of functionality.
    ///
    /// Depending on the configuration, the following layers may be added:
    /// - Retry layer: Adds retry logic to the object store.
    /// - Limit layer: Limits the number of concurrent requests to the object store.
    /// - Runtime layer: Executes IO related operations on a dedicated runtime.
    pub fn decorate_store<T: ObjectStore + Clone>(
        &self,
        store: T,
        table_root: &url::Url,
        handle: Option<Handle>,
    ) -> DeltaResult<Box<dyn ObjectStore>> {
        let inner = if let Some(runtime) = &self.runtime {
            Box::new(runtime.decorate(store, handle)) as Box<dyn ObjectStore>
        } else {
            Box::new(store) as Box<dyn ObjectStore>
        };

        let inner = Self::decorate_prefix(inner, table_root)?;

        Ok(inner)
    }

    pub(crate) fn decorate_prefix<T: ObjectStore>(
        store: T,
        table_root: &url::Url,
    ) -> DeltaResult<Box<dyn ObjectStore>> {
        let prefix = match ObjectStoreScheme::parse(table_root) {
            Ok((ObjectStoreScheme::AmazonS3, _)) => Path::parse(table_root.path())?,
            Ok((_, path)) => path,
            _ => Path::parse(table_root.path())?,
        };
        Ok(if prefix != Path::from("/") {
            Box::new(PrefixStore::new(store, prefix)) as Box<dyn ObjectStore>
        } else {
            Box::new(store) as Box<dyn ObjectStore>
        })
    }
}

impl<K, V> FromIterator<(K, V)> for StorageConfig
where
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut config = Self::default();
        config.raw = iter
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        let result = ParseResult::<RuntimeConfig>::from_iter(&config.raw);
        config.runtime = (!result.is_default).then_some(result.config);

        let result = ParseResult::<LimitConfig>::from_iter(result.unparsed);
        config.limit = (!result.is_default).then_some(result.config);

        let remainder = result.unparsed;

        #[cfg(feature = "delta-cache")]
        let remainder = {
            let result = ParseResult::<LogCacheConfig>::from_iter(remainder);
            config.cache = (!result.is_default).then_some(result.config);
            result.unparsed
        };

        #[cfg(feature = "cloud")]
        let remainder = {
            let result = ParseResult::<RetryConfig>::from_iter(remainder);
            config.retry = result.config;
            result.unparsed
        };

        config.unknown_properties = remainder;
        config
    }
}

impl StorageConfig {
    pub fn raw(&self) -> impl Iterator<Item = (&String, &String)> {
        self.raw.iter()
    }

    pub fn parse_options<K, V, I>(options: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str> + Into<String>,
        V: AsRef<str> + Into<String>,
    {
        let mut props = StorageConfig::default();
        props.raw = options
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        let (runtime, remainder): (RuntimeConfig, _) = try_parse_impl(&props.raw)?;
        // NOTE: we only want to assign an actual runtime config we consumed an option
        if props.raw.len() > remainder.len() {
            props.runtime = Some(runtime);
        }

        let result = ParseResult::<LimitConfig>::from_iter(remainder);
        result.raise_errors()?;
        props.limit = (!result.is_default).then_some(result.config);
        let remainder = result.unparsed;

        #[cfg(feature = "delta-cache")]
        let remainder = {
            let result = ParseResult::<LogCacheConfig>::from_iter(remainder);
            result.raise_errors()?;
            props.cache = (!result.is_default).then_some(result.config);
            result.unparsed
        };

        #[cfg(feature = "cloud")]
        let remainder = {
            let (retry, remainder): (RetryConfig, _) = try_parse_impl(remainder)?;
            props.retry = retry;
            remainder
        };

        props.unknown_properties = remainder;
        Ok(props)
    }
}

pub(super) fn try_parse_impl<T, K, V, I>(options: I) -> DeltaResult<(T, HashMap<String, String>)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
    T: TryUpdateKey + Default,
{
    let result = ParseResult::from_iter(options);
    result.raise_errors()?;
    Ok((result.config, result.unparsed))
}

pub(super) fn parse_usize(value: &str) -> DeltaResult<usize> {
    value
        .parse::<usize>()
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as usize")))
}

pub(super) fn parse_f64(value: &str) -> DeltaResult<f64> {
    value
        .parse::<f64>()
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as f64")))
}

pub(super) fn parse_duration(value: &str) -> DeltaResult<std::time::Duration> {
    humantime::parse_duration(value)
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as Duration")))
}

pub(super) fn parse_bool(value: &str) -> DeltaResult<bool> {
    Ok(super::storage::utils::str_is_truthy(value))
}

#[cfg(all(test, feature = "cloud"))]
mod tests {
    use maplit::hashmap;
    use object_store::RetryConfig;
    use std::time::Duration;

    #[test]
    fn test_retry_config_from_options() {
        let options = hashmap! {
            "max_retries".to_string() => "100".to_string() ,
            "retry_timeout".to_string()  => "300s".to_string() ,
            "backoff_config.init_backoff".to_string()  => "20s".to_string() ,
            "backoff_config.max_backoff".to_string()  => "1h".to_string() ,
            "backoff_config.base".to_string()  =>  "50.0".to_string() ,
        };
        let (retry_config, remainder): (RetryConfig, _) = super::try_parse_impl(options).unwrap();
        assert!(remainder.is_empty());

        assert_eq!(retry_config.max_retries, 100);
        assert_eq!(retry_config.retry_timeout, Duration::from_secs(300));
        assert_eq!(retry_config.backoff.init_backoff, Duration::from_secs(20));
        assert_eq!(retry_config.backoff.max_backoff, Duration::from_secs(3600));
        assert_eq!(retry_config.backoff.base, 50_f64);
    }
}
