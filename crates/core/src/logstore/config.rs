//! Configuration for the Delta Log Store.
//!
//! This module manages the various pieces of configuration for the Delta Log Store.
//! It provides methods for parsing and updating configuration settings. All configuration
//! is parsed from String -> String mappings.
//!
//! Specific pieces of configuration must implement the `TryUpdateKey` trait which
//! defines how to update internal fields based on key-value pairs.
#[cfg(feature = "cloud")]
use ::object_store::RetryConfig;
use object_store::{ObjectStore, path::Path, prefix::PrefixStore};
use std::collections::HashMap;

use super::storage::LimitConfig;
use super::{IORuntime, storage::runtime::RuntimeConfig};
use crate::{DeltaResult, DeltaTableError};

pub trait TryUpdateKey: Default {
    /// Update an internal field in the configuration.
    ///
    /// ## Returns
    /// - `Ok(Some(()))` if the key was updated.
    /// - `Ok(None)` if the key was not found and no internal field was updated.
    /// - `Err(_)` if the update failed. Failed updates may include finding a known key,
    ///   but failing to parse the value into the expected type.
    fn try_update_key(&mut self, key: &str, value: &str) -> DeltaResult<Option<()>>;

    /// Load configuration values from environment variables
    ///
    /// For Option<T> fields, this will only set values that are None
    /// For non-optional fields, environment variables will update the
    /// value if the current value corresponds to the default value.
    fn load_from_environment(&mut self) -> DeltaResult<()>;
}

#[derive(Debug)]
/// Generic container for parsing configuration
pub struct ParseResult<T: std::fmt::Debug> {
    /// Parsed configuration
    pub config: T,
    /// Unrecognized key value pairs.
    pub unparsed: HashMap<String, String>,
    /// Errors encountered during parsing
    pub errors: Vec<(String, String)>,
    /// Whether the configuration is defaults only - i.e. no custom values were provided
    pub is_default: bool,
}

impl<T: std::fmt::Debug> ParseResult<T> {
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

impl<T: std::fmt::Debug, K, V> FromIterator<(K, V)> for ParseResult<T>
where
    T: TryUpdateKey,
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
    /// Runtime configuration.
    ///
    /// Configuration to set up a dedicated IO runtime to execute IO related operations or
    /// dedicated handle.
    pub runtime: Option<IORuntime>,

    #[cfg(feature = "cloud")]
    pub retry: ::object_store::RetryConfig,

    /// Limit configuration.
    ///
    /// Configuration to limit the number of concurrent requests to the object store.
    pub limit: Option<LimitConfig>,

    /// Properties that are not recognized by the storage configuration.
    ///
    /// These properties are ignored by the storage configuration and can be used for custom purposes.
    pub unknown_properties: HashMap<String, String>,

    /// Original unprocessed properties.
    ///
    /// Since we remove properties during processing, but downstream integrations may
    /// use them for their own purposes, we keep a copy of the original properties.
    pub raw: HashMap<String, String>,
}

impl StorageConfig {
    /// Wrap an object store with additional layers of functionality.
    ///
    /// Depending on the configuration, the following layers may be added:
    /// - Retry layer: Adds retry logic to the object store.
    /// - Limit layer: Limits the number of concurrent requests to the object store.
    pub fn decorate_store<T: ObjectStore + Clone>(
        &self,
        store: T,
        table_root: &url::Url,
    ) -> DeltaResult<Box<dyn ObjectStore>> {
        let inner = Self::decorate_prefix(store, table_root)?;
        Ok(inner)
    }

    pub(crate) fn decorate_prefix<T: ObjectStore>(
        store: T,
        table_root: &url::Url,
    ) -> DeltaResult<Box<dyn ObjectStore>> {
        let prefix = super::object_store_path(table_root)?;
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
        let mut config = StorageConfig {
            raw: iter
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            ..Default::default()
        };

        let result = ParseResult::<RuntimeConfig>::from_iter(&config.raw);
        if let Some(runtime_config) = (!result.is_default).then_some(result.config) {
            config.runtime = Some(IORuntime::Config(runtime_config));
        };

        let result = ParseResult::<LimitConfig>::from_iter(result.unparsed);
        config.limit = (!result.is_default).then_some(result.config);

        let remainder = result.unparsed;

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

    /// Parse options into a StorageConfig.
    ///
    /// This method will raise if it cannot parse a value. StorageConfig can also
    /// be constructed from an iterator of key-value pairs which will ignore any
    /// parsing errors.
    ///
    /// # Raises
    ///
    /// Raises a `DeltaError` if any of the options are invalid - i.e. cannot be parsed into target type.
    pub fn parse_options<K, V, I>(options: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str> + Into<String>,
        V: AsRef<str> + Into<String>,
    {
        let mut props = StorageConfig {
            raw: options
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            ..Default::default()
        };

        let (runtime, remainder): (RuntimeConfig, _) = try_parse_impl(&props.raw)?;
        // NOTE: we only want to assign an actual runtime config we consumed an option
        if props.raw.len() > remainder.len() {
            props.runtime = Some(IORuntime::Config(runtime));
        }

        let result = ParseResult::<LimitConfig>::from_iter(remainder);
        result.raise_errors()?;
        props.limit = (!result.is_default).then_some(result.config);
        let remainder = result.unparsed;

        #[cfg(feature = "cloud")]
        let remainder = {
            let (retry, remainder): (RetryConfig, _) = try_parse_impl(remainder)?;
            props.retry = retry;
            remainder
        };

        props.unknown_properties = remainder;
        Ok(props)
    }

    // Provide an IO Runtime directly
    pub fn with_io_runtime(mut self, rt: IORuntime) -> Self {
        self.runtime = Some(rt);
        self
    }
}

pub(super) fn try_parse_impl<T: std::fmt::Debug, K, V, I>(
    options: I,
) -> DeltaResult<(T, HashMap<String, String>)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
    T: TryUpdateKey,
{
    let result = ParseResult::from_iter(options);
    result.raise_errors()?;
    Ok((result.config, result.unparsed))
}

pub fn parse_usize(value: &str) -> DeltaResult<usize> {
    value
        .parse::<usize>()
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as usize")))
}

pub fn parse_f64(value: &str) -> DeltaResult<f64> {
    value
        .parse::<f64>()
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as f64")))
}

#[cfg(feature = "cloud")]
pub fn parse_duration(value: &str) -> DeltaResult<std::time::Duration> {
    humantime::parse_duration(value)
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as Duration")))
}

pub fn parse_bool(value: &str) -> DeltaResult<bool> {
    Ok(str_is_truthy(value))
}

pub fn parse_string(value: &str) -> DeltaResult<String> {
    Ok(value.to_string())
}

/// Return true for all the stringly values typically associated with true
///
/// aka YAML booleans
///
/// ```rust
/// # use deltalake_core::logstore::config::*;
/// for value in ["1", "true", "on", "YES", "Y"] {
///     assert!(str_is_truthy(value));
/// }
/// for value in ["0", "FALSE", "off", "NO", "n", "bork"] {
///     assert!(!str_is_truthy(value));
/// }
/// ```
pub fn str_is_truthy(val: &str) -> bool {
    val.eq_ignore_ascii_case("1")
        | val.eq_ignore_ascii_case("true")
        | val.eq_ignore_ascii_case("on")
        | val.eq_ignore_ascii_case("yes")
        | val.eq_ignore_ascii_case("y")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "cloud")]
    use std::time::Duration;

    // Test retry config parsing
    #[cfg(feature = "cloud")]
    #[test]
    fn test_retry_config_from_options() {
        use object_store::RetryConfig;
        let options = HashMap::from([
            ("max_retries".to_string(), "100".to_string()),
            ("retry_timeout".to_string(), "300s".to_string()),
            ("backoff_config.init_backoff".to_string(), "20s".to_string()),
            ("backoff_config.max_backoff".to_string(), "1h".to_string()),
            ("backoff_config.base".to_string(), "50.0".to_string()),
        ]);
        let (retry_config, remainder): (RetryConfig, _) = super::try_parse_impl(options).unwrap();
        assert!(remainder.is_empty());

        assert_eq!(retry_config.max_retries, 100);
        assert_eq!(retry_config.retry_timeout, Duration::from_secs(300));
        assert_eq!(retry_config.backoff.init_backoff, Duration::from_secs(20));
        assert_eq!(retry_config.backoff.max_backoff, Duration::from_secs(3600));
        assert_eq!(retry_config.backoff.base, 50_f64);
    }

    // Test ParseResult functionality
    #[cfg(feature = "cloud")]
    #[test]
    fn test_parse_result_handling() {
        use object_store::RetryConfig;
        let options = HashMap::from([
            ("retry_timeout".to_string(), "300s".to_string()),
            ("max_retries".to_string(), "not_a_number".to_string()),
            ("unknown_key".to_string(), "value".to_string()),
        ]);

        let result: ParseResult<RetryConfig> = options.into_iter().collect();
        println!("result: {result:?}");
        assert!(!result.errors.is_empty());
        assert!(!result.unparsed.is_empty());
        assert!(!result.is_default);

        assert!(result.raise_errors().is_err());
    }

    // Test StorageConfig parsing
    #[cfg(feature = "cloud")]
    #[test]
    fn test_storage_config_parsing() {
        let options = HashMap::from([
            ("max_retries".to_string(), "5".to_string()),
            ("retry_timeout".to_string(), "10s".to_string()),
            ("unknown_prop".to_string(), "value".to_string()),
        ]);

        let config = StorageConfig::parse_options(options).unwrap();
        assert_eq!(config.retry.max_retries, 5);
        assert_eq!(config.retry.retry_timeout, Duration::from_secs(10));
        assert!(config.unknown_properties.contains_key("unknown_prop"));
    }

    // Test utility parsing functions
    #[test]
    #[allow(clippy::approx_constant)]
    fn test_parsing_utilities() {
        assert_eq!(parse_usize("42").unwrap(), 42);
        assert!(parse_usize("not_a_number").is_err());

        assert_eq!(parse_f64("3.14").unwrap(), 3.14);
        assert!(parse_f64("not_a_number").is_err());

        assert!(parse_bool("true").unwrap());
        assert!(parse_bool("1").unwrap());
        assert!(!parse_bool("false").unwrap());
        assert!(!parse_bool("0").unwrap());

        assert_eq!(parse_string("test").unwrap(), "test");
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_parsing_duration() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert!(parse_duration("invalid").is_err());
    }

    // Test str_is_truthy function
    #[test]
    fn test_str_is_truthy() {
        let truthy_values = ["1", "true", "on", "YES", "Y", "True", "ON"];
        let falsy_values = ["0", "false", "off", "NO", "n", "bork", "False", "OFF"];

        for value in truthy_values {
            assert!(str_is_truthy(value), "{value} should be truthy");
        }

        for value in falsy_values {
            assert!(!str_is_truthy(value), "{value} should be falsy");
        }
    }

    // Test StorageConfig with IO runtime
    #[test]
    fn test_storage_config_with_io_runtime() {
        let config =
            StorageConfig::default().with_io_runtime(IORuntime::Config(RuntimeConfig::default()));
        assert!(config.runtime.is_some());
    }
}
