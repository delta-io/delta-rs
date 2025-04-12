use std::collections::HashMap;

#[cfg(feature = "delta-cache")]
use super::storage::cache::LogCacheConfig;
use super::storage::runtime::RuntimeConfig;
use super::StorageOptions;
use crate::{DeltaResult, DeltaTableError};

#[derive(Default)]
pub struct StorageConfig {
    #[cfg(feature = "delta-cache")]
    pub cache: LogCacheConfig,

    pub runtime: Option<RuntimeConfig>,

    #[cfg(feature = "cloud")]
    pub retry: ::object_store::RetryConfig,

    pub unknown_properties: HashMap<String, String>,
}

impl StorageConfig {
    pub fn parse_runtime<K, V, I>(options: I) -> DeltaResult<RuntimeConfig>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str> + Into<String>,
        V: AsRef<str> + Into<String>,
    {
        parse_runtime(options).map(|c| c.0)
    }

    #[cfg(feature = "cloud")]
    pub fn parse_retry<K, V, I>(options: I) -> DeltaResult<::object_store::RetryConfig>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str> + Into<String>,
        V: AsRef<str> + Into<String>,
    {
        cloud::parse_retry(options).map(|c| c.0)
    }

    #[cfg(feature = "delta-cache")]
    pub fn parse_log_cache<K, V, I>(
        options: &HashMap<String, String>,
    ) -> DeltaResult<LogCacheConfig>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str> + Into<String>,
        V: AsRef<str> + Into<String>,
    {
        parse_log_cache(options).map(|c| c.0)
    }

    pub fn try_from_options(options: &HashMap<String, String>) -> DeltaResult<Self> {
        let mut props = StorageConfig::default();

        let opt_count = options.len();
        let (runtime, remainder) = parse_runtime(options)?;
        // NOTE: we only want to assign an actual runtime config we consumed an option
        if opt_count > remainder.len() {
            props.runtime = Some(runtime);
        }

        #[cfg(feature = "delta-cache")]
        let remainder = {
            let (cache_config, remainder) = parse_log_cache(remainder)?;
            props.cache = cache_config;
            remainder
        };

        #[cfg(feature = "cloud")]
        let remainder = {
            let (retry_config, remainder) = cloud::parse_retry(remainder)?;
            props.retry = retry_config;
            remainder
        };

        props.unknown_properties = remainder;
        Ok(props)
    }

    pub fn try_from_storage_options(options: &StorageOptions) -> DeltaResult<Self> {
        Self::try_from_options(&options.0)
    }
}

#[cfg(feature = "delta-cache")]
fn parse_log_cache<K, V, I>(options: I) -> DeltaResult<(LogCacheConfig, HashMap<String, String>)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn try_parse(props: &mut LogCacheConfig, k: &str, v: &str) -> DeltaResult<Option<()>> {
        match k {
            "max_size_memory_mb" => props.max_size_memory_mb = parse_usize(v)?,
            "max_size_disk_mb" => props.max_size_disk_mb = parse_usize(v)?,
            "directory" => props.directory = Some(v.into()),
            _ => return Ok(None),
        }
        Ok(Some(()))
    }

    parse_options_impl(options, try_parse)
}

fn parse_runtime<K, V, I>(options: I) -> DeltaResult<(RuntimeConfig, HashMap<String, String>)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
{
    fn try_parse(props: &mut RuntimeConfig, k: &str, v: &str) -> DeltaResult<Option<()>> {
        match k {
            "multi_threaded" => props.multi_threaded = Some(parse_bool(v)?),
            "worker_threads" => props.worker_threads = Some(parse_usize(v)?),
            "thread_name" => props.thread_name = Some(v.into()),
            "enable_io" => props.enable_io = Some(parse_bool(v)?),
            "enable_time" => props.enable_time = Some(parse_bool(v)?),
            _ => return Ok(None),
        }
        Ok(Some(()))
    }

    parse_options_impl(options, try_parse)
}

fn parse_options_impl<T, K, V, I, F>(
    options: I,
    parse_fn: F,
) -> DeltaResult<(T, HashMap<String, String>)>
where
    F: Fn(&mut T, &str, &str) -> DeltaResult<Option<()>>,
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str> + Into<String>,
    V: AsRef<str> + Into<String>,
    T: Default,
{
    let mut config = T::default();
    let mut unparsed = HashMap::new();
    for (k, v) in options {
        if parse_fn(&mut config, k.as_ref(), v.as_ref())?.is_none() {
            unparsed.insert(k.into(), v.into());
        }
    }
    Ok((config, unparsed))
}

fn parse_usize(value: &str) -> DeltaResult<usize> {
    value
        .parse::<usize>()
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as usize")))
}

fn parse_f64(value: &str) -> DeltaResult<f64> {
    value
        .parse::<f64>()
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as f64")))
}

fn parse_duration(value: &str) -> DeltaResult<std::time::Duration> {
    humantime::parse_duration(value)
        .map_err(|_| DeltaTableError::Generic(format!("failed to parse \"{value}\" as Duration")))
}

fn parse_bool(value: &str) -> DeltaResult<bool> {
    Ok(super::storage::utils::str_is_truthy(value))
}

#[cfg(feature = "cloud")]
mod cloud {
    use ::object_store::RetryConfig;

    use super::*;

    pub(super) fn parse_retry<K, V, I>(
        options: I,
    ) -> DeltaResult<(RetryConfig, HashMap<String, String>)>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str> + Into<String>,
        V: AsRef<str> + Into<String>,
    {
        fn try_parse(props: &mut RetryConfig, k: &str, v: &str) -> DeltaResult<Option<()>> {
            match k {
                "max_retries" => props.max_retries = parse_usize(v)?,
                "retry_timeout" => props.retry_timeout = parse_duration(v)?,
                "init_backoff" | "backoff_config.init_backoff" | "backoff.init_backoff" => {
                    props.backoff.init_backoff = parse_duration(v)?
                }
                "max_backoff" | "backoff_config.max_backoff" | "backoff.max_backoff" => {
                    props.backoff.max_backoff = parse_duration(v)?;
                }
                "base" | "backoff_config.base" | "backoff.base" => {
                    props.backoff.base = parse_f64(v)?;
                }
                _ => return Ok(None),
            }
            Ok(Some(()))
        }

        parse_options_impl(options, try_parse)
    }

    #[cfg(test)]
    mod tests {
        use maplit::hashmap;
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
            let (retry_config, remainder) = super::parse_retry(&options).unwrap();
            assert!(remainder.is_empty());

            assert_eq!(retry_config.max_retries, 100);
            assert_eq!(retry_config.retry_timeout, Duration::from_secs(300));
            assert_eq!(retry_config.backoff.init_backoff, Duration::from_secs(20));
            assert_eq!(retry_config.backoff.max_backoff, Duration::from_secs(3600));
            assert_eq!(retry_config.backoff.base, 50_f64);
        }
    }
}
