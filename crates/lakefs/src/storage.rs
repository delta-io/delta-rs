//! LakeFS storage backend (internally S3).

use deltalake_core::logstore::object_store::aws::AmazonS3ConfigKey;
use deltalake_core::logstore::{ObjectStoreFactory, ObjectStoreRef, StorageConfig};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::ObjectStoreScheme;
use object_store::aws::AmazonS3Builder;
use object_store::client::SpawnedReqwestConnector;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use tracing::log::*;
use url::Url;

#[derive(Clone, Default, Debug)]
pub struct LakeFSObjectStoreFactory {}

pub(crate) trait S3StorageOptionsConversion {
    fn with_env_s3(&self, options: &HashMap<String, String>) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = options
            .clone()
            .into_iter()
            .map(|(k, v)| {
                if let Ok(config_key) = AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()) {
                    (config_key.as_ref().to_string(), v)
                } else {
                    (k, v)
                }
            })
            .collect();

        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str())
                && let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
                && !options.contains_key(config_key.as_ref())
            {
                options.insert(config_key.as_ref().to_string(), value.to_string());
            }
        }

        // Conditional put is supported in LakeFS since v1.47
        if !options.keys().any(|key| {
            let key = key.to_ascii_lowercase();
            [
                AmazonS3ConfigKey::ConditionalPut.as_ref(),
                "conditional_put",
            ]
            .contains(&key.as_str())
        }) {
            options.insert("conditional_put".into(), "etag".into());
        }
        options
    }
}

impl S3StorageOptionsConversion for LakeFSObjectStoreFactory {}

impl ObjectStoreFactory for LakeFSObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        // Convert LakeFS URI to equivalent S3 URI.
        let s3_url = url.to_string().replace("lakefs://", "s3://");
        let s3_url = Url::parse(&s3_url)
            .map_err(|_| DeltaTableError::InvalidTableLocation(url.clone().into()))?;
        let mut builder = AmazonS3Builder::new().with_url(s3_url.to_string());

        // All S3-likes should start their builder the same way
        let options = self.with_env_s3(&config.raw);
        builder = builder.with_retry(config.retry.clone());
        if let Some(runtime) = &config.runtime {
            builder =
                builder.with_http_connector(SpawnedReqwestConnector::new(runtime.get_handle()));
        }

        let config = options
            .clone()
            .into_iter()
            .filter_map(|(k, v)| {
                if let Ok(key) = AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()) {
                    Some((key, v))
                } else {
                    None
                }
            })
            .collect::<HashMap<AmazonS3ConfigKey, String>>();

        let (_, path) =
            ObjectStoreScheme::parse(&s3_url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;

        for (key, value) in config.iter() {
            builder = builder.with_config(*key, value.clone());
        }

        let store = builder.build()?;

        debug!("Initialized the object store: {store:?}");
        Ok((Arc::new(store), prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use maplit::hashmap;
    use serial_test::serial;

    struct ScopedEnv {}

    impl ScopedEnv {
        pub fn new() -> Self {
            Self {}
        }

        pub fn run<T>(mut f: impl FnMut() -> T) -> T {
            let _env_scope = Self::new();
            f()
        }
    }

    fn clear_env_of_lakefs_s3_keys() {
        let keys_to_clear = std::env::vars().filter_map(|(k, _v)| {
            if AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()).is_ok() {
                Some(k)
            } else {
                None
            }
        });

        for k in keys_to_clear {
            unsafe {
                std::env::remove_var(k);
            }
        }
    }

    #[test]
    #[serial]
    fn when_merging_with_env_unsupplied_options_are_added() {
        ScopedEnv::run(|| {
            clear_env_of_lakefs_s3_keys();
            let raw_options = hashmap! {};
            unsafe {
                std::env::set_var("ACCESS_KEY_ID", "env_key");
                std::env::set_var("ENDPOINT", "env_key");
                std::env::set_var("SECRET_ACCESS_KEY", "env_key");
                std::env::set_var("REGION", "env_key");
            }
            let combined_options = LakeFSObjectStoreFactory {}.with_env_s3(&raw_options);

            // Four and then the conditional_put built-in
            assert_eq!(combined_options.len(), 5);

            for (key, v) in combined_options {
                if key != "conditional_put" {
                    assert_eq!(v, "env_key");
                }
            }
        });
    }

    #[tokio::test]
    #[serial]
    async fn when_merging_with_env_supplied_options_take_precedence() {
        ScopedEnv::run(|| {
            clear_env_of_lakefs_s3_keys();
            let raw_options = hashmap! {
                "ACCESS_KEY_ID".to_string() => "options_key".to_string(),
                "ENDPOINT_URL".to_string() => "options_key".to_string(),
                "SECRET_ACCESS_KEY".to_string() => "options_key".to_string(),
                "REGION".to_string() => "options_key".to_string()
            };
            unsafe {
                std::env::set_var("aws_access_key_id", "env_key");
                std::env::set_var("aws_endpoint", "env_key");
                std::env::set_var("aws_secret_access_key", "env_key");
                std::env::set_var("aws_region", "env_key");
            }

            let combined_options = LakeFSObjectStoreFactory {}.with_env_s3(&raw_options);

            for (key, v) in combined_options {
                if key != "conditional_put" {
                    assert_eq!(v, "options_key");
                }
            }
        });
    }
}
