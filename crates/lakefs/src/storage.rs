//! LakeFS storage backend (internally S3).

use deltalake_core::storage::object_store::aws::AmazonS3ConfigKey;
use deltalake_core::storage::{
    limit_store_handler, ObjectStoreFactory, ObjectStoreRef, RetryConfigParse, StorageOptions,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStoreScheme;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use tracing::log::*;
use url::Url;

#[derive(Clone, Default, Debug)]
pub struct LakeFSObjectStoreFactory {}

pub(crate) trait S3StorageOptionsConversion {
    fn with_env_s3(&self, options: &StorageOptions) -> StorageOptions {
        let mut options = StorageOptions(
            options
                .0
                .clone()
                .into_iter()
                .map(|(k, v)| {
                    if let Ok(config_key) = AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()) {
                        (config_key.as_ref().to_string(), v)
                    } else {
                        (k, v)
                    }
                })
                .collect(),
        );

        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                    if !options.0.contains_key(config_key.as_ref()) {
                        options
                            .0
                            .insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
            }
        }

        // Conditional put is supported in LakeFS since v1.47
        if !options.0.keys().any(|key| {
            let key = key.to_ascii_lowercase();
            [
                AmazonS3ConfigKey::ConditionalPut.as_ref(),
                "conditional_put",
            ]
            .contains(&key.as_str())
        }) {
            options.0.insert("conditional_put".into(), "etag".into());
        }
        options
    }
}

impl S3StorageOptionsConversion for LakeFSObjectStoreFactory {}

impl RetryConfigParse for LakeFSObjectStoreFactory {}

impl ObjectStoreFactory for LakeFSObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        storage_options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let options = self.with_env_s3(storage_options);

        // Convert LakeFS URI to equivalent S3 URI.
        let s3_url = url.to_string().replace("lakefs://", "s3://");

        let s3_url = Url::parse(&s3_url)
            .map_err(|_| DeltaTableError::InvalidTableLocation(url.clone().into()))?;

        // All S3-likes should start their builder the same way
        let config = options
            .clone()
            .0
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

        let mut builder = AmazonS3Builder::new().with_url(s3_url.to_string());

        for (key, value) in config.iter() {
            builder = builder.with_config(*key, value.clone());
        }

        let inner = builder
            .with_retry(self.parse_retry_config(&options)?)
            .build()?;

        let store = limit_store_handler(inner, &options);
        debug!("Initialized the object store: {store:?}");
        Ok((store, prefix))
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
            std::env::set_var("ACCESS_KEY_ID", "env_key");
            std::env::set_var("ENDPOINT", "env_key");
            std::env::set_var("SECRET_ACCESS_KEY", "env_key");
            std::env::set_var("REGION", "env_key");
            let combined_options =
                LakeFSObjectStoreFactory {}.with_env_s3(&StorageOptions(raw_options));

            // Four and then the conditional_put built-in
            assert_eq!(combined_options.0.len(), 5);

            for (key, v) in combined_options.0 {
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
            std::env::set_var("aws_access_key_id", "env_key");
            std::env::set_var("aws_endpoint", "env_key");
            std::env::set_var("aws_secret_access_key", "env_key");
            std::env::set_var("aws_region", "env_key");

            let combined_options =
                LakeFSObjectStoreFactory {}.with_env_s3(&StorageOptions(raw_options));

            for (key, v) in combined_options.0 {
                if key != "conditional_put" {
                    assert_eq!(v, "options_key");
                }
            }
        });
    }
}
