//! Custom AWS credential providers used by delta-rs
//!

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::sts::AssumeRoleProvider;
use aws_config::SdkConfig;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{future, ProvideCredentials};
use aws_credential_types::Credentials;

use deltalake_core::logstore::object_store::aws::{AmazonS3ConfigKey, AwsCredential};
use deltalake_core::logstore::object_store::{
    CredentialProvider, Error as ObjectStoreError, Result as ObjectStoreResult,
};
use deltalake_core::DeltaResult;
use tokio::sync::Mutex;
use tracing::log::*;

use crate::constants;

/// An [object_store::CredentialProvider] which handles converting a populated [SdkConfig]
/// into a necessary [AwsCredential] type for configuring [object_store::aws::AmazonS3]
#[derive(Clone, Debug)]
pub(crate) struct AWSForObjectStore {
    /// TODO: replace this with something with a credential cache instead of the sdkConfig
    sdk_config: SdkConfig,
    cache: Arc<Mutex<Option<Credentials>>>,
}

impl AWSForObjectStore {
    pub(crate) fn new(sdk_config: SdkConfig) -> Self {
        let cache = Arc::new(Mutex::new(None));
        Self { sdk_config, cache }
    }

    /// Return true if a credential has been cached
    #[cfg(test)]
    async fn has_cached_credentials(&self) -> bool {
        let guard = self.cache.lock().await;
        (*guard).is_some()
    }
}

#[async_trait::async_trait]
impl CredentialProvider for AWSForObjectStore {
    type Credential = AwsCredential;

    /// Provide the necessary configured credentials from the AWS SDK for use by
    /// [object_store::aws::AmazonS3]
    async fn get_credential(&self) -> ObjectStoreResult<Arc<Self::Credential>> {
        debug!("AWSForObjectStore is unlocking..");
        let mut guard = self.cache.lock().await;

        if let Some(cached) = guard.as_ref() {
            debug!("Located cached credentials");
            let now = SystemTime::now();

            // Credentials such as assume role credentials will have an expiry on them, whereas
            // environmental provided credentials will *not*.  In the latter case, it's still
            // useful avoid running through the provider chain again, so in both cases we should
            // still treat credentials as useful
            if cached.expiry().unwrap_or(now) >= now {
                debug!("Cached credentials are still valid, returning");
                return Ok(Arc::new(Self::Credential {
                    key_id: cached.access_key_id().into(),
                    secret_key: cached.secret_access_key().into(),
                    token: cached.session_token().map(|o| o.to_string()),
                }));
            } else {
                debug!("Cached credentials appear to no longer be valid, re-resolving");
            }
        }

        let provider = self
            .sdk_config
            .credentials_provider()
            .ok_or(ObjectStoreError::NotImplemented)?;

        let credentials =
            provider
                .provide_credentials()
                .await
                .map_err(|e| ObjectStoreError::NotSupported {
                    source: Box::new(e),
                })?;

        debug!(
            "CredentialProvider for Object Store using access key: {}",
            credentials.access_key_id()
        );

        let result = Ok(Arc::new(Self::Credential {
            key_id: credentials.access_key_id().into(),
            secret_key: credentials.secret_access_key().into(),
            token: credentials.session_token().map(|o| o.to_string()),
        }));

        // Update the mutex before exiting with the new Credentials from the AWS provider
        *guard = Some(credentials);
        return result;
    }
}

/// Name of the [OptionsCredentialsProvider] for AWS SDK use
const OPTS_PROVIDER: &str = "DeltaStorageOptionsProvider";

/// The [OptionsCredentialsProvider] helps users plug specific AWS credentials into their
/// [StorageOptions] in such a way that the AWS SDK code will be properly
/// loaded with those credentials before following the
/// [aws_config::default_provider::credentials::DefaultCredentialsChain]
#[derive(Clone, Debug)]
pub(crate) struct OptionsCredentialsProvider {
    options: HashMap<String, String>,
}

impl OptionsCredentialsProvider {
    /// Look at the options configured on the provider and return an appropriate
    /// [Credentials] instance for AWS SDK credential resolution
    fn credentials(&self) -> aws_credential_types::provider::Result {
        debug!("Attempting to pull credentials from `StorageOptions`");

        // StorageOptions can have a variety of flavors because
        // [object_store::aws::AmazonS3ConfigKey] supports a couple different variants for key
        // names.
        let config_keys: HashMap<AmazonS3ConfigKey, String> =
            HashMap::from_iter(self.options.iter().filter_map(|(k, v)| {
                match AmazonS3ConfigKey::from_str(&k.to_lowercase()) {
                    Ok(k) => Some((k, v.into())),
                    Err(_) => None,
                }
            }));

        let access_key = config_keys.get(&AmazonS3ConfigKey::AccessKeyId).ok_or(
            CredentialsError::not_loaded("access key not in StorageOptions"),
        )?;
        let secret_key = config_keys.get(&AmazonS3ConfigKey::SecretAccessKey).ok_or(
            CredentialsError::not_loaded("secret key not in StorageOptions"),
        )?;
        let session_token = config_keys.get(&AmazonS3ConfigKey::Token).cloned();

        Ok(Credentials::new(
            access_key,
            secret_key,
            session_token,
            None,
            OPTS_PROVIDER,
        ))
    }
}

impl ProvideCredentials for OptionsCredentialsProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::ready(self.credentials())
    }
}

#[cfg(test)]
mod options_tests {
    use super::*;

    #[test]
    fn test_empty_options_error() {
        let options = HashMap::default();
        let provider = OptionsCredentialsProvider { options };
        let result = provider.credentials();
        assert!(
            result.is_err(),
            "The default StorageOptions don't have credentials!"
        );
    }

    #[test]
    fn test_uppercase_options_resolve() {
        let options = HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), "key".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "secret".into()),
        ]);
        let provider = OptionsCredentialsProvider { options };
        let result = provider.credentials();
        assert!(result.is_ok(), "StorageOptions with at least AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY should resolve");
        let result = result.unwrap();
        assert_eq!(result.access_key_id(), "key");
        assert_eq!(result.secret_access_key(), "secret");
    }

    #[test]
    fn test_lowercase_options_resolve() {
        let options = HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), "key".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "secret".into()),
        ]);
        let provider = OptionsCredentialsProvider { options };
        let result = provider.credentials();
        assert!(result.is_ok(), "StorageOptions with at least AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY should resolve");
        let result = result.unwrap();
        assert_eq!(result.access_key_id(), "key");
        assert_eq!(result.secret_access_key(), "secret");
    }
}

/// Generate a random session name for assuming IAM roles
fn assume_role_session_name() -> String {
    let now = chrono::Utc::now();

    format!("delta-rs_{}", now.timestamp_millis())
}

/// Return the configured IAM role ARN or whatever is defined in the environment
fn assume_role_arn(options: &HashMap<String, String>) -> Option<String> {
    options
        .get(constants::AWS_IAM_ROLE_ARN)
        .or(
            #[allow(deprecated)]
            options.get(constants::AWS_S3_ASSUME_ROLE_ARN),
        )
        .or(std::env::var_os(constants::AWS_IAM_ROLE_ARN)
            .map(|o| {
                o.into_string()
                    .expect("Failed to unwrap AWS_IAM_ROLE_ARN which may have invalid data")
            })
            .as_ref())
        .or(
            #[allow(deprecated)]
            std::env::var_os(constants::AWS_S3_ASSUME_ROLE_ARN)
                .map(|o| {
                    o.into_string().expect(
                        "Failed to unwrap AWS_S3_ASSUME_ROLE_ARN which may have invalid data",
                    )
                })
                .as_ref(),
        )
        .cloned()
}

/// Return the configured IAM assume role session name or provide a unique one
fn assume_session_name(options: &HashMap<String, String>) -> String {
    let assume_session = options
        .get(constants::AWS_IAM_ROLE_SESSION_NAME)
        .or(
            #[allow(deprecated)]
            options.get(constants::AWS_S3_ROLE_SESSION_NAME),
        )
        .cloned();
    assume_session.unwrap_or_else(assume_role_session_name)
}

/// Take a set of [StorageOptions] and produce an appropriate AWS SDK [SdkConfig]
/// for use with various AWS SDK APIs, such as in our [S3DynamoDbLogStore](crate::logstore::S3DynamoDbLogStore)
pub async fn resolve_credentials(options: &HashMap<String, String>) -> DeltaResult<SdkConfig> {
    let default_provider = DefaultCredentialsChain::builder().build().await;

    let credentials_provider = match assume_role_arn(options) {
        Some(arn) => {
            debug!("Configuring AssumeRoleProvider with role arn: {arn}");
            CredentialsProviderChain::first_try(
                "AssumeRoleProvider",
                AssumeRoleProvider::builder(arn)
                    .session_name(assume_session_name(options))
                    .build()
                    .await,
            )
            .or_else(
                "StorageOptions",
                OptionsCredentialsProvider {
                    options: options.clone(),
                },
            )
            .or_else("DefaultChain", default_provider)
        }
        None => CredentialsProviderChain::first_try(
            "StorageOptions",
            OptionsCredentialsProvider {
                options: options.clone(),
            },
        )
        .or_else("DefaultChain", default_provider),
    };

    Ok(aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_options_credentials_provider() {
        let options = HashMap::from([
            (
                constants::AWS_ACCESS_KEY_ID.to_string(),
                "test_id".to_string(),
            ),
            (
                constants::AWS_SECRET_ACCESS_KEY.to_string(),
                "test_secret".to_string(),
            ),
        ]);

        let config = resolve_credentials(&options).await;
        assert!(config.is_ok(), "{config:?}");
        let config = config.unwrap();

        if let Some(provider) = &config.credentials_provider() {
            let credentials = provider
                .provide_credentials()
                .await
                .expect("Failed to provide credentials");
            assert_eq!(
                "test_id",
                credentials.access_key_id(),
                "The access key should come from our options! {credentials:?}"
            );
            assert_eq!(
                "test_secret",
                credentials.secret_access_key(),
                "The secret should come from our options! {credentials:?}"
            );
        } else {
            panic!("Could not retrieve credentials from the SdkConfig: {config:?}");
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_options_credentials_provider_session_token() {
        let options = HashMap::from([
            (
                constants::AWS_ACCESS_KEY_ID.to_string(),
                "test_id".to_string(),
            ),
            (
                constants::AWS_SECRET_ACCESS_KEY.to_string(),
                "test_secret".to_string(),
            ),
            (
                constants::AWS_SESSION_TOKEN.to_string(),
                "test_token".to_string(),
            ),
        ]);

        let config = resolve_credentials(&options)
            .await
            .expect("Failed to resolve_credentials");

        if let Some(provider) = &config.credentials_provider() {
            let credentials = provider
                .provide_credentials()
                .await
                .expect("Failed to provide credentials");
            assert_eq!(
                Some("test_token"),
                credentials.session_token(),
                "The session token should come from our options! {credentials:?}"
            );
        } else {
            panic!("Could not retrieve credentials from the SdkConfig: {config:?}");
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_object_store_credential_provider() -> DeltaResult<()> {
        let options = HashMap::from([
            (
                constants::AWS_ACCESS_KEY_ID.to_string(),
                "test_id".to_string(),
            ),
            (
                constants::AWS_SECRET_ACCESS_KEY.to_string(),
                "test_secret".to_string(),
            ),
        ]);
        let sdk_config = resolve_credentials(&options)
            .await
            .expect("Failed to resolve credentials for the test");
        let provider = AWSForObjectStore::new(sdk_config);
        let _credential = provider
            .get_credential()
            .await
            .expect("Failed to produce a credential");
        Ok(())
    }

    /// The [CredentialProvider] is called _repeatedly_ by the [object_store] create, in essence on
    /// every get/put/list/etc operation, the `get_credential` function will be invoked.
    ///
    /// In some cases, such as when assuming roles, this can result in an excessive amount of STS
    /// API calls in the scenarios where the delta-rs process is performing a large number of S3
    /// operations.
    #[tokio::test]
    #[serial]
    async fn test_object_store_credential_provider_consistency() -> DeltaResult<()> {
        let options = HashMap::from([
            (
                constants::AWS_ACCESS_KEY_ID.to_string(),
                "test_id".to_string(),
            ),
            (
                constants::AWS_SECRET_ACCESS_KEY.to_string(),
                "test_secret".to_string(),
            ),
        ]);
        let sdk_config = resolve_credentials(&options)
            .await
            .expect("Failed to resolve credentijals for the test");
        let provider = AWSForObjectStore::new(sdk_config);
        let credential_a = provider
            .get_credential()
            .await
            .expect("Failed to produce a credential");

        assert!(
            provider.has_cached_credentials().await,
            "The provider should have cached the credential on the first call!"
        );

        let credential_b = provider
            .get_credential()
            .await
            .expect("Failed to produce a credential");

        assert_ne!(
            Arc::as_ptr(&credential_a),
            Arc::as_ptr(&credential_b),
            "Repeated calls to get_credential() produced different results!"
        );
        Ok(())
    }
}
