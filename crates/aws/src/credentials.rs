//! Custom AWS credential providers used by delta-rs
//!

use std::sync::Arc;

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::sts::AssumeRoleProvider;
use aws_config::SdkConfig;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{future, ProvideCredentials};
use aws_credential_types::Credentials;

use deltalake_core::storage::object_store::aws::{AmazonS3ConfigKey, AwsCredential};
use deltalake_core::storage::object_store::{
    CredentialProvider, Error as ObjectStoreError, Result as ObjectStoreResult,
};
use deltalake_core::storage::StorageOptions;
use deltalake_core::DeltaResult;
use tracing::log::*;

use crate::constants;

/// An [object_store::CredentialProvider] which handles converting a populated [SdkConfig]
/// into a necessary [AwsCredential] type for configuring [object_store::aws::AmazonS3]
#[derive(Clone, Debug)]
pub(crate) struct AWSForObjectStore {
    sdk_config: SdkConfig,
}

impl AWSForObjectStore {
    pub(crate) fn new(sdk_config: SdkConfig) -> Self {
        Self { sdk_config }
    }
}

#[async_trait::async_trait]
impl CredentialProvider for AWSForObjectStore {
    type Credential = AwsCredential;

    /// Provide the necessary configured credentials from the AWS SDK for use by
    /// [object_store::aws::AmazonS3]
    async fn get_credential(&self) -> ObjectStoreResult<Arc<Self::Credential>> {
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

        Ok(Arc::new(Self::Credential {
            key_id: credentials.access_key_id().into(),
            secret_key: credentials.secret_access_key().into(),
            token: credentials.session_token().map(|o| o.to_string()),
        }))
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
    options: StorageOptions,
}

impl OptionsCredentialsProvider {
    /// Look at the options configured on the provider and return an appropriate
    /// [Credentials] instance for AWS SDK credential resolution
    fn credentials(&self) -> aws_credential_types::provider::Result {
        debug!("Attempting to pull credentials from `StorageOptions`");
        let access_key = self.options.0.get(constants::AWS_ACCESS_KEY_ID).ok_or(
            CredentialsError::not_loaded("access key not in StorageOptions"),
        )?;
        let secret_key = self.options.0.get(constants::AWS_SECRET_ACCESS_KEY).ok_or(
            CredentialsError::not_loaded("secret key not in StorageOptions"),
        )?;
        let session_token = self.options.0.get(constants::AWS_SESSION_TOKEN).cloned();

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

/// Generate a random session name for assuming IAM roles
fn assume_role_sessio_name() -> String {
    let now = chrono::Utc::now();

    format!("delta-rs_{}", now.timestamp_millis())
}

/// Return the configured IAM role ARN or whatever is defined in the environment
fn assume_role_arn(options: &StorageOptions) -> Option<String> {
    options
        .0
        .get(constants::AWS_IAM_ROLE_ARN)
        .or(options.0.get(constants::AWS_S3_ASSUME_ROLE_ARN))
        .or(std::env::var_os(constants::AWS_IAM_ROLE_ARN)
            .map(|o| {
                o.into_string()
                    .expect("Failed to unwrap AWS_IAM_ROLE_ARN which may have invalid data")
            })
            .as_ref())
        .or(std::env::var_os(constants::AWS_S3_ASSUME_ROLE_ARN)
            .map(|o| {
                o.into_string()
                    .expect("Failed to unwrap AWS_S3_ASSUME_ROLE_ARN which may have invalid data")
            })
            .as_ref())
        .cloned()
}

/// Return the configured IAM assume role session name or provide a unique one
fn assume_session_name(options: &StorageOptions) -> String {
    let assume_session = options
        .0
        .get(constants::AWS_IAM_ROLE_SESSION_NAME)
        .or(options.0.get(constants::AWS_S3_ROLE_SESSION_NAME))
        .cloned();

    match assume_session {
        Some(s) => s,
        None => assume_role_sessio_name(),
    }
}

/// Take a set of [StorageOptions] and produce an appropriate AWS SDK [SdkConfig]
/// for use with various AWS SDK APIs, such as in our [crate::logstore::S3DynamoDbLogStore]
pub async fn resolve_credentials(options: StorageOptions) -> DeltaResult<SdkConfig> {
    let default_provider = DefaultCredentialsChain::builder().build().await;

    let credentials_provider = match assume_role_arn(&options) {
        Some(arn) => {
            debug!("Configuring AssumeRoleProvider with role arn: {arn}");
            CredentialsProviderChain::first_try(
                "AssumeRoleProvider",
                AssumeRoleProvider::builder(arn)
                    .session_name(assume_session_name(&options))
                    .build()
                    .await,
            )
            .or_else("StorageOptions", OptionsCredentialsProvider { options })
            .or_else("DefaultChain", default_provider)
        }
        None => CredentialsProviderChain::first_try(
            "StorageOptions",
            OptionsCredentialsProvider { options },
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
    use crate::constants;
    use maplit::hashmap;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_options_credentials_provider() {
        let options = StorageOptions(hashmap! {
            constants::AWS_ACCESS_KEY_ID.to_string() => "test_id".to_string(),
            constants::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret".to_string(),
        });

        let config = resolve_credentials(options).await;
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
        let options = StorageOptions(hashmap! {
            constants::AWS_ACCESS_KEY_ID.to_string() => "test_id".to_string(),
            constants::AWS_SECRET_ACCESS_KEY.to_string() => "test_secret".to_string(),
            constants::AWS_SESSION_TOKEN.to_string() => "test_token".to_string(),
        });

        let config = resolve_credentials(options)
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
}
