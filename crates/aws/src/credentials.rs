use std::{sync::Arc, time::Duration};

use aws_config::{
    ecs::EcsCredentialsProvider,
    environment::{EnvironmentVariableCredentialsProvider, EnvironmentVariableRegionProvider},
    imds::credentials::ImdsCredentialsProvider,
    meta::{credentials::CredentialsProviderChain, region::RegionProviderChain},
    profile::ProfileFileCredentialsProvider,
    provider_config::ProviderConfig,
    web_identity_token::WebIdentityTokenCredentialsProvider,
};
use aws_credential_types::provider::{self, ProvideCredentials};
use tracing::Instrument;

const IMDS_PROVIDER_NAME: &str = "Ec2InstanceMetadata";

#[derive(Debug)]
pub struct ConfiguredCredentialChain {
    provider_chain: CredentialsProviderChain,
}

#[derive(Debug)]
pub struct NoOpCredentials {}

pub fn new_region_provider(
    configuration: &ProviderConfig,
    disable_imds: bool,
    imds_timeout: u64,
) -> RegionProviderChain {
    let env_provider = EnvironmentVariableRegionProvider::new();
    let profile_file = aws_config::profile::region::Builder::default()
        .configure(configuration)
        .build();
    if disable_imds {
        return RegionProviderChain::first_try(env_provider).or_else(profile_file);
    }

    RegionProviderChain::first_try(env_provider)
        .or_else(profile_file)
        .or_else(
            aws_config::imds::region::Builder::default()
                .configure(configuration)
                .imds_client(
                    aws_config::imds::Client::builder()
                        .connect_timeout(Duration::from_millis(imds_timeout))
                        .read_timeout(Duration::from_millis(imds_timeout))
                        .build(),
                )
                .build(),
        )
}

impl ConfiguredCredentialChain {
    pub fn new(disable_imds: bool, imds_timeout: u64, conf: &ProviderConfig) -> Self {
        let imds_provider = Self::build_imds_provider(conf, disable_imds, imds_timeout);
        let env_provider = EnvironmentVariableCredentialsProvider::default();
        let profile_provider = ProfileFileCredentialsProvider::builder()
            .configure(conf)
            .with_custom_provider(IMDS_PROVIDER_NAME, imds_provider.clone())
            .build();
        let web_identity_token_provider = WebIdentityTokenCredentialsProvider::builder()
            .configure(conf)
            .build();
        let ecs_provider = EcsCredentialsProvider::builder().configure(conf).build();

        let provider_chain = CredentialsProviderChain::first_try("Environment", env_provider)
            .or_else("Profile", profile_provider)
            .or_else("WebIdentityToken", web_identity_token_provider)
            .or_else("EcsContainer", ecs_provider)
            .or_else(IMDS_PROVIDER_NAME, imds_provider);

        Self { provider_chain }
    }

    async fn credentials(&self) -> provider::Result {
        self.provider_chain
            .provide_credentials()
            .instrument(tracing::debug_span!("provide_credentials", provider = %"default_chain"))
            .await
    }

    fn build_imds_provider(
        conf: &ProviderConfig,
        disable_imds: bool,
        imds_timeout: u64,
    ) -> Arc<dyn ProvideCredentials> {
        if disable_imds {
            return Arc::new(NoOpCredentials {});
        }

        let imds_provider = ImdsCredentialsProvider::builder()
            .configure(conf)
            .imds_client(
                aws_config::imds::Client::builder()
                    .connect_timeout(Duration::from_millis(imds_timeout))
                    .read_timeout(Duration::from_millis(imds_timeout))
                    .build(),
            )
            .build();
        Arc::new(imds_provider)
    }
}

impl ProvideCredentials for ConfiguredCredentialChain {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(self.credentials())
    }
}

impl ProvideCredentials for NoOpCredentials {
    fn provide_credentials<'a>(&'a self) -> provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(std::future::ready(Err(
            provider::error::CredentialsError::not_loaded_no_source(),
        )))
    }
}
