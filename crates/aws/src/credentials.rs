use std::time::Duration;

use aws_config::{
    ecs::EcsCredentialsProvider, environment::EnvironmentVariableCredentialsProvider,
    imds::credentials::ImdsCredentialsProvider, meta::credentials::CredentialsProviderChain,
    profile::ProfileFileCredentialsProvider, provider_config::ProviderConfig,
    web_identity_token::WebIdentityTokenCredentialsProvider,
};
use aws_credential_types::provider::{self, ProvideCredentials};
use tracing::Instrument;

#[derive(Debug)]
pub struct ConfiguredCredentialChain {
    provider_chain: CredentialsProviderChain,
}

impl ConfiguredCredentialChain {
    pub fn new(disable_imds: bool, imds_timeout: u64, config: Option<ProviderConfig>) -> Self {
        let conf = config.unwrap_or_default();

        let env_provider = EnvironmentVariableCredentialsProvider::default();
        let profile_provider = ProfileFileCredentialsProvider::builder()
            .configure(&conf)
            .build();
        let web_identity_token_provider = WebIdentityTokenCredentialsProvider::builder()
            .configure(&conf)
            .build();
        let ecs_provider = EcsCredentialsProvider::builder().configure(&conf).build();

        let mut provider_chain = CredentialsProviderChain::first_try("Environment", env_provider)
            .or_else("Profile", profile_provider)
            .or_else("WebIdentityToken", web_identity_token_provider)
            .or_else("EcsContainer", ecs_provider);
        if !disable_imds {
            let imds_provider = ImdsCredentialsProvider::builder()
                .configure(&conf)
                .imds_client(
                    aws_config::imds::Client::builder()
                        .connect_timeout(Duration::from_millis(imds_timeout))
                        .read_timeout(Duration::from_millis(imds_timeout))
                        .build(),
                )
                .build();
            provider_chain = provider_chain.or_else("Ec2InstanceMetadata", imds_provider);
        }

        Self { provider_chain }
    }

    async fn credentials(&self) -> provider::Result {
        self.provider_chain
            .provide_credentials()
            .instrument(tracing::debug_span!("provide_credentials", provider = %"default_chain"))
            .await
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
