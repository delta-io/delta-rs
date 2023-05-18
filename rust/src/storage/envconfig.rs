//! environment config
use std::collections::HashMap;
use std::str::FromStr;

use object_store::azure::AzureConfigKey;
use object_store::Error;

use crate::DeltaResult;

/// Credential
pub enum AzureCredential {
    /// Using the account master key
    AcessKey,
    /// Using a static bearer token
    BearerToken,
    /// Authorizing with secret
    ClientSecret,
    /// Using workload identity
    FederatedToken,
    /// Using a shared access signature
    SasKey,
}

impl AzureCredential {
    /// Reys required for config
    pub fn keys(&self) -> Vec<AzureConfigKey> {
        match self {
            Self::AcessKey => Vec::from_iter([AzureConfigKey::AccessKey]),
            Self::BearerToken => Vec::from_iter([AzureConfigKey::Token]),
            Self::ClientSecret => Vec::from_iter([
                AzureConfigKey::ClientId,
                AzureConfigKey::ClientSecret,
                AzureConfigKey::AuthorityId,
            ]),
            Self::FederatedToken => Vec::from_iter([
                AzureConfigKey::AuthorityId,
                AzureConfigKey::ClientId,
                AzureConfigKey::FederatedTokenFile,
            ]),
            Self::SasKey => Vec::from_iter([AzureConfigKey::SasKey]),
        }
    }
}

/// Helper struct to create full configuration from passed options and environment
///
/// Main concern is to pick the desired credential for connecting to starage backend
/// based on a provided configuration and configuration set in the environment.
pub struct ConfigHelper {
    config: HashMap<AzureConfigKey, String>,
    env_config: HashMap<AzureConfigKey, String>,
    priority: Vec<AzureCredential>,
}

impl ConfigHelper {
    /// Create a new [`ConfigHelper`]
    pub fn try_new(
        config: impl IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    ) -> DeltaResult<Self> {
        let mut env_config = HashMap::new();
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if key.starts_with("AZURE_") {
                    if let Ok(config_key) = AzureConfigKey::from_str(&key.to_ascii_lowercase()) {
                        env_config.insert(config_key, value.to_string());
                    }
                }
            }
        }

        Ok(Self {
            config: config
                .into_iter()
                .map(|(key, value)| Ok((AzureConfigKey::from_str(key.as_ref())?, value.into())))
                .collect::<Result<_, Error>>()?,
            env_config,
            priority: Vec::from_iter([
                AzureCredential::SasKey,
                AzureCredential::BearerToken,
                AzureCredential::ClientSecret,
                AzureCredential::FederatedToken,
            ]),
        })
    }

    /// Check if all credential keys are contained in passed config
    pub fn has_full_config(&self, cred: &AzureCredential) -> bool {
        cred.keys().iter().all(|key| self.config.contains_key(key))
    }

    /// Check if any credential keys are contained in passed config
    pub fn has_any_config(&self, cred: &AzureCredential) -> bool {
        cred.keys().iter().any(|key| self.config.contains_key(key))
    }

    /// Check if all credential keys can be provided using the env
    pub fn has_full_config_with_env(&self, cred: &AzureCredential) -> bool {
        cred.keys()
            .iter()
            .all(|key| self.config.contains_key(key) || self.env_config.contains_key(key))
    }

    /// Generate a cofiguration augmented with options from the environment
    pub fn build(mut self) -> DeltaResult<HashMap<AzureConfigKey, String>> {
        if self.config.contains_key(&AzureConfigKey::UseAzureCli) {
            return Ok(self.config);
        }
        for cred in &self.priority {
            if self.has_full_config(cred) {
                return Ok(self.config);
            }
        }
        for cred in &self.priority {
            if self.has_any_config(cred) && self.has_full_config_with_env(cred) {
                for key in cred.keys() {
                    if !self.config.contains_key(&key) {
                        self.config
                            .insert(key, self.env_config.get(&key).unwrap().to_owned());
                    }
                }
                return Ok(self.config);
            }
        }
        todo!()
    }
}
