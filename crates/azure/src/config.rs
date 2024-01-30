//! Auxiliary module for generating a valig Azure configuration.
//!
//! Azure offers many different ways to authenticate against storage accounts and
//! provide credentials for a service principal. Some of this configutaion may
//! partially be specified in the environment. This module establishes a structured
//! way how we discover valid credentials and some heuristics on how they are prioritized.
use std::collections::{hash_map::Entry, HashMap};
use std::str::FromStr;

use object_store::azure::AzureConfigKey;
use object_store::Error as ObjectStoreError;

use crate::error::Result;

lazy_static::lazy_static! {
    static ref CREDENTIAL_KEYS: Vec<AzureConfigKey> =
    Vec::from_iter([
        AzureConfigKey::ClientId,
        AzureConfigKey::ClientSecret,
        AzureConfigKey::FederatedTokenFile,
        AzureConfigKey::SasKey,
        AzureConfigKey::Token,
        AzureConfigKey::MsiEndpoint,
        AzureConfigKey::ObjectId,
        AzureConfigKey::MsiResourceId,
    ]);
}

/// Credential
enum AzureCredential {
    /// Using the account master key
    AccessKey,
    /// Using a static bearer token
    BearerToken,
    /// Authorizing with secret
    ClientSecret,
    /// Using a shared access signature
    #[allow(dead_code)]
    ManagedIdentity,
    /// Using a shared access signature
    SasKey,
    /// Using workload identity
    WorkloadIdentity,
}

impl AzureCredential {
    /// required configuration keys for variant
    fn keys(&self) -> Vec<AzureConfigKey> {
        match self {
            Self::AccessKey => Vec::from_iter([AzureConfigKey::AccessKey]),
            Self::BearerToken => Vec::from_iter([AzureConfigKey::Token]),
            Self::ClientSecret => Vec::from_iter([
                AzureConfigKey::ClientId,
                AzureConfigKey::ClientSecret,
                AzureConfigKey::AuthorityId,
            ]),
            Self::WorkloadIdentity => Vec::from_iter([
                AzureConfigKey::AuthorityId,
                AzureConfigKey::ClientId,
                AzureConfigKey::FederatedTokenFile,
            ]),
            Self::SasKey => Vec::from_iter([AzureConfigKey::SasKey]),
            Self::ManagedIdentity => Vec::new(),
        }
    }
}

/// Helper struct to create full configuration from passed options and environment
///
/// Main concern is to pick the desired credential for connecting to starage backend
/// based on a provided configuration and configuration set in the environment.
pub(crate) struct AzureConfigHelper {
    config: HashMap<AzureConfigKey, String>,
    env_config: HashMap<AzureConfigKey, String>,
    priority: Vec<AzureCredential>,
}

impl AzureConfigHelper {
    /// Create a new [`ConfigHelper`]
    pub fn try_new(
        config: impl IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    ) -> Result<Self> {
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
                .collect::<Result<_, ObjectStoreError>>()?,
            env_config,
            priority: Vec::from_iter([
                AzureCredential::AccessKey,
                AzureCredential::SasKey,
                AzureCredential::BearerToken,
                AzureCredential::ClientSecret,
                AzureCredential::WorkloadIdentity,
            ]),
        })
    }

    /// Check if all credential keys are contained in passed config
    fn has_full_config(&self, cred: &AzureCredential) -> bool {
        cred.keys().iter().all(|key| self.config.contains_key(key))
    }

    /// Check if any credential keys are contained in passed config
    fn has_any_config(&self, cred: &AzureCredential) -> bool {
        cred.keys().iter().any(|key| self.config.contains_key(key))
    }

    /// Check if all credential keys can be provided using the env
    fn has_full_config_with_env(&self, cred: &AzureCredential) -> bool {
        cred.keys()
            .iter()
            .all(|key| self.config.contains_key(key) || self.env_config.contains_key(key))
    }

    /// Generate a cofiguration augmented with options from the environment
    pub fn build(mut self) -> Result<HashMap<AzureConfigKey, String>> {
        let mut has_credential = false;

        if self.config.contains_key(&AzureConfigKey::UseAzureCli) {
            has_credential = true;
        }

        // try using only passed config options
        if !has_credential {
            for cred in &self.priority {
                if self.has_full_config(cred) {
                    has_credential = true;
                    break;
                }
            }
        }

        // try partially avaialbe credentials augmented by environment
        if !has_credential {
            for cred in &self.priority {
                if self.has_any_config(cred) && self.has_full_config_with_env(cred) {
                    for key in cred.keys() {
                        if let Entry::Vacant(e) = self.config.entry(key) {
                            e.insert(self.env_config.get(&key).unwrap().to_owned());
                        }
                    }
                    has_credential = true;
                    break;
                }
            }
        }

        // try getting credentials only from the environment
        if !has_credential {
            for cred in &self.priority {
                if self.has_full_config_with_env(cred) {
                    for key in cred.keys() {
                        if let Entry::Vacant(e) = self.config.entry(key) {
                            e.insert(self.env_config.get(&key).unwrap().to_owned());
                        }
                    }
                    has_credential = true;
                    break;
                }
            }
        }

        let omit_keys = if has_credential {
            CREDENTIAL_KEYS.clone()
        } else {
            Vec::new()
        };

        // Add keys from the environment to the configuration, as e.g. client configuration options.
        // NOTE We have to specifically configure omitting keys, since workload identity can
        // work purely using defaults, but partial config may be present in the environment.
        // Preference of conflicting configs (e.g. msi resource id vs. client id is handled in object store)
        for key in self.env_config.keys() {
            if !omit_keys.contains(key) {
                if let Entry::Vacant(e) = self.config.entry(*key) {
                    e.insert(self.env_config.get(key).unwrap().to_owned());
                }
            }
        }

        Ok(self.config)
    }
}
