//! Auxiliary module for generating a valid Google cloud configuration.
//!
//! Google offers few ways to authenticate against storage accounts and
//! provide credentials for a service principal. Some of this configuration may
//! partially be specified in the environment. This module establishes a structured
//! way how we discover valid credentials and some heuristics on how they are prioritized.
use std::collections::{hash_map::Entry, HashMap};
use std::ffi::OsStr;
use std::str::FromStr;
use std::sync::LazyLock;

use object_store::gcp::GoogleConfigKey;
use object_store::Error as ObjectStoreError;

use crate::error::Result;

static CREDENTIAL_KEYS: LazyLock<Vec<GoogleConfigKey>> = LazyLock::new(|| {
    vec![
        GoogleConfigKey::ServiceAccountKey,
        GoogleConfigKey::ApplicationCredentials,
    ]
});

/// Credential
enum GcpCredential {
    /// Using the service account key
    ServiceAccountKey,
    /// Using application credentials
    ApplicationCredentials,
}

impl GcpCredential {
    /// required configuration keys for variant
    fn keys(&self) -> Vec<GoogleConfigKey> {
        match self {
            Self::ServiceAccountKey => vec![GoogleConfigKey::ServiceAccountKey],
            Self::ApplicationCredentials => {
                vec![GoogleConfigKey::ApplicationCredentials]
            }
        }
    }
}

/// Helper struct to create full configuration from passed options and environment
///
/// Main concern is to pick the desired credential for connecting to storage backend
/// based on a provided configuration and configuration set in the environment.
pub(crate) struct GcpConfigHelper {
    config: HashMap<GoogleConfigKey, String>,
    env_config: HashMap<GoogleConfigKey, String>,
    priority: Vec<GcpCredential>,
}

/// Take the `GOOGLE_` environment variables and turn the relevant ones into a [HashMap] with
/// [GoogleConfigKey] from the `object_store` crate.
fn parse_environment(
    vars: impl IntoIterator<Item = (impl AsRef<OsStr>, impl AsRef<OsStr>)>,
) -> HashMap<GoogleConfigKey, String> {
    HashMap::from_iter(vars.into_iter().filter_map(|(os_key, os_value)| {
        if let (Some(key), Some(value)) = (os_key.as_ref().to_str(), os_value.as_ref().to_str()) {
            if key.starts_with("GOOGLE_") {
                if let Ok(config_key) = GoogleConfigKey::from_str(&key.to_ascii_lowercase()) {
                    return Some((config_key, value.to_string()));
                }
            }
        }
        None
    }))
}

impl GcpConfigHelper {
    /// Create a new [`ConfigHelper`]
    pub fn try_new(
        config: impl IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    ) -> Result<Self> {
        let env_config = parse_environment(std::env::vars_os());

        Ok(Self {
            config: config
                .into_iter()
                .map(|(key, value)| Ok((GoogleConfigKey::from_str(key.as_ref())?, value.into())))
                .collect::<Result<_, ObjectStoreError>>()?,
            env_config,
            priority: Vec::from_iter([
                GcpCredential::ServiceAccountKey,
                GcpCredential::ApplicationCredentials,
            ]),
        })
    }

    /// Check if all credential keys are contained in passed config
    fn has_full_config(&self, cred: &GcpCredential) -> bool {
        cred.keys().iter().all(|key| self.config.contains_key(key))
    }

    /// Check if any credential keys are contained in passed config
    fn has_any_config(&self, cred: &GcpCredential) -> bool {
        cred.keys().iter().any(|key| self.config.contains_key(key))
    }

    /// Check if all credential keys can be provided using the env
    fn has_full_config_with_env(&self, cred: &GcpCredential) -> bool {
        cred.keys()
            .iter()
            .all(|key| self.config.contains_key(key) || self.env_config.contains_key(key))
    }

    /// Generate a configuration augmented with options from the environment
    pub fn build(mut self) -> Result<HashMap<GoogleConfigKey, String>> {
        let mut has_credential = false;

        // try using only passed config options
        if !has_credential {
            for cred in &self.priority {
                if self.has_full_config(cred) {
                    has_credential = true;
                    break;
                }
            }
        }

        // try partially available credentials augmented by environment
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::format::parse;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_gcp_credential() {
        let c = GcpCredential::ServiceAccountKey;
        assert_eq!(c.keys(), vec![GoogleConfigKey::ServiceAccountKey]);
        let c = GcpCredential::ApplicationCredentials;
        assert_eq!(c.keys(), vec![GoogleConfigKey::ApplicationCredentials]);
    }

    #[test]
    fn test_build_helper() {
        let keys: Vec<(&str, String)> = vec![];
        let hash = GcpConfigHelper::try_new(keys)
            .expect("Failed to construct")
            .build()
            .expect("Failed to build GcpConfigHelper");
        assert_eq!(hash.keys().len(), 0);

        let keys: Vec<(&str, String)> = vec![("service_account", "test".into())];
        let hash = GcpConfigHelper::try_new(keys)
            .expect("Failed to construct")
            .build()
            .expect("Failed to build GcpConfigHelper");
        assert_eq!(
            hash.get(&GoogleConfigKey::ServiceAccount),
            Some(&"test".to_string())
        );
    }

    #[test]
    fn test_process_env_hashmap() {
        let processed = parse_environment(vec![(OsStr::new("left"), OsStr::new("right"))]);
        assert_eq!(processed.len(), 0);

        let processed = parse_environment(vec![
            (OsStr::new("GOOGLE_SERVICE_ACCOUNT"), OsStr::new("blah")),
            (OsStr::new("AWS_SECRET_ACCESS_KEY"), OsStr::new("blah")),
        ]);
        assert_eq!(processed.len(), 1);
        assert_eq!(
            processed.get(&GoogleConfigKey::ServiceAccount),
            Some(&"blah".to_string())
        );
    }
}
