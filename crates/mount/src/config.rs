//! Auxiliary module for generating a valig Mount configuration.
use std::collections::{hash_map::Entry, HashMap};
use std::str::FromStr;

use crate::error::{Error, Result};

/// Typed property keys that can be defined on a mounted path
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy)]
#[non_exhaustive]
pub enum MountConfigKey {
    /// If set to "true", allows creating commits without concurrent writer protection.
    /// Only safe if there is one writer to a given table.
    AllowUnsafeRename,
}

impl AsRef<str> for MountConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::AllowUnsafeRename => "mount_allow_unsafe_rename",
        }
    }
}

impl FromStr for MountConfigKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mount_allow_unsafe_rename" | "allow_unsafe_rename" => Ok(Self::AllowUnsafeRename),
            _ => Err(Error::UnknownConfigKey(s.to_string())),
        }
    }
}

/// Helper struct to create full configuration from passed options and environment
pub(crate) struct MountConfigHelper {
    config: HashMap<MountConfigKey, String>,
    env_config: HashMap<MountConfigKey, String>,
}

impl MountConfigHelper {
    /// Create a new [`ConfigHelper`]
    pub fn try_new(
        config: impl IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    ) -> Result<Self> {
        let mut env_config = HashMap::new();
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if key.starts_with("MOUNT_") {
                    if let Ok(config_key) = MountConfigKey::from_str(&key.to_ascii_lowercase()) {
                        env_config.insert(config_key, value.to_string());
                    }
                }
            }
        }

        Ok(Self {
            config: config
                .into_iter()
                .map(|(key, value)| Ok((MountConfigKey::from_str(key.as_ref())?, value.into())))
                .collect::<Result<_, Error>>()?,
            env_config,
        })
    }

    /// Generate a cofiguration augmented with options from the environment
    pub fn build(mut self) -> Result<HashMap<MountConfigKey, String>> {
        // Add keys from the environment to the configuration, as e.g. client configuration options.
        // NOTE We have to specifically configure omitting keys, since workload identity can
        // work purely using defaults, but partial config may be present in the environment.
        // Preference of conflicting configs (e.g. msi resource id vs. client id is handled in object store)
        for key in self.env_config.keys() {
            if let Entry::Vacant(e) = self.config.entry(*key) {
                e.insert(self.env_config.get(key).unwrap().to_owned());
            }
        }

        Ok(self.config)
    }
}
