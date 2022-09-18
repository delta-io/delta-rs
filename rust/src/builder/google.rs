use std::collections::HashMap;

use super::BuilderError;
use crate::DeltaResult;

use object_store::gcp::GoogleCloudStorageBuilder;
use once_cell::sync::Lazy;

#[derive(PartialEq, Eq)]
enum GoogleConfigKey {
    ServiceAccount,
}

impl GoogleConfigKey {
    fn get_from_env(&self) -> Option<String> {
        for (key, value) in ALIAS_MAP.iter() {
            if value == self {
                if let Ok(val) = std::env::var(key.to_ascii_uppercase()) {
                    return Some(val);
                }
            }
        }
        None
    }
}

static ALIAS_MAP: Lazy<HashMap<&'static str, GoogleConfigKey>> = Lazy::new(|| {
    HashMap::from([
        // service account
        ("google_service_account", GoogleConfigKey::ServiceAccount),
        ("service_account", GoogleConfigKey::ServiceAccount),
    ])
});

pub struct GoogleConfig {
    service_account: Option<String>,
}

impl GoogleConfig {
    fn new(options: &HashMap<String, String>) -> Self {
        let mut service_account = None;

        for (raw, value) in options {
            if let Some(key) = ALIAS_MAP.get(&*raw.to_ascii_lowercase()) {
                match key {
                    GoogleConfigKey::ServiceAccount => service_account = Some(value.clone()),
                }
            }
        }

        Self { service_account }
    }

    /// Check all options if a valid builder can be generated, if not, check if configuration
    /// can be read from the environment.
    pub fn get_builder(
        options: &HashMap<String, String>,
    ) -> DeltaResult<GoogleCloudStorageBuilder> {
        let config = Self::new(options);
        let service_account = if let Some(account) = config.service_account {
            account
        } else {
            GoogleConfigKey::ServiceAccount
                .get_from_env()
                .ok_or(BuilderError::MissingCredential)?
        };
        Ok(GoogleCloudStorageBuilder::default().with_service_account_path(service_account))
    }
}
