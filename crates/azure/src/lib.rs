use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::logstore::{factories, ObjectStoreFactory, ObjectStoreRef, StorageConfig};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::{ObjectStoreScheme, RetryConfig};
use url::Url;

mod config;
pub mod error;

trait AzureOptions {
    fn as_azure_options(&self) -> HashMap<AzureConfigKey, String>;
}

impl AzureOptions for HashMap<String, String> {
    fn as_azure_options(&self) -> HashMap<AzureConfigKey, String> {
        self.iter()
            .filter_map(|(key, value)| {
                Some((
                    AzureConfigKey::from_str(&key.to_ascii_lowercase()).ok()?,
                    value.clone(),
                ))
            })
            .collect()
    }
}

#[derive(Clone, Default, Debug)]
pub struct AzureFactory {}

impl ObjectStoreFactory for AzureFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &HashMap<String, String>,
        retry: &RetryConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let config = config::AzureConfigHelper::try_new(options.as_azure_options())?.build()?;

        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;

        let mut builder = MicrosoftAzureBuilder::new().with_url(url.to_string());
        for (key, value) in config.iter() {
            builder = builder.with_config(*key, value.clone());
        }
        let store = builder.with_retry(retry.clone()).build()?;

        Ok((Arc::new(store), prefix))
    }
}

impl LogStoreFactory for AzureFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for common Azure [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(AzureFactory {});
    for scheme in ["az", "adl", "azure", "abfs", "abfss"].iter() {
        let url = Url::parse(&format!("{scheme}://")).unwrap();
        factories().insert(url.clone(), factory.clone());
        logstores().insert(url.clone(), factory.clone());
    }
}
