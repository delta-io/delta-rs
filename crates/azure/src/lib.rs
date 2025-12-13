use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::{
    LogStore, LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig, default_logstore,
    logstore_factories, object_store_factories,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::ObjectStoreScheme;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::client::SpawnedReqwestConnector;
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
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let mut builder = MicrosoftAzureBuilder::new()
            .with_url(url.to_string())
            .with_retry(config.retry.clone());
        if let Some(runtime) = &config.runtime {
            builder =
                builder.with_http_connector(SpawnedReqwestConnector::new(runtime.get_handle()));
        }

        let config = config::AzureConfigHelper::try_new(config.raw.as_azure_options())?.build()?;

        for (key, value) in config.iter() {
            builder = builder.with_config(*key, value.clone());
        }
        let store = builder.build()?;

        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;

        Ok((Arc::new(store), prefix))
    }
}

impl LogStoreFactory for AzureFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Register an [ObjectStoreFactory] for common Azure [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(AzureFactory {});
    for scheme in ["az", "adl", "azure", "abfs", "abfss"].iter() {
        let url = Url::parse(&format!("{scheme}://")).unwrap();
        object_store_factories().insert(url.clone(), factory.clone());
        logstore_factories().insert(url.clone(), factory.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_as_azure_options() {
        use object_store::azure::AzureConfigKey;
        let mut options = HashMap::default();
        let key = "AZURE_STORAGE_ACCOUNT_KEY".to_string();
        let value = "value".to_string();
        options.insert(key, value.clone());

        let converted = options.as_azure_options();
        assert_eq!(converted.get(&AzureConfigKey::AccessKey), Some(&value));
    }
}
