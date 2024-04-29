use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{
    factories, limit_store_handler, url_prefix_handler, ObjectStoreFactory, ObjectStoreRef,
    StorageOptions,
};
use deltalake_core::{DeltaResult, Path};
use object_store::azure::AzureConfigKey;
use object_store::parse_url_opts;
use url::Url;

mod config;
pub mod error;

trait AzureOptions {
    fn as_azure_options(&self) -> HashMap<AzureConfigKey, String>;
}

impl AzureOptions for StorageOptions {
    fn as_azure_options(&self) -> HashMap<AzureConfigKey, String> {
        self.0
            .iter()
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
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let config = config::AzureConfigHelper::try_new(options.as_azure_options())?.build()?;
        let (inner, prefix) = parse_url_opts(url, config)?;
        let store = limit_store_handler(url_prefix_handler(inner, prefix.clone()), options);
        Ok((store, prefix))
    }
}

impl LogStoreFactory for AzureFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for common Azure [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(AzureFactory {});
    for scheme in ["az", "adl", "azure", "abfs", "abfss"].iter() {
        let url = Url::parse(&format!("{}://", scheme)).unwrap();
        factories().insert(url.clone(), factory.clone());
        logstores().insert(url.clone(), factory.clone());
    }
}
