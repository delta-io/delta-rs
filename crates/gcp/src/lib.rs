use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{
    factories, limit_store_handler, url_prefix_handler, ObjectStoreFactory, ObjectStoreRef,
    StorageOptions,
};
use deltalake_core::{DeltaResult, Path};
use object_store::gcp::GoogleConfigKey;
use object_store::parse_url_opts;
use url::Url;

mod config;
pub mod error;
mod storage;

trait GcpOptions {
    fn as_gcp_options(&self) -> HashMap<GoogleConfigKey, String>;
}

impl GcpOptions for StorageOptions {
    fn as_gcp_options(&self) -> HashMap<GoogleConfigKey, String> {
        self.0
            .iter()
            .filter_map(|(key, value)| {
                Some((
                    GoogleConfigKey::from_str(&key.to_ascii_lowercase()).ok()?,
                    value.clone(),
                ))
            })
            .collect()
    }
}

#[derive(Clone, Default, Debug)]
pub struct GcpFactory {}

impl ObjectStoreFactory for GcpFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let config = config::GcpConfigHelper::try_new(options.as_gcp_options())?.build()?;
        let (inner, prefix) = parse_url_opts(url, config)?;
        let gcs_backend = crate::storage::GcsStorageBackend::try_new(Arc::new(inner))?;
        let store = limit_store_handler(url_prefix_handler(gcs_backend, prefix.clone()), options);
        Ok((store, prefix))
    }
}

impl LogStoreFactory for GcpFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for common Google Cloud [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(GcpFactory {});
    let scheme = &"gs";
    let url = Url::parse(&format!("{}://", scheme)).unwrap();
    factories().insert(url.clone(), factory.clone());
    logstores().insert(url.clone(), factory.clone());
}
