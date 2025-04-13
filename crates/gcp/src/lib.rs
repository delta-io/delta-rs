use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use deltalake_core::logstore::object_store::{ObjectStoreScheme, RetryConfig};
use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::logstore::{factories, ObjectStoreFactory, ObjectStoreRef, StorageConfig};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use url::Url;

mod config;
pub mod error;
mod storage;

trait GcpOptions {
    fn as_gcp_options(&self) -> HashMap<GoogleConfigKey, String>;
}

impl GcpOptions for HashMap<String, String> {
    fn as_gcp_options(&self) -> HashMap<GoogleConfigKey, String> {
        self.iter()
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
        options: &HashMap<String, String>,
        retry: &RetryConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let config = config::GcpConfigHelper::try_new(options.as_gcp_options())?.build()?;

        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;

        let mut builder = GoogleCloudStorageBuilder::new().with_url(url.to_string());

        for (key, value) in config.iter() {
            builder = builder.with_config(*key, value.clone());
        }

        let inner = builder.with_retry(retry.clone()).build()?;
        let store = crate::storage::GcsStorageBackend::try_new(Arc::new(inner))?;

        Ok((Arc::new(store), prefix))
    }
}

impl LogStoreFactory for GcpFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for common Google Cloud [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(GcpFactory {});
    let scheme = &"gs";
    let url = Url::parse(&format!("{scheme}://")).unwrap();
    factories().insert(url.clone(), factory.clone());
    logstores().insert(url.clone(), factory.clone());
}
