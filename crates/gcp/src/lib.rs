use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use deltalake_core::logstore::object_store::{ObjectStoreScheme, RetryConfig};
use deltalake_core::logstore::{default_logstore, logstore_factories, LogStore, LogStoreFactory};
use deltalake_core::logstore::{
    object_store_factories, ObjectStoreFactory, ObjectStoreRef, StorageConfig,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::client::SpawnedReqwestConnector;
use tokio::runtime::Handle;
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
        handle: Option<Handle>,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let config = config::GcpConfigHelper::try_new(options.as_gcp_options())?.build()?;

        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;

        let mut builder = GoogleCloudStorageBuilder::new().with_url(url.to_string());

        if let Some(handle) = handle {
            builder = builder.with_http_connector(SpawnedReqwestConnector::new(handle));
        }

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

/// Register an [ObjectStoreFactory] for common Google Cloud [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(GcpFactory {});
    let scheme = &"gs";
    let url = Url::parse(&format!("{scheme}://")).unwrap();
    object_store_factories().insert(url.clone(), factory.clone());
    logstore_factories().insert(url.clone(), factory.clone());
}
