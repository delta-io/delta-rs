use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_logstore::object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use deltalake_logstore::object_store::ObjectStoreScheme;
use deltalake_logstore::{default_logstore, logstore_factories, LogStore, LogStoreFactory};
use deltalake_logstore::{
    object_store_factories, ObjectStoreFactory, ObjectStoreRef, StorageConfig,
};
use deltalake_logstore::{LogStoreError, LogStoreResult, Path};
use object_store::client::SpawnedReqwestConnector;
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
        config: &StorageConfig,
    ) -> LogStoreResult<(ObjectStoreRef, Path)> {
        let mut builder = GoogleCloudStorageBuilder::new().with_url(url.to_string());
        builder = builder.with_retry(config.retry.clone());

        if let Some(runtime) = &config.runtime {
            builder =
                builder.with_http_connector(SpawnedReqwestConnector::new(runtime.get_handle()));
        }
        let config = config::GcpConfigHelper::try_new(config.raw.as_gcp_options())?.build()?;

        let (_, path) = ObjectStoreScheme::parse(url)
            .map_err(|e| LogStoreError::Generic { source: e.into() })?;
        let prefix =
            Path::parse(path).map_err(|e| LogStoreError::ObjectStore { source: e.into() })?;

        for (key, value) in config.iter() {
            builder = builder.with_config(*key, value.clone());
        }

        let store = crate::storage::GcsStorageBackend::try_new(Arc::new(builder.build()?))?;

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
    ) -> LogStoreResult<Arc<dyn LogStore>> {
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
