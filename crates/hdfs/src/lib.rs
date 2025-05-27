use std::collections::HashMap;
use std::sync::Arc;

use deltalake_core::logstore::{
    default_logstore, logstore_factories, DeltaIOStorageBackend, LogStore, LogStoreFactory,
    StorageConfig,
};
use deltalake_core::logstore::{object_store_factories, ObjectStoreFactory, ObjectStoreRef};
use deltalake_core::{DeltaResult, Path};
use hdfs_native_object_store::HdfsObjectStore;
use object_store::RetryConfig;
use tokio::runtime::Handle;
use url::Url;

#[derive(Clone, Default, Debug)]
pub struct HdfsFactory {}

impl ObjectStoreFactory for HdfsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &HashMap<String, String>,
        _retry: &RetryConfig,
        handle: Option<Handle>,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let mut store: ObjectStoreRef =
            Arc::new(HdfsObjectStore::with_config(url.as_str(), options.clone())?);

        // HDFS doesn't have the spawnService, so we still wrap it in the old io storage backend (not as optimal though)
        if let Some(handle) = handle {
            store = Arc::new(DeltaIOStorageBackend::new(store, handle));
        };
        let prefix = Path::parse(url.path())?;
        Ok((store, prefix))
    }
}

impl LogStoreFactory for HdfsFactory {
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

/// Register an [ObjectStoreFactory] for common HDFS [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(HdfsFactory {});
    for scheme in ["hdfs", "viewfs"].iter() {
        let url = Url::parse(&format!("{scheme}://")).unwrap();
        object_store_factories().insert(url.clone(), factory.clone());
        logstore_factories().insert(url.clone(), factory.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url_opts() -> DeltaResult<()> {
        let factory = HdfsFactory::default();
        let _ = factory.parse_url_opts(
            &Url::parse("hdfs://localhost:9000").expect("Failed to parse hdfs://"),
            &HashMap::default(),
            &RetryConfig::default(),
            None,
        )?;
        Ok(())
    }
}
