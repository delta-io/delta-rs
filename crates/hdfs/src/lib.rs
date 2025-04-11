use std::collections::HashMap;
use std::sync::Arc;

use deltalake_core::logstore::{
    default_logstore, logstore_factories, LogStore, LogStoreFactory, StorageConfig,
};
use deltalake_core::logstore::{object_store_factories, ObjectStoreFactory, ObjectStoreRef};
use deltalake_core::{DeltaResult, Path};
use hdfs_native_object_store::HdfsObjectStore;
use object_store::RetryConfig;
use url::Url;

#[derive(Clone, Default, Debug)]
pub struct HdfsFactory {}

impl ObjectStoreFactory for HdfsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &HashMap<String, String>,
        _retry: &RetryConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        /*
        Needs a new hdfs object store release

        let store: ObjectStoreRef =
            Arc::new(HdfsObjectStore::with_config(url.as_str(), options.clone())?);
        let prefix = Path::parse(url.path())?;
        Ok((store, prefix))
         */
        todo!()
    }
}

impl LogStoreFactory for HdfsFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
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
