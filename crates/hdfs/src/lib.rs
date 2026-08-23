use std::sync::Arc;

use deltalake_core::logstore::{
    LogStore, LogStoreFactory, StorageConfig, default_logstore, logstore_factories,
};
use deltalake_core::logstore::{ObjectStoreFactory, ObjectStoreRef, object_store_factories};
use deltalake_core::{DeltaResult, Path};
use hdfs_native_object_store::HdfsObjectStoreBuilder;
use url::Url;

#[derive(Clone, Default, Debug)]
pub struct HdfsFactory {}

impl ObjectStoreFactory for HdfsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let mut builder = HdfsObjectStoreBuilder::new()
            .with_url(url.as_str())
            .with_config(&config.raw);

        if let Some(runtime) = &config.runtime {
            builder = builder.with_io_runtime(runtime.get_handle());
        }

        let store = Arc::new(builder.build()?);

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
            &StorageConfig::default(),
        )?;
        Ok(())
    }
}
