//! AWS S3 and similar tooling for delta-rs
//!
//! This module also contains the [S3DynamoDbLogStore](crate::logstore::S3DynamoDbLogStore)
//! implementation for concurrent writer support with AWS S3 specifically.

pub mod constants;
mod credentials;
pub mod logstore;
pub mod storage;

pub use aws_credential_types::provider::SharedCredentialsProvider;
use deltalake_core::DeltaResult;
use deltalake_core::logstore::{
    LogStore, LogStoreFactory, ObjectStoreRef, StorageConfig, default_logstore, logstore_factories,
    object_store_factories,
};
use std::sync::Arc;
use storage::S3StorageOptionsConversion;
use storage::{S3ObjectStoreFactory, S3StorageOptions};
use tracing::log::*;
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct S3LogStoreFactory {}

impl S3StorageOptionsConversion for S3LogStoreFactory {}

impl LogStoreFactory for S3LogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        let s3_options = self.with_env_s3(&options.raw.clone());
        let s3_options = S3StorageOptions::from_map(&s3_options)?;

        if s3_options.locking_provider.as_deref() == Some("dynamodb") {
            error!(
                "delta-rs **no longer** supports S3DynamoDBLogStores, see <https://github.com/delta-io/delta-rs/discussions/4482> for more"
            );
        }

        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Register an [ObjectStoreFactory] for common S3 url schemes.
///
/// [ObjectStoreFactory]: deltalake_core::logstore::ObjectStoreFactory
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let object_stores = Arc::new(S3ObjectStoreFactory::default());
    let log_stores = Arc::new(S3LogStoreFactory::default());
    for scheme in ["s3", "s3a"].iter() {
        let url = Url::parse(&format!("{scheme}://")).unwrap();
        object_store_factories().insert(url.clone(), object_stores.clone());
        logstore_factories().insert(url.clone(), log_stores.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    use object_store::memory::InMemory;
    use serial_test::serial;
    /// In cases where there is no dynamodb specified locking provider, this should get a default
    /// logstore
    #[test]
    #[serial]
    fn test_logstore_factory_default() {
        let factory = S3LogStoreFactory::default();
        let store = Arc::new(InMemory::new());
        let url = Url::parse("s3://test-bucket").unwrap();
        unsafe {
            std::env::remove_var(crate::constants::AWS_S3_LOCKING_PROVIDER);
        }
        let logstore = factory
            .with_options(store.clone(), store, &url, &Default::default())
            .unwrap();
        assert_eq!(logstore.name(), "DefaultLogStore");
    }
}
