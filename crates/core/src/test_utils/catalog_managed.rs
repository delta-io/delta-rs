use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use delta_kernel::Engine;
use object_store::ObjectStore;
use url::Url;

use crate::kernel::{Action, Version};
use crate::logstore::{CatalogLogTail, LogStore, LogStoreConfig, LogStoreRef};
use crate::test_utils::{ActionFactory, TestSchemas};
use crate::{DeltaResult, DeltaTable, DeltaTableError};

#[derive(Debug)]
pub(crate) struct CatalogManagedTestStore {
    inner: LogStoreRef,
    pub(crate) fail_catalog_tail: AtomicBool,
}

impl CatalogManagedTestStore {
    pub(crate) fn new(inner: LogStoreRef) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fail_catalog_tail: AtomicBool::new(false),
        })
    }
}

#[async_trait::async_trait]
impl LogStore for CatalogManagedTestStore {
    fn name(&self) -> String {
        "CatalogManagedTestStore".into()
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        self.inner.read_commit_entry(version).await
    }

    async fn get_latest_version(&self, start_version: Version) -> DeltaResult<Version> {
        self.inner.get_latest_version(start_version).await
    }

    async fn catalog_log_tail(&self) -> DeltaResult<Option<CatalogLogTail>> {
        if self.fail_catalog_tail.load(Ordering::SeqCst) {
            return Err(DeltaTableError::Generic("catalog unavailable".to_string()));
        }
        Ok(None)
    }

    fn is_catalog_managed(&self) -> bool {
        true
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.inner.object_store()
    }

    fn root_object_store(&self) -> Arc<dyn ObjectStore> {
        self.inner.root_object_store()
    }

    fn engine(&self) -> Arc<dyn Engine> {
        self.inner.engine()
    }

    fn config(&self) -> &LogStoreConfig {
        self.inner.config()
    }
}

pub(crate) fn write_commits(dir: &std::path::Path, commits: &[Vec<serde_json::Value>]) {
    let log_dir = dir.join("_delta_log");
    std::fs::create_dir_all(&log_dir).unwrap();
    for (version, actions) in commits.iter().enumerate() {
        let body = actions
            .iter()
            .map(|action| action.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(log_dir.join(format!("{version:020}.json")), body).unwrap();
    }
}

pub(crate) fn metadata_action(configuration: serde_json::Value) -> serde_json::Value {
    let metadata = ActionFactory::metadata(
        TestSchemas::simple(),
        None::<[&str; 0]>,
        Some(serde_json::from_value(configuration).unwrap()),
    );
    serde_json::to_value(Action::Metadata(metadata)).unwrap()
}

pub(crate) async fn open(
    dir: &std::path::Path,
    catalog_managed: bool,
) -> (DeltaTable, Option<Arc<CatalogManagedTestStore>>) {
    let url = Url::from_directory_path(dir).unwrap();
    let table = crate::open_table(url).await.unwrap();
    if !catalog_managed {
        return (table, None);
    }
    let store = CatalogManagedTestStore::new(table.log_store());
    let mut table = DeltaTable::new(store.clone());
    table.load().await.unwrap();
    (table, Some(store))
}
