use std::sync::Arc;

use crate::UnityCatalog;
use async_trait::async_trait;
use bytes::Bytes;
use delta_kernel::LogPath;
use deltalake_core::kernel::Version;
use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::logstore::object_store::ObjectStore;
use deltalake_core::logstore::{
    CatalogLogTail, CommitOrBytes, LogStore, LogStoreConfig, ObjectStoreRef,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use unity_catalog_delta_client_api::Commit;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct CommitList {
    pub commits: Vec<Commit>,
    pub max_version: u64,
}

#[async_trait]
pub trait CommitCoordinator: Send + Sync + std::fmt::Debug {
    async fn get_commits(&self) -> DeltaResult<CommitList>;
}

#[derive(Debug)]
pub struct UnityCommitCoordinator {
    client: UnityCatalog,
    catalog: String,
    schema: String,
    table: String,
}

impl UnityCommitCoordinator {
    pub fn new(
        client: UnityCatalog,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            client,
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }
}

#[async_trait]
impl CommitCoordinator for UnityCommitCoordinator {
    async fn get_commits(&self) -> DeltaResult<CommitList> {
        let resp = self
            .client
            .delta_rest_client()
            .await?
            .load_table(&self.catalog, &self.schema, &self.table)
            .await
            .map_err(|e| DeltaTableError::Generic(format!("UC load_table failed: {e}")))?;

        let max_version = resp
            .latest_table_version
            .or(resp.metadata.last_commit_version)
            .unwrap_or(0) as u64;

        Ok(CommitList {
            commits: resp.commits,
            max_version,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CatalogManagedLogStore<C: CommitCoordinator> {
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    config: LogStoreConfig,
    coordinator: Arc<C>,
}

impl<C: CommitCoordinator> CatalogManagedLogStore<C> {
    pub fn new(
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        config: LogStoreConfig,
        coordinator: Arc<C>,
    ) -> Self {
        Self {
            prefixed_store,
            root_store,
            config,
            coordinator,
        }
    }
}

#[async_trait]
impl<C: CommitCoordinator + 'static> LogStore for CatalogManagedLogStore<C> {
    fn name(&self) -> String {
        "CatalogManagedLogStore".into()
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        deltalake_core::logstore::read_commit_entry(self.prefixed_store.as_ref(), version).await
    }

    async fn write_commit_entry(
        &self,
        _version: Version,
        _commit_or_bytes: CommitOrBytes,
        _operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        Err(TransactionError::LogStoreError {
            msg: "writes to catalog-managed Unity Catalog tables are not implemented yet".into(),
            source: "catalog-managed writes unimplemented".into(),
        })
    }

    async fn abort_commit_entry(
        &self,
        _version: Version,
        _commit_or_bytes: CommitOrBytes,
        _operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        Err(TransactionError::LogStoreError {
            msg: "writes to catalog-managed Unity Catalog tables are not implemented yet".into(),
            source: "catalog-managed writes unimplemented".into(),
        })
    }

    async fn get_latest_version(&self, _start_version: Version) -> DeltaResult<Version> {
        Ok(self.coordinator.get_commits().await?.max_version)
    }

    async fn catalog_log_tail(&self) -> DeltaResult<Option<CatalogLogTail>> {
        let list = self.coordinator.get_commits().await?;
        let base = self.config.location();
        let mut log_tail = Vec::with_capacity(list.commits.len());
        for c in &list.commits {
            let file_path = Path::parse(&c.file_name)?;
            let file_name = file_path.filename().unwrap_or(&c.file_name);
            let path = LogPath::staged_commit(
                base.clone(),
                file_name,
                c.file_modification_timestamp,
                c.file_size as u64,
            )
            .map_err(|e| {
                DeltaTableError::Generic(format!(
                    "invalid staged commit path for {}: {e}",
                    c.file_name
                ))
            })?;
            log_tail.push(path);
        }

        Ok(Some(CatalogLogTail {
            log_tail,
            max_version: list.max_version,
        }))
    }

    fn object_store(&self, _operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.prefixed_store.clone()
    }

    fn root_object_store(&self, _operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.root_store.clone()
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake_core::logstore::StorageConfig;
    use deltalake_core::logstore::object_store::memory::InMemory;
    use reqwest::Url;

    #[derive(Debug)]
    struct MockCoordinator(CommitList);

    #[async_trait]
    impl CommitCoordinator for MockCoordinator {
        async fn get_commits(&self) -> DeltaResult<CommitList> {
            Ok(self.0.clone())
        }
    }

    fn store() -> ObjectStoreRef {
        Arc::new(InMemory::new())
    }

    fn log_store(list: CommitList) -> CatalogManagedLogStore<MockCoordinator> {
        let url = Url::parse("memory:///cat.schema.table").unwrap();
        let config = LogStoreConfig::new(&url, StorageConfig::default());
        CatalogManagedLogStore::new(store(), store(), config, Arc::new(MockCoordinator(list)))
    }

    fn sample_commits() -> CommitList {
        CommitList {
            commits: vec![
                Commit::new(
                    1,
                    100,
                    "_staged_commits/00000000000000000001.1b1c9f7e-1234-5678-9012-345678901234.json",
                    10,
                    200,
                ),
                Commit::new(
                    2,
                    200,
                    "00000000000000000002.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json",
                    20,
                    200,
                ),
            ],
            max_version: 2,
        }
    }

    #[tokio::test]
    async fn get_latest_version_uses_catalog_max() {
        let ls = log_store(sample_commits());
        assert_eq!(ls.get_latest_version(0).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn catalog_log_tail_is_sorted_and_capped() {
        let ls = log_store(sample_commits());
        let tail = ls
            .catalog_log_tail()
            .await
            .unwrap()
            .expect("catalog-managed store must provide a log tail");
        assert_eq!(tail.max_version, 2);
        assert_eq!(tail.log_tail.len(), 2);

        let urls: Vec<String> = tail
            .log_tail
            .iter()
            .map(|lp| {
                let parsed: delta_kernel::path::ParsedLogPath = lp.clone().into();
                parsed.location.location.to_string()
            })
            .collect();
        assert!(urls[0].ends_with(
            "_delta_log/_staged_commits/00000000000000000001.1b1c9f7e-1234-5678-9012-345678901234.json"
        ));
        assert!(urls[1].ends_with(
            "_delta_log/_staged_commits/00000000000000000002.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json"
        ));
    }

    #[tokio::test]
    async fn empty_commits_yield_empty_tail() {
        let ls = log_store(CommitList {
            commits: vec![],
            max_version: 5,
        });
        let tail = ls.catalog_log_tail().await.unwrap().unwrap();
        assert_eq!(tail.max_version, 5);
        assert!(tail.log_tail.is_empty());
    }

    #[test]
    fn parse_uc_identity_splits_three_parts() {
        let url = Url::parse("uc://main.sales.orders").unwrap();
        assert_eq!(
            crate::parse_uc_identity(&url),
            Some(("main".into(), "sales".into(), "orders".into()))
        );
        assert_eq!(
            crate::parse_uc_identity(&Url::parse("uc://main.sales").unwrap()),
            None
        );
    }

    #[test]
    fn catalog_managed_opt_in_detected() {
        let mut cfg = StorageConfig::default();
        assert!(!crate::is_catalog_managed_requested(&cfg));
        cfg.raw
            .insert("unity_catalog_managed".into(), "true".into());
        assert!(crate::is_catalog_managed_requested(&cfg));
    }
}
