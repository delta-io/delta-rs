//! Read support for *catalog-managed commits* (CCv2) on Unity Catalog tables.
//!
//! Catalog-managed tables delegate commit coordination to an external catalog rather than the
//! filesystem. Writers hand commits to the catalog, which stages them as unpublished commits
//! under `_delta_log/_staged_commits/` and later ratifies (publishes) them. For readers this
//! inverts the usual source of truth: the **catalog** decides which commits exist and what the
//! latest table version is, and the filesystem may contain *more* commits than the catalog has
//! ratified. Readers must therefore cap log replay at the catalog-ratified maximum version and
//! source the log tail from the catalog.
//!
//! This module implements the read path:
//!
//! - [`CommitCoordinator`] abstracts the "ask the catalog for commits" RPC so it can be mocked
//!   in tests and so catalog-specific implementations can coexist behind one interface.
//! - [`UnityCommitCoordinator`] is the production implementation, backed by the Unity Catalog
//!   Delta REST API (`load_table`).
//! - [`CatalogManagedLogStore`] is a [`LogStore`] that serves version metadata from the
//!   coordinator while delegating file I/O to the underlying object store, handing the kernel
//!   a [`CatalogLogTail`] it uses to bound and drive replay.
//!
//! Catalog-managed reads are **opt-in**: pass `catalog_managed=true` as a storage option when
//! opening a `uc://catalog.schema.table` table. The option is intentionally conservative —
//! detecting catalog management automatically would require a catalog RPC for every table
//! open — and may be relaxed in the future. Only the read path is implemented so far;
//! catalog-managed writes (routing new commits through the catalog) are future work.
//!
//! See the [Databricks catalog commits documentation] for the protocol overview and
//! [delta-rs#4549] for the tracking issue.
//!
//! [Databricks catalog commits documentation]: https://docs.databricks.com/aws/en/tables/features/catalog-commits
//! [delta-rs#4549]: https://github.com/delta-io/delta-rs/issues/4549

use std::sync::Arc;

use crate::UnityCatalog;
use async_trait::async_trait;
use bytes::Bytes;
use delta_kernel::LogPath;
use deltalake_core::kernel::Version;
use deltalake_core::logstore::object_store::ObjectStore;
use deltalake_core::logstore::{CatalogLogTail, LogStore, LogStoreConfig, ObjectStoreRef};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use unity_catalog_delta_client_api::Commit;

/// The commits a catalog currently reports for a table, plus the highest catalog-ratified
/// version.
#[derive(Debug, Clone, Default)]
pub struct CommitList {
    /// Staged (not yet published to `_delta_log/`) commits known to the catalog.
    ///
    /// Ordering is catalog-defined and must not be relied on: Unity Catalog returns commits
    /// newest-first, while the Delta kernel requires an oldest-first, gapless tail, so
    /// consumers (e.g. [`CatalogManagedLogStore::catalog_log_tail`]) sort before use.
    pub commits: Vec<Commit>,
    /// The highest catalog-ratified table version.
    ///
    /// Acts as an upper bound on log replay: commit files on the filesystem beyond this
    /// version have not been ratified by the catalog and are invisible to catalog-managed
    /// readers.
    pub max_version: Version,
}

/// Source of truth for the commits of a catalog-managed table.
///
/// A commit coordinator answers "which staged commits does the catalog know about, and what is
/// the latest ratified version?" for one table. It exists as a trait so the catalog-managed
/// read path can be driven by mock coordinators in tests, and so catalog-specific
/// implementations (Databricks Unity Catalog, OSS Unity Catalog, ...) can be introduced
/// without changing [`CatalogManagedLogStore`].
#[async_trait]
pub trait CommitCoordinator: Send + Sync + std::fmt::Debug {
    /// Fetch the commits the catalog currently reports for the table.
    ///
    /// Implementations typically perform one catalog RPC per call; callers should not assume
    /// the result is cached or cheap.
    async fn get_commits(&self) -> DeltaResult<CommitList>;
}

/// A [`CommitCoordinator`] backed by the Unity Catalog Delta REST API.
///
/// Each [`get_commits`](CommitCoordinator::get_commits) call issues a UC `load_table` request
/// for the configured `catalog.schema.table` identity and translates the response into a
/// [`CommitList`].
#[derive(Debug)]
pub struct UnityCommitCoordinator {
    client: UnityCatalog,
    catalog: String,
    schema: String,
    table: String,
}

impl UnityCommitCoordinator {
    /// Create a coordinator for the table identified by `catalog.schema.table`, querying it
    /// through `client`.
    ///
    /// The client must carry bearer-token credentials, the only credential kind the UC Delta
    /// REST client is built from; see [`UnityCatalog::delta_rest_client`].
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
    /// Query UC `load_table` and translate the response into a [`CommitList`].
    ///
    /// The latest ratified version comes from `latest_table_version`, falling back to the
    /// table metadata's `last_commit_version`. Unity Catalog reports `-1` when a freshly
    /// created table has no ratified commits yet; the sentinel is clamped to `0` rather than
    /// cast, because `-1i64 as u64` would underflow to `u64::MAX` and silently disable the
    /// kernel's max-catalog-version cap. A table with no ratified commits therefore reports
    /// `max_version` 0 and loads no version beyond 0.
    async fn get_commits(&self) -> DeltaResult<CommitList> {
        let resp = self
            .client
            .delta_rest_client()?
            .load_table(&self.catalog, &self.schema, &self.table)
            .await
            .map_err(|e| DeltaTableError::Generic(format!("UC load_table failed: {e}")))?;

        let raw_max_version = resp
            .latest_table_version
            .or(resp.metadata.last_commit_version)
            .unwrap_or(-1);
        let max_version = if raw_max_version < 0 {
            0
        } else {
            raw_max_version as u64
        };

        Ok(CommitList {
            commits: resp.commits,
            max_version,
        })
    }
}

/// A [`LogStore`] for catalog-managed tables that serves version metadata from a
/// [`CommitCoordinator`].
///
/// File I/O (reading commit files, checkpoints, and data files) goes to the underlying object
/// stores exactly as for filesystem-managed tables. What changes is *version discovery*:
///
/// - [`get_latest_version`](LogStore::get_latest_version) returns the catalog-ratified maximum
///   version, which may lag behind the newest commit file on the filesystem.
/// - [`catalog_log_tail`](LogStore::catalog_log_tail) returns the catalog's staged commits as
///   a [`CatalogLogTail`], which the kernel both replays and uses as an upper bound
///   (`max_version`) on filesystem versions.
///
/// The store is generic over the coordinator so tests can substitute a mock; production use
/// goes through [`UnityCommitCoordinator`].
#[derive(Debug, Clone)]
pub struct CatalogManagedLogStore<C: CommitCoordinator> {
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    config: LogStoreConfig,
    coordinator: Arc<C>,
}

impl<C: CommitCoordinator> CatalogManagedLogStore<C> {
    /// Create a store over `prefixed_store` (rooted at the table) and `root_store` (rooted at
    /// the storage system root), serving version metadata from `coordinator`.
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

    /// Read a published commit entry from the object store.
    ///
    /// Ratified commits of a catalog-managed table still live in `_delta_log/`, so reads are
    /// served by the prefixed store exactly as for filesystem-managed tables.
    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        deltalake_core::logstore::read_commit_entry(self.prefixed_store.as_ref(), version).await
    }

    /// Return the catalog-ratified maximum version reported by the coordinator.
    ///
    /// `start_version` is ignored: the catalog, not a filesystem listing, decides the latest
    /// version of a catalog-managed table.
    async fn get_latest_version(&self, _start_version: Version) -> DeltaResult<Version> {
        Ok(self.coordinator.get_commits().await?.max_version)
    }

    /// Return the catalog's staged commits as a [`CatalogLogTail`].
    ///
    /// Unity Catalog returns commits newest-first, so they are sorted ascending by version
    /// before being turned into `LogPath::staged_commit` entries: the kernel requires an
    /// oldest-first, gapless tail. Commit file names are resolved relative to the table's
    /// `_delta_log/` location. The returned `max_version` is the catalog-ratified maximum the
    /// kernel caps replay at.
    async fn catalog_log_tail(&self) -> DeltaResult<Option<CatalogLogTail>> {
        let list = self.coordinator.get_commits().await?;
        let mut base = self.config.location().clone();
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let mut log_tail = Vec::with_capacity(list.commits.len());
        let mut commits = list.commits;
        commits.sort_by_key(|c| c.version);
        for c in commits {
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

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.prefixed_store.clone()
    }

    fn root_object_store(&self) -> Arc<dyn ObjectStore> {
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
        cfg.raw.insert("catalog_managed".into(), "true".into());
        assert!(crate::is_catalog_managed_requested(&cfg));
    }
}
