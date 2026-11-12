use crate::UnityCatalogError;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use delta_kernel::LogPath;
use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::kernel::{Action, CommitInfo, Version};
use deltalake_core::logstore::object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode,
};
use deltalake_core::logstore::{
    CatalogLogTail, CommitOrBytes, CommitResponse, Committer, LogStore, LogStoreConfig,
    ObjectStoreRef, PayloadKind, commit_uri_from_version, get_actions,
};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use deltalake_core::{DeltaResult, DeltaTableError, Path};
use tracing::warn;
use unity_catalog_delta_client_api::{
    Commit, DeltaTableRequirement, DeltaTableUpdate, Error as UCError, TableIdentifier,
    UpdateTableClient, UpdateTableRequest,
};
use unity_catalog_delta_rest_client::{UCDeltaTableClient, UCUpdateTableRestClient};
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct CommitList {
    pub commits: Vec<Commit>,
    pub max_version: Version,
}

#[async_trait]
pub trait CommitCoordinator: Send + Sync + Debug {
    fn table_uuid(&self) -> &str;
    async fn get_commits(&self) -> DeltaResult<CommitList>;
    async fn publish(&self, store: &ObjectStoreRef, up_to: Version) -> DeltaResult<()>;
}

fn staged_commit_path(file_name: &str) -> DeltaResult<Path> {
    Ok(Path::from(format!(
        "_delta_log/_staged_commits/{}",
        staged_file_name(file_name)?
    )))
}

fn staged_file_name(file_name: &str) -> DeltaResult<String> {
    let parsed = Path::parse(file_name)?;
    Ok(parsed.filename().unwrap_or(file_name).to_string())
}

#[derive(Debug)]
pub struct UnityCommitCoordinator {
    rest: Arc<UCDeltaTableClient>,
    update: Arc<UCUpdateTableRestClient>,
    catalog: String,
    schema: String,
    table: String,
    table_uuid: String,
    store: ObjectStoreRef,
    last_published_version: AtomicI64,
}

impl UnityCommitCoordinator {
    pub fn new(
        rest: Arc<UCDeltaTableClient>,
        update: Arc<UCUpdateTableRestClient>,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        table_uuid: impl Into<String>,
        store: ObjectStoreRef,
    ) -> Self {
        Self {
            rest,
            update,
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
            table_uuid: table_uuid.into(),
            store,
            last_published_version: AtomicI64::new(-1),
        }
    }

    fn target(&self) -> TableIdentifier {
        TableIdentifier::new(&self.catalog, &self.schema, &self.table)
    }

    fn update_request(
        &self,
        update: DeltaTableUpdate,
    ) -> Result<UpdateTableRequest, TransactionError> {
        let reqs = DeltaTableRequirement::AssertTableUuid {
            uuid: self.table_uuid.clone(),
        };
        let mut updates = vec![update];
        let latest_published_version = self.last_published_version.load(Ordering::Relaxed);
        if latest_published_version >= 0 {
            let last_version = DeltaTableUpdate::SetLatestBackfilledVersion {
                latest_published_version,
            };
            updates.push(last_version);
        }

        UpdateTableRequest::new(vec![reqs], updates).map_err(|e| TransactionError::LogStoreError {
            msg: e.to_string(),
            source: Box::new(e),
        })
    }
}

/// The staged-commit `file_name` and object-store `Path` for `version` under
/// `_delta_log/_staged_commits/`, using a random UUID to keep concurrent writers' files distinct.
fn staged_commit_file(version: Version, uuid: Uuid) -> (String, Path) {
    let file_name = format!("{version:020}.{uuid}.json");
    let path = Path::from(format!("_delta_log/_staged_commits/{file_name}"));
    (file_name, path)
}

/// Wrap an error as a `TransactionError` the commit loop treats as a hard (non-conflict) failure.
fn commit_failed(msg: String) -> TransactionError {
    TransactionError::LogStoreError {
        source: Box::new(DeltaTableError::Generic(msg.clone())),
        msg,
    }
}

async fn classify_attempt(
    store: &dyn ObjectStore,
    version: Version,
    file_name: &str,
    txn_id: Option<&str>,
    list: &CommitList,
) -> Option<CommitResponse> {
    if let Some(commit) = list.commits.iter().find(|c| c.version as u64 == version) {
        return match staged_file_name(&commit.file_name) {
            Ok(name) if name == file_name => Some(CommitResponse::Committed),
            _ => Some(CommitResponse::Conflict { version }),
        };
    }
    if list.max_version < version || txn_id.is_none() {
        return None;
    }

    let bytes = store
        .get(&commit_uri_from_version(Some(version)))
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    let published_txn_id = get_actions(version, &bytes)
        .ok()?
        .into_iter()
        .find_map(|action| match action {
            Action::CommitInfo(info) => Some(info.txn_id),
            _ => None,
        })?;

    if published_txn_id.as_deref() == txn_id {
        Some(CommitResponse::Committed)
    } else {
        Some(CommitResponse::Conflict { version })
    }
}

#[async_trait]
impl Committer for UnityCommitCoordinator {
    async fn commit(
        &self,
        version: Version,
        payload: CommitOrBytes,
        commit_info: Option<&CommitInfo>,
    ) -> Result<CommitResponse, TransactionError> {
        let CommitOrBytes::LogBytes(bytes) = payload else {
            return Err(commit_failed(
                "CatalogManagedLogStore requires a LogBytes payload".to_string(),
            ));
        };

        let (file_name, staged_path) = staged_commit_file(version, Uuid::new_v4());
        self.store.put(&staged_path, bytes.clone().into()).await?;
        let (file_size, modified) = match self.store.head(&staged_path).await {
            Ok(meta) => (meta.size as i64, meta.last_modified.timestamp_millis()),
            Err(_) => (bytes.len() as i64, Utc::now().timestamp_millis()),
        };
        let timestamp = commit_info
            .and_then(|info| info.in_commit_timestamp.or(info.timestamp))
            .unwrap_or(modified);
        let commit = Commit::new(
            version as i64,
            timestamp,
            file_name.clone(),
            file_size,
            modified,
        );
        let request = self.update_request(DeltaTableUpdate::AddCommit {
            commit: commit.clone(),
        })?;
        let target = self.target();

        let mut outcome = self.update.update_table(&target, request).await;
        // TODO: this is not correct, need to stop it from doubling up on requests
        if matches!(
            outcome,
            Err(UCError::MaxUnpublishedCommitsExceeded(_num_commits))
        ) {
            warn!(
                version,
                "UC reports too many unpublished commits; publishing then retrying ratify"
            );
            self.publish(&self.store, version.saturating_sub(1))
                .await
                .map_err(|e| commit_failed(format!("Failed to publish changes: {}", e)))?;
            let request = self.update_request(DeltaTableUpdate::AddCommit { commit })?;
            outcome = self.update.update_table(&target, request).await;
        }

        match outcome {
            Ok(()) => Ok(CommitResponse::Committed),
            // Not sure what to do on conflicts right now from the catalog, not even sure what the catalog returns
            // when a conflict occurs
            // Err(err) if is_version_conflict(&err) => Ok(CommitResponse::Conflict { version }),
            Err(err) => {
                let txn_id = commit_info.and_then(|info| info.txn_id.as_deref());
                let resolved = match self.get_commits().await {
                    Ok(list) => {
                        classify_attempt(self.store.as_ref(), version, &file_name, txn_id, &list)
                            .await
                    }
                    Err(_) => None,
                };
                resolved.ok_or_else(|| {
                    commit_failed(format!(
                        "UC update_table failed for version {version}: {err}. The staged commit \
                         {file_name} is kept because the catalog may still ratify it; reload the \
                         table to see whether version {version} was committed before retrying"
                    ))
                })
            }
        }
    }

    async fn abort(
        &self,
        _version: Version,
        _payload: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        Ok(())
    }

    fn payload_kind(&self) -> PayloadKind {
        PayloadKind::Bytes
    }
}

#[async_trait]
impl CommitCoordinator for UnityCommitCoordinator {
    fn table_uuid(&self) -> &str {
        &self.table_uuid
    }

    async fn get_commits(&self) -> DeltaResult<CommitList> {
        let resp = self
            .rest
            .load_table(&self.catalog, &self.schema, &self.table)
            .await
            .map_err(UnityCatalogError::from)?;

        let raw_max_version = resp
            .latest_table_version
            .or(resp.metadata.last_commit_version)
            .unwrap_or(-1);

        let last_published_version = resp
            .commits
            .iter()
            .map(|c| c.version - 1)
            .min()
            .unwrap_or(raw_max_version);

        self.last_published_version
            .fetch_max(last_published_version, Ordering::Relaxed);

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

    async fn publish(&self, store: &ObjectStoreRef, up_to: Version) -> DeltaResult<()> {
        let list = self.get_commits().await?;
        let mut commits = list.commits.to_vec();
        commits.sort_by_key(|c| c.version);
        let mut published = commits.first().map(|c| c.version - 1).unwrap_or(0) as Version;
        for commit in commits {
            let version = commit.version as u64;
            if version > up_to {
                break;
            }
            if published + 1 != version {
                return Err(DeltaTableError::Generic(format!(
                    "catalog commits to publish are not contiguous: version {published} is followed by {version}",
                )));
            }
            let numbered = commit_uri_from_version(Some(version));
            if store.head(&numbered).await.is_err() {
                let staged_path = staged_commit_path(&commit.file_name)?;
                let bytes = store.get(&staged_path).await?.bytes().await?;
                match store
                    .put_opts(&numbered, bytes.into(), PutMode::Create.into())
                    .await
                {
                    Ok(_) | Err(ObjectStoreError::AlreadyExists { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            published = version;
            self.last_published_version
                .fetch_max(published as i64, Ordering::Relaxed);
        }

        Ok(())
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
impl<C: CommitCoordinator + Committer + 'static> LogStore for CatalogManagedLogStore<C> {
    fn name(&self) -> String {
        "CatalogManagedLogStore".into()
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        let list = self.coordinator.get_commits().await?;
        if version > list.max_version {
            return Ok(None);
        }
        if let Some(commit) = list.commits.iter().find(|c| c.version as u64 == version) {
            let staged_path = staged_commit_path(&commit.file_name)?;
            return Ok(Some(
                self.prefixed_store.get(&staged_path).await?.bytes().await?,
            ));
        }
        deltalake_core::logstore::read_commit_entry(self.prefixed_store.as_ref(), version).await
    }

    async fn get_latest_version(&self, _start_version: Version) -> DeltaResult<Version> {
        Ok(self.coordinator.get_commits().await?.max_version)
    }

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
            log_tail.push(LogPath::staged_commit(
                base.clone(),
                &staged_file_name(&c.file_name)?,
                c.file_modification_timestamp,
                c.file_size as u64,
            )?);
        }

        Ok(Some(CatalogLogTail {
            log_tail,
            max_version: list.max_version,
        }))
    }

    fn is_catalog_managed(&self) -> bool {
        true
    }

    async fn publish_logs(&self, up_to: Version) -> DeltaResult<()> {
        self.coordinator.publish(&self.prefixed_store, up_to).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.prefixed_store.clone()
    }

    fn root_object_store(&self) -> Arc<dyn ObjectStore> {
        self.root_store.clone()
    }

    fn committer(&self) -> Arc<dyn Committer> {
        self.coordinator.clone()
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

    /// Test double: serves a fixed `CommitList` for reads.
    #[derive(Debug)]
    struct MockCoordinator {
        list: CommitList,
    }

    #[async_trait]
    impl CommitCoordinator for MockCoordinator {
        fn table_uuid(&self) -> &str {
            TABLE_UUID
        }

        async fn get_commits(&self) -> DeltaResult<CommitList> {
            Ok(self.list.clone())
        }

        async fn publish(&self, _store: &ObjectStoreRef, _up_to: Version) -> DeltaResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl Committer for MockCoordinator {
        async fn commit(
            &self,
            _version: Version,
            _payload: CommitOrBytes,
            _commit_info: Option<&CommitInfo>,
        ) -> Result<CommitResponse, TransactionError> {
            unimplemented!()
        }

        async fn abort(
            &self,
            _version: Version,
            _payload: CommitOrBytes,
        ) -> Result<(), TransactionError> {
            Ok(())
        }

        fn payload_kind(&self) -> PayloadKind {
            PayloadKind::Bytes
        }
    }

    const TABLE_UUID: &str = "3a0d65cd-4a56-49a8-937b-95f9e3ee90e5";

    fn store() -> ObjectStoreRef {
        Arc::new(InMemory::new())
    }

    fn log_store(list: CommitList) -> CatalogManagedLogStore<MockCoordinator> {
        let url = Url::parse("memory:///cat.schema.table").unwrap();
        let config = LogStoreConfig::new(&url, StorageConfig::default());
        CatalogManagedLogStore::new(store(), store(), config, Arc::new(MockCoordinator { list }))
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
    fn staged_commit_file_is_zero_padded_under_staged_commits() {
        let (file_name, path) = staged_commit_file(5, Uuid::nil());
        assert_eq!(
            file_name,
            "00000000000000000005.00000000-0000-0000-0000-000000000000.json"
        );
        assert_eq!(
            path.as_ref(),
            "_delta_log/_staged_commits/00000000000000000005.00000000-0000-0000-0000-000000000000.json"
        );
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

    #[tokio::test]
    async fn read_commit_entry_falls_back_to_staged_tail() {
        let prefixed: ObjectStoreRef = Arc::new(InMemory::new());
        let staged = Path::from(
            "_delta_log/_staged_commits/00000000000000000002.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json",
        );
        prefixed
            .put(&staged, Bytes::from_static(b"{\"commitInfo\":{}}").into())
            .await
            .unwrap();

        let url = Url::parse("memory:///cat.schema.table").unwrap();
        let config = LogStoreConfig::new(&url, StorageConfig::default());
        let ls = CatalogManagedLogStore::new(
            prefixed.clone(),
            store(),
            config,
            Arc::new(MockCoordinator {
                list: sample_commits(),
            }),
        );

        assert!(ls.read_commit_entry(2).await.unwrap().is_some());
        assert!(ls.read_commit_entry(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_commit_entry_defers_to_catalog_over_numbered_files() {
        let prefixed: ObjectStoreRef = Arc::new(InMemory::new());
        let staged = Path::from(
            "_delta_log/_staged_commits/00000000000000000002.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json",
        );
        prefixed
            .put(&staged, Bytes::from_static(b"staged").into())
            .await
            .unwrap();
        for version in [0, 2, 3] {
            prefixed
                .put(
                    &commit_uri_from_version(Some(version)),
                    Bytes::from_static(b"numbered").into(),
                )
                .await
                .unwrap();
        }

        let url = Url::parse("memory:///cat.schema.table").unwrap();
        let config = LogStoreConfig::new(&url, StorageConfig::default());
        let ls = CatalogManagedLogStore::new(
            prefixed,
            store(),
            config,
            Arc::new(MockCoordinator {
                list: sample_commits(),
            }),
        );

        // Published version outside the catalog tail is read from the numbered file.
        assert_eq!(ls.read_commit_entry(0).await.unwrap().unwrap(), "numbered");
        // The catalog-supplied staged commit wins over a numbered copy.
        assert_eq!(ls.read_commit_entry(2).await.unwrap().unwrap(), "staged");
        // Versions above the catalog maximum are not ratified, even if a file exists.
        assert!(ls.read_commit_entry(3).await.unwrap().is_none());
    }

    const OURS: &str = "00000000000000000003.11111111-1111-4111-8111-111111111111.json";
    const THEIRS: &str = "00000000000000000003.22222222-2222-4222-8222-222222222222.json";

    fn listed(version: i64, file_name: &str, max_version: Version) -> CommitList {
        CommitList {
            commits: vec![Commit::new(version, 0, file_name, 10, 0)],
            max_version,
        }
    }

    const OUR_TXN: &str = "our-txn-id";

    fn all_published(max_version: Version) -> CommitList {
        CommitList {
            commits: vec![],
            max_version,
        }
    }

    async fn publish_commit(store: &ObjectStoreRef, version: Version, txn_id: Option<&str>) {
        let info = CommitInfo {
            txn_id: txn_id.map(str::to_owned),
            ..Default::default()
        };
        let bytes = serde_json::to_string(&Action::CommitInfo(info)).unwrap();
        store
            .put(&commit_uri_from_version(Some(version)), bytes.into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn classify_listed_attempt_by_staged_file_name() {
        let store = store();
        let list = listed(3, OURS, 3);
        assert_eq!(
            classify_attempt(store.as_ref(), 3, OURS, Some(OUR_TXN), &list).await,
            Some(CommitResponse::Committed)
        );
        let prefixed = listed(3, &format!("_staged_commits/{OURS}"), 3);
        assert_eq!(
            classify_attempt(store.as_ref(), 3, OURS, Some(OUR_TXN), &prefixed).await,
            Some(CommitResponse::Committed)
        );
        let lost = listed(3, THEIRS, 3);
        assert_eq!(
            classify_attempt(store.as_ref(), 3, OURS, Some(OUR_TXN), &lost).await,
            Some(CommitResponse::Conflict { version: 3 })
        );
    }

    #[tokio::test]
    async fn classify_unratified_attempt_is_unknown() {
        let list = listed(2, "00000000000000000002.x.json", 2);
        assert_eq!(
            classify_attempt(store().as_ref(), 3, OURS, Some(OUR_TXN), &list).await,
            None
        );
    }

    #[tokio::test]
    async fn classify_published_attempt_by_txn_id() {
        let ours = store();
        publish_commit(&ours, 3, Some(OUR_TXN)).await;
        assert_eq!(
            classify_attempt(ours.as_ref(), 3, OURS, Some(OUR_TXN), &all_published(4)).await,
            Some(CommitResponse::Committed)
        );

        let theirs = store();
        publish_commit(&theirs, 3, Some("their-txn-id")).await;
        assert_eq!(
            classify_attempt(theirs.as_ref(), 3, OURS, Some(OUR_TXN), &all_published(3)).await,
            Some(CommitResponse::Conflict { version: 3 })
        );

        let untagged = store();
        publish_commit(&untagged, 3, None).await;
        assert_eq!(
            classify_attempt(untagged.as_ref(), 3, OURS, Some(OUR_TXN), &all_published(3)).await,
            Some(CommitResponse::Conflict { version: 3 })
        );
    }

    #[tokio::test]
    async fn classify_published_attempt_without_evidence_is_unknown() {
        let missing = store();
        assert_eq!(
            classify_attempt(missing.as_ref(), 3, OURS, Some(OUR_TXN), &all_published(3)).await,
            None
        );

        let published = store();
        publish_commit(&published, 3, Some(OUR_TXN)).await;
        assert_eq!(
            classify_attempt(published.as_ref(), 3, OURS, None, &all_published(3)).await,
            None
        );
    }
}
