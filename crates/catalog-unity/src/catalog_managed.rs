use std::fmt::Debug;
use std::sync::Arc;

use crate::UnityCatalogError;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use delta_kernel::LogPath;
use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::kernel::{CommitInfo, Version};
use deltalake_core::logstore::object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode,
};
use deltalake_core::logstore::{
    CatalogLogTail, CommitOrBytes, CommitResponse, Committer, LogStore, LogStoreConfig,
    ObjectStoreRef, PayloadKind, commit_uri_from_version,
};

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
        }
    }

    fn target(&self) -> TableIdentifier {
        TableIdentifier::new(&self.catalog, &self.schema, &self.table)
    }

    fn update_request(&self, update: DeltaTableUpdate) -> UpdateTableRequest {
        let reqs = DeltaTableRequirement::AssertTableUuid {
            uuid: self.table_uuid.clone(),
        };
        UpdateTableRequest::new(vec![reqs], vec![update]).unwrap()
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

fn classify_attempt(
    version: Version,
    file_name: &str,
    list: &CommitList,
) -> Option<CommitResponse> {
    let commit = list.commits.iter().find(|c| c.version as u64 == version)?;
    match staged_file_name(&commit.file_name) {
        Ok(name) if name == file_name => Some(CommitResponse::Committed),
        _ => Some(CommitResponse::Conflict { version }),
    }
}

async fn publish_commits(
    store: &ObjectStoreRef,
    commits: &[Commit],
    up_to: Version,
) -> DeltaResult<Option<Version>> {
    let mut published = 0;
    let mut commits = commits.to_vec();
    commits.sort_by_key(|c| c.version);
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
    }
    Ok(Some(published))
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
        let request = self.update_request(DeltaTableUpdate::AddCommit { commit });
        let target = self.target();

        let mut outcome = self.update.update_table(&target, request.clone()).await;
        // TODO: this is not correct, need to stop it from doubling up on requests
        if matches!(
            outcome,
            Err(UCError::MaxUnpublishedCommitsExceeded(_num_commits))
        ) {
            warn!(
                version,
                "UC reports too many unpublished commits; publishing then retrying ratify"
            );
            if let Err(e) = self.publish(&self.store, version.saturating_sub(1)).await {
                warn!(error = %e, "publish before ratify retry failed");
            }
            outcome = self.update.update_table(&target, request).await;
        }

        match outcome {
            Ok(()) => Ok(CommitResponse::Committed),
            // Not sure what to do on conflicts right now from the catalog, not even sure what the catalog returns
            // when a conflict occurs
            // Err(err) if is_version_conflict(&err) => Ok(CommitResponse::Conflict { version }),
            Err(err) => self
                .get_commits()
                .await
                .ok()
                .and_then(|list| classify_attempt(version, &file_name, &list))
                .ok_or_else(|| {
                    commit_failed(format!(
                        "UC update_table failed for version {version}: {err}. The staged commit \
                         {file_name} is kept because the catalog may still ratify it; reload the \
                         table to see whether version {version} was committed before retrying"
                    ))
                }),
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
        if let Some(published) = publish_commits(store, &list.commits, up_to).await? {
            let request = self.update_request(DeltaTableUpdate::SetLatestBackfilledVersion {
                latest_published_version: published as i64,
            });
            self.update
                .update_table(&self.target(), request)
                .await
                .map_err(UnityCatalogError::from)?;
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
        if let Some(bytes) =
            deltalake_core::logstore::read_commit_entry(self.prefixed_store.as_ref(), version)
                .await?
        {
            return Ok(Some(bytes));
        }
        let list = self.coordinator.get_commits().await?;
        let Some(commit) = list.commits.iter().find(|c| c.version as u64 == version) else {
            return Ok(None);
        };
        let staged_path = staged_commit_path(&commit.file_name)?;
        Ok(Some(
            self.prefixed_store.get(&staged_path).await?.bytes().await?,
        ))
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

    #[test]
    fn catalog_managed_is_opt_in() {
        let mut cfg = StorageConfig::default();
        assert!(!crate::is_catalog_managed_requested(&cfg));
        cfg.raw.insert("catalog_managed".into(), "true".into());
        assert!(crate::is_catalog_managed_requested(&cfg));
        cfg.raw.insert("catalog_managed".into(), "false".into());
        assert!(!crate::is_catalog_managed_requested(&cfg));
    }
    const OURS: &str = "00000000000000000003.11111111-1111-4111-8111-111111111111.json";
    const THEIRS: &str = "00000000000000000003.22222222-2222-4222-8222-222222222222.json";

    fn listed(version: i64, file_name: &str, max_version: Version) -> CommitList {
        CommitList {
            commits: vec![Commit::new(version, 0, file_name, 10, 0)],
            max_version,
        }
    }

    #[test]
    fn classify_listed_attempt_by_staged_file_name() {
        let list = listed(3, OURS, 3);
        assert_eq!(
            classify_attempt(3, OURS, &list),
            Some(CommitResponse::Committed)
        );
        let prefixed = listed(3, &format!("_staged_commits/{OURS}"), 3);
        assert_eq!(
            classify_attempt(3, OURS, &prefixed),
            Some(CommitResponse::Committed)
        );
        let lost = listed(3, THEIRS, 3);
        assert_eq!(
            classify_attempt(3, OURS, &lost),
            Some(CommitResponse::Conflict { version: 3 })
        );
    }

    #[test]
    fn classify_unratified_attempt_is_unknown() {
        let list = listed(2, "00000000000000000002.x.json", 2);
        assert_eq!(classify_attempt(3, OURS, &list), None);
    }

    async fn put(store: &ObjectStoreRef, path: &str, body: &'static [u8]) {
        store
            .put(&Path::from(path), Bytes::from_static(body).into())
            .await
            .unwrap();
    }

    async fn read(store: &ObjectStoreRef, path: &str) -> Option<Bytes> {
        match store.get(&Path::from(path)).await {
            Ok(res) => Some(res.bytes().await.unwrap()),
            Err(ObjectStoreError::NotFound { .. }) => None,
            Err(e) => panic!("{e}"),
        }
    }

    fn staged(version: i64) -> (String, String) {
        let name = format!("{version:020}.{}.json", Uuid::nil());
        let path = format!("_delta_log/_staged_commits/{name}");
        (name, path)
    }

    #[tokio::test]
    async fn publish_copies_contiguous_commits_in_order() {
        let store = store();
        let (n1, p1) = staged(1);
        let (n2, p2) = staged(2);
        put(&store, &p1, b"one").await;
        put(&store, &p2, b"two").await;
        put(&store, "_delta_log/00000000000000000001.json", b"one").await;
        let commits = vec![
            Commit::new(2, 0, n2, 3, 0),
            Commit::new(1, 0, format!("_staged_commits/{n1}"), 3, 0),
        ];

        let published = publish_commits(&store, &commits, 2).await.unwrap();
        assert_eq!(published, Some(2));
        assert_eq!(
            read(&store, "_delta_log/00000000000000000002.json").await,
            Some(Bytes::from_static(b"two"))
        );
    }

    #[tokio::test]
    async fn publish_fails_on_missing_staged_source() {
        let store = store();
        let (n1, p1) = staged(1);
        let (n2, _p2) = staged(2);
        put(&store, &p1, b"one").await;
        let commits = vec![Commit::new(1, 0, n1, 3, 0), Commit::new(2, 0, n2, 3, 0)];

        let err = publish_commits(&store, &commits, 2).await.unwrap_err();
        assert!(
            matches!(
                err,
                DeltaTableError::ObjectStore {
                    source: ObjectStoreError::NotFound { .. }
                }
            ),
            "{err}"
        );
        assert!(
            read(&store, "_delta_log/00000000000000000001.json")
                .await
                .is_some()
        );
        assert!(
            read(&store, "_delta_log/00000000000000000002.json")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn publish_refuses_non_contiguous_commits() {
        let store = store();
        let (n1, p1) = staged(1);
        let (n3, p3) = staged(3);
        put(&store, &p1, b"one").await;
        put(&store, &p3, b"three").await;
        let commits = vec![Commit::new(1, 0, n1, 3, 0), Commit::new(3, 0, n3, 5, 0)];

        let err = publish_commits(&store, &commits, 3).await.unwrap_err();
        assert!(err.to_string().contains("not contiguous"), "{err}");
        assert!(
            read(&store, "_delta_log/00000000000000000001.json")
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn publish_keeps_an_existing_published_file() {
        let store = store();
        let (n1, p1) = staged(1);
        put(&store, &p1, b"staged").await;
        put(&store, "_delta_log/00000000000000000001.json", b"published").await;
        let commits = vec![Commit::new(1, 0, n1, 6, 0)];

        assert_eq!(publish_commits(&store, &commits, 1).await.unwrap(), Some(1));
        assert_eq!(
            read(&store, "_delta_log/00000000000000000001.json").await,
            Some(Bytes::from_static(b"published"))
        );
    }
}
