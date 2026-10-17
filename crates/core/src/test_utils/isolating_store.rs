//! In-memory log store that isolates every operation, for tests of the scope machinery.
//!
//! [`IsolatingLogStore`] behaves like a branching backend: every operation writes below
//! `scopes/{n}/{table}` until its scope is published, and the store records what the backend saw.
//! Tests use it to prove that an operation opens exactly one scope, keeps every write inside it,
//! and finishes or aborts it.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result as ObjectStoreResult,
};
use url::Url;

use crate::kernel::Version;
use crate::kernel::transaction::TransactionError;
use crate::logstore::default_logstore::DefaultLogStore;
use crate::logstore::{
    CommitOrBytes, CommitResponse, Committer, LogStore, LogStoreConfig, OperationContext,
    OperationTransaction, PayloadKind, StorageConfig, commit_uri_from_version,
};
use crate::{DeltaResult, DeltaTableError};

/// What the backend saw, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeEvent {
    /// A scope with this number was opened.
    Begin(usize),
    /// A commit of `version` was published from the scope.
    Commit {
        /// The scope that committed.
        scope: usize,
        /// The version that was committed.
        version: Version,
    },
    /// A commit of `version` was refused because the table already had that version.
    Conflict {
        /// The scope that tried to commit.
        scope: usize,
        /// The version that already existed.
        version: Version,
    },
    /// The scope was finished; `dirty` tells whether unpublished writes were pending.
    Finish {
        /// The scope that finished.
        scope: usize,
        /// Whether writes were pending since the last published commit.
        dirty: bool,
    },
    /// The scope was aborted.
    Abort(usize),
}

/// Root store that records the path of every write and can stall writes on demand.
struct RecordingRoot {
    inner: InMemory,
    writes: Mutex<Vec<Path>>,
    deletes: Arc<Mutex<Vec<Path>>>,
    stall_writes: AtomicBool,
}

impl fmt::Debug for RecordingRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecordingRoot")
    }
}

impl fmt::Display for RecordingRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecordingRoot")
    }
}

impl RecordingRoot {
    async fn wait_if_stalled(&self) {
        while self.stall_writes.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
}

#[async_trait]
impl ObjectStore for RecordingRoot {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.wait_if_stalled().await;
        self.writes.lock().unwrap().push(location.clone());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.wait_if_stalled().await;
        self.writes.lock().unwrap().push(location.clone());
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let deletes = self.deletes.clone();
        let locations = locations
            .map(move |location| {
                if let Ok(path) = &location {
                    deletes.lock().unwrap().push(path.clone());
                }
                location
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.writes.lock().unwrap().push(to.clone());
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.writes.lock().unwrap().push(to.clone());
        self.deletes.lock().unwrap().push(from.clone());
        self.inner.rename_opts(from, to, options).await
    }
}

struct Shared {
    root: Arc<RecordingRoot>,
    table_prefix: String,
    events: Mutex<Vec<ScopeEvent>>,
    fail_finish_of_scope: Mutex<Option<usize>>,
}

impl Shared {
    fn scope_prefix(&self, scope: usize) -> String {
        format!("scopes/{scope}/{}", self.table_prefix)
    }

    fn record(&self, event: ScopeEvent) {
        self.events.lock().unwrap().push(event);
    }

    async fn list_prefix(&self, prefix: &str) -> Vec<Path> {
        self.root
            .inner
            .list(Some(&Path::from(prefix)))
            .map_ok(|meta| meta.location)
            .try_collect()
            .await
            .unwrap()
    }

    /// Copy every object below `from` to the same relative location below `to`.
    async fn copy_tree(&self, from: &str, to: &str) {
        for path in self.list_prefix(from).await {
            let relative = path.as_ref().strip_prefix(from).unwrap();
            let target = Path::from(format!("{to}{relative}"));
            let bytes = self
                .root
                .inner
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            self.root.inner.put(&target, bytes.into()).await.unwrap();
        }
    }

    /// Publish the scope: copy every object of the scope into the table and replay deletes.
    async fn publish(&self, scope: usize) {
        let scope_prefix = self.scope_prefix(scope);
        let deletes: Vec<Path> = self.root.deletes.lock().unwrap().clone();
        for path in deletes {
            if let Some(relative) = path.as_ref().strip_prefix(&scope_prefix) {
                let target = Path::from(format!("{}{relative}", self.table_prefix));
                let _ = self.root.inner.delete(&target).await;
            }
        }
        self.copy_tree(&scope_prefix, &self.table_prefix).await;
    }

    async fn drop_scope(&self, scope: usize) {
        for path in self.list_prefix(&self.scope_prefix(scope)).await {
            self.root.inner.delete(&path).await.unwrap();
        }
    }
}

struct FakeCommitter {
    shared: Arc<Shared>,
    scope: usize,
}

#[async_trait]
impl Committer for FakeCommitter {
    async fn commit(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<CommitResponse, TransactionError> {
        let CommitOrBytes::LogBytes(bytes) = payload else {
            panic!("the fake isolating store only accepts commit bytes")
        };
        let relative = commit_uri_from_version(Some(version));
        let scope_commit = Path::from(format!(
            "{}/{relative}",
            self.shared.scope_prefix(self.scope)
        ));
        let parent_commit = Path::from(format!("{}/{relative}", self.shared.table_prefix));
        let create = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let inner = &self.shared.root.inner;
        match inner
            .put_opts(&scope_commit, bytes.into(), create.clone())
            .await
        {
            Ok(_) => {}
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                return Ok(CommitResponse::Conflict { version });
            }
            Err(err) => return Err(err.into()),
        }
        if inner.head(&parent_commit).await.is_ok() {
            inner.delete(&scope_commit).await.unwrap();
            self.shared.record(ScopeEvent::Conflict {
                scope: self.scope,
                version,
            });
            return Ok(CommitResponse::Conflict { version });
        }
        self.shared.publish(self.scope).await;
        self.shared.record(ScopeEvent::Commit {
            scope: self.scope,
            version,
        });
        Ok(CommitResponse::Committed)
    }

    async fn abort(
        &self,
        version: Version,
        _payload: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        let scope_commit = Path::from(format!(
            "{}/{}",
            self.shared.scope_prefix(self.scope),
            commit_uri_from_version(Some(version))
        ));
        let _ = self.shared.root.inner.delete(&scope_commit).await;
        Ok(())
    }

    fn payload_kind(&self) -> PayloadKind {
        PayloadKind::Bytes
    }
}

struct FakeTransaction {
    shared: Arc<Shared>,
    scope: usize,
}

#[async_trait]
impl OperationTransaction for FakeTransaction {
    async fn finish(&self, dirty: bool) -> DeltaResult<()> {
        self.shared.record(ScopeEvent::Finish {
            scope: self.scope,
            dirty,
        });
        if *self.shared.fail_finish_of_scope.lock().unwrap() == Some(self.scope) {
            return Err(DeltaTableError::Generic(
                "injected failure while publishing the scope".into(),
            ));
        }
        if dirty {
            self.shared.publish(self.scope).await;
        }
        self.shared.drop_scope(self.scope).await;
        Ok(())
    }

    async fn abort(&self) -> DeltaResult<()> {
        self.shared.record(ScopeEvent::Abort(self.scope));
        self.shared.drop_scope(self.scope).await;
        Ok(())
    }
}

/// A log store over an in-memory root where every operation writes below
/// `scopes/{n}/{table}` until it is published.
pub struct IsolatingLogStore {
    parent: DefaultLogStore,
    shared: Arc<Shared>,
    next_scope: AtomicUsize,
}

impl fmt::Debug for IsolatingLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IsolatingLogStore")
    }
}

impl IsolatingLogStore {
    /// A store for the table at `memory:///table`.
    pub fn new() -> Arc<Self> {
        Self::new_at("table")
    }

    /// A store for the table at `memory:///{table_prefix}`.
    pub fn new_at(table_prefix: &str) -> Arc<Self> {
        let root = Arc::new(RecordingRoot {
            inner: InMemory::new(),
            writes: Mutex::new(Vec::new()),
            deletes: Arc::new(Mutex::new(Vec::new())),
            stall_writes: AtomicBool::new(false),
        });
        let location = Url::parse(&format!("memory:///{table_prefix}")).unwrap();
        let parent = DefaultLogStore::new(
            Arc::new(PrefixStore::new(root.clone(), table_prefix)),
            root.clone(),
            LogStoreConfig::new(&location, StorageConfig::default()),
        );
        Arc::new(Self {
            parent,
            shared: Arc::new(Shared {
                root,
                table_prefix: table_prefix.to_string(),
                events: Mutex::new(Vec::new()),
                fail_finish_of_scope: Mutex::new(None),
            }),
            next_scope: AtomicUsize::new(1),
        })
    }

    /// Everything the backend saw so far, in order.
    pub fn events(&self) -> Vec<ScopeEvent> {
        self.shared.events.lock().unwrap().clone()
    }

    /// Forget the recorded events.
    pub fn clear_events(&self) {
        self.shared.events.lock().unwrap().clear();
    }

    /// Forget recorded events and writes and number the next scope 1 again.
    pub fn reset(&self) {
        self.clear_events();
        self.take_recorded_writes();
        self.shared.root.deletes.lock().unwrap().clear();
        self.next_scope.store(1, Ordering::SeqCst);
    }

    /// Paths written through the store since the last call, in order.
    pub fn take_recorded_writes(&self) -> Vec<Path> {
        std::mem::take(&mut *self.shared.root.writes.lock().unwrap())
    }

    /// The scope number the next `begin_operation` returns.
    pub fn next_scope(&self) -> usize {
        self.next_scope.load(Ordering::SeqCst)
    }

    /// Make `finish` of scope `scope` fail.
    pub fn fail_finish_of_scope(&self, scope: usize) {
        *self.shared.fail_finish_of_scope.lock().unwrap() = Some(scope);
    }

    /// Block every write until `stall_writes(false)` is called.
    pub fn stall_writes(&self, stall: bool) {
        self.shared.root.stall_writes.store(stall, Ordering::SeqCst);
    }

    /// Object paths below the table root, relative to it.
    pub async fn table_objects(&self) -> BTreeSet<String> {
        let prefix = format!("{}/", self.shared.table_prefix);
        self.shared
            .list_prefix(&prefix)
            .await
            .into_iter()
            .map(|p| p.as_ref().strip_prefix(&prefix).unwrap().to_string())
            .collect()
    }

    /// Write an object directly into the table, bypassing every scope.
    pub async fn put_in_table(&self, relative: &str, bytes: impl Into<PutPayload>) {
        let path = Path::from(format!("{}/{relative}", self.shared.table_prefix));
        self.shared
            .root
            .inner
            .put(&path, bytes.into())
            .await
            .unwrap();
    }

    /// Delete an object directly from the table, bypassing every scope.
    pub async fn delete_from_table(&self, relative: &str) {
        let path = Path::from(format!("{}/{relative}", self.shared.table_prefix));
        self.shared.root.inner.delete(&path).await.unwrap();
    }
}

#[async_trait]
impl LogStore for IsolatingLogStore {
    fn name(&self) -> String {
        "IsolatingLogStore".into()
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        self.parent.read_commit_entry(version).await
    }

    async fn get_latest_version(&self, start_version: Version) -> DeltaResult<Version> {
        self.parent.get_latest_version(start_version).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.parent.object_store()
    }

    fn root_object_store(&self) -> Arc<dyn ObjectStore> {
        self.parent.root_object_store()
    }

    fn committer(&self) -> Arc<dyn Committer> {
        self.parent.committer()
    }

    async fn begin_operation(&self) -> DeltaResult<Option<OperationContext>> {
        let scope = self.next_scope.fetch_add(1, Ordering::SeqCst);
        let scope_prefix = self.shared.scope_prefix(scope);
        // A branch starts as a copy of its source.
        self.shared
            .copy_tree(
                &format!("{}/", self.shared.table_prefix),
                &format!("{scope_prefix}/"),
            )
            .await;
        self.shared.record(ScopeEvent::Begin(scope));
        let write_root = Url::parse(&format!("memory:///{scope_prefix}/")).unwrap();
        Ok(Some(OperationContext {
            object_store: Arc::new(PrefixStore::new(
                self.shared.root.clone(),
                scope_prefix.as_str(),
            )),
            root_object_store: self.shared.root.clone(),
            write_root,
            committer: Arc::new(FakeCommitter {
                shared: self.shared.clone(),
                scope,
            }),
            transaction: Arc::new(FakeTransaction {
                shared: self.shared.clone(),
                scope,
            }),
        }))
    }

    fn config(&self) -> &LogStoreConfig {
        self.parent.config()
    }
}
