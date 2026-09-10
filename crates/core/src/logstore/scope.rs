//! Operation-scoped write isolation for [`LogStore`] backends.
//!
//! Some backends must isolate every write of one Delta operation. LakeFS, for example, writes
//! data files and the commit file to a hidden transaction branch and merges that branch into the
//! source branch once per Delta commit. Such a backend returns an [`OperationContext`] from
//! [`LogStore::begin_operation`]. Core wraps the context in a crate-private `ScopedLogStore`
//! that routes log reads to the parent store and every write to the context, and hands that
//! store to the operation as an ordinary [`LogStoreRef`]. Backends never implement the adapter.
//!
//! [`OperationScope`] owns the lifetime of one context. `with_operation` is the helper every
//! writing operation uses: it opens a scope, runs the operation against the scoped store,
//! finishes the scope on success and aborts it on error. A scope that is dropped before it was
//! finished or aborted spawns the abort on the current tokio runtime.

use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
#[cfg(feature = "datafusion")]
use datafusion::datasource::object_store::ObjectStoreUrl;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult, path::Path,
};
use tracing::*;
use url::Url;

use super::committer::{CommitResponse, Committer, PayloadKind};
use super::{CommitOrBytes, LogStore, LogStoreConfig, LogStoreRef};
use crate::kernel::Version;
use crate::kernel::transaction::TransactionError;
use crate::{DeltaResult, DeltaTableError};

/// Write-side handles for one in-flight operation. Produced by [`LogStore::begin_operation`].
///
/// Every write of the operation goes through these handles. Log reads keep going to the store
/// that produced the context, so the context's root store must resolve source paths as well as
/// paths below `write_root`.
pub struct OperationContext {
    /// Store rooted at the table root on the write target (`_delta_log/` is a child).
    pub object_store: Arc<dyn ObjectStore>,
    /// Unprefixed store for the write target. Kernel engines read the log through it, so it must
    /// also resolve the paths of the source table root.
    pub root_object_store: Arc<dyn ObjectStore>,
    /// Table root URL on the write target. Kernel checkpoint and compaction writers get this.
    pub write_root: Url,
    /// Commit authority for this operation. A successful commit must also publish every write
    /// the operation made through the context so far.
    pub committer: Arc<dyn Committer>,
    /// Backend transaction that publishes file-only work or discards the write set.
    pub transaction: Arc<dyn OperationTransaction>,
}

/// Lifecycle of the write set of one operation. Produced together with an [`OperationContext`].
#[async_trait]
pub trait OperationTransaction: Send + Sync {
    /// Publish file-only work (checkpoints, log cleanup, vacuum deletes) and release resources.
    /// `dirty` is `false` when nothing was written since the last successful commit. Idempotent.
    async fn finish(&self, dirty: bool) -> DeltaResult<()>;

    /// Discard unpublished work and release resources. Idempotent and safe after `finish`.
    async fn abort(&self) -> DeltaResult<()>;
}

/// Error returned by an operation-scoped log store after its scope was finished or aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeClosed;

impl fmt::Display for ScopeClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "the operation scope is closed; an operation-scoped log store must not be used after finish or abort",
        )
    }
}

impl std::error::Error for ScopeClosed {}

impl From<ScopeClosed> for DeltaTableError {
    fn from(err: ScopeClosed) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(err),
        }
    }
}

impl From<ScopeClosed> for TransactionError {
    fn from(err: ScopeClosed) -> Self {
        TransactionError::LogStoreError {
            msg: err.to_string(),
            source: Box::new(err),
        }
    }
}

impl From<ScopeClosed> for ObjectStoreError {
    fn from(err: ScopeClosed) -> Self {
        ObjectStoreError::Generic {
            store: "ScopedLogStore",
            source: Box::new(err),
        }
    }
}

#[derive(Debug, Default)]
struct ScopeState {
    dirty: AtomicBool,
    closed: AtomicBool,
}

impl ScopeState {
    fn ensure_open(&self) -> Result<(), ScopeClosed> {
        if self.closed.load(Ordering::SeqCst) {
            Err(ScopeClosed)
        } else {
            Ok(())
        }
    }

    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::SeqCst);
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

/// Object store delegate that records writes and refuses every call once the scope is closed.
#[derive(Clone)]
struct TrackedStore {
    inner: Arc<dyn ObjectStore>,
    state: Arc<ScopeState>,
}

impl fmt::Debug for TrackedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TrackedStore({:?})", self.inner)
    }
}

impl fmt::Display for TrackedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TrackedStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for TrackedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.state.ensure_open()?;
        self.state.mark_dirty();
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.state.ensure_open()?;
        self.state.mark_dirty();
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.state.ensure_open()?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.state.ensure_open()?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        if let Err(err) = self.state.ensure_open() {
            return locations.map(move |_| Err(err.into())).boxed();
        }
        self.state.mark_dirty();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        if let Err(err) = self.state.ensure_open() {
            return stream::once(async move { Err(err.into()) }).boxed();
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        if let Err(err) = self.state.ensure_open() {
            return stream::once(async move { Err(err.into()) }).boxed();
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.state.ensure_open()?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.state.ensure_open()?;
        self.state.mark_dirty();
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.state.ensure_open()?;
        self.state.mark_dirty();
        self.inner.rename_opts(from, to, options).await
    }
}

/// Committer delegate that resets the dirty flag after a successful commit and refuses calls
/// once the scope is closed.
struct ScopedCommitter {
    inner: Arc<dyn Committer>,
    state: Arc<ScopeState>,
}

#[async_trait]
impl Committer for ScopedCommitter {
    async fn commit(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<CommitResponse, TransactionError> {
        self.state.ensure_open()?;
        let response = self.inner.commit(version, payload).await?;
        if response == CommitResponse::Committed {
            // The commit published every write made so far.
            self.state.dirty.store(false, Ordering::SeqCst);
        }
        Ok(response)
    }

    async fn abort(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        self.state.ensure_open()?;
        self.inner.abort(version, payload).await
    }

    fn payload_kind(&self) -> PayloadKind {
        self.inner.payload_kind()
    }
}

/// Crate-private adapter over a parent store and an [`OperationContext`].
///
/// Log reads (`read_commit_entry`, `get_latest_version`, `refresh`, `config`, `root_url`,
/// `object_store_url`) go to the parent. Writes (`object_store`,
/// `root_object_store`, `engine`, `write_root_url`, `committer`) go to the context.
/// `begin_operation` forwards to the parent so that post-commit work can open a sibling scope.
/// After the scope is finished or aborted every write-side handle returns [`ScopeClosed`].
pub(crate) struct ScopedLogStore {
    parent: LogStoreRef,
    object_store: Arc<dyn ObjectStore>,
    root_object_store: Arc<dyn ObjectStore>,
    write_root: Url,
    committer: Arc<dyn Committer>,
    state: Arc<ScopeState>,
}

impl fmt::Debug for ScopedLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedLogStore")
            .field("parent", &self.parent)
            .field("write_root", &self.write_root.as_str())
            .field("dirty", &self.state.dirty.load(Ordering::SeqCst))
            .field("closed", &self.state.closed.load(Ordering::SeqCst))
            .finish()
    }
}

impl ScopedLogStore {
    fn new(parent: LogStoreRef, ctx: &OperationContext, state: Arc<ScopeState>) -> Self {
        Self {
            parent,
            object_store: Arc::new(TrackedStore {
                inner: ctx.object_store.clone(),
                state: state.clone(),
            }),
            root_object_store: Arc::new(TrackedStore {
                inner: ctx.root_object_store.clone(),
                state: state.clone(),
            }),
            write_root: ctx.write_root.clone(),
            committer: Arc::new(ScopedCommitter {
                inner: ctx.committer.clone(),
                state: state.clone(),
            }),
            state,
        }
    }
}

#[async_trait]
impl LogStore for ScopedLogStore {
    fn name(&self) -> String {
        self.parent.name()
    }

    async fn refresh(&self) -> DeltaResult<()> {
        self.parent.refresh().await
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        self.parent.read_commit_entry(version).await
    }

    async fn get_latest_version(&self, start_version: Version) -> DeltaResult<Version> {
        self.parent.get_latest_version(start_version).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    fn root_object_store(&self) -> Arc<dyn ObjectStore> {
        self.root_object_store.clone()
    }

    fn write_root_url(&self) -> Url {
        self.write_root.clone()
    }

    fn committer(&self) -> Arc<dyn Committer> {
        self.committer.clone()
    }

    async fn begin_operation(&self) -> DeltaResult<Option<OperationContext>> {
        self.parent.begin_operation().await
    }

    async fn is_delta_table_location(&self) -> DeltaResult<bool> {
        self.parent.is_delta_table_location().await
    }

    fn config(&self) -> &LogStoreConfig {
        self.parent.config()
    }

    #[cfg(feature = "datafusion")]
    fn object_store_url(&self) -> ObjectStoreUrl {
        self.parent.object_store_url()
    }
}

struct ScopeInner {
    transaction: Arc<dyn OperationTransaction>,
    state: Arc<ScopeState>,
}

/// Owner of one operation's write scope.
///
/// Open a scope with [`OperationScope::open`], run the operation against
/// [`OperationScope::log_store`], and end it with [`OperationScope::finish`] or
/// [`OperationScope::abort`]. When the parent store needs no isolation the scope is transparent
/// and `log_store` is the parent itself.
pub struct OperationScope {
    store: LogStoreRef,
    inner: Option<ScopeInner>,
}

impl fmt::Debug for OperationScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperationScope")
            .field("store", &self.store)
            .field("isolated", &self.inner.is_some())
            .finish()
    }
}

impl OperationScope {
    /// Ask `parent` for an [`OperationContext`] and wrap it.
    pub async fn open(parent: &LogStoreRef) -> DeltaResult<Self> {
        match parent.begin_operation().await? {
            None => Ok(Self {
                store: parent.clone(),
                inner: None,
            }),
            Some(ctx) => {
                let state = Arc::new(ScopeState::default());
                let store: LogStoreRef =
                    Arc::new(ScopedLogStore::new(parent.clone(), &ctx, state.clone()));
                debug!(write_root = %ctx.write_root, "opened operation scope");
                Ok(Self {
                    store,
                    inner: Some(ScopeInner {
                        transaction: ctx.transaction,
                        state,
                    }),
                })
            }
        }
    }

    /// The store the operation must use for every read and write.
    pub fn log_store(&self) -> &LogStoreRef {
        &self.store
    }

    /// `true` when the parent store returned a context and writes are isolated.
    pub fn is_isolated(&self) -> bool {
        self.inner.is_some()
    }

    /// Publish unpublished work and release the scope. When `finish` fails the scope is aborted
    /// and the `finish` error is returned.
    pub async fn finish(mut self) -> DeltaResult<()> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        inner.state.close();
        let dirty = inner.state.dirty.load(Ordering::SeqCst);
        debug!(dirty, "finishing operation scope");
        match inner.transaction.finish(dirty).await {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Err(abort_err) = inner.transaction.abort().await {
                    warn!(error = %abort_err, "failed to abort operation scope after finish failed");
                }
                Err(err)
            }
        }
    }

    /// Discard unpublished work and release the scope.
    pub async fn abort(mut self) -> DeltaResult<()> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        inner.state.close();
        debug!("aborting operation scope");
        inner.transaction.abort().await
    }
}

impl Drop for OperationScope {
    fn drop(&mut self) {
        // Reached only when the operation future was dropped or panicked before `finish` or
        // `abort` ran.
        let Some(inner) = self.inner.take() else {
            return;
        };
        inner.state.close();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    if let Err(err) = inner.transaction.abort().await {
                        warn!(error = %err, "failed to abort a dropped operation scope");
                    }
                });
            }
            Err(_) => {
                warn!(
                    "operation scope dropped outside a tokio runtime; its transaction was not aborted"
                );
            }
        }
    }
}

/// Run `f` inside an operation scope opened on `parent`.
///
/// The closure receives the scoped store and must use it for every read and write of the
/// operation. The scope is finished when `f` returns `Ok` and aborted when it returns `Err`.
/// A `finish` error is returned to the caller; an `abort` error is logged and the original error
/// is returned.
pub(crate) async fn with_operation<T, F, Fut>(parent: &LogStoreRef, f: F) -> DeltaResult<T>
where
    F: FnOnce(LogStoreRef) -> Fut,
    Fut: Future<Output = DeltaResult<T>>,
{
    let scope = OperationScope::open(parent).await?;
    let log_store = scope.log_store().clone();
    match f(log_store).await {
        Ok(value) => {
            scope.finish().await?;
            Ok(value)
        }
        Err(err) => {
            if let Err(abort_err) = scope.abort().await {
                warn!(error = %abort_err, "failed to abort operation scope after an error");
            }
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::ObjectStoreExt as _;
    use object_store::path::Path;

    use super::*;
    use crate::kernel::{DataType, PrimitiveType, StructField};
    use crate::operations::create::CreateBuilder;
    use crate::test_utils::isolating_store::{IsolatingLogStore, ScopeEvent};
    use crate::{DeltaTable, DeltaTableConfig};

    async fn create_table(store: &Arc<IsolatingLogStore>) -> DeltaTable {
        let log_store: LogStoreRef = store.clone();
        let table = CreateBuilder::new()
            .with_log_store(log_store)
            .with_columns(vec![StructField::new(
                "id",
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await
            .unwrap();
        store.reset();
        table
    }

    fn payload() -> CommitOrBytes {
        CommitOrBytes::LogBytes(Bytes::from_static(b"{\"commitInfo\":{}}\n"))
    }

    #[tokio::test]
    async fn scope_is_transparent_for_stores_without_isolation() {
        let table = DeltaTable::new_in_memory();
        let parent = table.log_store();
        let scope = OperationScope::open(&parent).await.unwrap();
        assert!(!scope.is_isolated());
        assert!(Arc::ptr_eq(scope.log_store(), &parent));
        scope.finish().await.unwrap();
    }

    #[tokio::test]
    async fn create_runs_in_one_scope_and_publishes_version_zero() {
        let store = IsolatingLogStore::new();
        let log_store: LogStoreRef = store.clone();
        let table = CreateBuilder::new()
            .with_log_store(log_store.clone())
            .with_columns(vec![StructField::new(
                "id",
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert!(
            Arc::ptr_eq(&table.log_store(), &log_store),
            "the table keeps the parent store"
        );
        assert_eq!(
            store.events(),
            vec![
                ScopeEvent::Begin(1),
                ScopeEvent::Commit {
                    scope: 1,
                    version: 0
                },
                ScopeEvent::Finish {
                    scope: 1,
                    dirty: false
                },
            ]
        );
        assert!(
            store
                .take_recorded_writes()
                .iter()
                .all(|p| p.as_ref().starts_with("scopes/1/")),
            "every write goes through the scope"
        );
        assert!(
            store
                .table_objects()
                .await
                .contains("_delta_log/00000000000000000000.json")
        );
    }

    #[tokio::test]
    async fn scoped_store_routes_log_reads_to_parent_and_writes_to_context() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        let scope = OperationScope::open(&parent).await.unwrap();
        assert!(scope.is_isolated());
        let scoped = scope.log_store().clone();

        // A commit written straight to the parent is visible through the scoped store.
        store
            .put_in_table("_delta_log/00000000000000000001.json", &b"{}\n"[..])
            .await;
        assert_eq!(scoped.get_latest_version(0).await.unwrap(), 1);
        assert!(scoped.read_commit_entry(1).await.unwrap().is_some());

        // Configuration and URLs stay on the source; only the write root moves.
        assert_eq!(scoped.root_url(), parent.root_url());
        assert_eq!(scoped.name(), parent.name());
        assert_eq!(
            scoped.write_root_url().as_str(),
            "memory:///scopes/1/table/"
        );

        // Writes land in the scope prefix, not in the table.
        scoped
            .object_store()
            .put(&Path::from("part-1.parquet"), "data".into())
            .await
            .unwrap();
        let writes = store.take_recorded_writes();
        assert_eq!(writes, vec![Path::from("scopes/1/table/part-1.parquet")]);
        assert!(!store.table_objects().await.contains("part-1.parquet"));

        scope.abort().await.unwrap();
        assert_eq!(
            store.events(),
            vec![ScopeEvent::Begin(1), ScopeEvent::Abort(1)]
        );
    }

    #[tokio::test]
    async fn committer_publishes_and_resets_the_dirty_flag() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        let scope = OperationScope::open(&parent).await.unwrap();
        let scoped = scope.log_store().clone();

        scoped
            .object_store()
            .put(&Path::from("part-1.parquet"), "data".into())
            .await
            .unwrap();
        let response = scoped.committer().commit(1, payload()).await.unwrap();
        assert_eq!(response, CommitResponse::Committed);
        assert!(store.table_objects().await.contains("part-1.parquet"));

        scope.finish().await.unwrap();
        assert_eq!(
            store.events(),
            vec![
                ScopeEvent::Begin(1),
                ScopeEvent::Commit {
                    scope: 1,
                    version: 1
                },
                ScopeEvent::Finish {
                    scope: 1,
                    dirty: false
                },
            ]
        );
    }

    #[tokio::test]
    async fn file_only_work_leaves_the_scope_dirty_until_finish() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        let scope = OperationScope::open(&parent).await.unwrap();
        scope
            .log_store()
            .object_store()
            .put(&Path::from("manifest"), "data".into())
            .await
            .unwrap();
        scope.finish().await.unwrap();
        assert_eq!(
            store.events(),
            vec![
                ScopeEvent::Begin(1),
                ScopeEvent::Finish {
                    scope: 1,
                    dirty: true
                },
            ]
        );
        assert!(store.table_objects().await.contains("manifest"));
    }

    #[tokio::test]
    async fn committer_reports_conflicts_without_retrying() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        let scope = OperationScope::open(&parent).await.unwrap();
        store
            .put_in_table("_delta_log/00000000000000000001.json", &b"{}\n"[..])
            .await;
        let response = scope
            .log_store()
            .committer()
            .commit(1, payload())
            .await
            .unwrap();
        assert_eq!(response, CommitResponse::Conflict { version: 1 });
        scope.abort().await.unwrap();
        assert_eq!(
            store.events(),
            vec![
                ScopeEvent::Begin(1),
                ScopeEvent::Conflict {
                    scope: 1,
                    version: 1
                },
                ScopeEvent::Abort(1),
            ]
        );
    }

    #[tokio::test]
    async fn scoped_store_is_poisoned_after_finish() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        let scope = OperationScope::open(&parent).await.unwrap();
        let scoped = scope.log_store().clone();
        scope.finish().await.unwrap();

        let err = scoped
            .object_store()
            .put(&Path::from("late.parquet"), "data".into())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("scope is closed"), "{err}");
        let err = scoped
            .root_object_store()
            .get(&Path::from("table/_delta_log/00000000000000000000.json"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("scope is closed"), "{err}");
        let err = scoped.committer().commit(1, payload()).await.unwrap_err();
        assert!(err.to_string().contains("scope is closed"), "{err}");
        assert!(store.take_recorded_writes().is_empty());
        // Log reads still go to the parent.
        assert_eq!(scoped.get_latest_version(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn with_operation_finishes_on_success_and_aborts_on_error() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();

        with_operation(&parent, |_| async { Ok(()) }).await.unwrap();
        let err = with_operation(&parent, |_| async {
            Err::<(), _>(DeltaTableError::Generic("boom".into()))
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("boom"));
        assert_eq!(
            store.events(),
            vec![
                ScopeEvent::Begin(1),
                ScopeEvent::Finish {
                    scope: 1,
                    dirty: false
                },
                ScopeEvent::Begin(2),
                ScopeEvent::Abort(2),
            ]
        );
    }

    #[tokio::test]
    async fn finish_failure_is_surfaced_and_the_scope_is_aborted() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        store.fail_finish_of_scope(store.next_scope());
        let err = with_operation(&parent, |_| async { Ok(()) })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("injected failure"), "{err}");
        assert_eq!(
            store.events(),
            vec![
                ScopeEvent::Begin(1),
                ScopeEvent::Finish {
                    scope: 1,
                    dirty: false
                },
                ScopeEvent::Abort(1),
            ]
        );
    }

    #[tokio::test]
    async fn dropping_an_open_scope_spawns_the_abort() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        let scope = OperationScope::open(&parent).await.unwrap();
        drop(scope);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            store.events(),
            vec![ScopeEvent::Begin(1), ScopeEvent::Abort(1)]
        );
    }

    #[tokio::test]
    async fn no_abort_after_finish_or_abort() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        let scope = OperationScope::open(&parent).await.unwrap();
        scope.finish().await.unwrap();
        let scope = OperationScope::open(&parent).await.unwrap();
        scope.abort().await.unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            store.events(),
            vec![
                ScopeEvent::Begin(1),
                ScopeEvent::Finish {
                    scope: 1,
                    dirty: false
                },
                ScopeEvent::Begin(2),
                ScopeEvent::Abort(2),
            ]
        );
    }

    #[tokio::test]
    async fn sibling_scope_is_opened_from_a_scoped_store() {
        let store = IsolatingLogStore::new();
        create_table(&store).await;
        let parent: LogStoreRef = store.clone();
        let outer = OperationScope::open(&parent).await.unwrap();
        let inner = OperationScope::open(outer.log_store()).await.unwrap();
        assert!(inner.is_isolated());
        assert_eq!(
            inner.log_store().write_root_url().as_str(),
            "memory:///scopes/2/table/"
        );
        inner.finish().await.unwrap();
        outer.finish().await.unwrap();
        assert_eq!(
            store.events(),
            vec![
                ScopeEvent::Begin(1),
                ScopeEvent::Begin(2),
                ScopeEvent::Finish {
                    scope: 2,
                    dirty: false
                },
                ScopeEvent::Finish {
                    scope: 1,
                    dirty: false
                },
            ]
        );
        let _ = DeltaTableConfig::default();
    }
}
