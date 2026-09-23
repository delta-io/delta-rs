# Rust 1.1.0 Migration Guide

## `CustomExecuteHandler` removed

`CustomExecuteHandler`, the `with_custom_execute_handler` builder methods and `CommitBuilder::with_post_commit_hook_handler` no longer exist. LakeFS users only delete the `LakeFSCustomExecuteHandler` calls, because the LakeFS log store now isolates each operation itself. To keep a custom handler, wrap the table's log store and override `LogStore::begin_operation`. Each writing operation calls it when it starts, and the post-commit work of each commit (checkpoint and log cleanup) calls it again. Each call returns an `OperationTransaction`: its `finish` runs on success and its `abort` runs on failure. Unlike a handler, the wrapper applies to every operation on the table, cannot tell an operation from its post-commit work, and gets no operation id.

```rust
use std::sync::Arc;

use bytes::Bytes;
use deltalake::kernel::Version;
use deltalake::logstore::{
    Committer, LogStore, LogStoreConfig, LogStoreRef, ObjectStoreRef, OperationContext,
    OperationTransaction,
};
use deltalake::{DeltaResult, DeltaTable, DeltaTableBuilder};
use url::Url;

/// Runs hooks around every writing operation. `inner` must not isolate writes itself
/// (LakeFS does), because this store replaces its `begin_operation`.
struct HookedLogStore {
    inner: LogStoreRef,
}

/// One value per scope. Keep per-operation state, such as an operation id, here.
struct Hooks;

#[async_trait::async_trait]
impl OperationTransaction for Hooks {
    async fn finish(&self, _dirty: bool) -> DeltaResult<()> {
        // Was `post_execute` and `after_post_commit_hook`.
        Ok(())
    }

    async fn abort(&self) -> DeltaResult<()> {
        // New: the operation failed or was dropped. Release what `begin_operation` acquired.
        Ok(())
    }
}

#[async_trait::async_trait]
impl LogStore for HookedLogStore {
    async fn begin_operation(&self) -> DeltaResult<Option<OperationContext>> {
        // Was `pre_execute` and `before_post_commit_hook`.
        Ok(Some(OperationContext {
            object_store: self.inner.object_store(),
            root_object_store: self.inner.root_object_store(),
            write_root: self.inner.write_root_url(),
            committer: self.inner.committer(),
            transaction: Arc::new(Hooks),
        }))
    }

    // Delegate the rest to `inner`.
    fn name(&self) -> String {
        self.inner.name()
    }

    async fn refresh(&self) -> DeltaResult<()> {
        self.inner.refresh().await
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        self.inner.read_commit_entry(version).await
    }

    async fn get_latest_version(&self, start_version: Version) -> DeltaResult<Version> {
        self.inner.get_latest_version(start_version).await
    }

    fn object_store(&self) -> ObjectStoreRef {
        self.inner.object_store()
    }

    fn root_object_store(&self) -> ObjectStoreRef {
        self.inner.root_object_store()
    }

    fn committer(&self) -> Arc<dyn Committer> {
        self.inner.committer()
    }

    fn config(&self) -> &LogStoreConfig {
        self.inner.config()
    }
}

/// Opens a table whose writing operations all run the hooks.
async fn open_with_hooks(url: Url) -> DeltaResult<DeltaTable> {
    let inner = DeltaTableBuilder::from_url(url)?.build_storage()?;
    let mut table = DeltaTable::new(Arc::new(HookedLogStore { inner }));
    table.load().await?;
    Ok(table)
}
```
