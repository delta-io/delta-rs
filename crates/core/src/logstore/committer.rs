//! Commit authority for a [`LogStore`](super::LogStore).
//!
//! A [`Committer`] ratifies one version of the Delta log. The commit loop in
//! [`CommitBuilder`](crate::kernel::transaction::CommitBuilder) prepares the payload, asks the
//! committer to commit it, and owns the retry and conflict-resolution loop. A committer must not
//! retry on its own: for an operation-scoped store a retry against stale state would be wrong.
//!
//! The trait mirrors `delta_kernel::committer::Committer`.

use std::sync::{Arc, OnceLock};

use object_store::{Attributes, Error as ObjectStoreError, ObjectStore, PutOptions, TagSet};
use tracing::*;

use super::CommitOrBytes;
use super::storage::utils::commit_uri_from_version;
use crate::DeltaTableError;
use crate::kernel::Version;
use crate::kernel::transaction::TransactionError;

/// How a caller must prepare the payload it hands to [`Committer::commit`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadKind {
    /// The committer accepts the serialized commit as bytes and writes them atomically.
    Bytes,
    /// The committer expects a temporary file under `_delta_log/` and moves it into place.
    TmpCommit,
}

/// Outcome of one commit attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitResponse {
    /// The version was ratified.
    Committed,
    /// Another writer already ratified this version. The caller checks for conflicts and retries.
    Conflict {
        /// The version that already exists.
        version: Version,
    },
}

/// Ratifies commit versions for a Delta table.
///
/// Implementations must be atomic per version: exactly one caller can commit a given version,
/// every other caller receives [`CommitResponse::Conflict`]. Implementations must not retry.
#[async_trait::async_trait]
pub trait Committer: Send + Sync {
    /// Commit `payload` as `version`. Returns [`CommitResponse::Conflict`] when the version
    /// already exists.
    async fn commit(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<CommitResponse, TransactionError>;

    /// Release the resources of a commit attempt that did not succeed, for example a temporary
    /// commit file. Never called for an attempt that returned [`CommitResponse::Conflict`] while
    /// the caller still retries with the same payload.
    async fn abort(&self, version: Version, payload: CommitOrBytes)
    -> Result<(), TransactionError>;

    /// How the caller must prepare the payload.
    fn payload_kind(&self) -> PayloadKind;
}

/// How a [`FileSystemCommitter`] writes the commit file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitStrategy {
    /// Put the commit bytes with a create-only (put-if-absent) request.
    ConditionalPut,
    /// Rename a temporary commit file into place with rename-if-not-exists.
    TmpCommit,
}

impl CommitStrategy {
    /// The payload the strategy needs.
    pub fn payload_kind(self) -> PayloadKind {
        match self {
            CommitStrategy::ConditionalPut => PayloadKind::Bytes,
            CommitStrategy::TmpCommit => PayloadKind::TmpCommit,
        }
    }
}

fn put_options() -> &'static PutOptions {
    static PUT_OPTS: OnceLock<PutOptions> = OnceLock::new();
    PUT_OPTS.get_or_init(|| PutOptions {
        mode: object_store::PutMode::Create,
        tags: TagSet::default(),
        attributes: Attributes::default(),
        extensions: Default::default(),
    })
}

/// [`Committer`] that ratifies versions through the atomic primitives of an [`ObjectStore`]
/// rooted at the table root. This is the commit authority of every store that is not managed by
/// a catalog.
#[derive(Debug, Clone)]
pub struct FileSystemCommitter {
    store: Arc<dyn ObjectStore>,
    strategy: CommitStrategy,
}

impl FileSystemCommitter {
    /// Create a committer over `store`, which must be rooted at the table root.
    pub fn new(store: Arc<dyn ObjectStore>, strategy: CommitStrategy) -> Self {
        Self { store, strategy }
    }

    /// The strategy this committer uses.
    pub fn strategy(&self) -> CommitStrategy {
        self.strategy
    }
}

fn payload_mismatch(expected: PayloadKind, strategy: CommitStrategy) -> TransactionError {
    let msg = format!(
        "commit payload does not match the committer: {strategy:?} requires a {expected:?} payload"
    );
    TransactionError::LogStoreError {
        source: Box::new(DeltaTableError::Generic(msg.clone())),
        msg,
    }
}

#[async_trait::async_trait]
impl Committer for FileSystemCommitter {
    async fn commit(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<CommitResponse, TransactionError> {
        match (self.strategy, payload) {
            (CommitStrategy::ConditionalPut, CommitOrBytes::LogBytes(bytes)) => {
                match self
                    .store
                    .put_opts(
                        &commit_uri_from_version(Some(version)),
                        bytes.into(),
                        put_options().clone(),
                    )
                    .await
                {
                    Ok(_) => Ok(CommitResponse::Committed),
                    Err(ObjectStoreError::AlreadyExists { .. }) => {
                        warn!(version, "commit entry already exists");
                        Ok(CommitResponse::Conflict { version })
                    }
                    Err(err) => Err(TransactionError::from(err)),
                }
            }
            (CommitStrategy::TmpCommit, CommitOrBytes::TmpCommit(tmp_commit)) => {
                match super::write_commit_entry(self.store.as_ref(), version, &tmp_commit).await {
                    Ok(()) => Ok(CommitResponse::Committed),
                    Err(TransactionError::VersionAlreadyExists(version)) => {
                        Ok(CommitResponse::Conflict { version })
                    }
                    Err(err) => Err(err),
                }
            }
            (strategy, _) => Err(payload_mismatch(strategy.payload_kind(), strategy)),
        }
    }

    async fn abort(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        match payload {
            CommitOrBytes::LogBytes(_) => Ok(()),
            CommitOrBytes::TmpCommit(tmp_commit) => {
                super::abort_commit_entry(self.store.as_ref(), version, &tmp_commit).await
            }
        }
    }

    fn payload_kind(&self) -> PayloadKind {
        self.strategy.payload_kind()
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;

    #[tokio::test]
    async fn conditional_put_reports_conflicts() {
        let store = Arc::new(InMemory::new());
        let committer = FileSystemCommitter::new(store.clone(), CommitStrategy::ConditionalPut);
        assert_eq!(committer.payload_kind(), PayloadKind::Bytes);

        let payload = CommitOrBytes::LogBytes(bytes::Bytes::from_static(b"{}"));
        assert_eq!(
            committer.commit(0, payload.clone()).await.unwrap(),
            CommitResponse::Committed
        );
        assert_eq!(
            committer.commit(0, payload.clone()).await.unwrap(),
            CommitResponse::Conflict { version: 0 }
        );
        assert_eq!(
            committer.commit(1, payload.clone()).await.unwrap(),
            CommitResponse::Committed
        );
        committer.abort(2, payload).await.unwrap();
    }

    #[tokio::test]
    async fn tmp_commit_renames_and_aborts() {
        let store = Arc::new(InMemory::new());
        let committer = FileSystemCommitter::new(store.clone(), CommitStrategy::TmpCommit);
        assert_eq!(committer.payload_kind(), PayloadKind::TmpCommit);

        let tmp = Path::from("_delta_log/_commit_abc.json.tmp");
        store.put(&tmp, "{}".into()).await.unwrap();
        assert_eq!(
            committer
                .commit(0, CommitOrBytes::TmpCommit(tmp.clone()))
                .await
                .unwrap(),
            CommitResponse::Committed
        );
        assert!(store.head(&tmp).await.is_err());
        assert!(store.head(&commit_uri_from_version(Some(0))).await.is_ok());

        store.put(&tmp, "{}".into()).await.unwrap();
        assert_eq!(
            committer
                .commit(0, CommitOrBytes::TmpCommit(tmp.clone()))
                .await
                .unwrap(),
            CommitResponse::Conflict { version: 0 }
        );
        assert!(
            store.head(&tmp).await.is_ok(),
            "the tmp file survives a conflict"
        );
        committer
            .abort(0, CommitOrBytes::TmpCommit(tmp.clone()))
            .await
            .unwrap();
        assert!(
            store.head(&tmp).await.is_err(),
            "abort deletes the tmp file"
        );
    }

    /// A store that overrides nothing gets the temporary-file committer, whatever its name.
    #[tokio::test]
    async fn trait_default_committer_uses_the_tmp_commit_strategy() {
        use crate::DeltaResult;
        use crate::logstore::{LogStore, LogStoreConfig, StorageConfig};

        struct BareStore {
            store: Arc<InMemory>,
            config: LogStoreConfig,
        }

        #[async_trait::async_trait]
        impl LogStore for BareStore {
            fn name(&self) -> String {
                "DefaultLogStore".into()
            }
            async fn read_commit_entry(&self, _: Version) -> DeltaResult<Option<bytes::Bytes>> {
                Ok(None)
            }
            async fn get_latest_version(&self, _: Version) -> DeltaResult<Version> {
                Ok(0)
            }
            fn object_store(&self) -> Arc<dyn ObjectStore> {
                self.store.clone()
            }
            fn root_object_store(&self) -> Arc<dyn ObjectStore> {
                self.store.clone()
            }
            fn config(&self) -> &LogStoreConfig {
                &self.config
            }
        }

        let store = Arc::new(InMemory::new());
        let bare = BareStore {
            store: store.clone(),
            config: LogStoreConfig::new(
                &url::Url::parse("memory:///bare").unwrap(),
                StorageConfig::default(),
            ),
        };
        let committer = bare.committer();
        assert_eq!(committer.payload_kind(), PayloadKind::TmpCommit);

        let tmp = Path::from("_delta_log/_commit_x.json.tmp");
        store.put(&tmp, "{}".into()).await.unwrap();
        assert_eq!(
            committer
                .commit(0, CommitOrBytes::TmpCommit(tmp))
                .await
                .unwrap(),
            CommitResponse::Committed
        );
        assert!(store.head(&commit_uri_from_version(Some(0))).await.is_ok());
    }

    #[tokio::test]
    async fn payload_kind_mismatch_is_an_error() {
        let store = Arc::new(InMemory::new());
        let committer = FileSystemCommitter::new(store, CommitStrategy::ConditionalPut);
        let err = committer
            .commit(0, CommitOrBytes::TmpCommit(Path::from("x")))
            .await
            .unwrap_err();
        assert!(matches!(err, TransactionError::LogStoreError { .. }));
    }
}
