//! Errors for LakeFS log store

use deltalake_core::DeltaTableError;
use deltalake_core::kernel::transaction::TransactionError;
use reqwest::Error;

#[derive(thiserror::Error, Debug)]
pub enum LakeFSConfigError {
    /// Missing endpoint
    #[error("LakeFS endpoint is missing in storage options. Set `endpoint`.")]
    EndpointMissing,

    /// Missing username
    #[error("LakeFS username is missing in storage options. Set `access_key_id`.")]
    UsernameCredentialMissing,

    /// Missing password
    #[error("LakeFS password is missing in storage options. Set `secret_access_key`.")]
    PasswordCredentialMissing,
}

#[derive(thiserror::Error, Debug)]
pub enum LakeFSOperationError {
    /// Failed to send http request to LakeFS
    #[error("Failed to send request to LakeFS: {source}")]
    HttpRequestFailed { source: Error },

    /// Missing authentication in LakeFS
    #[error("LakeFS request was unauthorized. Check permissions.")]
    UnauthorizedAction,

    /// LakeFS commit has failed
    #[error("LakeFS commit failed. Reason: {0}")]
    CommitFailed(String),

    /// LakeFS merge has failed
    #[error("LakeFS merge failed. Reason: {0}")]
    MergeFailed(String),

    /// The destination branch of a merge has uncommitted changes.
    ///
    /// LakeFS refuses to merge into a dirty branch and no API flag bypasses that check. Commit or
    /// revert the staged changes on the branch and retry the operation.
    #[error(
        "LakeFS refused to merge into branch `{branch}` because the branch has uncommitted changes. Commit or revert the staged changes on `{branch}` and retry. Reason: {reason}"
    )]
    DirtyBranch { branch: String, reason: String },

    /// A file-only merge (checkpoints, log cleanup, vacuum) conflicted with the destination branch.
    #[error(
        "LakeFS merge of `{source_branch}` into `{target_branch}` conflicted. Reason: {reason}"
    )]
    MergeConflict {
        source_branch: String,
        target_branch: String,
        reason: String,
    },

    /// LakeFS create branch has failed
    #[error("LakeFS create branch failed. Reason: {0}")]
    CreateBranchFailed(String),

    /// LakeFS delete branch has failed
    #[error("LakeFS delete branch failed. Reason: {0}")]
    DeleteBranchFailed(String),

    /// The diff request between two refs failed
    #[error("LakeFS diff failed. Reason: {0}")]
    DiffFailed(String),
}

impl From<LakeFSOperationError> for TransactionError {
    fn from(err: LakeFSOperationError) -> Self {
        TransactionError::LogStoreError {
            msg: err.to_string(),
            source: Box::new(err),
        }
    }
}

impl From<LakeFSOperationError> for DeltaTableError {
    fn from(err: LakeFSOperationError) -> Self {
        DeltaTableError::Transaction {
            source: TransactionError::LogStoreError {
                msg: err.to_string(),
                source: Box::new(err),
            },
        }
    }
}

impl From<LakeFSConfigError> for DeltaTableError {
    fn from(err: LakeFSConfigError) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(err),
        }
    }
}
