//! Errors for LakeFS log store

use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::DeltaTableError;
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

    /// LakeFS create branch has failed
    #[error("LakeFS create branch failed. Reason: {0}")]
    CreateBranchFailed(String),

    /// LakeFS delete branch has failed
    #[error("LakeFS delete branch failed. Reason: {0}")]
    DeleteBranchFailed(String),

    /// LakeFS delete branch has failed
    #[error("Transaction ID ({0}) not found. Something went wrong.")]
    TransactionIdNotFound(String),
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
