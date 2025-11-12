use deltalake_logstore::LogStoreError;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[allow(dead_code)]
    #[error("failed to parse config: {0}")]
    Parse(String),

    /// Unknown configuration key
    #[error("Unknown configuration key: {0}")]
    UnknownConfigKey(String),

    #[error("The `allow_unsafe_rename` parameter must be specified")]
    AllowUnsafeRenameNotSpecified,

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl From<Error> for LogStoreError {
    fn from(e: Error) -> Self {
        match e {
            Error::Parse(msg) => LogStoreError::Generic { source: msg.into() },
            Error::UnknownConfigKey(_) => LogStoreError::Generic { source: e.into() },
            Error::AllowUnsafeRenameNotSpecified => LogStoreError::Generic { source: e.into() },
            Error::ObjectStore(e) => LogStoreError::ObjectStore { source: e },
        }
    }
}
