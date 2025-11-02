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
            Error::Parse(msg) => LogStoreError::Generic {
                source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, msg)),
            },
            Error::UnknownConfigKey(msg) => LogStoreError::Generic {
                source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, msg)),
            },
            Error::AllowUnsafeRenameNotSpecified => LogStoreError::Generic {
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "The `allow_unsafe_rename` parameter must be specified",
                )),
            },
            Error::ObjectStore(e) => LogStoreError::ObjectStore { source: e },
        }
    }
}
