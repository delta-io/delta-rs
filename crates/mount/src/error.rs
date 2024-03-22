use deltalake_core::errors::DeltaTableError;

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

impl From<Error> for DeltaTableError {
    fn from(e: Error) -> Self {
        match e {
            Error::Parse(msg) => DeltaTableError::Generic(msg),
            Error::UnknownConfigKey(msg) => DeltaTableError::Generic(msg),
            Error::AllowUnsafeRenameNotSpecified => DeltaTableError::Generic(
                "The `allow_unsafe_rename` parameter must be specified".to_string(),
            ),
            Error::ObjectStore(e) => DeltaTableError::ObjectStore { source: e },
        }
    }
}
