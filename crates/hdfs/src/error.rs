use deltalake_core::errors::DeltaTableError;

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[allow(dead_code)]
    #[error("failed to parse config: {0}")]
    Parse(String),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl From<Error> for DeltaTableError {
    fn from(e: Error) -> Self {
        match e {
            Error::Parse(msg) => DeltaTableError::Generic(msg),
            Error::ObjectStore(e) => DeltaTableError::ObjectStore { source: e },
        }
    }
}
