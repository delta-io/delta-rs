//! Error types for Delta Lake operations.

/// A specialized [`Result`] type for Delta Lake operations.
pub type DeltaResult<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum Error {
    #[cfg(feature = "arrow")]
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("Generic delta kernel error: {0}")]
    Generic(String),

    #[error("Generic error: {source}")]
    GenericError {
        /// Source error
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[cfg(feature = "parquet")]
    #[error("Arrow error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Error interacting with object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("{0}")]
    MissingColumn(String),

    #[error("Expected column type: {0}")]
    UnexpectedColumnType(String),

    #[error("Expected is missing: {0}")]
    MissingData(String),

    #[error("No table version found.")]
    MissingVersion,

    #[error("Deletion Vector error: {0}")]
    DeletionVector(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Invalid url: {0}")]
    InvalidUrl(#[from] url::ParseError),

    #[error("Invalid url: {0}")]
    MalformedJson(#[from] serde_json::Error),

    #[error("No table metadata found in delta log.")]
    MissingMetadata,

    /// Error returned when the log contains invalid stats JSON.
    #[error("Invalid JSON in invariant expression, line=`{line}`, err=`{json_err}`")]
    InvalidInvariantJson {
        /// JSON error details returned when parsing the invariant expression JSON.
        json_err: serde_json::error::Error,
        /// Invariant expression.
        line: String,
    },

    #[error("Table metadata is invalid: {0}")]
    MetadataError(String),
}

#[cfg(feature = "object_store")]
impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        match value {
            object_store::Error::NotFound { path, .. } => Self::FileNotFound(path),
            err => Self::ObjectStore(err),
        }
    }
}
