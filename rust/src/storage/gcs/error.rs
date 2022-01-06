/// Error enum that represents an issue encountered
/// during interaction with the GCS service
#[derive(thiserror::Error, Debug)]
pub enum GCSClientError {
    #[error("Authentication error: {source}")]
    AuthError {
        #[from]
        source: tame_oauth::Error,
    },

    #[error("Error interacting with GCS: {source}")]
    GCSError {
        #[from]
        source: tame_gcs::Error,
    },

    #[error("Reqwest error: {source}")]
    ReqwestError {
        #[from]
        source: reqwest::Error,
    },

    #[error("IO error: {source}")]
    IOError {
        #[from]
        source: std::io::Error,
    },

    #[error("HTTP error: {source}")]
    HttpError {
        #[from]
        source: tame_gcs::http::Error,
    },

    #[error("Resource Not Found")]
    NotFound,

    #[error("Precondition Failed")]
    PreconditionFailed,

    #[error("Error: {0}")]
    Other(String),

    #[error("Credentials error: {source}")]
    CredentialsError { source: std::io::Error },
}
