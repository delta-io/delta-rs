//! Errors for S3 log store backed by DynamoDb

use std::num::ParseIntError;

use rusoto_core::RusotoError;
use rusoto_dynamodb::{CreateTableError, GetItemError, PutItemError, QueryError, UpdateItemError};

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum DynamoDbConfigError {
    /// Error raised creating http client
    #[error("Failed to create request dispatcher: {source}")]
    HttpClient {
        /// The underlying Rusoto TlsError
        #[from]
        source: rusoto_core::request::TlsError,
    },

    /// Error raised getting credentials
    #[error("Failed to retrieve AWS credentials: {source}")]
    Credentials {
        /// The underlying Rusoto CredentialsError
        #[from]
        source: rusoto_credential::CredentialsError,
    },

    /// Billing mode string invalid
    #[error("Invalid billing mode : {0}, supported values : ['provided', 'pay_per_request']")]
    InvalidBillingMode(String),

    /// Cannot parse max_elapsed_request_time value into u64
    #[error("Cannot parse max elapsed request time into u64: {source}")]
    ParseMaxElapsedRequestTime {
        // config_value: String,
        source: ParseIntError,
    },
}

/// Errors produced by `DynamoDbLockClient`
#[derive(thiserror::Error, Debug)]
pub enum LockClientError {
    #[error("Log item has invalid content: '{description}'")]
    InconsistentData { description: String },

    #[error("Lock table '{name}': creation failed: {source}")]
    LockTableCreateFailure {
        name: String,
        source: RusotoError<CreateTableError>,
    },

    #[error("Log entry for table '{table_path}' and version '{version}' already exists")]
    VersionAlreadyExists { table_path: String, version: i64 },

    #[error("Provisioned table throughput exceeded")]
    ProvisionedThroughputExceeded,

    #[error("Lock table not found")]
    LockTableNotFound,

    #[error("error in DynamoDb")]
    GenericDynamoDb {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("configuration error: {source}")]
    Credentials {
        source: rusoto_credential::CredentialsError,
    },

    #[error(
        "Atomic rename requires a LockClient for S3 backends. \
         Either configure the LockClient, or set AWS_S3_ALLOW_UNSAFE_RENAME=true \
         to opt out of support for concurrent writers."
    )]
    LockClientRequired,
}

impl From<GetItemError> for LockClientError {
    fn from(err: GetItemError) -> Self {
        match err {
            GetItemError::InternalServerError(_) => err.into(),
            GetItemError::ProvisionedThroughputExceeded(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            GetItemError::RequestLimitExceeded(_) => LockClientError::ProvisionedThroughputExceeded,
            GetItemError::ResourceNotFound(_) => LockClientError::LockTableNotFound,
        }
    }
}

impl From<QueryError> for LockClientError {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::InternalServerError(_) => err.into(),
            QueryError::ProvisionedThroughputExceeded(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            QueryError::RequestLimitExceeded(_) => LockClientError::ProvisionedThroughputExceeded,
            QueryError::ResourceNotFound(_) => LockClientError::LockTableNotFound,
        }
    }
}

impl From<PutItemError> for LockClientError {
    fn from(err: PutItemError) -> Self {
        match err {
            PutItemError::ConditionalCheckFailed(_) => {
                unreachable!("error must be handled explicitely")
            }
            PutItemError::InternalServerError(_) => err.into(),
            PutItemError::ProvisionedThroughputExceeded(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            PutItemError::RequestLimitExceeded(_) => LockClientError::ProvisionedThroughputExceeded,
            PutItemError::ResourceNotFound(_) => LockClientError::LockTableNotFound,
            PutItemError::ItemCollectionSizeLimitExceeded(_) => err.into(),
            PutItemError::TransactionConflict(_) => err.into(),
        }
    }
}

impl From<UpdateItemError> for LockClientError {
    fn from(err: UpdateItemError) -> Self {
        match err {
            UpdateItemError::ConditionalCheckFailed(_) => {
                unreachable!("condition check failure in update is not an error")
            }
            UpdateItemError::InternalServerError(_) => err.into(),
            UpdateItemError::ProvisionedThroughputExceeded(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            UpdateItemError::RequestLimitExceeded(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            UpdateItemError::ResourceNotFound(_) => LockClientError::LockTableNotFound,
            UpdateItemError::ItemCollectionSizeLimitExceeded(_) => err.into(),
            UpdateItemError::TransactionConflict(_) => err.into(),
        }
    }
}

impl<E> From<RusotoError<E>> for LockClientError
where
    E: Into<LockClientError> + std::error::Error + Send + Sync + 'static,
{
    fn from(err: RusotoError<E>) -> Self {
        match err {
            RusotoError::Service(e) => e.into(),
            RusotoError::Credentials(e) => LockClientError::Credentials { source: e },
            _ => LockClientError::GenericDynamoDb {
                source: Box::new(err),
            },
        }
    }
}
