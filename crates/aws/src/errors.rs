//! Errors for S3 log store backed by DynamoDb

use std::num::ParseIntError;

use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        create_table::CreateTableError, delete_item::DeleteItemError, get_item::GetItemError,
        put_item::PutItemError, query::QueryError, update_item::UpdateItemError,
    },
};
use aws_smithy_runtime_api::client::result::ServiceError;

macro_rules! impl_from_service_error {
    ($error_type:ty) => {
        impl<R> From<SdkError<$error_type, R>> for LockClientError
        where
            R: Send + Sync + std::fmt::Debug + 'static,
        {
            fn from(err: SdkError<$error_type, R>) -> Self {
                match err {
                    SdkError::ServiceError(e) => e.into(),
                    _ => LockClientError::GenericDynamoDb {
                        source: Box::new(err),
                    },
                }
            }
        }

        impl<R> From<ServiceError<$error_type, R>> for LockClientError
        where
            R: Send + Sync + std::fmt::Debug + 'static,
        {
            fn from(value: ServiceError<$error_type, R>) -> Self {
                value.into_err().into()
            }
        }
    };
}

#[derive(thiserror::Error, Debug)]
pub enum DynamoDbConfigError {
    /// Billing mode string invalid
    #[error("Invalid billing mode : {0}, supported values : ['provided', 'pay_per_request']")]
    InvalidBillingMode(String),

    /// Cannot parse max_elapsed_request_time value into u64
    #[error("Cannot parse max elapsed request time into u64: {source}")]
    ParseMaxElapsedRequestTime {
        // config_value: String,
        source: ParseIntError,
    },
    /// Cannot initialize DynamoDbConfiguration due to some sort of threading issue
    #[error("Cannot initialize dynamodb lock configuration")]
    InitializationError,
}

/// Errors produced by `DynamoDbLockClient`
#[derive(thiserror::Error, Debug)]
pub enum LockClientError {
    #[error("Log item has invalid content: '{description}'")]
    InconsistentData { description: String },

    #[error("Lock table '{name}': creation failed: {source}")]
    LockTableCreateFailure {
        name: String,
        source: Box<CreateTableError>,
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
    Credentials { source: CredentialsError },
    #[error(
        "Atomic rename requires a LockClient for S3 backends. \
         Either configure the LockClient, or set AWS_S3_ALLOW_UNSAFE_RENAME=true \
         to opt out of support for concurrent writers."
    )]
    LockClientRequired,

    #[error("Log entry for table '{table_path}' and version '{version}' is already complete")]
    VersionAlreadyCompleted { table_path: String, version: i64 },
}

impl From<GetItemError> for LockClientError {
    fn from(err: GetItemError) -> Self {
        match err {
            GetItemError::ProvisionedThroughputExceededException(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            GetItemError::RequestLimitExceeded(_) => LockClientError::ProvisionedThroughputExceeded,
            GetItemError::ResourceNotFoundException(_) => LockClientError::LockTableNotFound,
            _ => LockClientError::GenericDynamoDb {
                source: Box::new(err),
            },
        }
    }
}

impl From<QueryError> for LockClientError {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::ProvisionedThroughputExceededException(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            QueryError::RequestLimitExceeded(_) => LockClientError::ProvisionedThroughputExceeded,
            QueryError::ResourceNotFoundException(_) => LockClientError::LockTableNotFound,
            _ => LockClientError::GenericDynamoDb {
                source: Box::new(err),
            },
        }
    }
}

impl From<PutItemError> for LockClientError {
    fn from(err: PutItemError) -> Self {
        match err {
            PutItemError::ConditionalCheckFailedException(_) => {
                unreachable!("error must be handled explicitely")
            }
            PutItemError::ProvisionedThroughputExceededException(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            PutItemError::RequestLimitExceeded(_) => LockClientError::ProvisionedThroughputExceeded,
            PutItemError::ResourceNotFoundException(_) => LockClientError::LockTableNotFound,
            PutItemError::ItemCollectionSizeLimitExceededException(_) => err.into(),
            PutItemError::TransactionConflictException(_) => err.into(),
            _ => LockClientError::GenericDynamoDb {
                source: Box::new(err),
            },
        }
    }
}

impl From<UpdateItemError> for LockClientError {
    fn from(err: UpdateItemError) -> Self {
        match err {
            UpdateItemError::ConditionalCheckFailedException(_) => {
                unreachable!("condition check failure in update is not an error")
            }
            UpdateItemError::InternalServerError(_) => err.into(),
            UpdateItemError::ProvisionedThroughputExceededException(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            UpdateItemError::RequestLimitExceeded(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            UpdateItemError::ResourceNotFoundException(_) => LockClientError::LockTableNotFound,
            UpdateItemError::ItemCollectionSizeLimitExceededException(_) => err.into(),
            UpdateItemError::TransactionConflictException(_) => err.into(),
            _ => LockClientError::GenericDynamoDb {
                source: Box::new(err),
            },
        }
    }
}

impl From<DeleteItemError> for LockClientError {
    fn from(err: DeleteItemError) -> Self {
        match err {
            DeleteItemError::ConditionalCheckFailedException(_) => {
                unreachable!("error must be handled explicitly")
            }
            DeleteItemError::InternalServerError(_) => err.into(),
            DeleteItemError::ProvisionedThroughputExceededException(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            DeleteItemError::RequestLimitExceeded(_) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            DeleteItemError::ResourceNotFoundException(_) => LockClientError::LockTableNotFound,
            DeleteItemError::ItemCollectionSizeLimitExceededException(_) => err.into(),
            DeleteItemError::TransactionConflictException(_) => err.into(),
            _ => LockClientError::GenericDynamoDb {
                source: Box::new(err),
            },
        }
    }
}

impl_from_service_error!(GetItemError);
impl_from_service_error!(PutItemError);
impl_from_service_error!(QueryError);
impl_from_service_error!(UpdateItemError);
impl_from_service_error!(DeleteItemError);
