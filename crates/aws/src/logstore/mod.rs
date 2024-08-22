//! Contains the different logstore implementations for S3.
//! - S3LogStore (used when copy-if-not-exists or unsafe_rename is passed)
//! - S3DynamoDBLogStore (used when DynamoDB is the locking client)

mod default_logstore;
mod dynamodb_logstore;

pub use default_logstore::default_s3_logstore;
pub use default_logstore::S3LogStore;
pub use dynamodb_logstore::RepairLogEntryResult;
pub use dynamodb_logstore::S3DynamoDbLogStore;
