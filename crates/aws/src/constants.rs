//! Constants used for modifying and configuring various AWS S3 (or similar) connections with
//! delta-rs
//!

use lazy_static::lazy_static;
use std::time::Duration;

/// Custom S3 endpoint.
pub const AWS_ENDPOINT_URL: &str = "AWS_ENDPOINT_URL";
/// Custom DynamoDB endpoint.
/// If DynamoDB endpoint is not supplied, will use S3 endpoint (AWS_ENDPOINT_URL)
/// If it is supplied, this endpoint takes precedence over the global endpoint set in AWS_ENDPOINT_URL for DynamoDB
pub const AWS_ENDPOINT_URL_DYNAMODB: &str = "AWS_ENDPOINT_URL_DYNAMODB";
/// The AWS region.
pub const AWS_REGION: &str = "AWS_REGION";
/// The AWS profile.
pub const AWS_PROFILE: &str = "AWS_PROFILE";
/// The AWS_ACCESS_KEY_ID to use for S3.
pub const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
/// The AWS_SECRET_ACCESS_KEY to use for S3.
pub const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
/// The AWS_SESSION_TOKEN to use for S3.
pub const AWS_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";
/// Uses either "path" (the default) or "virtual", which turns on
/// [virtual host addressing](http://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html).
pub const AWS_S3_ADDRESSING_STYLE: &str = "AWS_S3_ADDRESSING_STYLE";
/// Locking provider to use for safe atomic rename.
/// `dynamodb` is currently the only supported locking provider.
/// If not set, safe atomic rename is not available.
pub const AWS_S3_LOCKING_PROVIDER: &str = "AWS_S3_LOCKING_PROVIDER";
/// The role to assume for S3 writes.
pub const AWS_IAM_ROLE_ARN: &str = "AWS_IAM_ROLE_ARN";
/// The role to assume. Please use [AWS_IAM_ROLE_ARN] instead
#[deprecated(since = "0.20.0", note = "Please use AWS_IAM_ROLE_ARN instead")]
pub const AWS_S3_ASSUME_ROLE_ARN: &str = "AWS_S3_ASSUME_ROLE_ARN";
/// The role session name to use when a role is assumed. If not provided a random session name is generated.
pub const AWS_IAM_ROLE_SESSION_NAME: &str = "AWS_IAM_ROLE_SESSION_NAME";
/// The role session name to use when a role is assumed. If not provided a random session name is generated.
#[deprecated(
    since = "0.20.0",
    note = "Please use AWS_IAM_ROLE_SESSION_NAME instead"
)]
pub const AWS_S3_ROLE_SESSION_NAME: &str = "AWS_S3_ROLE_SESSION_NAME";
/// The `pool_idle_timeout` option of aws http client. Has to be lower than 20 seconds, which is
/// default S3 server timeout <https://aws.amazon.com/premiumsupport/knowledge-center/s3-socket-connection-timeout-error/>.
/// However, since rusoto uses hyper as a client, its default timeout is 90 seconds
/// <https://docs.rs/hyper/0.13.2/hyper/client/struct.Builder.html#method.keep_alive_timeout>.
/// Hence, the `connection closed before message completed` could occur.
/// To avoid that, the default value of this setting is 15 seconds if it's not set otherwise.
pub const AWS_S3_POOL_IDLE_TIMEOUT_SECONDS: &str = "AWS_S3_POOL_IDLE_TIMEOUT_SECONDS";
/// The `pool_idle_timeout` for the as3_constants sts client. See
/// the reasoning in `AWS_S3_POOL_IDLE_TIMEOUT_SECONDS`.
pub const AWS_STS_POOL_IDLE_TIMEOUT_SECONDS: &str = "AWS_STS_POOL_IDLE_TIMEOUT_SECONDS";
/// The number of retries for S3 GET requests failed with 500 Internal Server Error.
pub const AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES: &str =
    "AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES";
/// The web identity token file to use when using a web identity provider.
/// NOTE: web identity related options are set in the environment when
/// creating an instance of [crate::storage::s3::S3StorageOptions].
/// See also <https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env>.
pub const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
/// The role name to use for web identity.
/// NOTE: web identity related options are set in the environment when
/// creating an instance of [crate::storage::s3::S3StorageOptions].
/// See also <https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env>.
pub const AWS_ROLE_ARN: &str = "AWS_ROLE_ARN";
/// The role session name to use for web identity.
/// NOTE: web identity related options are set in the environment when
/// creating an instance of [crate::storage::s3::S3StorageOptions].
/// See also <https://docs.rs/rusoto_sts/0.47.0/rusoto_sts/struct.WebIdentityProvider.html#method.from_k8s_env>.
pub const AWS_ROLE_SESSION_NAME: &str = "AWS_ROLE_SESSION_NAME";
/// Allow http connections - mainly useful for integration tests
pub const AWS_ALLOW_HTTP: &str = "AWS_ALLOW_HTTP";

/// If set to "true", allows creating commits without concurrent writer protection.
/// Only safe if there is one writer to a given table.
pub const AWS_S3_ALLOW_UNSAFE_RENAME: &str = "AWS_S3_ALLOW_UNSAFE_RENAME";

/// If set to "true", disables the imds client
/// Defaults to "true"
pub const AWS_EC2_METADATA_DISABLED: &str = "AWS_EC2_METADATA_DISABLED";

/// The timeout in milliseconds for the EC2 metadata endpoint
/// Defaults to 100
pub const AWS_EC2_METADATA_TIMEOUT: &str = "AWS_EC2_METADATA_TIMEOUT";

/// The list of option keys owned by the S3 module.
/// Option keys not contained in this list will be added to the `extra_opts`
/// field of [crate::storage::s3::S3StorageOptions].
pub const S3_OPTS: &[&str] = &[
    AWS_ENDPOINT_URL,
    AWS_ENDPOINT_URL_DYNAMODB,
    AWS_REGION,
    AWS_PROFILE,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    AWS_S3_LOCKING_PROVIDER,
    AWS_S3_ASSUME_ROLE_ARN,
    AWS_S3_ROLE_SESSION_NAME,
    AWS_WEB_IDENTITY_TOKEN_FILE,
    AWS_ROLE_ARN,
    AWS_ROLE_SESSION_NAME,
    AWS_S3_POOL_IDLE_TIMEOUT_SECONDS,
    AWS_STS_POOL_IDLE_TIMEOUT_SECONDS,
    AWS_S3_GET_INTERNAL_SERVER_ERROR_RETRIES,
    AWS_EC2_METADATA_DISABLED,
    AWS_EC2_METADATA_TIMEOUT,
];

pub const DEFAULT_LOCK_TABLE_NAME: &str = "delta_log";
pub const LOCK_TABLE_KEY_NAME: &str = "DELTA_DYNAMO_TABLE_NAME";
pub const BILLING_MODE_KEY_NAME: &str = "DELTA_DYNAMO_BILLING_MODE";
pub const MAX_ELAPSED_REQUEST_TIME_KEY_NAME: &str = "DELTA_DYNAMO_MAX_ELAPSED_REQUEST_TIME";

pub const ATTR_TABLE_PATH: &str = "tablePath";
pub const ATTR_FILE_NAME: &str = "fileName";
pub const ATTR_TEMP_PATH: &str = "tempPath";
pub const ATTR_COMPLETE: &str = "complete";
pub const ATTR_EXPIRE_TIME: &str = "expireTime";

pub const STRING_TYPE: &str = "S";

pub const KEY_TYPE_HASH: &str = "HASH";
pub const KEY_TYPE_RANGE: &str = "RANGE";

lazy_static! {
    pub static ref CONDITION_EXPR_CREATE: String = format!(
        "attribute_not_exists({ATTR_TABLE_PATH}) and attribute_not_exists({ATTR_FILE_NAME})"
    );

    pub static ref CONDITION_DELETE_INCOMPLETE: String = format!(
        "(complete = :f) or (attribute_not_exists({ATTR_TABLE_PATH}) and attribute_not_exists({ATTR_FILE_NAME}))"
    );
}

pub const CONDITION_UPDATE_INCOMPLETE: &str = "complete = :f";
pub const DEFAULT_COMMIT_ENTRY_EXPIRATION_DELAY: Duration = Duration::from_secs(86_400);
