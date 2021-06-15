//! Lambda function for writing checkpoints from S3 event triggers on _delta_log JSON files.
//!

// Reference:
//
// https://aws.amazon.com/blogs/opensource/rust-runtime-for-aws-lambda
// https://docs.aws.amazon.com/cli/latest/reference/s3api/put-bucket-notification-configuration.html
// https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-filtering.html

use deltalake::checkpoints::{CheckPointWriter, CheckPointWriterError};
use deltalake::DeltaDataTypeVersion;
use lambda_runtime::{handler_fn, Context, Error};
use lazy_static::lazy_static;
use log::*;
use regex::Regex;
use serde_json::Value;
use std::num::ParseIntError;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let _ = pretty_env_logger::try_init();

    let func = handler_fn(func);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn func(event: Value, _: Context) -> Result<(), CheckPointLambdaError> {
    process_event(&event).await
}

async fn process_event(event: &Value) -> Result<(), CheckPointLambdaError> {
    let (bucket, key) = bucket_and_key_from_event(&event)?;
    let (path, version) = table_path_and_version_from_key(key.as_str())?;
    let table_uri = table_uri_from_parts(bucket.as_str(), path.as_str())?;

    // Checkpoints are created for every 10th delta log commit.
    // Follows the reference implementation described in the delta protocol doc.
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints.
    if version % 10 == 0 && version != 0 {
        info!(
            "Writing checkpoint for table uri {} with delta version {}.",
            table_uri, version
        );

        let checkpoint_writer = CheckPointWriter::new_for_table_uri(table_uri.as_str())?;

        let _ = checkpoint_writer
            .create_checkpoint_for_version(version)
            .await?;
    } else {
        info!(
            "Not writing checkpoint for table uri {} at delta version {}.",
            table_uri, version
        );
    }

    Ok(())
}

fn bucket_and_key_from_event(event: &Value) -> Result<(String, String), CheckPointLambdaError> {
    let s3_value = event
        .get("Records")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.get(0))
        .and_then(|elem| elem.get("s3"))
        .ok_or_else(|| CheckPointLambdaError::InvalidEventStructure(event.to_string()))?;

    let bucket = s3_value
        .get("bucket")
        .and_then(|v| v.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| CheckPointLambdaError::InvalidEventStructure(event.to_string()))?
        .to_string();

    let key = s3_value
        .get("object")
        .and_then(|v| v.get("key"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| CheckPointLambdaError::InvalidEventStructure(event.to_string()))?
        .to_string();

    Ok((bucket, key))
}

fn table_path_and_version_from_key(
    key: &str,
) -> Result<(String, DeltaDataTypeVersion), CheckPointLambdaError> {
    lazy_static! {
        static ref JSON_LOG_ENTRY_REGEX: Regex =
            Regex::new(r#"(.*)/_delta_log/0*(\d+)\.json$"#).unwrap();
    }

    match JSON_LOG_ENTRY_REGEX.captures(key.as_ref()) {
        Some(captures) => {
            let table_path = captures
                .get(1)
                .ok_or_else(|| CheckPointLambdaError::ObjectKeyParseFailed(key.to_string()))?
                .as_str()
                .to_string();
            let version_str = captures
                .get(2)
                .ok_or_else(|| CheckPointLambdaError::ObjectKeyParseFailed(key.to_string()))?
                .as_str();
            let version = version_str.parse::<DeltaDataTypeVersion>()?;

            Ok((table_path, version))
        }
        _ => Err(CheckPointLambdaError::ObjectKeyParseFailed(key.to_string())),
    }
}

fn table_uri_from_parts(bucket: &str, path: &str) -> Result<String, CheckPointLambdaError> {
    let mut table_uri = PathBuf::new();

    table_uri.push(format!("s3://{}", bucket));
    table_uri.push(path);

    Ok(table_uri
        .to_str()
        .ok_or_else(|| CheckPointLambdaError::InvalidTableUri(table_uri.clone()))?
        .to_string())
}

#[derive(thiserror::Error, Debug)]
enum CheckPointLambdaError {
    #[error("Invalid table uri: {0}")]
    InvalidTableUri(PathBuf),

    #[error("Invalid event structure: {0}")]
    InvalidEventStructure(String),

    #[error("Failed to parse object key: {0}")]
    ObjectKeyParseFailed(String),

    #[error("Failed to parse version {}", .source)]
    InvalidVersionParsed {
        #[from]
        source: ParseIntError,
    },

    #[error("Failed to deserialize JSON {}", source)]
    JSONError {
        #[from]
        source: serde_json::Error,
    },

    #[error("CheckPointWriter invocation failed {}", .source)]
    CheckPointWriter {
        #[from]
        source: CheckPointWriterError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn checkpoint_bucket_and_key_from_value_test() {
        let event = json!({
            "Records": [{
                "eventName": "...",
                "s3": {
                    "bucket": {
                        "name": "my_bucket",
                    },
                    "object": {
                        "key": "my_database/my_table/_delta_log/00000000000000000000.json",
                    },
                },
            }]
        });

        let (bucket, key) = bucket_and_key_from_event(&event).unwrap();

        assert_eq!("my_bucket", bucket.as_str());
        assert_eq!(
            "my_database/my_table/_delta_log/00000000000000000000.json",
            key.as_str()
        );
    }

    #[test]
    fn checkpoint_table_path_and_version_from_key_test() {
        let key = "database_name/table_name/_delta_log/00000000000000000102.json";

        let (table_path, version) = table_path_and_version_from_key(key).unwrap();

        assert_eq!("database_name/table_name", table_path.as_str());
        assert_eq!(102, version);
    }

    #[test]
    fn checkpoint_table_uri_from_parts_test() {
        let table_uri = table_uri_from_parts("my_bucket", "database_name/table_name").unwrap();

        assert_eq!(
            "s3://my_bucket/database_name/table_name",
            table_uri.as_str()
        );
    }
}
