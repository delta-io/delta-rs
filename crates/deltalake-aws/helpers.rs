use super::TestContext;
use chrono::Utc;
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::process::Command;

/// Create a bucket and dynamodb lock table for s3 backend tests
/// Requires local stack running in the background
pub async fn setup_s3_context() -> TestContext {
    // Create a new prefix per test run.
    let rand: u16 = rand::thread_rng().gen();
    let cli = S3Cli::default();
    let endpoint = "http://localhost:4566".to_string();
    let bucket_name = "delta-rs-tests";
    let uri = format!("s3://{}/{}/{}/", bucket_name, Utc::now().timestamp(), rand);
    let lock_table = format!("delta_rs_lock_table_{rand}");

    let region = "us-east-1".to_string();

    env::set_var("AWS_ACCESS_KEY_ID", "deltalake");
    env::set_var("AWS_SECRET_ACCESS_KEY", "weloverust");
    env::set_var("AWS_DEFAULT_REGION", &region);
    env::set_var("AWS_ALLOW_HTTP", "TRUE");

    cli.create_bucket(bucket_name, &endpoint);
    cli.create_table(
        &lock_table,
        "AttributeName=key,AttributeType=S",
        "AttributeName=key,KeyType=HASH",
        "ReadCapacityUnits=10,WriteCapacityUnits=10",
        &endpoint,
    );

    let mut config = HashMap::new();
    config.insert("URI".to_owned(), uri.clone());
    config.insert("AWS_ENDPOINT_URL".to_owned(), endpoint.clone());
    config.insert("AWS_REGION".to_owned(), region);
    config.insert("AWS_ACCESS_KEY_ID".to_owned(), "deltalake".to_owned());
    config.insert("AWS_SECRET_ACCESS_KEY".to_owned(), "weloverust".to_owned());
    config.insert("AWS_S3_LOCKING_PROVIDER".to_owned(), "dynamodb".to_owned());
    config.insert(constants::LOCK_TABLE_KEY_NAME.to_owned(), lock_table.clone());
    config.insert("AWS_ALLOW_HTTP".to_owned(), "TRUE".to_string());

    TestContext {
        config,
        storage_context: Some(Box::new(S3 {
            endpoint,
            uri,
            lock_table,
        })),
        ..TestContext::default()
    }
}

#[derive(Default)]
struct S3Cli {}

impl S3Cli {
    pub fn create_bucket(&self, bucket_name: &str, endpoint: &str) {
        let mut child = Command::new("aws")
            .args([
                "s3api",
                "create-bucket",
                "--bucket",
                bucket_name,
                "--endpoint-url",
                endpoint,
            ])
            .spawn()
            .expect("aws command is installed");
        child.wait().unwrap();
    }

    pub fn rm_recurive(&self, prefix: &str, endpoint: &str) {
        let mut child = Command::new("aws")
            .args([
                "s3",
                "rm",
                "--recursive",
                prefix,
                "--endpoint-url",
                endpoint,
            ])
            .spawn()
            .expect("aws command is installed");
        child.wait().unwrap();
    }

    pub fn delete_table(&self, table_name: &str, endpoint: &str) {
        let mut child = Command::new("aws")
            .args([
                "dynamodb",
                "delete-table",
                "--table-name",
                table_name,
                "--endpoint-url",
                endpoint,
            ])
            .spawn()
            .expect("aws command is installed");
        child.wait().unwrap();
    }

    pub fn create_table(
        &self,
        table_name: &str,
        attribute_definitions: &str,
        key_schema: &str,
        provisioned_throughput: &str,
        endpoint: &str,
    ) {
        let mut child = Command::new("aws")
            .args([
                "dynamodb",
                "create-table",
                "--table-name",
                table_name,
                "--endpoint-url",
                endpoint,
                "--attribute-definitions",
                attribute_definitions,
                "--key-schema",
                key_schema,
                "--provisioned-throughput",
                provisioned_throughput,
            ])
            .spawn()
            .expect("aws command is installed");
        child.wait().unwrap();
    }
}

struct S3 {
    endpoint: String,
    uri: String,
    lock_table: String,
}

impl Drop for S3 {
    fn drop(&mut self) {
        let cli = S3Cli::default();
        cli.rm_recurive(&self.uri, &self.endpoint);
        cli.delete_table(&self.lock_table, &self.endpoint);
    }
}
