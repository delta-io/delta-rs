use super::TestContext;
use std::collections::HashMap;
use std::process::Command;
use rand::Rng;
use chrono::Utc;
use std::env;

/// TODO: Currently this requires local stack to be setup prior to test execution.
/// If we want to have this automated these are the key requirements:
///     Multiple tests using the backend can run at the same time
///     The start time for local stack is expensive so it should only setup once per execution not per test run
///     When all tests have finished executing, local stack must terminate
///  
/// A possible solution is to have global variable to indicate if it was setup is complete.
/// First obtain a read lock and check if local stack was setup. If it was not
/// setup, then release the read lock and obtain a write lock. When the write
/// lock is obtained, check the global variable again and return early if it was
/// set. Otherwise, Spawn a new process which will setup local stack and then
/// notify the parent of completion. The parent can then set the global variable
/// and release the lock. The child will then wait until the parent process
/// terminates then will clean up local stack.
/// 
/// Problems: This solution might have some issues on Windows and Mac
/// 
pub async fn setup_s3_context() -> TestContext {
    
    // Create a new prefix per test run.
    let rand: u16 = rand::thread_rng().gen();
    let cli = S3Cli::default();
    let endpoint = "http://localhost:4566".to_string();
    let bucket_name = "delta-rs-tests";
    let uri = format!("s3://{}/{}/{}/", bucket_name, Utc::now().timestamp(), rand);
    let lock_table = format!("delta_rs_lock_table_{}", rand);

    let region = "us-east-1".to_string();;

    env::set_var("AWS_ACCESS_KEY_ID", "test");
    env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    env::set_var("AWS_DEFAULT_REGION", &region);

    cli.create_bucket(bucket_name, &endpoint);
    cli.create_table(&lock_table,
    "AttributeName=key,AttributeType=S",
    "AttributeName=key,KeyType=HASH",
    "ReadCapacityUnits=10,WriteCapacityUnits=10", &endpoint);
    

    let mut config = HashMap::new();
    config.insert("URI".to_owned(), uri.clone());
    config.insert("AWS_ENDPOINT_URL".to_owned(), endpoint.clone());
    config.insert("AWS_REGION".to_owned(), region.clone());
    config.insert("AWS_ACCESS_KEY_ID".to_owned(), "test".to_owned());
    config.insert("AWS_SECRET_ACCESS_KEY".to_owned(), "test".to_owned());
    config.insert("AWS_S3_LOCKING_PROVIDER".to_owned(), "dynamodb".to_owned());
    config.insert("DYNAMO_LOCK_TABLE_NAME".to_owned(), lock_table.clone());

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
struct S3Cli { }

impl S3Cli {

    pub fn create_bucket(&self, bucket_name: &str, endpoint: &str) {
        let mut child = Command::new("aws")
            .args(["s3api", "create-bucket", "--bucket", bucket_name, "--endpoint-url", endpoint])
            .spawn()
            .expect("aws command is installed");
        child.wait();
    }

    pub fn rm_recurive(&self, prefix: &str, endpoint: &str) {
        let mut child = Command::new("aws")
            .args(["s3", "rm", "--recursive", prefix, "--endpoint-url", endpoint])
            .spawn()
            .expect("aws command is installed");
        child.wait();
    }

    pub fn delete_table(&self, table_name: &str, endpoint: &str) {
        let mut child = Command::new("aws")
            .args(["dynamodb", "delete-table", "--table-name", table_name, "--endpoint-url", endpoint])
            .spawn()
            .expect("aws command is installed");
        child.wait();
    }

    pub fn create_table(&self, table_name: &str, attribute_definitions: &str,
    key_schema: &str, provisioned_throughput:  &str,endpoint: &str) {
        let mut child = Command::new("aws")
            .args(["dynamodb", "create-table", "--table-name", table_name,
            "--endpoint-url", endpoint, "--attribute-definitions",
            attribute_definitions, "--key-schema",  key_schema, "--provisioned-throughput", provisioned_throughput])
            .spawn()
            .expect("aws command is installed");
        child.wait();

    }
}

struct S3 {
    endpoint: String,
    uri: String,
    lock_table: String
}

impl Drop for S3 {
    fn drop(&mut self) {
        let cli = S3Cli::default();
        cli.rm_recurive(&self.uri, &self.endpoint);
        cli.delete_table(&self.lock_table, &self.endpoint);
    }
}
