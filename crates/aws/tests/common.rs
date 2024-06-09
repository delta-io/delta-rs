use chrono::Utc;
use deltalake_aws::constants;
use deltalake_aws::register_handlers;
use deltalake_aws::storage::*;
use deltalake_test::utils::*;
use rand::Rng;
use std::process::{Command, ExitStatus, Stdio};

#[derive(Clone, Debug)]
pub struct S3Integration {
    bucket_name: String,
}

impl Default for S3Integration {
    fn default() -> Self {
        register_handlers(None);
        Self {
            bucket_name: format!("test-delta-table-{}", Utc::now().timestamp()),
        }
    }
}

impl StorageIntegration for S3Integration {
    /// Create a new bucket
    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        Self::create_lock_table()?;
        let mut child = Command::new("aws")
            .args(["s3", "mb", &self.root_uri()])
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }

    fn bucket_name(&self) -> String {
        self.bucket_name.clone()
    }

    fn root_uri(&self) -> String {
        format!("s3://{}", &self.bucket_name())
    }

    /// prepare_env
    fn prepare_env(&self) {
        set_env_if_not_set(
            constants::LOCK_TABLE_KEY_NAME,
            format!("delta_log_it_{}", rand::thread_rng().gen::<u16>()),
        );
        match std::env::var(s3_constants::AWS_ENDPOINT_URL).ok() {
            Some(endpoint_url) if endpoint_url.to_lowercase() == "none" => {
                std::env::remove_var(s3_constants::AWS_ENDPOINT_URL)
            }
            Some(_) => (),
            None => std::env::set_var(s3_constants::AWS_ENDPOINT_URL, "http://localhost:4566"),
        }
        set_env_if_not_set(s3_constants::AWS_ACCESS_KEY_ID, "deltalake");
        set_env_if_not_set(s3_constants::AWS_SECRET_ACCESS_KEY, "weloverust");
        set_env_if_not_set(s3_constants::AWS_REGION, "us-east-1");
        set_env_if_not_set(s3_constants::AWS_S3_LOCKING_PROVIDER, "dynamodb");
    }

    /// copy directory
    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        let destination = format!("{}/{destination}", self.root_uri());
        let mut child = Command::new("aws")
            .args(["s3", "cp", source, &destination, "--recursive"])
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }
}

impl S3Integration {
    /// delete bucket
    fn delete_bucket(bucket_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let mut child = Command::new("aws")
            .args(["s3", "rb", bucket_name.as_ref(), "--force"])
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }
    fn create_dynamodb_table(
        table_name: &str,
        attr_definitions: &[&str],
        key_schema: &[&str],
    ) -> std::io::Result<ExitStatus> {
        let args = [
            "dynamodb",
            "create-table",
            "--table-name",
            table_name,
            "--provisioned-throughput",
            "ReadCapacityUnits=1,WriteCapacityUnits=1",
            "--attribute-definitions",
        ];
        let mut child = Command::new("aws")
            .args(args)
            .args(attr_definitions.iter())
            .arg("--key-schema")
            .args(key_schema)
            .stdout(Stdio::null())
            .spawn()
            .expect("aws command is installed");
        let status = child.wait()?;
        Self::wait_for_table(table_name)?;
        Ok(status)
    }

    fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    fn wait_for_table(table_name: &str) -> std::io::Result<()> {
        let args = ["dynamodb", "describe-table", "--table-name", table_name];
        loop {
            let output = Command::new("aws")
                .args(args)
                .output()
                .expect("aws command is installed");
            if Self::find_subsequence(&output.stdout, "CREATING".as_bytes()).is_some() {
                std::thread::sleep(std::time::Duration::from_millis(200));
                continue;
            } else {
                return Ok(());
            }
        }
    }

    pub fn create_lock_table() -> std::io::Result<ExitStatus> {
        let table_name =
            std::env::var(constants::LOCK_TABLE_KEY_NAME).unwrap_or_else(|_| "delta_log".into());
        Self::create_dynamodb_table(
            &table_name,
            &[
                "AttributeName=tablePath,AttributeType=S",
                "AttributeName=fileName,AttributeType=S",
            ],
            &[
                "AttributeName=tablePath,KeyType=HASH",
                "AttributeName=fileName,KeyType=RANGE",
            ],
        )
    }

    fn delete_dynamodb_table(table_name: &str) -> std::io::Result<ExitStatus> {
        let mut child = Command::new("aws")
            .args(["dynamodb", "delete-table", "--table-name", table_name])
            .stdout(Stdio::null())
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }

    pub fn delete_lock_table() -> std::io::Result<ExitStatus> {
        let table_name =
            std::env::var(constants::LOCK_TABLE_KEY_NAME).unwrap_or_else(|_| "delta_log".into());
        Self::delete_dynamodb_table(&table_name)
    }
}

impl Drop for S3Integration {
    fn drop(&mut self) {
        Self::delete_bucket(self.root_uri()).expect("Failed to drop bucket");
        Self::delete_lock_table().expect("Failed to delete lock table");
    }
}
