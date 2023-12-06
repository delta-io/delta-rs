#![allow(dead_code, missing_docs)]
use crate::storage::utils::copy_table;
use crate::DeltaTableBuilder;
use chrono::Utc;
use fs_extra::dir::{copy, CopyOptions};
use object_store::DynObjectStore;
use rand::Rng;
use serde_json::json;
use std::env;
use std::sync::Arc;
use tempdir::TempDir;

pub type TestResult = Result<(), Box<dyn std::error::Error + 'static>>;

/// The IntegrationContext provides temporary resources to test against cloud storage services.
pub struct IntegrationContext {
    pub integration: StorageIntegration,
    bucket: String,
    store: Arc<DynObjectStore>,
    tmp_dir: TempDir,
}

impl IntegrationContext {
    pub fn new(
        integration: StorageIntegration,
    ) -> Result<Self, Box<dyn std::error::Error + 'static>> {
        // environment variables are loaded from .env files if found. Otherwise
        // default values based on the default setting of the respective emulators are set.
        #[cfg(test)]
        dotenvy::dotenv().ok();

        integration.prepare_env();

        let tmp_dir = TempDir::new("")?;
        // create a fresh bucket in every context. THis is done via CLI...
        let bucket = match integration {
            StorageIntegration::Local => tmp_dir.as_ref().to_str().unwrap().to_owned(),
            StorageIntegration::Onelake => {
                let account_name =
                    env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap_or(String::from("onelake"));
                let container_name =
                    env::var("AZURE_STORAGE_CONTAINER_NAME").unwrap_or(String::from("delta-rs"));
                format!(
                    "{0}.dfs.fabric.microsoft.com/{1}",
                    account_name, container_name
                )
            }
            StorageIntegration::OnelakeAbfs => {
                let account_name =
                    env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap_or(String::from("onelake"));
                let container_name =
                    env::var("AZURE_STORAGE_CONTAINER_NAME").unwrap_or(String::from("delta-rs"));
                format!(
                    "{0}@{1}.dfs.fabric.microsoft.com",
                    container_name, account_name
                )
            }
            _ => format!("test-delta-table-{}", Utc::now().timestamp()),
        };

        if let StorageIntegration::Google = integration {
            gs_cli::prepare_env();
            let base_url = std::env::var("GOOGLE_BASE_URL")?;
            let token = json!({"gcs_base_url": base_url, "disable_oauth": true, "client_email": "", "private_key": ""});
            let account_path = tmp_dir.path().join("gcs.json");
            std::fs::write(&account_path, serde_json::to_vec(&token)?)?;
            set_env_if_not_set(
                "GOOGLE_SERVICE_ACCOUNT",
                account_path.as_path().to_str().unwrap(),
            );
        }

        integration.create_bucket(&bucket)?;
        let store_uri = match integration {
            StorageIntegration::Amazon => format!("s3://{}", &bucket),
            StorageIntegration::Microsoft => format!("az://{}", &bucket),
            StorageIntegration::Onelake => format!("https://{}", &bucket),
            StorageIntegration::OnelakeAbfs => format!("abfss://{}", &bucket),
            StorageIntegration::Google => format!("gs://{}", &bucket),
            StorageIntegration::Local => format!("file://{}", &bucket),
            StorageIntegration::Hdfs => format!("hdfs://localhost:9000/{}", &bucket),
        };
        // the "storage_backend" will always point to the root ofg the object store.
        // TODO should we provide the store via object_Store builders?
        let store = match integration {
            StorageIntegration::Local => Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(tmp_dir.path())?,
            ),
            _ => DeltaTableBuilder::from_uri(store_uri)
                .with_allow_http(true)
                .build_storage()?
                .object_store(),
        };

        Ok(Self {
            integration,
            bucket,
            store,
            tmp_dir,
        })
    }

    /// Get a a reference to the root object store
    pub fn object_store(&self) -> Arc<DynObjectStore> {
        self.store.clone()
    }

    /// Get the URI for initializing a store at the root
    pub fn root_uri(&self) -> String {
        match self.integration {
            StorageIntegration::Amazon => format!("s3://{}", &self.bucket),
            StorageIntegration::Microsoft => format!("az://{}", &self.bucket),
            StorageIntegration::Onelake => format!("https://{}", &self.bucket),
            StorageIntegration::OnelakeAbfs => format!("abfss://{}", &self.bucket),
            StorageIntegration::Google => format!("gs://{}", &self.bucket),
            StorageIntegration::Local => format!("file://{}", &self.bucket),
            StorageIntegration::Hdfs => format!("hdfs://localhost:9000/{}", &self.bucket),
        }
    }

    pub fn table_builder(&self, table: TestTables) -> DeltaTableBuilder {
        let name = table.as_name();
        let table_uri = format!("{}/{}", self.root_uri(), &name);
        DeltaTableBuilder::from_uri(table_uri).with_allow_http(true)
    }

    pub fn uri_for_table(&self, table: TestTables) -> String {
        format!("{}/{}", self.root_uri(), table.as_name())
    }

    pub async fn load_table(&self, table: TestTables) -> TestResult {
        let name = table.as_name();
        self.load_table_with_name(table, name).await
    }

    pub async fn load_table_with_name(
        &self,
        table: TestTables,
        name: impl AsRef<str>,
    ) -> TestResult {
        match self.integration {
            StorageIntegration::Local => {
                let mut options = CopyOptions::new();
                options.content_only = true;
                let dest_path = self.tmp_dir.path().join(name.as_ref());
                std::fs::create_dir_all(&dest_path)?;
                copy(table.as_path(), &dest_path, &options)?;
            }
            StorageIntegration::Amazon => {
                let dest = format!("{}/{}", self.root_uri(), name.as_ref());
                s3_cli::copy_directory(table.as_path(), dest)?;
            }
            StorageIntegration::Microsoft => {
                let dest = format!("{}/{}", self.bucket, name.as_ref());
                az_cli::copy_directory(table.as_path(), dest)?;
            }
            _ => {
                let from = table.as_path().as_str().to_owned();
                let to = format!("{}/{}", self.root_uri(), name.as_ref());
                copy_table(from, None, to, None, true).await?;
            }
        };
        Ok(())
    }
}

impl Drop for IntegrationContext {
    fn drop(&mut self) {
        match self.integration {
            StorageIntegration::Amazon => {
                s3_cli::delete_bucket(self.root_uri()).unwrap();
                s3_cli::delete_lock_table().unwrap();
            }
            StorageIntegration::Microsoft => {
                az_cli::delete_container(&self.bucket).unwrap();
            }
            StorageIntegration::Google => {
                gs_cli::delete_bucket(&self.bucket).unwrap();
            }
            StorageIntegration::Onelake => (),
            StorageIntegration::OnelakeAbfs => (),
            StorageIntegration::Local => (),
            StorageIntegration::Hdfs => {
                hdfs_cli::delete_dir(&self.bucket).unwrap();
            }
        };
    }
}

/// Kinds of storage integration
pub enum StorageIntegration {
    Amazon,
    Microsoft,
    Onelake,
    Google,
    Local,
    Hdfs,
    OnelakeAbfs,
}

impl StorageIntegration {
    fn prepare_env(&self) {
        match self {
            Self::Microsoft => az_cli::prepare_env(),
            Self::Onelake => onelake_cli::prepare_env(),
            Self::Amazon => s3_cli::prepare_env(),
            Self::Google => gs_cli::prepare_env(),
            Self::OnelakeAbfs => onelake_cli::prepare_env(),
            Self::Local => (),
            Self::Hdfs => (),
        }
    }

    fn create_bucket(&self, name: impl AsRef<str>) -> std::io::Result<()> {
        match self {
            Self::Microsoft => {
                az_cli::create_container(name)?;
                Ok(())
            }
            Self::Onelake => Ok(()),
            Self::OnelakeAbfs => Ok(()),
            Self::Amazon => {
                std::env::set_var(
                    "DELTA_DYNAMO_TABLE_NAME",
                    format!("delta_log_it_{}", rand::thread_rng().gen::<u16>()),
                );
                s3_cli::create_bucket(format!("s3://{}", name.as_ref()))?;
                set_env_if_not_set(
                    "DYNAMO_LOCK_PARTITION_KEY_VALUE",
                    format!("s3://{}", name.as_ref()),
                );
                s3_cli::create_lock_table()?;
                Ok(())
            }
            Self::Google => {
                gs_cli::create_bucket(name)?;
                Ok(())
            }
            Self::Local => Ok(()),
            Self::Hdfs => {
                hdfs_cli::create_dir(name)?;
                Ok(())
            }
        }
    }
}

/// Reference tables from the test data folder
pub enum TestTables {
    Simple,
    SimpleCommit,
    Golden,
    Delta0_8_0Partitioned,
    Delta0_8_0SpecialPartitioned,
    Custom(String),
}

impl TestTables {
    fn as_path(&self) -> String {
        // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
        // set by `cargo run` or `cargo test`, see:
        // https://doc.rust-lang.org/cargo/reference/environment-variables.html
        let dir = env!("CARGO_MANIFEST_DIR");
        let data_path = std::path::Path::new(dir).join("tests/data");
        match self {
            Self::Simple => data_path.join("simple_table").to_str().unwrap().to_owned(),
            Self::SimpleCommit => data_path.join("simple_commit").to_str().unwrap().to_owned(),
            Self::Golden => data_path
                .join("golden/data-reader-array-primitives")
                .to_str()
                .unwrap()
                .to_owned(),
            Self::Delta0_8_0Partitioned => data_path
                .join("delta-0.8.0-partitioned")
                .to_str()
                .unwrap()
                .to_owned(),
            Self::Delta0_8_0SpecialPartitioned => data_path
                .join("delta-0.8.0-special-partition")
                .to_str()
                .unwrap()
                .to_owned(),
            // the data path for upload does not apply to custom tables.
            Self::Custom(_) => todo!(),
        }
    }

    pub fn as_name(&self) -> String {
        match self {
            Self::Simple => "simple".into(),
            Self::SimpleCommit => "simple_commit".into(),
            Self::Golden => "golden".into(),
            Self::Delta0_8_0Partitioned => "delta-0.8.0-partitioned".into(),
            Self::Delta0_8_0SpecialPartitioned => "delta-0.8.0-special-partition".into(),
            Self::Custom(name) => name.to_owned(),
        }
    }
}

/// Set environment variable if it is not set
pub fn set_env_if_not_set(key: impl AsRef<str>, value: impl AsRef<str>) {
    if std::env::var(key.as_ref()).is_err() {
        std::env::set_var(key.as_ref(), value.as_ref())
    };
}

//cli for onelake
pub mod onelake_cli {
    use super::set_env_if_not_set;
    /// prepare_env
    pub fn prepare_env() {
        let token = "jwt-token";
        set_env_if_not_set("AZURE_STORAGE_USE_EMULATOR", "0");
        set_env_if_not_set("AZURE_STORAGE_ACCOUNT_NAME", "daily-onelake");
        set_env_if_not_set(
            "AZURE_STORAGE_CONTAINER_NAME",
            "86bc63cf-5086-42e0-b16d-6bc580d1dc87",
        );
        set_env_if_not_set("AZURE_STORAGE_TOKEN", token);
    }
}

/// small wrapper around az cli
pub mod az_cli {
    use super::set_env_if_not_set;
    use std::process::{Command, ExitStatus};

    /// Create a new bucket
    pub fn create_container(container_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let mut child = Command::new("az")
            .args([
                "storage",
                "container",
                "create",
                "-n",
                container_name.as_ref(),
            ])
            .spawn()
            .expect("az command is installed");
        child.wait()
    }

    /// delete bucket
    pub fn delete_container(container_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let mut child = Command::new("az")
            .args([
                "storage",
                "container",
                "delete",
                "-n",
                container_name.as_ref(),
            ])
            .spawn()
            .expect("az command is installed");
        child.wait()
    }

    /// copy directory
    pub fn copy_directory(
        source: impl AsRef<str>,
        destination: impl AsRef<str>,
    ) -> std::io::Result<ExitStatus> {
        let mut child = Command::new("az")
            .args([
                "storage",
                "blob",
                "upload-batch",
                "-s",
                source.as_ref(),
                "-d",
                destination.as_ref(),
            ])
            .spawn()
            .expect("az command is installed");
        child.wait()
    }

    /// prepare_env
    pub fn prepare_env() {
        set_env_if_not_set("AZURE_STORAGE_USE_EMULATOR", "1");
        set_env_if_not_set("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1");
        set_env_if_not_set("AZURE_STORAGE_ACCOUNT_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        set_env_if_not_set(
            "AZURE_STORAGE_CONNECTION_STRING",
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
        );
    }
}

/// small wrapper around s3 cli
pub mod s3_cli {
    use super::set_env_if_not_set;
    use crate::table::builder::s3_storage_options;
    use std::process::{Command, ExitStatus, Stdio};

    /// Create a new bucket
    pub fn create_bucket(bucket_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let endpoint = std::env::var(s3_storage_options::AWS_ENDPOINT_URL)
            .expect("variable ENDPOINT must be set to connect to S3");
        let region = std::env::var(s3_storage_options::AWS_REGION)
            .expect("variable AWS_REGION must be set to connect to S3");
        let mut child = Command::new("aws")
            .args([
                "s3",
                "mb",
                bucket_name.as_ref(),
                "--endpoint-url",
                &endpoint,
                "--region",
                &region,
            ])
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }

    /// delete bucket
    pub fn delete_bucket(bucket_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let endpoint = std::env::var(s3_storage_options::AWS_ENDPOINT_URL)
            .expect("variable ENDPOINT must be set to connect to S3");
        let mut child = Command::new("aws")
            .args([
                "s3",
                "rb",
                bucket_name.as_ref(),
                "--endpoint-url",
                &endpoint,
                "--force",
            ])
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }

    /// copy directory
    pub fn copy_directory(
        source: impl AsRef<str>,
        destination: impl AsRef<str>,
    ) -> std::io::Result<ExitStatus> {
        let endpoint = std::env::var(s3_storage_options::AWS_ENDPOINT_URL)
            .expect("variable ENDPOINT must be set to connect to S3");
        let mut child = Command::new("aws")
            .args([
                "s3",
                "cp",
                source.as_ref(),
                destination.as_ref(),
                "--endpoint-url",
                &endpoint,
                "--recursive",
            ])
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }

    /// prepare_env
    pub fn prepare_env() {
        set_env_if_not_set(
            s3_storage_options::AWS_ENDPOINT_URL,
            "http://localhost:4566",
        );
        set_env_if_not_set(s3_storage_options::AWS_ACCESS_KEY_ID, "deltalake");
        set_env_if_not_set(s3_storage_options::AWS_SECRET_ACCESS_KEY, "weloverust");
        set_env_if_not_set("AWS_DEFAULT_REGION", "us-east-1");
        set_env_if_not_set(s3_storage_options::AWS_REGION, "us-east-1");
        set_env_if_not_set(s3_storage_options::AWS_S3_LOCKING_PROVIDER, "dynamodb");
        set_env_if_not_set("DYNAMO_LOCK_TABLE_NAME", "test_table");
        set_env_if_not_set("DYNAMO_LOCK_REFRESH_PERIOD_MILLIS", "100");
        set_env_if_not_set("DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS", "100");
    }

    fn create_dynamodb_table(
        table_name: &str,
        endpoint_url: &str,
        attr_definitions: &[&str],
        key_schema: &[&str],
    ) -> std::io::Result<ExitStatus> {
        println!("creating table {}", table_name);
        let args01 = [
            "dynamodb",
            "create-table",
            "--table-name",
            &table_name,
            "--endpoint-url",
            &endpoint_url,
            "--provisioned-throughput",
            "ReadCapacityUnits=10,WriteCapacityUnits=10",
            "--attribute-definitions",
        ];
        let args: Vec<_> = args01
            .iter()
            .chain(attr_definitions.iter())
            .chain(["--key-schema"].iter())
            .chain(key_schema)
            .collect();
        let mut child = Command::new("aws")
            .args(args)
            .stdout(Stdio::null())
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }

    pub fn create_lock_table() -> std::io::Result<ExitStatus> {
        let endpoint_url = std::env::var(s3_storage_options::AWS_ENDPOINT_URL)
            .expect("variable AWS_ENDPOINT_URL must be set to connect to S3 emulator");
        let table_name =
            std::env::var("DELTA_DYNAMO_TABLE_NAME").unwrap_or_else(|_| "delta_log".into());
        create_dynamodb_table(
            &table_name,
            &endpoint_url,
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

    fn delete_dynamodb_table(table_name: &str, endpoint_url: &str) -> std::io::Result<ExitStatus> {
        let mut child = Command::new("aws")
            .args([
                "dynamodb",
                "delete-table",
                "--table-name",
                &table_name,
                "--endpoint-url",
                &endpoint_url,
            ])
            .stdout(Stdio::null())
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }

    pub fn delete_lock_table() -> std::io::Result<ExitStatus> {
        let endpoint_url = std::env::var(s3_storage_options::AWS_ENDPOINT_URL)
            .expect("variable AWS_ENDPOINT_URL must be set to connect to S3 emulator");
        let table_name =
            std::env::var("DELTA_DYNAMO_TABLE_NAME").unwrap_or_else(|_| "delta_log".into());
        delete_dynamodb_table(&table_name, &endpoint_url)
    }
}

/// small wrapper around google api
pub mod gs_cli {
    use super::set_env_if_not_set;
    use serde_json::json;
    use std::process::{Command, ExitStatus};

    pub fn create_bucket(container_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let endpoint = std::env::var("GOOGLE_ENDPOINT_URL")
            .expect("variable GOOGLE_ENDPOINT_URL must be set to connect to GCS Emulator");
        let payload = json!({ "name": container_name.as_ref() });
        let mut child = Command::new("curl")
            .args([
                "--insecure",
                "-v",
                "-X",
                "POST",
                "--data-binary",
                &serde_json::to_string(&payload)?,
                "-H",
                "Content-Type: application/json",
                &endpoint,
            ])
            .spawn()
            .expect("curl command is installed");
        child.wait()
    }

    pub fn delete_bucket(container_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let endpoint = std::env::var("GOOGLE_ENDPOINT_URL")
            .expect("variable GOOGLE_ENDPOINT_URL must be set to connect to GCS Emulator");
        let payload = json!({ "name": container_name.as_ref() });
        let mut child = Command::new("curl")
            .args([
                "--insecure",
                "-v",
                "-X",
                "DELETE",
                "--data-binary",
                &serde_json::to_string(&payload)?,
                "-H",
                "Content-Type: application/json",
                &endpoint,
            ])
            .spawn()
            .expect("curl command is installed");
        child.wait()
    }

    /// prepare_env
    pub fn prepare_env() {
        set_env_if_not_set("GOOGLE_BASE_URL", "http://localhost:4443");
        set_env_if_not_set("GOOGLE_ENDPOINT_URL", "http://localhost:4443/storage/v1/b");
    }
}

/// small wrapper around hdfs cli
pub mod hdfs_cli {
    use std::env;
    use std::path::PathBuf;
    use std::process::{Command, ExitStatus};

    fn hdfs_cli_path() -> PathBuf {
        let hadoop_home =
            env::var("HADOOP_HOME").expect("HADOOP_HOME environment variable not set");
        PathBuf::from(hadoop_home).join("bin").join("hdfs")
    }

    pub fn create_dir(dir_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let path = hdfs_cli_path();
        let mut child = Command::new(path)
            .args([
                "dfs",
                "-mkdir",
                "-p",
                format!("/{}", dir_name.as_ref()).as_str(),
            ])
            .spawn()
            .expect("hdfs command is installed");
        child.wait()
    }

    pub fn delete_dir(dir_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let path = hdfs_cli_path();
        let mut child = Command::new(path)
            .args([
                "dfs",
                "-rm",
                "-r",
                "-f",
                format!("/{}", dir_name.as_ref()).as_str(),
            ])
            .spawn()
            .expect("hdfs command is installed");
        child.wait()
    }
}
