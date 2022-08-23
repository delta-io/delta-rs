#![allow(dead_code, missing_docs)]
use crate::DeltaTableBuilder;
use chrono::Utc;
use object_store::DynObjectStore;
use std::process::ExitStatus;
use std::sync::Arc;

pub type TestResult = Result<(), Box<dyn std::error::Error + 'static>>;

/// The IntegrationContext provides temporary resources to test against cloud storage services.
pub struct IntegrationContext {
    integration: StorageIntegration,
    bucket: String,
    store: Arc<DynObjectStore>,
}

impl IntegrationContext {
    pub fn new(
        integration: StorageIntegration,
    ) -> Result<Self, Box<dyn std::error::Error + 'static>> {
        // environment variables are loaded from .env files if found. Otherwise
        // default values based on the default setting of the respective emulators are set.
        #[cfg(test)]
        dotenv::dotenv().ok();

        integration.prepare_env();

        // create a fresh bucket in every context. THis is done via CLI...
        let bucket = format!("test-delta-table-{}", Utc::now().timestamp());
        integration.crate_bucket(&bucket)?;
        let store_uri = match integration {
            StorageIntegration::Amazon => format!("s3://{}", &bucket),
            StorageIntegration::Microsoft => format!("az://{}", &bucket),
            StorageIntegration::Google => format!("gs://{}", &bucket),
        };

        // the "storage_backend" will always point to the root ofg the object store.
        // TODO should we provide the store via object_Store builders?
        let store = DeltaTableBuilder::from_uri(store_uri)
            .with_allow_http(true)
            .build_storage()?
            .storage_backend();

        Ok(Self {
            integration,
            bucket,
            store,
        })
    }

    pub fn new_with_tables(
        integration: StorageIntegration,
        tables: impl IntoIterator<Item = TestTables>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static>> {
        let context = Self::new(integration)?;
        for table in tables {
            context.load_table(table)?;
        }
        Ok(context)
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
            StorageIntegration::Google => format!("gs://{}", &self.bucket),
        }
    }

    pub fn uri_for_table(&self, table: TestTables) -> String {
        format!("{}/{}", self.root_uri(), table.as_name())
    }

    pub fn load_table(&self, table: TestTables) -> TestResult {
        match self.integration {
            StorageIntegration::Amazon => {
                s3_cli::upload_table(table.as_path().as_str(), &self.uri_for_table(table))?;
            }
            StorageIntegration::Microsoft => {
                let uri = format!("{}/{}", self.bucket, table.as_name());
                az_cli::upload_table(&table.as_path(), &uri)?;
            }
            StorageIntegration::Google => todo!(),
        };
        Ok(())
    }
}

impl Drop for IntegrationContext {
    fn drop(&mut self) {
        match self.integration {
            StorageIntegration::Amazon => s3_cli::delete_bucket(&self.root_uri()).unwrap(),
            StorageIntegration::Microsoft => az_cli::delete_container(&self.bucket).unwrap(),
            _ => todo!(),
        };
    }
}

/// Kinds of storage integration
pub enum StorageIntegration {
    Amazon,
    Microsoft,
    Google,
}

impl StorageIntegration {
    fn prepare_env(&self) {
        match self {
            Self::Microsoft => az_cli::prepare_env(),
            Self::Amazon => s3_cli::prepare_env(),
            _ => todo!(),
        }
    }

    fn crate_bucket(&self, name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        match self {
            Self::Microsoft => az_cli::create_container(name),
            Self::Amazon => s3_cli::create_bucket(name),
            _ => todo!(),
        }
    }
}

/// Reference tables from the test data folder
pub enum TestTables {
    Simple,
    Golden,
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
            Self::Golden => data_path
                .join("golden/data-reader-array-primitives")
                .to_str()
                .unwrap()
                .to_owned(),
        }
    }

    pub fn as_name(&self) -> String {
        match self {
            Self::Simple => "simple".into(),
            Self::Golden => "golden".into(),
        }
    }
}

fn set_env_if_not_set(key: impl AsRef<str>, value: impl AsRef<str>) {
    match std::env::var(key.as_ref()) {
        Err(_) => std::env::set_var(key.as_ref(), value.as_ref()),
        Ok(_) => (),
    };
}

/// small wrapper around az cli
pub mod az_cli {
    use super::set_env_if_not_set;
    use crate::builder::azure_storage_options;
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

    /// prepare_env
    pub fn prepare_env() {
        set_env_if_not_set(azure_storage_options::AZURE_STORAGE_USE_EMULATOR, "1");
        set_env_if_not_set(
            azure_storage_options::AZURE_STORAGE_ACCOUNT_NAME,
            "devstoreaccount1",
        );
        set_env_if_not_set(azure_storage_options::AZURE_STORAGE_ACCOUNT_KEY, "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        set_env_if_not_set(
            "AZURE_STORAGE_CONNECTION_STRING",
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
        );
    }

    pub fn upload_table(src: &str, dst: &str) -> std::io::Result<ExitStatus> {
        let mut child = Command::new("az")
            .args(["storage", "blob", "upload-batch", "-d", dst, "-s", src])
            .spawn()
            .expect("az command is installed");
        child.wait()
    }
}

/// small wrapper around s3 cli
mod s3_cli {
    use super::set_env_if_not_set;
    use crate::builder::s3_storage_options;
    use std::process::{Command, ExitStatus};

    /// Create a new bucket
    pub fn create_bucket(bucket_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let endpoint = std::env::var(s3_storage_options::AWS_ENDPOINT_URL)
            .expect("variable ENDPOINT must be set to connect to S3");
        let mut child = Command::new("aws")
            .args([
                "s3api",
                "create-bucket",
                "--bucket",
                bucket_name.as_ref(),
                "--endpoint-url",
                &endpoint,
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

    /// prepare_env
    pub fn prepare_env() {
        set_env_if_not_set(
            s3_storage_options::AWS_ENDPOINT_URL,
            "http://localhost:4566",
        );
        set_env_if_not_set(s3_storage_options::AWS_ACCESS_KEY_ID, "test");
        set_env_if_not_set(s3_storage_options::AWS_SECRET_ACCESS_KEY, "test");
        set_env_if_not_set("AWS_DEFAULT_REGION", "us-east-1");
        set_env_if_not_set(s3_storage_options::AWS_REGION, "us-east-1");
        set_env_if_not_set(s3_storage_options::AWS_S3_LOCKING_PROVIDER, "dynamodb");
    }

    pub fn upload_table(src: &str, dst: &str) -> std::io::Result<ExitStatus> {
        let endpoint = std::env::var(s3_storage_options::AWS_ENDPOINT_URL)
            .expect("variable ENDPOINT must be set to connect to S3");
        let mut child = Command::new("aws")
            .args([
                "s3",
                "sync",
                src,
                dst,
                "--delete",
                "--endpoint-url",
                &endpoint,
            ])
            .spawn()
            .expect("aws command is installed");
        child.wait()
    }
}
