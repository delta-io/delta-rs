#![allow(dead_code, missing_docs)]
use deltalake_core::storage::ObjectStoreRef;
use deltalake_core::{DeltaResult, DeltaTableBuilder};
use fs_extra::dir::{copy, CopyOptions};
use std::collections::HashMap;
use std::env;
use std::process::ExitStatus;
use tempfile::{tempdir, TempDir};

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + 'static>>;

pub trait StorageIntegration {
    fn create_bucket(&self) -> std::io::Result<ExitStatus>;
    fn prepare_env(&self);
    fn bucket_name(&self) -> String;
    fn root_uri(&self) -> String;
    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus>;

    fn object_store(&self) -> DeltaResult<ObjectStoreRef> {
        Ok(DeltaTableBuilder::from_uri(self.root_uri())
            .with_allow_http(true)
            .build_storage()?
            .object_store())
    }
}

pub struct LocalStorageIntegration {
    tmp_dir: TempDir,
}

impl Default for LocalStorageIntegration {
    fn default() -> Self {
        Self {
            tmp_dir: tempdir().expect("Failed to make temp dir"),
        }
    }
}

impl StorageIntegration for LocalStorageIntegration {
    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        Ok(ExitStatus::default())
    }

    fn prepare_env(&self) {}
    fn bucket_name(&self) -> String {
        self.tmp_dir.as_ref().to_str().unwrap().to_owned()
    }
    fn root_uri(&self) -> String {
        format!("file://{}", self.bucket_name())
    }
    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        let mut options = CopyOptions::new();
        options.content_only = true;
        let dest_path = self.tmp_dir.path().join(destination);
        std::fs::create_dir_all(&dest_path)?;
        copy(source, &dest_path, &options).expect("Failed to copy");
        Ok(ExitStatus::default())
    }
}

/// The IntegrationContext provides temporary resources to test against cloud storage services.
pub struct IntegrationContext {
    pub integration: Box<dyn StorageIntegration>,
    bucket: String,
    store: ObjectStoreRef,
    tmp_dir: TempDir,
    /// environment variables valid before `prepare_env()` modified them
    env_vars: HashMap<String, String>,
}

impl IntegrationContext {
    pub fn new(
        integration: Box<dyn StorageIntegration>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static>> {
        // environment variables are loaded from .env files if found. Otherwise
        // default values based on the default setting of the respective emulators are set.
        #[cfg(test)]
        dotenvy::dotenv().ok();

        // save existing environment variables
        let env_vars = std::env::vars().collect();

        integration.prepare_env();

        let tmp_dir = tempdir()?;
        // create a fresh bucket in every context. THis is done via CLI...
        integration.create_bucket()?;
        let store = integration.object_store()?;
        let bucket = integration.bucket_name();

        Ok(Self {
            integration,
            bucket,
            store,
            tmp_dir,
            env_vars,
        })
    }

    /// Get a a reference to the root object store
    pub fn object_store(&self) -> ObjectStoreRef {
        self.store.clone()
    }

    /// Get the URI for initializing a store at the root
    pub fn root_uri(&self) -> String {
        self.integration.root_uri()
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
        self.integration
            .copy_directory(&table.as_path(), name.as_ref())?;
        Ok(())
    }

    fn restore_env(&self) {
        let env_vars: HashMap<_, _> = std::env::vars().collect();
        for (key, _) in env_vars {
            if !self.env_vars.contains_key(&key) {
                std::env::remove_var(key)
            }
        }
        for (key, value) in self.env_vars.iter() {
            std::env::set_var(key, value);
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
