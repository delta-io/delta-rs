#![allow(dead_code, missing_docs)]
use deltalake_core::logstore::ObjectStoreRef;
use deltalake_core::{DeltaResult, DeltaTableBuilder, DeltaTableError};
use fs_extra::dir::{copy, CopyOptions};
use std::collections::HashMap;
use std::process::ExitStatus;
use tempfile::{tempdir, TempDir};

pub use deltalake_core::test_utils::TestTables;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + 'static>>;

pub trait StorageIntegration {
    fn create_bucket(&self) -> std::io::Result<ExitStatus>;
    fn prepare_env(&self);
    fn bucket_name(&self) -> String;
    fn root_uri(&self) -> String;
    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus>;

    fn object_store(&self) -> DeltaResult<ObjectStoreRef> {
        let table_url = url::Url::parse(&self.root_uri())
            .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;
        Ok(DeltaTableBuilder::from_url(table_url)?
            .with_allow_http(true)
            .build_storage()?
            .object_store(None))
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
        integration
            .create_bucket()
            .expect("Failed to create the bucket!");
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
        let table_url = url::Url::parse(&table_uri).unwrap();
        DeltaTableBuilder::from_url(table_url)
            .unwrap()
            .with_allow_http(true)
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
            .copy_directory(table.as_path().to_str().unwrap(), name.as_ref())?;
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

/// Set environment variable if it is not set
pub fn set_env_if_not_set(key: impl AsRef<str>, value: impl AsRef<str>) {
    if std::env::var(key.as_ref()).is_err() {
        std::env::set_var(key.as_ref(), value.as_ref())
    };
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

#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let mut expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let formatted = arrow::util::pretty::pretty_format_batches($CHUNKS)
            .unwrap()
            .to_string();
        // fix for windows: \r\n -->

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{expected_lines:#?}\nactual:\n\n{actual_lines:#?}\n\n",
        );
    };
}

pub use assert_batches_sorted_eq;
