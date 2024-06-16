#![cfg(feature = "integration_test")]
use deltalake_hdfs::register_handlers;
use deltalake_test::utils::*;
use hdfs_native::{minidfs::MiniDfs, Client};
use std::{
    collections::HashSet,
    process::{Command, ExitStatus},
};

use which::which;

/// Kinds of storage integration
#[allow(dead_code)]
pub struct HdfsIntegration {
    minidfs: MiniDfs,
    client: Client,
}

impl Default for HdfsIntegration {
    fn default() -> Self {
        register_handlers(None);
        let minidfs = MiniDfs::with_features(&HashSet::new());
        let client = Client::new(&minidfs.url).unwrap();
        Self { minidfs, client }
    }
}

impl StorageIntegration for HdfsIntegration {
    fn prepare_env(&self) {
        println!("Preparing env");
    }

    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        let hadoop_exc = which("hadoop").expect("Failed to find hadoop executable");

        Ok(Command::new(hadoop_exc)
            .args(["fs", "-mkdir", &self.root_uri()])
            .status()
            .unwrap())
    }

    fn bucket_name(&self) -> String {
        "/test-deltalake".to_string()
    }

    fn root_uri(&self) -> String {
        format!("{}{}", self.minidfs.url, self.bucket_name())
    }

    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        println!("Copy directory called with {} {}", source, destination);
        let hadoop_exc = which("hadoop").expect("Failed to find hadoop executable");
        Ok(Command::new(hadoop_exc)
            .args([
                "fs",
                "-copyFromLocal",
                "-p",
                source,
                &format!("{}/{}", self.root_uri(), destination),
            ])
            .status()
            .unwrap())
    }
}
