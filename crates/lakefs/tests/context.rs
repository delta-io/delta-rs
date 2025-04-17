#![cfg(feature = "integration_test_lakefs")]
use deltalake_lakefs::register_handlers;
use deltalake_test::utils::*;
use std::process::{Command, ExitStatus};

use which::which;

pub struct LakeFSIntegration {}

impl Default for LakeFSIntegration {
    fn default() -> Self {
        register_handlers(None);
        Self {}
    }
}

impl StorageIntegration for LakeFSIntegration {
    fn prepare_env(&self) {
        println!("Preparing env");

        set_env_if_not_set("endpoint", "http://127.0.0.1:8000");
        set_env_if_not_set("access_key_id", "LAKEFSID");
        set_env_if_not_set("secret_access_key", "LAKEFSKEY");
        set_env_if_not_set("allow_http", "true");

        set_env_if_not_set("LAKECTL_CREDENTIALS_ACCESS_KEY_ID", "LAKEFSID");
        set_env_if_not_set("LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY", "LAKEFSKEY");
        set_env_if_not_set("LAKECTL_SERVER_ENDPOINT_URL", "http://127.0.0.1:8000");
    }

    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        // Bucket is already created in docker-compose
        Ok(ExitStatus::default())
    }

    fn bucket_name(&self) -> String {
        "bronze".to_string()
    }

    fn root_uri(&self) -> String {
        // Default branch is always main
        format!("lakefs://{}/main", self.bucket_name())
    }

    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        println!(
            "Copy directory called with {source} {}/{destination}",
            self.root_uri()
        );
        let lakectl = which("lakectl").expect("Failed to find lakectl executable");

        // Upload files to branch
        Command::new(lakectl.clone())
            .args([
                "fs",
                "upload",
                "-r",
                "--source",
                &format!("{source}/"),
                &format!("{}/{destination}/", self.root_uri()),
            ])
            .status()?;

        // Commit changes
        Command::new(lakectl)
            .args([
                "commit",
                &format!("{}/", self.root_uri()),
                "--allow-empty-message",
            ])
            .status()
    }
}
