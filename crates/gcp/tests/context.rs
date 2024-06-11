use chrono::Utc;
use deltalake_core::errors::DeltaTableError;
use deltalake_core::logstore::LogStore;
use deltalake_core::table::builder::DeltaTableBuilder;
use deltalake_gcp::register_handlers;
use deltalake_test::utils::*;
use futures::StreamExt;
use std::collections::HashMap;
use std::process::ExitStatus;
use std::sync::Arc;
use tempfile::TempDir;

/// Kinds of storage integration
#[derive(Debug)]
pub struct GcpIntegration {
    bucket_name: String,
    temp_dir: TempDir,
}

impl Default for GcpIntegration {
    fn default() -> Self {
        register_handlers(None);
        Self {
            bucket_name: format!("test-delta-table-{}", Utc::now().timestamp()),
            temp_dir: TempDir::new().unwrap(),
        }
    }
}

/// Synchronize the contents of two object stores
pub async fn sync_stores(
    from_store: Arc<dyn LogStore>,
    to_store: Arc<dyn LogStore>,
) -> Result<(), DeltaTableError> {
    let from_store = from_store.object_store().clone();
    let to_store = to_store.object_store().clone();
    // TODO if a table is copied within the same root store (i.e bucket), using copy would be MUCH more efficient
    let mut meta_stream = from_store.list(None);
    while let Some(file) = meta_stream.next().await {
        if let Ok(meta) = file {
            let bytes = from_store.get(&meta.location).await?.bytes().await?;
            to_store.put(&meta.location, bytes.into()).await?;
        }
    }
    Ok(())
}

pub async fn copy_table(
    from: impl AsRef<str>,
    from_options: Option<HashMap<String, String>>,
    to: impl AsRef<str>,
    to_options: Option<HashMap<String, String>>,
    allow_http: bool,
) -> Result<(), DeltaTableError> {
    let from_store = DeltaTableBuilder::from_uri(from)
        .with_storage_options(from_options.unwrap_or_default())
        .with_allow_http(allow_http)
        .build_storage()?;
    let to_store = DeltaTableBuilder::from_uri(to)
        .with_storage_options(to_options.unwrap_or_default())
        .with_allow_http(allow_http)
        .build_storage()?;
    sync_stores(from_store, to_store).await
}

impl StorageIntegration for GcpIntegration {
    fn prepare_env(&self) {
        gs_cli::prepare_env();
        let base_url = std::env::var("GOOGLE_BASE_URL").unwrap();
        let token = serde_json::json!({"gcs_base_url": base_url, "disable_oauth": true, "client_email": "", "private_key": "", "private_key_id": ""});
        let account_path = self.temp_dir.path().join("gcs.json");
        println!("accoutn_path: {account_path:?}");
        std::fs::write(&account_path, serde_json::to_vec(&token).unwrap()).unwrap();
        std::env::set_var(
            "GOOGLE_SERVICE_ACCOUNT",
            account_path.as_path().to_str().unwrap(),
        );
    }

    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        gs_cli::create_bucket(self.bucket_name())
    }

    fn bucket_name(&self) -> String {
        self.bucket_name.clone()
    }

    fn root_uri(&self) -> String {
        format!("gs://{}", self.bucket_name())
    }

    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        use futures::executor::block_on;

        let to = format!("{}/{}", self.root_uri(), destination);
        let _ = block_on(copy_table(source.to_owned(), None, to, None, true));
        Ok(ExitStatus::default())
    }
}

impl GcpIntegration {
    fn delete_bucket(&self) -> std::io::Result<ExitStatus> {
        gs_cli::delete_bucket(self.bucket_name.clone())
    }
}

/// small wrapper around google api
pub mod gs_cli {
    use super::set_env_if_not_set;
    use std::process::{Command, ExitStatus};

    pub fn create_bucket(container_name: impl AsRef<str>) -> std::io::Result<ExitStatus> {
        let endpoint = std::env::var("GOOGLE_ENDPOINT_URL")
            .expect("variable GOOGLE_ENDPOINT_URL must be set to connect to GCS Emulator");
        let payload = serde_json::json!({ "name": container_name.as_ref() });
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
        let payload = serde_json::json!({ "name": container_name.as_ref() });
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
