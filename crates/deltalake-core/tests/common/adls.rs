use super::TestContext;
use chrono::Utc;
use rand::Rng;
use std::collections::HashMap;

pub struct AzureGen2 {
    #[allow(dead_code)]
    account_name: String,
    #[allow(dead_code)]
    account_key: String,
    file_system_name: String,
}

impl Drop for AzureGen2 {
    fn drop(&mut self) {
        let file_system_name = self.file_system_name.clone();
        az_cli::delete_container(file_system_name).unwrap();
    }
}

pub async fn setup_azure_gen2_context() -> TestContext {
    let mut config = HashMap::new();

    let storage_account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
    let storage_account_key = std::env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();
    let storage_container_name =
        std::env::var("AZURE_STORAGE_CONTAINER_NAME").unwrap_or_else(|_| "deltars".to_string());

    let rand: u16 = rand::thread_rng().gen();
    let file_system_name = format!("delta-rs-test-{}-{}", Utc::now().timestamp(), rand);

    az_cli::create_container(&file_system_name).unwrap();

    let table_uri = format!("azure://{file_system_name}/");

    config.insert("URI".to_string(), table_uri);
    config.insert(
        "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
        storage_account_name.clone(),
    );
    config.insert(
        "AZURE_STORAGE_ACCOUNT_KEY".to_string(),
        storage_account_key.clone(),
    );
    config.insert(
        "AZURE_STORAGE_CONTAINER_NAME".to_string(),
        storage_container_name,
    );

    TestContext {
        storage_context: Some(Box::new(AzureGen2 {
            account_name: storage_account_name,
            account_key: storage_account_key,
            file_system_name,
        })),
        config,
        ..TestContext::default()
    }
}

pub mod az_cli {
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
}
