use super::az_cli;
use super::TestContext;
use chrono::Utc;
use rand::Rng;
use std::collections::HashMap;
use std::process::Command;

pub struct AzureGen2 {
    account_name: String,
    account_key: String,
    file_system_name: String,
}

impl Drop for AzureGen2 {
    fn drop(&mut self) {
        let storage_account_name = self.account_name.clone();
        let storage_account_key = self.account_key.clone();
        let file_system_name = self.file_system_name.clone();
        az_cli::delete_container(file_system_name);
    }
}

pub async fn setup_azure_gen2_context() -> TestContext {
    let mut config = HashMap::new();

    let storage_account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
    let storage_account_key = std::env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();
    let storage_container_name =
        std::env::var("AZURE_STORAGE_CONTAINER_NAME").unwrap_or("deltars".to_string());

    let rand: u16 = rand::thread_rng().gen();
    let table_folder = format!("delta-rs-test-{}-{}", Utc::now().timestamp(), rand);

    az_cli::create_container(file_system_name);

    let table_uri = format!("azure://{}/", table_folder);

    config.insert("URI".to_string(), table_uri);
    config.insert(
        "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
        storage_account_name.clone(),
    );
    config.insert(
        "AZRUE_STORAGE_ACCOUNT_KEY".to_string(),
        storage_account_key.clone(),
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
