use chrono::Utc;
use std::collections::HashMap;

use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_datalake::clients::DataLakeClient;
use rand::Rng;

use super::Context;

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

        let thread_handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let data_lake_client = DataLakeClient::new(
                StorageSharedKeyCredential::new(
                    storage_account_name.to_owned(),
                    storage_account_key.to_owned(),
                ),
                None,
            );
            let file_system_client =
                data_lake_client.into_file_system_client(file_system_name.to_owned());
            runtime
                .block_on(file_system_client.delete().into_future())
                .unwrap();
        });

        thread_handle.join().unwrap();
    }
}

pub async fn setup_azure_gen2_context() -> Context {
    let mut config = HashMap::new();

    let storage_account_name = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
    let storage_account_key = std::env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();

    let data_lake_client = DataLakeClient::new(
        StorageSharedKeyCredential::new(
            storage_account_name.to_owned(),
            storage_account_key.to_owned(),
        ),
        None,
    );
    let rand: u16 = rand::thread_rng().gen();
    let file_system_name = format!("delta-rs-test-{}-{}", Utc::now().timestamp(), rand);

    let file_system_client = data_lake_client.into_file_system_client(file_system_name.to_owned());
    file_system_client.create().into_future().await.unwrap();

    let table_uri = format!("adls2://{}/{}/", storage_account_name, file_system_name);

    config.insert("URI".to_string(), table_uri);
    config.insert(
        "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
        storage_account_name.clone(),
    );
    config.insert(
        "AZRUE_STORAGE_ACCOUNT_KEY".to_string(),
        storage_account_key.clone(),
    );

    Context {
        storage_context: Some(Box::new(AzureGen2 {
            account_name: storage_account_name,
            account_key: storage_account_key,
            file_system_name,
        })),
        config,
        ..Context::default()
    }
}
