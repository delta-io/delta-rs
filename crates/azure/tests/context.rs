use azure_storage_blobs::prelude::*;
use chrono::Utc;
use deltalake_azure::register_handlers;
use deltalake_test::utils::*;
use rand::RngCore;
use std::path::PathBuf;
use std::process::ExitStatus;
use tokio::runtime::Handle;

/// Kinds of storage integration
#[derive(Clone, Debug)]
pub enum MsftIntegration {
    Azure(String),
    Onelake,
    OnelakeAbfs,
}

impl Default for MsftIntegration {
    fn default() -> Self {
        register_handlers(None);
        let task_id = format!("{}", rand::thread_rng().next_u64());
        Self::Azure(format!("delta-table-{}-{task_id}", Utc::now().timestamp()))
    }
}

impl StorageIntegration for MsftIntegration {
    fn prepare_env(&self) {
        set_env_if_not_set("AZURE_STORAGE_USE_EMULATOR", "1");
        set_env_if_not_set("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1");
        set_env_if_not_set(
            "AZURE_STORAGE_ACCOUNT_KEY",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        );
        set_env_if_not_set(
            "AZURE_STORAGE_CONNECTION_STRING",
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;",
        );
    }

    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        let name = self.bucket_name();
        let container_client = ClientBuilder::emulator().container_client(name.clone());

        let handle = Handle::current();
        tokio::task::block_in_place(move || {
            handle.block_on(async {
                if !container_client.exists().await.unwrap() {
                    container_client
                        .create()
                        .await
                        .expect("Failed to make the container");
                } else {
                    println!("Container {name} already exists at start of test!");
                }
            })
        });
        Ok(ExitStatus::default())
    }

    fn bucket_name(&self) -> String {
        match self {
            Self::Azure(name) => name.clone(),
            Self::Onelake => {
                let account_name =
                    std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap_or(String::from("onelake"));
                let container_name = std::env::var("AZURE_STORAGE_CONTAINER_NAME")
                    .unwrap_or(String::from("delta-rs"));
                format!("{account_name}.dfs.fabric.microsoft.com/{container_name}")
            }
            Self::OnelakeAbfs => {
                let account_name =
                    std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap_or(String::from("onelake"));
                let container_name = std::env::var("AZURE_STORAGE_CONTAINER_NAME")
                    .unwrap_or(String::from("delta-rs"));
                format!("{container_name}@{account_name}.dfs.fabric.microsoft.com")
            }
        }
    }

    fn root_uri(&self) -> String {
        format!("abfs://{}", self.bucket_name())
    }

    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        let name = self.bucket_name();
        println!("copy from {source} to {name}/{destination}");
        let container_client = ClientBuilder::emulator().container_client(name.clone());

        async fn upload_folder(
            source: PathBuf,
            to: &str,
            client: &ContainerClient,
        ) -> std::io::Result<()> {
            for entry in std::fs::read_dir(source).expect("Failed to read dir") {
                let entry = entry?;
                let file_type = entry.file_type()?;
                let destination = format!(
                    "{to}/{}",
                    entry
                        .file_name()
                        .into_string()
                        .expect("Failed to turn file_name into a string")
                );

                if file_type.is_dir() {
                    println!("uploading folder {destination}");
                    Box::pin(upload_folder(entry.path(), &destination, client))
                        .await
                        .expect("Failed to upload directory");
                } else if file_type.is_file() {
                    let blob_client = client.blob_client(&destination);
                    println!("putting entry: {:?} into {destination}", entry.file_name());
                    let body = std::fs::read(entry.path()).expect("Failed to read file");
                    let r = blob_client
                        .put_block_blob(body)
                        .await
                        .expect("Failed to upload file");
                    println!("upload result: {r:?}");
                }
            }
            Ok(())
        }

        let handle = Handle::current();
        tokio::task::block_in_place(move || {
            handle.block_on(async {
                upload_folder(source.into(), destination, &container_client)
                    .await
                    .expect("Failed to upload root");
            })
        });
        Ok(ExitStatus::default())
    }
}

impl Drop for MsftIntegration {
    fn drop(&mut self) {
        let name = self.bucket_name();
        let container_client = ClientBuilder::emulator().container_client(name.clone());

        let handle = Handle::current();
        tokio::task::block_in_place(move || {
            handle.block_on(async {
                if container_client.exists().await.unwrap() {
                    container_client
                        .delete()
                        .await
                        .expect("Failed to delete the container");
                } else {
                    println!("Container named {name} doesn't exist, so nothing to delete");
                }
            })
        });
    }
}
