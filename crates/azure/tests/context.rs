use chrono::Utc;
use deltalake_azure::register_handlers;
use deltalake_test::utils::*;
use std::process::ExitStatus;

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
        Self::Azure(format!("test-delta-table-{}", Utc::now().timestamp()))
    }
}

impl StorageIntegration for MsftIntegration {
    fn prepare_env(&self) {
        match self {
            Self::Azure(_) => az_cli::prepare_env(),
            Self::Onelake => onelake_cli::prepare_env(),
            Self::OnelakeAbfs => onelake_cli::prepare_env(),
        }
    }

    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        match self {
            Self::Azure(_) => az_cli::create_container(self.bucket_name()),
            Self::Onelake => Ok(ExitStatus::default()),
            Self::OnelakeAbfs => Ok(ExitStatus::default()),
        }
    }

    fn bucket_name(&self) -> String {
        match self {
            Self::Azure(name) => name.clone(),
            Self::Onelake => {
                let account_name =
                    std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap_or(String::from("onelake"));
                let container_name = std::env::var("AZURE_STORAGE_CONTAINER_NAME")
                    .unwrap_or(String::from("delta-rs"));
                format!(
                    "{0}.dfs.fabric.microsoft.com/{1}",
                    account_name, container_name
                )
            }
            Self::OnelakeAbfs => {
                let account_name =
                    std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap_or(String::from("onelake"));
                let container_name = std::env::var("AZURE_STORAGE_CONTAINER_NAME")
                    .unwrap_or(String::from("delta-rs"));
                format!(
                    "{0}@{1}.dfs.fabric.microsoft.com",
                    container_name, account_name
                )
            }
        }
    }

    fn root_uri(&self) -> String {
        format!("az://{}", self.bucket_name())
    }

    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        let destination = format!("{}/{}", self.bucket_name(), destination);
        az_cli::copy_directory(source, destination)
    }
}

impl Drop for MsftIntegration {
    fn drop(&mut self) {
        az_cli::delete_container(self.bucket_name()).expect("Failed to drop bucket");
    }
}

//cli for onelake
mod onelake_cli {
    use super::set_env_if_not_set;
    /// prepare_env
    pub fn prepare_env() {
        let token = "jwt-token";
        set_env_if_not_set("AZURE_STORAGE_USE_EMULATOR", "0");
        set_env_if_not_set("AZURE_STORAGE_ACCOUNT_NAME", "daily-onelake");
        set_env_if_not_set(
            "AZURE_STORAGE_CONTAINER_NAME",
            "86bc63cf-5086-42e0-b16d-6bc580d1dc87",
        );
        set_env_if_not_set("AZURE_STORAGE_TOKEN", token);
    }
}

/// small wrapper around az cli
mod az_cli {
    use super::set_env_if_not_set;
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

    /// copy directory
    pub fn copy_directory(
        source: impl AsRef<str>,
        destination: impl AsRef<str>,
    ) -> std::io::Result<ExitStatus> {
        let mut child = Command::new("az")
            .args([
                "storage",
                "blob",
                "upload-batch",
                "-s",
                source.as_ref(),
                "-d",
                destination.as_ref(),
            ])
            .spawn()
            .expect("az command is installed");
        child.wait()
    }

    /// prepare_env
    pub fn prepare_env() {
        set_env_if_not_set("AZURE_STORAGE_USE_EMULATOR", "1");
        set_env_if_not_set("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1");
        set_env_if_not_set("AZURE_STORAGE_ACCOUNT_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        set_env_if_not_set(
            "AZURE_STORAGE_CONNECTION_STRING",
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
        );
    }
}
