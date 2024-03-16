use deltalake_mount::register_handlers;
use deltalake_test::utils::{set_env_if_not_set, StorageIntegration};
use fs_extra::dir::{copy, CopyOptions};
use std::process::ExitStatus;
use tempfile::{tempdir, TempDir};

pub struct MountIntegration {
    tmp_dir: TempDir,
}

impl Default for MountIntegration {
    fn default() -> Self {
        register_handlers(None);
        Self {
            tmp_dir: tempdir().expect("Failed to make temp dir"),
        }
    }
}

impl StorageIntegration for MountIntegration {
    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        Ok(ExitStatus::default())
    }

    fn prepare_env(&self) {
        set_env_if_not_set("MOUNT_ALLOW_UNSAFE_RENAME", "true");
    }
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

pub struct DbfsIntegration {
    tmp_dir: TempDir,
}

impl Default for DbfsIntegration {
    fn default() -> Self {
        register_handlers(None);
        Self {
            tmp_dir: tempdir().expect("Failed to make temp dir"),
        }
    }
}

impl StorageIntegration for DbfsIntegration {
    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        Ok(ExitStatus::default())
    }

    fn prepare_env(&self) {
        set_env_if_not_set("MOUNT_ALLOW_UNSAFE_RENAME", "true");
        std::fs::create_dir_all(format!("/dbfs{}", self.tmp_dir.as_ref().to_str().unwrap()))
            .expect("Failed to create dir");
    }
    fn bucket_name(&self) -> String {
        self.tmp_dir.as_ref().to_str().unwrap().to_owned()
    }
    fn root_uri(&self) -> String {
        format!("dbfs:{}", self.bucket_name())
    }
    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        let mut options = CopyOptions::new();
        options.content_only = true;
        let dest_path = format!(
            "/dbfs{}/{}",
            self.tmp_dir.as_ref().to_str().unwrap(),
            destination
        );
        std::fs::create_dir_all(&dest_path)?;
        copy(source, &dest_path, &options).expect("Failed to copy");
        Ok(ExitStatus::default())
    }
}
