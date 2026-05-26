#![cfg(feature = "integration_test")]
use deltalake_hdfs::register_handlers;
use deltalake_test::utils::*;
use hdfs_native_object_store::minidfs::MiniDfs;
use std::{
    collections::HashSet,
    env, fs,
    path::PathBuf,
    process::{Command, ExitStatus},
};

use which::which;

pub struct HdfsIntegration {
    minidfs: MiniDfs,
}

impl Default for HdfsIntegration {
    fn default() -> Self {
        register_handlers(None);
        ensure_non_security_kdestroy_shim();
        let minidfs = MiniDfs::with_features(&HashSet::new());
        Self { minidfs }
    }
}

fn ensure_non_security_kdestroy_shim() {
    if which("kdestroy").is_ok() {
        return;
    }

    // hdfs-native 0.13.x unconditionally shells out to `kdestroy` when MiniDfs
    // is dropped, even for the non-security configuration used by this test.
    let shim_dir = env::current_dir()
        .expect("failed to resolve current directory for HDFS test shims")
        .join(PathBuf::from("target/test-shims"));
    let shim_path = shim_dir.join("kdestroy");
    fs::create_dir_all(&shim_dir).expect("failed to create HDFS test shim directory");

    if !shim_path.exists() {
        fs::write(&shim_path, "#!/bin/sh\nexit 0\n").expect("failed to write kdestroy shim");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = fs::metadata(&shim_path)
                .expect("failed to read kdestroy shim metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&shim_path, permissions)
                .expect("failed to mark kdestroy shim executable");
        }
    }

    let existing_path = env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![shim_dir];
    paths.extend(env::split_paths(&existing_path));
    let joined_paths = env::join_paths(paths).expect("failed to construct PATH for HDFS tests");
    unsafe {
        env::set_var("PATH", joined_paths);
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
        println!("Copy directory called with {source} {destination}");
        let hadoop_exc = which("hadoop").expect("Failed to find hadoop executable");
        Ok(Command::new(hadoop_exc)
            .args([
                "fs",
                "-copyFromLocal",
                "-p",
                source,
                &format!("{}/{destination}", self.root_uri()),
            ])
            .status()
            .unwrap())
    }
}
