// Verifies that tables with fully-qualified file paths behave as expected
// and that reading them yields the same data as the corresponding table
// with relative file paths.
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use url::Url;

// Small utility to build original/cloned URIs from a base URI and two prefixes
fn build_prefixed_uris(
    base_uri: &str,
    original_prefix: &str,
    cloned_prefix: &str,
) -> (String, String) {
    let base = base_uri.trim_end_matches('/');
    let original = format!("{}/{}", base, original_prefix);
    let cloned = format!("{}/{}", base, cloned_prefix);
    (original, cloned)
}

// A generic RAII cleanup helper that runs a command on drop. Errors are ignored.
struct CommandCleanup {
    program: &'static str,
    args: Vec<String>,
}

impl CommandCleanup {
    fn new(program: &'static str, args: &[&str]) -> Self {
        Self {
            program,
            args: args.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Drop for CommandCleanup {
    fn drop(&mut self) {
        use std::process::Command;
        let _ = Command::new(self.program).args(&self.args).status();
    }
}

// Test to use absolute file paths
#[tokio::test]
async fn compare_table_with_full_paths() {
    // std::env::set_var("DELTA_RS_ALLOW_UNRESTRICTED_FILE_ACCESS", "1");
    let expected_rel = Path::new("../test/tests/data/delta-0.8.0");
    let expected_abs = fs::canonicalize(expected_rel).unwrap();

    // Use a fresh temporary directory as the target table location and clone the
    // expected table there while rewriting data file paths to absolute paths.
    let tmpdir = tempfile::tempdir().unwrap();
    let cloned_dir: PathBuf = tmpdir.path().to_path_buf();
    clone_test_dir_with_abs_paths(&expected_abs, &cloned_dir);

    // Compare the cloned table against expected URIs and data from the original table.
    assert_table_uris_and_data(&cloned_dir, &tmpdir.path(), &expected_abs).await;
}

// Test to use absolute file paths pointing to an original source table directory
// Similar to what a shallow clone with full paths would produce.
#[tokio::test]
async fn compare_table_with_full_paths_to_original_table() {
    // std::env::set_var("DELTA_RS_ALLOW_UNRESTRICTED_FILE_ACCESS", "1");

    // Path to the original test table (with relative paths in _delta_log)
    let expected_rel = Path::new("../test/tests/data/delta-0.8.0");
    let expected_abs = fs::canonicalize(expected_rel).unwrap();

    // Create a fresh temp directory and clone the table there, but rewrite
    // all add/remove paths in the log to point to ABSOLUTE paths under the
    // ORIGINAL table directory (expected_abs), not the cloned directory.
    let tmpdir = tempfile::tempdir().unwrap();
    let cloned_dir: PathBuf = tmpdir.path().to_path_buf();
    clone_test_dir_with_abs_paths_from_src(&expected_abs, &cloned_dir);

    // Open the cloned table and compare URIs to original table dir and data to original.
    assert_table_uris_and_data(&cloned_dir, &expected_abs, &expected_abs).await;
}

// Test to use absolute file paths of the form file://<path> in _delta_log entries.
#[tokio::test]
async fn compare_table_with_uri_paths() {
    // std::env::set_var("DELTA_RS_ALLOW_UNRESTRICTED_FILE_ACCESS", "1");

    // Path to the original test table (with relative paths in _delta_log)
    let expected_rel = Path::new("../test/tests/data/delta-0.8.0");
    let expected_abs = fs::canonicalize(expected_rel).unwrap();

    // Use a fresh temporary directory as the target table location and clone the
    // expected table there while rewriting data file paths to file:// URIs.
    let tmpdir = tempfile::tempdir().unwrap();
    let cloned_dir: PathBuf = tmpdir.path().to_path_buf();

    // Re-create target by copying contents of expected table
    use fs_extra::dir::{copy as copy_dir, CopyOptions};
    let mut opts = CopyOptions::new();
    opts.overwrite = true;
    opts.copy_inside = true;
    opts.content_only = true; // copy contents of source into dest root
    copy_dir(&expected_abs, &cloned_dir, &opts).unwrap();

    // Rewrite _delta_log entries so that add/remove.path values are file:// URIs
    let log_dir = cloned_dir.join("_delta_log");
    rewrite_log_paths(&log_dir, &cloned_dir, true);

    // Compare the cloned table against expected file:// URIs and data from the original table.
    let table_uri = Url::from_directory_path(&cloned_dir).unwrap().to_string();
    let expected_prefix_uri = table_uri.clone();
    assert_table_uris_and_data_cloud(&table_uri, &expected_prefix_uri, &expected_abs, None).await;
}

// Test using S3 bucket paths, s3://
// To run: start Localstack via `docker compose up -d` in repo root.
// S3 Test requires the AWS CLI (`aws` command) to be installed and available in PATH.
#[cfg(feature = "cloud")]
#[tokio::test]
async fn compare_table_with_full_paths_to_original_table_s3() {
    use std::process::Command;

    // Register S3 handlers for deltalake-core tests. In normal usage the `deltalake`
    // meta-crate auto-registers these via a ctor, but core tests run without it.
    // This ensures that `s3://` URLs are recognized instead of yielding
    // InvalidTableLocation errors.
    deltalake_aws::register_handlers(None);

    // Ensure environment is prepared for localstack
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    std::env::set_var("AWS_ACCESS_KEY_ID", "deltalake");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "weloverust");
    std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
    std::env::set_var("AWS_ALLOW_HTTP", "true");

    // Path to the original local test table (with relative paths in _delta_log)
    let expected_rel = Path::new("../test/tests/data/delta-0.8.0");
    let expected_abs = fs::canonicalize(expected_rel).unwrap();

    // Create unique bucket and prefixes (avoid extra deps; use time-based suffix)
    let suffix = unique_suffix();
    let bucket = format!("test-delta-core-{}", suffix);
    let original_prefix = "original";
    let cloned_prefix = "cloned";
    let bucket_uri = format!("s3://{}", bucket);
    let (original_uri, cloned_uri) =
        build_prefixed_uris(&bucket_uri, original_prefix, cloned_prefix);

    // Helper to run a command and assert success
    let run = |program: &str, args: &[&str]| run_cmd(program, args);

    // Create bucket
    run("aws", &["s3", "mb", &bucket_uri]);

    // Ensure cleanup at the end
    let _cleanup = CommandCleanup::new("aws", &["s3", "rb", &bucket_uri, "--force"]);

    // 1) Copy the original local table directory to s3://bucket/original
    run(
        "aws",
        &[
            "s3",
            "cp",
            expected_abs.to_str().unwrap(),
            &original_uri,
            "--recursive",
        ],
    );

    // 2) Clone original -> cloned in S3
    run(
        "aws",
        &["s3", "cp", &original_uri, &cloned_uri, "--recursive"],
    );

    // 3) Download cloned _delta_log locally, rewrite paths to absolute paths under expected_abs, upload back
    let tmpdir = tempfile::tempdir().unwrap();
    let local_log_dir = tmpdir.path().join("_delta_log");
    fs::create_dir_all(&local_log_dir).unwrap();

    run(
        "aws",
        &[
            "s3",
            "cp",
            &format!("{}/_delta_log", &cloned_uri),
            local_log_dir.to_str().unwrap(),
            "--recursive",
        ],
    );

    // Rewrite JSON actions inside downloaded log files. For S3 table location,
    // rewrite to fully-qualified s3:// URIs pointing at the original S3 bucket.
    rewrite_log_paths_with_prefix(&local_log_dir, &original_uri);

    // Upload rewritten log back to S3 cloned table
    run(
        "aws",
        &[
            "s3",
            "cp",
            local_log_dir.to_str().unwrap(),
            &format!("{}/_delta_log", &cloned_uri),
            "--recursive",
        ],
    );

    // 4) Open the cloned S3 table and verify URIs and data against the local original table
    assert_table_uris_and_data_cloud(&cloned_uri, &original_uri, &expected_abs, None).await;
}

// Test using Azure Blob Storage paths, abfs://
// Azure Test requires azure CLI (`az`) to be installed.
// The azurite emulator is in docker: start `docker compose up -d` in repo root.
#[cfg(feature = "cloud")]
#[tokio::test]
async fn compare_table_with_full_paths_to_original_table_azure() {
    use std::process::Command;

    // Register Azure handlers for deltalake-core tests. In normal usage the `deltalake`
    // meta-crate auto-registers these via a ctor, but core tests run without it.
    // This ensures that `abfs://` URLs are recognized instead of yielding
    // InvalidTableLocation errors.
    deltalake_azure::register_handlers(None);

    // Ensure environment is prepared for Azurite emulator
    std::env::set_var("AZURE_STORAGE_USE_EMULATOR", "1");
    std::env::set_var("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1");
    std::env::set_var(
        "AZURE_STORAGE_ACCOUNT_KEY",
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    );
    std::env::set_var(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
    );

    // Path to the original local test table (with relative paths in _delta_log)
    let expected_rel = Path::new("../test/tests/data/delta-0.8.0");
    let expected_abs = fs::canonicalize(expected_rel).unwrap();

    // Create unique container and prefixes (avoid extra deps; use time-based suffix)
    let suffix = unique_suffix();
    let container = format!("test-delta-core-{}", suffix);
    let original_prefix = "original";
    let cloned_prefix = "cloned";
    let container_uri = format!("abfs://{}", container);
    let (original_uri, cloned_uri) =
        build_prefixed_uris(&container_uri, original_prefix, cloned_prefix);

    // Helper to run a command and assert success
    let run = |program: &str, args: &[&str]| run_cmd(program, args);

    // Create container
    run("az", &["storage", "container", "create", "-n", &container]);

    // Ensure cleanup at the end
    let _cleanup = CommandCleanup::new("az", &["storage", "container", "delete", "-n", &container]);

    // 1) Copy the original local table directory to abfs://container/original
    run(
        "az",
        &[
            "storage",
            "blob",
            "upload-batch",
            "-s",
            expected_abs.to_str().unwrap(),
            "-d",
            &format!("{}/{}", container, original_prefix),
        ],
    );

    // 2) Clone original -> cloned in Azure
    // Azure doesn't have a direct copy like S3, so we download and re-upload
    let tmpdir_clone = tempfile::tempdir().unwrap();
    let local_clone_dir = tmpdir_clone.path().join("clone");
    fs::create_dir_all(&local_clone_dir).unwrap();

    run(
        "az",
        &[
            "storage",
            "blob",
            "download-batch",
            "-s",
            &container,
            "-d",
            local_clone_dir.to_str().unwrap(),
            "--pattern",
            &format!("{}/*", original_prefix),
        ],
    );

    // Move files from original/ subdirectory to root of local_clone_dir for upload
    let original_subdir = local_clone_dir.join(original_prefix);
    if original_subdir.exists() {
        for entry in fs::read_dir(&original_subdir).unwrap() {
            let entry = entry.unwrap();
            let dest = local_clone_dir.join(entry.file_name());
            fs::rename(entry.path(), dest).unwrap();
        }
        fs::remove_dir(&original_subdir).ok();
    }

    run(
        "az",
        &[
            "storage",
            "blob",
            "upload-batch",
            "-s",
            local_clone_dir.to_str().unwrap(),
            "-d",
            &format!("{}/{}", container, cloned_prefix),
        ],
    );

    // 3) Download cloned _delta_log locally, rewrite paths to absolute paths, upload back
    let tmpdir = tempfile::tempdir().unwrap();
    let local_log_dir = tmpdir.path().join("_delta_log");
    fs::create_dir_all(&local_log_dir).unwrap();

    run(
        "az",
        &[
            "storage",
            "blob",
            "download-batch",
            "-s",
            &container,
            "-d",
            tmpdir.path().to_str().unwrap(),
            "--pattern",
            &format!("{}/_delta_log/*", cloned_prefix),
        ],
    );

    // The download creates cloned/_delta_log structure, move files to local_log_dir
    let downloaded_log_dir = tmpdir.path().join(cloned_prefix).join("_delta_log");
    if downloaded_log_dir.exists() {
        for entry in fs::read_dir(&downloaded_log_dir).unwrap() {
            let entry = entry.unwrap();
            let dest = local_log_dir.join(entry.file_name());
            fs::rename(entry.path(), dest).unwrap();
        }
    }

    // Rewrite JSON actions inside downloaded log files. For Azure table location,
    // rewrite to fully-qualified abfs:// URIs pointing at the original Azure container.
    rewrite_log_paths_with_prefix(&local_log_dir, &original_uri);

    // Delete existing log files in cloned location and upload rewritten log
    run(
        "az",
        &[
            "storage",
            "blob",
            "delete-batch",
            "-s",
            &container,
            "--pattern",
            &format!("{}/_delta_log/*", cloned_prefix),
        ],
    );

    run(
        "az",
        &[
            "storage",
            "blob",
            "upload-batch",
            "-s",
            local_log_dir.to_str().unwrap(),
            "-d",
            &format!("{}/{}/_delta_log", container, cloned_prefix),
        ],
    );

    // 4) Open the cloned Azure table and verify URIs and data against the local original table
    assert_table_uris_and_data_cloud(&cloned_uri, &original_uri, &expected_abs, None).await;
}

// Test using Google Cloud Storage paths, gs://
// The gcp emulator is in docker: start `docker compose up -d` in repo root.
#[cfg(feature = "cloud")]
#[tokio::test]
async fn compare_table_with_full_paths_to_original_table_gcp() {
    use std::process::Command;

    // Register GCP handlers for deltalake-core tests. In normal usage the `deltalake`
    // meta-crate auto-registers these via a ctor, but core tests run without it.
    // This ensures that `gs://` URLs are recognized instead of yielding
    // InvalidTableLocation errors.
    deltalake_gcp::register_handlers(None);

    // Ensure environment is prepared for GCS emulator (fake-gcs-server)
    let gcs_base_url =
        std::env::var("GOOGLE_BASE_URL").unwrap_or_else(|_| "http://localhost:4443".to_string());
    let gcs_endpoint_url = std::env::var("GOOGLE_ENDPOINT_URL")
        .unwrap_or_else(|_| "http://localhost:4443/storage/v1/b".to_string());

    // Create a temporary directory for the fake service account JSON
    let tmpdir_creds = tempfile::tempdir().unwrap();
    let token = serde_json::json!({
        "gcs_base_url": gcs_base_url,
        "disable_oauth": true,
        "client_email": "",
        "private_key": "",
        "private_key_id": ""
    });
    let account_path = tmpdir_creds.path().join("gcs.json");
    fs::write(&account_path, serde_json::to_vec(&token).unwrap()).unwrap();
    std::env::set_var("GOOGLE_SERVICE_ACCOUNT", account_path.to_str().unwrap());

    // Path to the original local test table (with relative paths in _delta_log)
    let expected_rel = Path::new("../test/tests/data/delta-0.8.0");
    let expected_abs = fs::canonicalize(expected_rel).unwrap();

    // Create unique bucket and prefixes (avoid extra deps; use time-based suffix)
    let suffix = unique_suffix();
    let bucket = format!("test-delta-core-{}", suffix);
    let original_prefix = "original";
    let cloned_prefix = "cloned";
    let bucket_uri = format!("gs://{}", bucket);
    let (original_uri, cloned_uri) =
        build_prefixed_uris(&bucket_uri, original_prefix, cloned_prefix);

    // Helper to run a command and assert success
    let run = |program: &str, args: &[&str]| run_cmd(program, args);

    // Create bucket using curl to GCS emulator API
    let create_bucket_payload = serde_json::json!({ "name": bucket });
    run(
        "curl",
        &[
            "--insecure",
            "-X",
            "POST",
            "--data-binary",
            &serde_json::to_string(&create_bucket_payload).unwrap(),
            "-H",
            "Content-Type: application/json",
            &gcs_endpoint_url,
        ],
    );

    // No explicit cleanup is needed for the GCS emulator in this test context,
    // as the emulator runs in an isolated environment and is cleaned up automatically.

    // For GCS emulator, we use the object_store API to upload files since gsutil
    // may not be available. We'll use deltalake's built-in storage capabilities.
    use deltalake_core::table::builder::DeltaTableBuilder;
    use futures::StreamExt;

    // Helper to copy local directory to GCS
    async fn copy_local_to_gcs(local_dir: &Path, gcs_uri: &str) {
        let gcs_url = deltalake_core::table::builder::parse_table_uri(gcs_uri).unwrap();
        let gcs_store = DeltaTableBuilder::from_url(gcs_url)
            .unwrap()
            .with_allow_http(true)
            .build_storage()
            .unwrap();
        let store = gcs_store.object_store(None);

        fn visit_dirs(dir: &Path, base: &Path, files: &mut Vec<(PathBuf, String)>) {
            if dir.is_dir() {
                for entry in fs::read_dir(dir).unwrap() {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    if path.is_dir() {
                        visit_dirs(&path, base, files);
                    } else {
                        let rel = path.strip_prefix(base).unwrap();
                        files.push((path.clone(), rel.to_str().unwrap().to_string()));
                    }
                }
            }
        }

        let mut files = Vec::new();
        visit_dirs(local_dir, local_dir, &mut files);

        for (local_path, rel_path) in files {
            let bytes = fs::read(&local_path).unwrap();
            let object_path = object_store::path::Path::from(rel_path);
            store.put(&object_path, bytes.into()).await.unwrap();
        }
    }

    // Helper to copy GCS prefix to another GCS prefix
    async fn copy_gcs_to_gcs(from_uri: &str, to_uri: &str) {
        let from_url = deltalake_core::table::builder::parse_table_uri(from_uri).unwrap();
        let from_store = DeltaTableBuilder::from_url(from_url)
            .unwrap()
            .with_allow_http(true)
            .build_storage()
            .unwrap();
        let from_obj_store = from_store.object_store(None);

        let to_url = deltalake_core::table::builder::parse_table_uri(to_uri).unwrap();
        let to_store = DeltaTableBuilder::from_url(to_url)
            .unwrap()
            .with_allow_http(true)
            .build_storage()
            .unwrap();
        let to_obj_store = to_store.object_store(None);

        let mut stream = from_obj_store.list(None);
        while let Some(meta) = stream.next().await {
            if let Ok(meta) = meta {
                let bytes = from_obj_store
                    .get(&meta.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                to_obj_store
                    .put(&meta.location, bytes.into())
                    .await
                    .unwrap();
            }
        }
    }

    // Helper to download GCS files to local
    async fn download_gcs_to_local(gcs_uri: &str, local_dir: &Path, pattern: &str) {
        let gcs_url = deltalake_core::table::builder::parse_table_uri(gcs_uri).unwrap();
        let gcs_store = DeltaTableBuilder::from_url(gcs_url)
            .unwrap()
            .with_allow_http(true)
            .build_storage()
            .unwrap();
        let store = gcs_store.object_store(None);

        let prefix = object_store::path::Path::from(pattern);
        let mut stream = store.list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            if let Ok(meta) = meta {
                let bytes = store
                    .get(&meta.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let local_path = local_dir.join(meta.location.as_ref());
                if let Some(parent) = local_path.parent() {
                    fs::create_dir_all(parent).ok();
                }
                fs::write(&local_path, &bytes).unwrap();
            }
        }
    }

    // Helper to upload local files to GCS with a prefix
    async fn upload_local_to_gcs_with_prefix(local_dir: &Path, gcs_uri: &str, prefix: &str) {
        let gcs_url = deltalake_core::table::builder::parse_table_uri(gcs_uri).unwrap();
        let gcs_store = DeltaTableBuilder::from_url(gcs_url)
            .unwrap()
            .with_allow_http(true)
            .build_storage()
            .unwrap();
        let store = gcs_store.object_store(None);

        for entry in fs::read_dir(local_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                let bytes = fs::read(&path).unwrap();
                let filename = path.file_name().unwrap().to_str().unwrap();
                let object_path =
                    object_store::path::Path::from(format!("{}/{}", prefix, filename));
                store.put(&object_path, bytes.into()).await.unwrap();
            }
        }
    }

    // 1) Copy the original local table directory to gs://bucket/original
    copy_local_to_gcs(&expected_abs, &original_uri).await;

    // 2) Clone original -> cloned in GCS
    copy_gcs_to_gcs(&original_uri, &cloned_uri).await;

    // 3) Download cloned _delta_log locally, rewrite paths to absolute paths, upload back
    let tmpdir = tempfile::tempdir().unwrap();
    let local_log_dir = tmpdir.path().join("_delta_log");
    fs::create_dir_all(&local_log_dir).unwrap();

    download_gcs_to_local(&cloned_uri, tmpdir.path(), "_delta_log").await;

    // Rewrite JSON actions inside downloaded log files. For the table location,
    // rewrite to fully-qualified URIs (e.g., gs://, s3://, abfs://) pointing at the original bucket/container.
    rewrite_log_paths_with_prefix(&local_log_dir, &original_uri);

    // Upload rewritten log back to GCS cloned table
    upload_local_to_gcs_with_prefix(&local_log_dir, &cloned_uri, "_delta_log").await;

    // 4) Open the cloned GCS table and verify URIs and data against the local original table
    let mut opts: HashMap<String, String> = HashMap::new();
    opts.insert("allow_http".to_string(), "true".to_string());
    assert_table_uris_and_data_cloud(&cloned_uri, &original_uri, &expected_abs, Some(opts)).await;
}

// Known parquet file names used by the test table (delta-0.8.0)
const TEST_PARQUET_FILES: &[&str] = &[
    "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
    "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet",
];

// Helper to generate the expected file URIs (two known parquet parts) for a base directory
fn expected_file_uris(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = TEST_PARQUET_FILES
        .iter()
        .map(|f| dir.join(f).to_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

// Common helper to load data from a table and compare against an expected local table.
async fn assert_data_matches_expected(
    table: deltalake_core::DeltaTable,
    expected_table_dir: &Path,
) {
    use deltalake_core::arrow::array::RecordBatch;
    use deltalake_core::datafusion::assert_batches_sorted_eq;
    use deltalake_core::datafusion::common::test_util::format_batches;
    use deltalake_core::operations::collect_sendable_stream;
    use deltalake_core::DeltaOps;

    // Load data from target table
    let (_table, stream) = DeltaOps(table).load().await.unwrap();
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await.unwrap();

    // Load data from expected local table and compare
    let expected_table =
        deltalake_core::open_table(Url::from_directory_path(expected_table_dir).unwrap())
            .await
            .unwrap();
    let (_table, stream) = DeltaOps(expected_table).load().await.unwrap();
    let expected_data: Vec<RecordBatch> = collect_sendable_stream(stream).await.unwrap();
    let expected_lines = format_batches(&*expected_data).unwrap().to_string();
    let expected_lines_vec: Vec<&str> = expected_lines.trim().lines().collect();
    assert_batches_sorted_eq!(&expected_lines_vec, &data);
}

// Helper that opens `target_table_dir`, checks its file URIs match those under `expected_uris_dir`,
// then loads data and compares against data loaded from `expected_table_dir`.
async fn assert_table_uris_and_data(
    target_table_dir: &Path,
    expected_uris_dir: &Path,
    expected_table_dir: &Path,
) {
    // Open target table and verify file URIs
    let table = deltalake_core::open_table(Url::from_directory_path(target_table_dir).unwrap())
        .await
        .unwrap();
    let mut files: Vec<String> = table.get_file_uris().unwrap().collect();
    files.sort();
    let expected_uris = expected_file_uris(expected_uris_dir);
    assert_eq!(files, expected_uris);

    // Load and compare data
    assert_data_matches_expected(table, expected_table_dir).await;
}

// Helper to open a cloud table by URI, verify its data file URIs match those under
// `expected_prefix_uri`, and compare loaded data against a reference local table directory.
async fn assert_table_uris_and_data_cloud(
    table_uri: &str,
    expected_prefix_uri: &str,
    expected_table_dir: &Path,
    storage_options: Option<HashMap<String, String>>,
) {
    let table_url = url::Url::parse(table_uri).unwrap();
    let table = if let Some(opts) = storage_options {
        deltalake_core::open_table_with_storage_options(table_url, opts)
            .await
            .unwrap()
    } else {
        deltalake_core::open_table(table_url).await.unwrap()
    };

    // Verify file URIs
    let mut files: Vec<String> = table.get_file_uris().unwrap().collect();
    files.sort();
    let mut expected_uris: Vec<String> = expected_file_uris_from_prefix(expected_prefix_uri);
    expected_uris.sort();
    assert_eq!(files, expected_uris);

    // Load and compare data
    assert_data_matches_expected(table, expected_table_dir).await;
}

fn clone_test_dir_with_abs_paths_common(
    src_dir: &Path,
    target_dir: &Path,
    rewrite_base_dir: &Path,
) {
    // Re-create target directory by copying from the expected (relative-path) table.
    if target_dir.exists() {
        fs::remove_dir_all(&target_dir).unwrap();
    }
    fs::create_dir_all(&target_dir).unwrap();

    // Copy contents of expected table into target directory.
    use fs_extra::dir::{copy as copy_dir, CopyOptions};
    let mut opts = CopyOptions::new();
    opts.overwrite = true;
    opts.copy_inside = true;
    opts.content_only = true; // copy contents of source into dest root
    copy_dir(&src_dir, &target_dir, &opts).unwrap();

    // Now, rewrite _delta_log entries so that add/remove.path values are absolute
    // under the specified base directory (rewrite_base_dir).
    let log_dir = target_dir.join("_delta_log");
    // Local filesystem table: keep absolute native paths (no scheme)
    rewrite_log_paths(&log_dir, rewrite_base_dir, false);
}

fn clone_test_dir_with_abs_paths(src_dir: &Path, target_dir: &Path) {
    // Rewrite add/remove paths to be absolute under the copied table directory (target_dir).
    clone_test_dir_with_abs_paths_common(src_dir, target_dir, target_dir);
}

fn clone_test_dir_with_abs_paths_from_src(src_dir: &Path, target_dir: &Path) {
    // Rewrite add/remove paths to be absolute under the ORIGINAL table directory (src_dir).
    clone_test_dir_with_abs_paths_common(src_dir, target_dir, src_dir);
}

fn absify_action_path(base_dir: &Path, action: &mut Value, as_file_uri: bool) {
    if let Some(obj) = action.as_object_mut() {
        if let Some(path_val) = obj.get_mut("path") {
            if let Some(rel) = path_val.as_str() {
                let abs = base_dir.join(rel);
                // Canonicalize to eliminate any relative segments before forming URI/str
                let abs = fs::canonicalize(abs).unwrap_or_else(|_| base_dir.join(rel));
                let new_val = if as_file_uri {
                    Url::from_file_path(&abs)
                        .expect("Absolute path should convert to file URI")
                        .to_string()
                } else {
                    abs.to_str()
                        .expect("Path should be valid UTF-8")
                        .to_string()
                };
                *path_val = serde_json::Value::String(new_val);
            }
        }
    }
}

// Common helper to iterate over JSON log files and rewrite add/remove paths using a provided closure.
fn rewrite_log_paths_common<F>(log_dir: &Path, mut rewrite_action: F)
where
    F: FnMut(&mut Value),
{
    for entry in fs::read_dir(log_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let original = fs::read_to_string(&path).unwrap();
        let mut rewritten_lines: Vec<String> = Vec::new();
        for line in original.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(add) = v.get_mut("add") {
                rewrite_action(add);
            }
            if let Some(remove) = v.get_mut("remove") {
                rewrite_action(remove);
            }
            rewritten_lines.push(serde_json::to_string(&v).unwrap());
        }
        let new_contents = rewritten_lines.join("\n");
        fs::write(&path, format!("{}\n", new_contents)).unwrap();
    }
}

fn rewrite_log_paths(log_dir: &Path, base_dir: &Path, as_file_uri: bool) {
    rewrite_log_paths_common(log_dir, |action| {
        absify_action_path(base_dir, action, as_file_uri);
    });
}

// Rewrite add/remove.path values to fully-qualified URIs under the given prefix
// Example: prefix_uri = "s3://bucket/cloned" and path "part-00000.parquet" ->
//          "s3://bucket/cloned/part-00000.parquet"
fn rewrite_log_paths_with_prefix(log_dir: &Path, prefix_uri: &str) {
    let prefix = prefix_uri.trim_end_matches('/');
    rewrite_log_paths_common(log_dir, |action| {
        if let Some(obj) = action.as_object_mut() {
            if let Some(path_val) = obj.get_mut("path") {
                if let Some(rel) = path_val.as_str() {
                    let rel = rel.trim_start_matches('/');
                    let full = format!("{}/{}", prefix, rel);
                    *path_val = serde_json::Value::String(full);
                }
            }
        }
    });
}

// Build expected fully-qualified URIs for the two known parquet files under a prefix
fn expected_file_uris_from_prefix(prefix_uri: &str) -> Vec<String> {
    let prefix = prefix_uri.trim_end_matches('/');
    let mut v: Vec<String> = TEST_PARQUET_FILES
        .iter()
        .map(|f| format!("{}/{}", prefix, f))
        .collect();
    v.sort();
    v
}

// Small utility to create a time-based unique suffix for cloud buckets/containers
fn unique_suffix() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}", millis)
}

// Small utility to run an external command and assert success, with a helpful message
fn run_cmd(program: &str, args: &[&str]) {
    use std::process::Command;
    let output = Command::new(program)
        .args(args)
        .output()
        .expect("failed to run command");
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "command failed: {} {:?}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            program, args, output.status, stdout, stderr
        );
    }
}
