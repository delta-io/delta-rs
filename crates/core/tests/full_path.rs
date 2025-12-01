// Verifies that tables with fully-qualified file paths behave as expected
// and that reading them yields the same data as the corresponding table
// with relative file paths.
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use url::Url;

#[tokio::test]
async fn compare_table_with_full_paths() {
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

#[tokio::test]
async fn compare_table_with_full_paths_to_original_table() {
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

// Helper to generate the expected file URIs (two known parquet parts) for a base directory
fn expected_file_uris(dir: &Path) -> Vec<String> {
    let mut v = vec![
        dir.join("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet")
            .to_str()
            .unwrap()
            .to_string(),
        dir.join("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet")
            .to_str()
            .unwrap()
            .to_string(),
    ];
    v.sort();
    v
}

// Helper that opens `target_table_dir`, checks its file URIs match those under `expected_uris_dir`,
// then loads data and compares against data loaded from `expected_table_dir`.
async fn assert_table_uris_and_data(
    target_table_dir: &Path,
    expected_uris_dir: &Path,
    expected_table_dir: &Path,
) {
    use deltalake_core::arrow::array::RecordBatch;
    use deltalake_core::datafusion::assert_batches_sorted_eq;
    use deltalake_core::datafusion::common::test_util::format_batches;
    use deltalake_core::operations::collect_sendable_stream;
    use deltalake_core::DeltaOps;

    // Open target table and verify file URIs
    let table = deltalake_core::open_table(Url::from_directory_path(target_table_dir).unwrap())
        .await
        .unwrap();
    let mut files: Vec<String> = table.get_file_uris().unwrap().collect();
    files.sort();
    let expected_uris = expected_file_uris(expected_uris_dir);
    assert_eq!(files, expected_uris);

    // Load data from target table
    let (_table, stream) = DeltaOps(table).load().await.unwrap();
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await.unwrap();

    // Load data from expected table and compare
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
    rewrite_log_paths(&log_dir, rewrite_base_dir);
}

fn clone_test_dir_with_abs_paths(src_dir: &Path, target_dir: &Path) {
    // Rewrite add/remove paths to be absolute under the copied table directory (target_dir).
    clone_test_dir_with_abs_paths_common(src_dir, target_dir, target_dir);
}

fn clone_test_dir_with_abs_paths_from_src(src_dir: &Path, target_dir: &Path) {
    // Rewrite add/remove paths to be absolute under the ORIGINAL table directory (src_dir).
    clone_test_dir_with_abs_paths_common(src_dir, target_dir, src_dir);
}

fn absify_action_path(base_dir: &Path, action: &mut Value) {
    if let Some(obj) = action.as_object_mut() {
        if let Some(path_val) = obj.get_mut("path") {
            if let Some(rel) = path_val.as_str() {
                let abs = base_dir.join(rel);
                *path_val = serde_json::Value::String(
                    abs.to_str()
                        .expect("Path should be valid UTF-8")
                        .to_string(),
                );
            }
        }
    }
}

fn rewrite_log_paths(log_dir: &Path, base_dir: &Path) {
    for entry in fs::read_dir(&log_dir).unwrap() {
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
                absify_action_path(base_dir, add);
            }
            if let Some(remove) = v.get_mut("remove") {
                absify_action_path(base_dir, remove);
            }
            rewritten_lines.push(serde_json::to_string(&v).unwrap());
        }
        let new_contents = rewritten_lines.join("\n");
        fs::write(&path, new_contents).unwrap();
    }
}
