extern crate deltalake;

use deltalake::checkpoints::CheckPointWriter;
use deltalake::storage;
use std::fs;
use std::path::{Path, PathBuf};

// NOTE: The below is a useful external command for inspecting the written checkpoint schema visually:
// parquet-tools inspect tests/data/checkpoints/_delta_log/00000000000000000005.checkpoint.parquet

#[tokio::test]
async fn write_simple_checkpoint() {
    let table_location = "./tests/data/checkpoints";
    let table_path = PathBuf::from(table_location);
    let log_path = table_path.join("_delta_log");

    // Delete checkpoint files from previous runs
    cleanup_checkpoint_files(log_path.as_path());

    // Load the delta table at version 5
    let table = deltalake::open_table_with_version(table_location, 5)
        .await
        .unwrap();

    // Write a checkpoint
    let storage_backend = storage::get_backend_for_uri(table_location).unwrap();
    let checkpoint_writer = CheckPointWriter::new(table_location, storage_backend);
    let _ = checkpoint_writer
        .create_checkpoint_from_state(table.version, table.get_state())
        .await
        .unwrap();

    // checkpoint should exist
    let checkpoint_path = log_path.join("00000000000000000005.checkpoint.parquet");
    assert!(checkpoint_path.as_path().exists());

    // _last_checkpoint should exist and point to the correct version
    let version = get_last_checkpoint_version(&log_path);
    assert_eq!(5, version);

    /* uncomment once map support is merged to arrow
    // Setting table version to 10 for another checkpoint
    table.load_version(10).await.unwrap();
    checkpoint_writer
        .create_checkpoint_from_state(table.version, table.get_state())
        .await
        .unwrap();

    // checkpoint should exist
    let checkpoint_path = log_path.join("00000000000000000010.checkpoint.parquet");
    assert!(checkpoint_path.as_path().exists());

    // _last_checkpoint should exist and point to the correct version
    let version = get_last_checkpoint_version(&log_path);
    assert_eq!(10, version);

    // delta table should load just fine with the checkpoint in place
    let table_result = deltalake::open_table(table_location).await.unwrap();
    let table = table_result;
    let files = table.get_files();
    assert_eq!(11, files.len());
    */
}

fn get_last_checkpoint_version(log_path: &PathBuf) -> i64 {
    let last_checkpoint_path = log_path.join("_last_checkpoint");
    assert!(last_checkpoint_path.as_path().exists());

    let last_checkpoint_content = fs::read_to_string(last_checkpoint_path.as_path()).unwrap();
    let last_checkpoint_content: serde_json::Value =
        serde_json::from_str(last_checkpoint_content.trim()).unwrap();

    last_checkpoint_content
        .get("version")
        .unwrap()
        .as_i64()
        .unwrap()
}

fn cleanup_checkpoint_files(log_path: &Path) {
    let paths = fs::read_dir(log_path).unwrap();

    for p in paths {
        match p {
            Ok(d) => {
                let path = d.path();

                if path.file_name().unwrap() == "_last_checkpoint"
                    || (path.extension().is_some() && path.extension().unwrap() == "parquet")
                {
                    fs::remove_file(path).unwrap();
                }
            }
            _ => {}
        }
    }
}
