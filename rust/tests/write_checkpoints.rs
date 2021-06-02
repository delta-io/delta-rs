extern crate deltalake;

use deltalake::storage;
use deltalake::CheckPointWriter;
use std::fs;
use std::path::{Path, PathBuf};

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
    let checkpoint_path = log_path.join("00000000000000000005.parquet");
    assert!(checkpoint_path.as_path().exists());

    // _last_checkpoint should exist
    let last_checkpoint_path = log_path.join("_last_checkpoint");
    assert!(last_checkpoint_path.as_path().exists());

    // _last_checkpoint should point to checkpoint
    let last_checkpoint_content = fs::read_to_string(last_checkpoint_path.as_path()).unwrap();
    let last_checkpoint_content: serde_json::Value =
        serde_json::from_str(last_checkpoint_content.as_str()).unwrap();

    println!("{:?}", last_checkpoint_content);

    // delta table should load just fine with the checkpoint in place
    let table_result = deltalake::open_table(table_location).await.unwrap();
    let table = table_result;
    let files = table.get_files();
    println!("{:?}", files);
}

fn cleanup_checkpoint_files(log_path: &Path) {
    let paths = fs::read_dir(log_path).unwrap();

    for p in paths {
        match p {
            Ok(d) => {
                let path = d.path();

                println!("Checking path {:?}", path);

                if path.file_name().unwrap() == "_last_checkpoint"
                    || path.extension().unwrap() == "parquet"
                {
                    println!("Deleting {:?}", path);
                    fs::remove_file(path).unwrap();
                }
            }
            _ => {}
        }
    }
}
