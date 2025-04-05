use deltalake_core::*;
use serial_test::serial;
use std::fs;

#[tokio::test]
#[serial]
async fn simple_tgroup_test() {
    let table_path = "/home/naveen/Projects/delta-rs-mt/crates/test/tests/data/t_group_table_2";
    let logdir = format!("{table_path}/_delta_log");

    println!("Initializing delta table at {table_path}...");

    let dt = deltalake_core::open_table(table_path).await.unwrap();

    println!("\nListing files in delta log:");
    if let Ok(entries) = fs::read_dir(&logdir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() {
                    println!("{}", path.display());
                }
            }
        }
    } else {
        eprintln!("Could not read directory: {}", &logdir);
    }

    println!("\nAdding table to t-group...");

    tgroup::initiate_add_to_tgroup(
        &dt,
        "/home/naveen/Projects/delta-rs-mt/crates/test/tests/data/t_group_1",
    )
    .await
    .unwrap();

    println!("\nListing files in delta log:");
    if let Ok(entries) = fs::read_dir(&logdir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() {
                    println!("{}", path.display());
                }
            }
        }
    } else {
        eprintln!("Could not read directory: {}", &logdir);
    }
}
