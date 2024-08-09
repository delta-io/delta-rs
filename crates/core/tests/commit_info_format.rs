#![allow(dead_code)]
mod fs_common;

use deltalake_core::kernel::Action;
use deltalake_core::operations::transaction::CommitBuilder;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use serde_json::json;
use std::error::Error;

#[tokio::test]
async fn test_operational_parameters() -> Result<(), Box<dyn Error>> {
    let path = tempfile::tempdir().unwrap();
    let mut table = fs_common::create_table(path.path().to_str().unwrap(), None).await;

    let add = fs_common::add(0);
    let actions = vec![Action::Add(add)];
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: Some(vec!["some_partition".to_string()]),
        predicate: None,
    };

    CommitBuilder::default()
        .with_actions(actions)
        .build(Some(table.snapshot()?), table.log_store(), operation)
        .await?;
    table.update().await?;

    let commit_info = table.history(None).await?;
    let last_commit = &commit_info[0];
    let parameters = last_commit.operation_parameters.clone().unwrap();
    assert_eq!(parameters["mode"], json!("Append"));
    assert_eq!(parameters["partitionBy"], json!("[\"some_partition\"]"));
    // assert_eq!(parameters["predicate"], None);

    Ok(())
}
