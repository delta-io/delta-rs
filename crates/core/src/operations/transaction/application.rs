#[cfg(test)]
mod tests {
    use crate::{
        checkpoints, kernel::Transaction, operations::transaction::CommitProperties,
        protocol::SaveMode, writer::test_utils::get_record_batch, DeltaOps, DeltaTableBuilder,
    };

    #[tokio::test]
    async fn test_app_txn_workload() {
        // Test that the transaction ids can be read from different scenarios
        // 1. Write new table to storage
        // 2. Read new table
        // 3. Write to table a new txn id and then update a different table state that uses the same underlying table
        // 4. Write a checkpoint and read that checkpoint.

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();

        let batch = get_record_batch(None, false);
        let table = DeltaOps::try_from_uri(tmp_path.to_str().unwrap())
            .await
            .unwrap()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified"])
            .with_commit_properties(
                CommitProperties::default()
                    .with_application_transaction(Transaction::new("my-app", 1)),
            )
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_files_count(), 2);

        let app_txns = table.get_app_transaction_version();
        assert_eq!(app_txns.len(), 1);
        assert_eq!(app_txns.get("my-app").map(|t| t.version), Some(1));

        // Test Txn Id can be read from existing table

        let mut table2 = DeltaTableBuilder::from_uri(tmp_path.to_str().unwrap())
            .load()
            .await
            .unwrap();
        let app_txns2 = table2.get_app_transaction_version();

        assert_eq!(app_txns2.len(), 1);
        assert_eq!(app_txns2.get("my-app").map(|t| t.version), Some(1));

        // Write new data to the table and check that `update` functions work

        let table = DeltaOps::from(table)
            .write(vec![get_record_batch(None, false)])
            .with_commit_properties(
                CommitProperties::default()
                    .with_application_transaction(Transaction::new("my-app", 3)),
            )
            .await
            .unwrap();

        assert_eq!(table.version(), 1);
        let app_txns = table.get_app_transaction_version();
        assert_eq!(app_txns.len(), 1);
        assert_eq!(app_txns.get("my-app").map(|t| t.version), Some(3));

        table2.update_incremental(None).await.unwrap();
        assert_eq!(table2.version(), 1);
        let app_txns2 = table2.get_app_transaction_version();
        assert_eq!(app_txns2.len(), 1);
        assert_eq!(app_txns2.get("my-app").map(|t| t.version), Some(3));

        // Create a checkpoint and then load
        checkpoints::create_checkpoint(&table).await.unwrap();
        let table3 = DeltaTableBuilder::from_uri(tmp_path.to_str().unwrap())
            .load()
            .await
            .unwrap();
        let app_txns3 = table2.get_app_transaction_version();
        assert_eq!(app_txns3.len(), 1);
        assert_eq!(app_txns3.get("my-app").map(|t| t.version), Some(3));
        assert_eq!(table3.version(), 1);
    }

    #[tokio::test]
    async fn test_app_txn_conflict() {
        // A conflict must be raised whenever the same application id is used for two concurrent transactions

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();

        let batch = get_record_batch(None, false);
        let table = DeltaOps::try_from_uri(tmp_path.to_str().unwrap())
            .await
            .unwrap()
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_partition_columns(["modified"])
            .with_commit_properties(
                CommitProperties::default()
                    .with_application_transaction(Transaction::new("my-app", 1)),
            )
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let table2 = DeltaTableBuilder::from_uri(tmp_path.to_str().unwrap())
            .load()
            .await
            .unwrap();
        assert_eq!(table2.version(), 0);

        let table = DeltaOps::from(table)
            .write(vec![get_record_batch(None, false)])
            .with_commit_properties(
                CommitProperties::default()
                    .with_application_transaction(Transaction::new("my-app", 2)),
            )
            .await
            .unwrap();
        assert_eq!(table.version(), 1);

        let res = DeltaOps::from(table2)
            .write(vec![get_record_batch(None, false)])
            .with_commit_properties(
                CommitProperties::default()
                    .with_application_transaction(Transaction::new("my-app", 3)),
            )
            .await;

        let err = res.err().unwrap();
        assert_eq!(
            err.to_string(),
            "Transaction failed: Failed to commit transaction: Concurrent transaction failed."
        );
    }
}
