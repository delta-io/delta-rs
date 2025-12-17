#[cfg(feature = "datafusion")]
#[cfg(test)]
mod tests {
    use crate::{
        DeltaTable, DeltaTableBuilder, checkpoints, ensure_table_uri,
        kernel::{Transaction, transaction::CommitProperties},
        protocol::SaveMode,
        writer::test_utils::get_record_batch,
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
        let table = DeltaTable::try_from_url(ensure_table_uri(tmp_path.to_str().unwrap()).unwrap())
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
        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 0);
        assert_eq!(state.log_data().num_files(), 2);

        let app_txn = table
            .snapshot()
            .unwrap()
            .transaction_version(&table.log_store(), "my-app")
            .await
            .unwrap();
        assert_eq!(app_txn, Some(1));

        let log_store = table.log_store();
        let txn_version = table
            .snapshot()
            .unwrap()
            .transaction_version(log_store.as_ref(), "my-app")
            .await
            .unwrap();
        assert_eq!(txn_version, Some(1));

        // Test Txn Id can be read from existing table

        let mut table2 =
            DeltaTableBuilder::from_url(ensure_table_uri(tmp_path.to_str().unwrap()).unwrap())
                .unwrap()
                .load()
                .await
                .unwrap();
        let app_txn2 = table2
            .snapshot()
            .unwrap()
            .transaction_version(&table2.log_store(), "my-app")
            .await
            .unwrap();

        assert_eq!(app_txn2, Some(1));
        let txn_version = table2
            .snapshot()
            .unwrap()
            .transaction_version(log_store.as_ref(), "my-app")
            .await
            .unwrap();
        assert_eq!(txn_version, Some(1));

        // Write new data to the table and check that `update` functions work

        let table = table
            .write(vec![get_record_batch(None, false)])
            .with_commit_properties(
                CommitProperties::default()
                    .with_application_transaction(Transaction::new("my-app", 3)),
            )
            .await
            .unwrap();

        assert_eq!(table.version(), Some(1));
        let app_txn = table
            .snapshot()
            .unwrap()
            .transaction_version(&table.log_store(), "my-app")
            .await
            .unwrap();
        assert_eq!(app_txn, Some(3));
        let txn_version = table
            .snapshot()
            .unwrap()
            .transaction_version(log_store.as_ref(), "my-app")
            .await
            .unwrap();
        assert_eq!(txn_version, Some(3));

        table2.update_incremental(None).await.unwrap();
        assert_eq!(table2.version(), Some(1));
        let app_txn2 = table2
            .snapshot()
            .unwrap()
            .transaction_version(&table2.log_store(), "my-app")
            .await
            .unwrap();
        assert_eq!(app_txn2, Some(3));
        let txn_version = table2
            .snapshot()
            .unwrap()
            .transaction_version(log_store.as_ref(), "my-app")
            .await
            .unwrap();
        assert_eq!(txn_version, Some(3));

        // Create a checkpoint and then load
        checkpoints::create_checkpoint(&table, None).await.unwrap();
        let table3 =
            DeltaTableBuilder::from_url(ensure_table_uri(tmp_path.to_str().unwrap()).unwrap())
                .unwrap()
                .load()
                .await
                .unwrap();
        let app_txn3 = table3
            .snapshot()
            .unwrap()
            .transaction_version(&table3.log_store(), "my-app")
            .await
            .unwrap();
        assert_eq!(app_txn3, Some(3));
        assert_eq!(table3.version(), Some(1));
        let txn_version = table3
            .snapshot()
            .unwrap()
            .transaction_version(log_store.as_ref(), "my-app")
            .await
            .unwrap();
        assert_eq!(txn_version, Some(3));
    }
}
