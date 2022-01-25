#[cfg(feature = "azure")]
mod adls_gen2_table {
    use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
    use azure_storage_datalake::prelude::DataLakeClient;
    use chrono::Utc;
    use deltalake::{
        action, DeltaTable, DeltaTableConfig, DeltaTableMetaData, Schema, SchemaDataType,
        SchemaField,
    };
    use serial_test::serial;
    use std::collections::HashMap;
    use std::env;

    /*
     * The storage account to run this test must be provided by the developer and test are executed locally.
     *
     * To prepare test execution, make sure a file system with the name "simple" exists within the account
     * and upload the contents of ./rust/tests/data/simple_table into that file system.
     *
     * Set the environment variables used for authentication as outlined in rust/src/storage/azure/mod.rs
     * Also set AZURE_STORAGE_ACCOUNT_NAME for setting up the test.
     *
     * remove the ignore statement below and execute...
     * via 'cargo test --features azure' or
     * via 'cargo test --features azure -p deltalake --test adls_gen2_table_test -- --nocapture'
     */
    #[ignore]
    #[tokio::test]
    #[serial]
    async fn read_simple_table() {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
        let table = deltalake::open_table(format!("adls2://{}/simple/", account).as_str())
            .await
            .unwrap();

        assert_eq!(table.version, 4);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
                "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
                "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
                "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
                "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
            ]
        );

        let tombstones = table.get_state().all_tombstones();
        assert_eq!(tombstones.len(), 31);
        let remove = deltalake::action::Remove {
            path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1587968596250),
            data_change: true,
            ..Default::default()
        };
        assert!(tombstones.contains(&remove));
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn create_simple_table() {
        // Arrange
        let storage_account_name = env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
        let storage_account_key = env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();

        let data_lake_client = DataLakeClient::new(
            StorageSharedKeyCredential::new(
                storage_account_name.to_owned(),
                storage_account_key.to_owned(),
            ),
            None,
        );

        // Create a new file system for test isolation
        let file_system_name = format!("fs-create-simple-table-{}", Utc::now().timestamp());
        let file_system_client =
            data_lake_client.into_file_system_client(file_system_name.to_owned());
        file_system_client.create().into_future().await.unwrap();

        let schema = Schema::new(vec![SchemaField::new(
            "Id".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        )]);

        let metadata = DeltaTableMetaData::new(
            Some("Azure Test Table".to_string()),
            None,
            None,
            schema,
            vec![],
            HashMap::new(),
        );

        let protocol = action::Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        let table_uri = &format!("adls2://{}/{}/", storage_account_name, file_system_name);
        let backend = deltalake::get_backend_for_uri(table_uri).unwrap();
        let mut dt = DeltaTable::new(table_uri, backend, DeltaTableConfig::default()).unwrap();

        // Act
        dt.create(metadata.clone(), protocol.clone(), None)
            .await
            .unwrap();

        // Assert
        assert_eq!(dt.version, 0);
        assert_eq!(dt.get_min_reader_version(), 1);
        assert_eq!(dt.get_min_writer_version(), 2);
        assert_eq!(dt.get_files().len(), 0);
        assert_eq!(dt.table_uri, table_uri.trim_end_matches('/').to_string());

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }
}
