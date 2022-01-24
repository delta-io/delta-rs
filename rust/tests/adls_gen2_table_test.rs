#[cfg(feature = "azure")]
mod adls_gen2_table {
    use serial_test::serial;

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
        let table = deltalake::open_table(format!("dl://{}/simple/", account).as_str())
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
}
