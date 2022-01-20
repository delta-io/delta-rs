#[cfg(feature = "azure")]
mod adls_gen2_backend {
    use chrono::Utc;
    use deltalake::StorageError;
    use serial_test::serial;

    /*
     * The storage account to run this test must be provided by the developer and test are executed locally.
     *
     * Set the environment variables used for authentication as outlined in rust/src/storage/azure/mod.rs
     * Also set AZURE_STORAGE_ACCOUNT_NAME for setting up the test.
     *
     * remove the ignore statement below and execute...
     * via 'cargo test --features azure' or
     * via 'cargo test --features azure -p deltalake --test adls_gen2_backend_test -- --nocapture'
     */
    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_adls_gen2_backend_put_and_delete_obj() {
        // The AdlsGen2Backend currently uses both the Blob and Data Lake APIs. This is why the
        // paths are a bit of a mess here. This will soon be improved.

        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
        let base_path = &format!("abfss://simple@{}.dfs.core.windows.net/", account);
        let backend = deltalake::get_backend_for_uri(base_path).unwrap();

        let ts = Utc::now().timestamp();
        let data_lake_path = &format!("test_azure_delete_obj-{}.txt", ts);
        let blob_file_path = &format!("{}test_azure_delete_obj-{}.txt", base_path, ts);
        println!("azure_test data_lake_path = '{}'\n", data_lake_path);
        println!("azure_test blob_file_path = '{}'\n", blob_file_path);

        backend
            .put_obj(data_lake_path, &[12, 13, 14])
            .await
            .unwrap();

        let file_meta_data = backend.head_obj(blob_file_path).await.unwrap();
        assert_eq!(file_meta_data.path, *blob_file_path);

        backend.delete_obj(data_lake_path).await.unwrap();

        let head_err = backend.head_obj(blob_file_path).await.err().unwrap();
        assert!(matches!(head_err, StorageError::NotFound));
    }
}
