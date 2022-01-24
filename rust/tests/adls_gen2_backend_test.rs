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
    // #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_adls_gen2_backend_put_and_delete_obj_with_dir() {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
        let base_path = &format!("dl://{}/fs-put-and-delete-obj-with-dir/", account);
        let backend = deltalake::get_backend_for_uri(base_path).unwrap();

        let ts = Utc::now().timestamp();
        let file_path = &format!("{}dir1/file-{}.txt", base_path, ts);

        backend
            .put_obj(file_path, &[12, 13, 14])
            .await
            .unwrap();

        let file_meta_data = backend.head_obj(file_path).await.unwrap();
        assert_eq!(file_meta_data.path, *file_path);

        // Note: dir1 itself does not get deleted here, just the file
        backend.delete_obj(file_path).await.unwrap();

        let head_err = backend.head_obj(file_path).await.err().unwrap();
        assert!(matches!(head_err, StorageError::NotFound));
    }
}
