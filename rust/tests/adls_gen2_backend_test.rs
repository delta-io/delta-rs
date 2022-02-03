#[cfg(feature = "azure")]
mod adls_gen2_backend {
    use chrono::Utc;
    use deltalake::StorageError;
    use serial_test::serial;

    /*
     * An Azure Data Lake Gen2 Storage Account is required to run these tests and must be provided by
     * the developer. Because of this requirement, the tests cannot run in CI and are therefore marked
     * #[ignore]. As a result, the developer must execute these tests on their machine.
     * In order to execute tests, remove the desired #[ignore] below and execute via:
     * 'cargo test --features azure --test adls_gen2_backend_test -- --nocapture'
     * `AZURE_STORAGE_ACCOUNT_NAME` is required to be set in the environment.
     * `AZURE_STORAGE_ACCOUNT_KEY` is required to be set in the environment.
     */
    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_adls_gen2_backend_put_and_delete_obj_with_dir() {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
        let base_path = &format!("adls2://{}/fs-put-and-delete-obj-with-dir/", account);
        let backend = deltalake::get_backend_for_uri(base_path).unwrap();

        let ts = Utc::now().timestamp();
        let file_path = &format!("{}dir1/file-{}.txt", base_path, ts);

        backend.put_obj(file_path, &[12, 13, 14]).await.unwrap();

        let file_meta_data = backend.head_obj(file_path).await.unwrap();
        assert_eq!(file_meta_data.path, *file_path);

        // Note: dir1 itself does not get deleted here, just the file
        backend.delete_obj(file_path).await.unwrap();

        let head_err = backend.head_obj(file_path).await.err().unwrap();
        assert!(matches!(head_err, StorageError::NotFound));
    }
}
