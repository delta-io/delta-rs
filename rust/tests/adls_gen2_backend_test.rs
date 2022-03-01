#[cfg(feature = "azure")]
/// An Azure Data Lake Gen2 Storage Account is required to run these tests and must be provided by
/// the developer. Because of this requirement, the tests cannot run in CI and are therefore marked
/// #[ignore]. As a result, the developer must execute these tests on their machine.
/// In order to execute tests, remove the desired #[ignore] below and execute via:
/// 'cargo test --features azure --test adls_gen2_backend_test -- --nocapture'
/// `AZURE_STORAGE_ACCOUNT_NAME` is required to be set in the environment.
/// `AZURE_STORAGE_ACCOUNT_KEY` is required to be set in the environment.
mod adls_gen2_backend {
    use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
    use azure_storage_datalake::clients::{DataLakeClient, FileSystemClient};
    use chrono::Utc;
    use deltalake::{StorageBackend, StorageError};
    use serial_test::serial;
    use std::env;

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_put() {
        // Arrange
        let file_system_prefix = "test-adls-gen2-backend-put";
        let file_system_name = format!("{}-{}", file_system_prefix, Utc::now().timestamp());
        let (file_system_client, table_uri, backend) = setup(&file_system_name).await;

        // Act
        let file_path = &format!("{}dir1/file1-{}.txt", table_uri, Utc::now().timestamp());
        let file_contents = &[12, 13, 14];
        let result = backend.put_obj(file_path, file_contents).await;

        // Assert
        result.unwrap();
        let downloaded_file_contents = backend.get_obj(file_path).await.unwrap();
        assert_eq!(downloaded_file_contents, file_contents.to_vec());

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_put_overwrite() {
        // Arrange
        let file_system_prefix = "test-adls-gen2-backend-put-overwrite";
        let file_system_name = format!("{}-{}", file_system_prefix, Utc::now().timestamp());
        let (file_system_client, table_uri, backend) = setup(&file_system_name).await;

        let file_path = &format!("{}dir1/file1-{}.txt", table_uri, Utc::now().timestamp());
        backend.put_obj(file_path, &[12, 13, 14]).await.unwrap();

        // Act
        let file_contents = &[15, 16, 17];
        let result = backend.put_obj(file_path, file_contents).await;

        // Assert
        result.unwrap();
        let downloaded_file_contents = backend.get_obj(file_path).await.unwrap();
        assert_eq!(downloaded_file_contents, file_contents.to_vec());

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_head_of_missing_file() {
        // Arrange
        let file_system_prefix = "test-adls-gen2-backend-head-of-missing-file";
        let file_system_name = format!("{}-{}", file_system_prefix, Utc::now().timestamp());
        let (file_system_client, table_uri, backend) = setup(&file_system_name).await;

        // Act
        let file_path = &format!("{}dir1/file1-{}.txt", table_uri, Utc::now().timestamp());
        let result = backend.head_obj(file_path).await;

        // Assert
        let head_err = result.err().unwrap();
        assert!(matches!(head_err, StorageError::NotFound));

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_head_of_existing_file() {
        // Arrange
        let file_system_prefix = "test-adls-gen2-backend-head-of-existing-file";
        let file_system_name = format!("{}-{}", file_system_prefix, Utc::now().timestamp());
        let (file_system_client, table_uri, backend) = setup(&file_system_name).await;

        let file_path = &format!("{}dir1/file1-{}.txt", table_uri, Utc::now().timestamp());
        backend.put_obj(file_path, &[12, 13, 14]).await.unwrap();

        // Act
        let result = backend.head_obj(file_path).await;

        // Assert
        let file_meta_data = result.unwrap();
        assert_eq!(file_meta_data.path, *file_path);

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_delete_existing_file() {
        // Arrange
        let file_system_prefix = "test-adls-gen2-backend-delete-existing-file";
        let file_system_name = format!("{}-{}", file_system_prefix, Utc::now().timestamp());
        let (file_system_client, table_uri, backend) = setup(&file_system_name).await;

        let file_path = &format!("{}dir1/file1-{}.txt", table_uri, Utc::now().timestamp());
        backend.put_obj(file_path, &[12, 13, 14]).await.unwrap();

        // Act
        let result = backend.delete_obj(file_path).await;

        // Assert
        result.unwrap();
        let head_err = backend.head_obj(file_path).await.err().unwrap();
        assert!(matches!(head_err, StorageError::NotFound));

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_get() {
        // Arrange
        let file_system_prefix = "test-adls-gen2-backend-get";
        let file_system_name = format!("{}-{}", file_system_prefix, Utc::now().timestamp());
        let (file_system_client, table_uri, backend) = setup(&file_system_name).await;

        let file_path = &format!("{}dir1/file1-{}.txt", table_uri, Utc::now().timestamp());
        let file_contents = &[12, 13, 14];
        backend.put_obj(file_path, file_contents).await.unwrap();

        // Act
        let downloaded_file_contents = backend.get_obj(file_path).await.unwrap();
        assert_eq!(downloaded_file_contents, file_contents.to_vec());

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_rename_noreplace_succeeds() {
        // Arrange
        let file_system_prefix = "test-adls-gen2-backend-rename-noreplace-succeeds";
        let file_system_name = format!("{}-{}", file_system_prefix, Utc::now().timestamp());
        let (file_system_client, table_uri, backend) = setup(&file_system_name).await;

        let file_path1 = &format!("{}dir1/file1-{}.txt", table_uri, Utc::now().timestamp());
        let file_contents = &[12, 13, 14];
        backend.put_obj(file_path1, file_contents).await.unwrap();

        let file_path2 = &format!("{}dir1/file2-{}.txt", table_uri, Utc::now().timestamp());

        // Act
        let result = backend.rename_obj_noreplace(file_path1, file_path2).await;

        // Assert
        result.unwrap();
        let downloaded_file_contents = backend.get_obj(file_path2).await.unwrap();
        assert_eq!(downloaded_file_contents, file_contents.to_vec());

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_rename_noreplace_fails() {
        // Arrange
        let file_system_prefix = "test-adls-gen2-backend-rename-noreplace-fails";
        let file_system_name = format!("{}-{}", file_system_prefix, Utc::now().timestamp());
        let (file_system_client, table_uri, backend) = setup(&file_system_name).await;

        let file_path1 = &format!("{}dir1/file1-{}.txt", table_uri, Utc::now().timestamp());
        backend.put_obj(file_path1, &[12, 13, 14]).await.unwrap();

        let file_path2 = &format!("{}dir1/file2-{}.txt", table_uri, Utc::now().timestamp());
        backend.put_obj(file_path2, &[12, 13, 14]).await.unwrap();

        // Act
        let result = backend.rename_obj_noreplace(file_path1, file_path2).await;

        // Assert
        let rename_obj_noreplace_error = result.err().unwrap();
        assert!(
            matches!(rename_obj_noreplace_error, StorageError::AlreadyExists(path) if path == *file_path2)
        );

        // Cleanup
        file_system_client.delete().into_future().await.unwrap();
    }

    async fn setup(
        file_system_name: &String,
    ) -> (FileSystemClient, String, Box<dyn StorageBackend>) {
        let storage_account_name = env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap();
        let storage_account_key = env::var("AZURE_STORAGE_ACCOUNT_KEY").unwrap();

        let file_system_client = create_file_system_client(
            &storage_account_name,
            &storage_account_key,
            &file_system_name,
        )
        .await;

        let table_uri = &format!("adls2://{}/{}/", storage_account_name, file_system_name);
        let backend = deltalake::get_backend_for_uri(table_uri).unwrap();

        (file_system_client, table_uri.to_owned(), backend)
    }

    async fn create_file_system_client(
        storage_account_name: &String,
        storage_account_key: &String,
        file_system_name: &String,
    ) -> FileSystemClient {
        let data_lake_client = DataLakeClient::new(
            StorageSharedKeyCredential::new(
                storage_account_name.to_owned(),
                storage_account_key.to_owned(),
            ),
            None,
        );

        let file_system_client =
            data_lake_client.into_file_system_client(file_system_name.to_owned());
        file_system_client.create().into_future().await.unwrap();

        file_system_client
    }
}
