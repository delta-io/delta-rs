#[cfg(feature = "azure")]
mod azure {
    use deltalake::StorageError;
    use serial_test::serial;
    /*
     * The storage account used below resides in @rtyler's personal Azure account
     *
     * Should there be test failures, or if you need more files uploaded into this account, let him
     * know
     */
    #[ignore]
    #[tokio::test]
    async fn test_azure_simple() {
        std::env::set_var("AZURE_STORAGE_ACCOUNT", "deltars");
        // Expires January 2026
        std::env::set_var("AZURE_STORAGE_SAS", "?sv=2019-12-12&ss=b&srt=co&sp=rl&se=2026-01-06T06:45:33Z&st=2021-01-09T22:45:33Z&spr=https&sig=X9QtnFSA9UyMq3s4%2Fu2obCYeybdHsd2wVpbyvoTjECM%3D");
        // https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri
        let table = deltalake::open_table("abfss://simple@deltars.dfs.core.windows.net/")
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
        assert!(tombstones.contains(&deltalake::action::Remove {
            path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1587968596250),
            data_change: true,
            ..Default::default()
        }));
    }

    /*
     * Azure tests require running the azurite docker compose file. Currently only configuration via
     * connection string works, since non-standard endpoints need to be configured-
     */
    #[tokio::test]
    #[serial]
    async fn test_azure_simple_with_connection_string() {
        std::env::set_var("AZURE_STORAGE_CONNECTION_STRING", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;");
        // https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri
        let table = deltalake::open_table(
            "abfss://deltars@devstoreaccount1.dfs.core.windows.net/simple_table",
        )
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
        assert!(tombstones.contains(&deltalake::action::Remove {
            path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1587968596250),
            data_change: true,
            ..Default::default()
        }));
    }

    #[tokio::test]
    #[serial]
    async fn test_azure_simple_with_version() {
        std::env::set_var("AZURE_STORAGE_CONNECTION_STRING", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;");
        let table = deltalake::open_table_with_version(
            "abfss://deltars@devstoreaccount1.dfs.core.windows.net/simple_table",
            3,
        )
        .await
        .unwrap();
        println!("{}", table);
        assert_eq!(table.version, 3);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
                "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
                "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
                "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
                "part-00000-f17fcbf5-e0dc-40ba-adae-ce66d1fcaef6-c000.snappy.parquet",
                "part-00001-bb70d2ba-c196-4df2-9c85-f34969ad3aa9-c000.snappy.parquet",
            ]
        );
        let tombstones = table.get_state().all_tombstones();
        assert_eq!(tombstones.len(), 29);
        assert!(tombstones.contains(&deltalake::action::Remove {
            path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1587968596250),
            data_change: true,
            ..Default::default()
        }));
    }

    #[tokio::test]
    #[serial]
    async fn test_azure_simple_with_trailing_slash() {
        std::env::set_var("AZURE_STORAGE_CONNECTION_STRING", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;");
        let table = deltalake::open_table(
            "abfss://deltars@devstoreaccount1.dfs.core.windows.net/simple_table/",
        )
        .await
        .unwrap();
        println!("{}", table);
        assert_eq!(table.version, 4);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_azure_simple_golden() {
        std::env::set_var("AZURE_STORAGE_CONNECTION_STRING", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;");
        let table = deltalake::open_table("abfss://deltars@devstoreaccount1.dfs.core.windows.net/golden/data-reader-array-primitives")
            .await
            .unwrap();
        println!("{}", table);
        assert_eq!(table.version, 0);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_azure_head_obj_missing() {
        std::env::set_var("AZURE_STORAGE_CONNECTION_STRING", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;");
        let key = "abfss://deltars@devstoreaccount1.dfs.core.windows.net/missing";
        let backend = deltalake::get_backend_for_uri(key).unwrap();
        let err = backend.head_obj(key).await.err().unwrap();

        assert!(matches!(err, StorageError::NotFound));
    }
}
