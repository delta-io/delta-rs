#[cfg(feature = "s3")]
#[allow(dead_code)]
mod s3_common;

#[cfg(feature = "s3")]
mod s3 {
    use crate::s3_common::setup;
    use deltalake::s3_storage_options;
    use deltalake::storage;
    use dynamodb_lock::dynamo_lock_options;
    use maplit::hashmap;
    use serial_test::serial;

    /*
     * The S3 bucket used below resides in @rtyler's personal AWS account
     *
     * Should there be test failures, or if you need more files uploaded into this account, let him
     * know
     */
    use deltalake::StorageError;

    #[tokio::test]
    #[serial]
    async fn test_s3_simple() {
        setup();

        // Use the manual options API so we have some basic integrationcoverage.
        let table_uri = "s3://deltars/simple";
        let storage = storage::get_backend_for_uri_with_options(
            table_uri,
            hashmap! {
                s3_storage_options::AWS_REGION.to_string() => "us-east-2".to_string(),
                dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME.to_string() => "s3::deltars/simple".to_string(),
            },
        )
        .unwrap();
        let mut table =
            deltalake::DeltaTable::new(table_uri, storage, deltalake::DeltaTableConfig::default())
                .unwrap();
        table.load().await.unwrap();
        println!("{}", table);

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
    async fn test_s3_simple_with_version() {
        setup();
        let table = deltalake::open_table_with_version("s3://deltars/simple", 3)
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
    async fn test_s3_simple_with_trailing_slash() {
        setup();
        let table = deltalake::open_table("s3://deltars/simple/").await.unwrap();
        println!("{}", table);
        assert_eq!(table.version, 4);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_s3_simple_golden() {
        setup();

        let table = deltalake::open_table("s3://deltars/golden/data-reader-array-primitives")
            .await
            .unwrap();
        println!("{}", table);
        assert_eq!(table.version, 0);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_s3_head_obj() {
        setup();

        let key = "s3://deltars/missing";
        let backend = deltalake::get_backend_for_uri(key).unwrap();
        let err = backend.head_obj(key).await.err().unwrap();

        assert!(matches!(err, StorageError::NotFound));
    }

    #[tokio::test]
    #[serial]
    async fn test_s3_delete_obj() {
        setup();

        let path = "s3://deltars/delete.snappy.parquet";
        let backend = deltalake::get_backend_for_uri(path).unwrap();
        backend.put_obj(path, &[]).await.unwrap();
        backend.delete_obj(path).await.unwrap();
        let err = backend.head_obj(path).await.err().unwrap();

        assert!(matches!(err, StorageError::NotFound));
    }

    #[tokio::test]
    #[serial]
    async fn test_s3_delete_objs() {
        setup();

        let path1 = "s3://deltars/delete1.snappy.parquet";
        let path2 = "s3://deltars/delete2.snappy.parquet";
        let backend = deltalake::get_backend_for_uri(path1).unwrap();
        backend.put_obj(path1, &[]).await.unwrap();
        backend.put_obj(path2, &[]).await.unwrap();

        backend
            .delete_objs(&[path1.to_string(), path2.to_string()])
            .await
            .unwrap();
        let err1 = backend.head_obj(path1).await.err().unwrap();
        let err2 = backend.head_obj(path2).await.err().unwrap();

        assert!(matches!(err1, StorageError::NotFound));
        assert!(matches!(err2, StorageError::NotFound));
    }
}
