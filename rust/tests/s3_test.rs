#[cfg(feature = "s3")]
#[allow(dead_code)]
mod s3_common;

#[cfg(feature = "s3")]
mod s3 {
    use crate::s3_common::setup;
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
        let table = deltalake::open_table("s3://deltars/simple").await.unwrap();
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
        let tombstones = table.get_tombstones();
        assert_eq!(tombstones.len(), 31);
        assert_eq!(
            tombstones[0],
            deltalake::action::Remove {
                path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet"
                    .to_string(),
                deletion_timestamp: 1587968596250,
                data_change: true,
                ..Default::default()
            }
        );
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
        let tombstones = table.get_tombstones();
        assert_eq!(tombstones.len(), 29);
        assert_eq!(
            tombstones[0],
            deltalake::action::Remove {
                path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet"
                    .to_string(),
                deletion_timestamp: 1587968596250,
                data_change: true,
                ..Default::default()
            }
        );
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
}
