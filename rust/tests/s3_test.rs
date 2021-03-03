#[macro_use]
extern crate serial_test;

#[cfg(feature = "s3")]
mod s3 {
    /*
     * The S3 bucket used below resides in @rtyler's personal AWS account
     *
     * Should there be test failures, or if you need more files uploaded into this account, let him
     * know
     */
    use deltalake::StorageError;

    fn setup() {
        std::env::set_var("AWS_REGION", "us-west-2");
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAX7EGEQ7FT6CLQGWH");
        std::env::set_var(
            "AWS_SECRET_ACCESS_KEY",
            "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",
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_s3_simple() {
        setup();
        let table = deltalake::open_table("s3://deltars/simple").await.unwrap();
        println!("{}", table);
        assert_eq!(table.version, 4);
        assert_eq!(table.min_writer_version, 2);
        assert_eq!(table.min_reader_version, 1);
        assert_eq!(
            table.get_files(),
            &vec![
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
                deletionTimestamp: 1587968596250,
                dataChange: true
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
        assert_eq!(table.min_writer_version, 2);
        assert_eq!(table.min_reader_version, 1);
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
        assert_eq!(table.min_writer_version, 2);
        assert_eq!(table.min_reader_version, 1);
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
}
