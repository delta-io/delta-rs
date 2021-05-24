#[cfg(feature = "azure")]
mod azure {
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
}
