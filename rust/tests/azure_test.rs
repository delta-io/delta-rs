#[cfg(feature = "azure")]
mod azure {
    #[tokio::test]
    async fn test_azure_simple() {
        std::env::set_var("AZURE_STORAGE_ACCOUNT", "deltarstests");
        std::env::set_var("AZURE_STORAGE_SAS", "?sv=2019-12-12&ss=b&srt=co&sp=rl&se=2050-12-31T23:59:59Z&st=2020-11-30T10:19:31Z&spr=https&sig=%2FVV88TK0pwkY%2FoF3qXwweisDs63gfzdBlHAhB1zsED8%3D");
        let table =
            deltalake::open_table("abfss://simple@deltarstests.dfs.core.windows.net/simple/table")
                .await
                .unwrap();
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
}
