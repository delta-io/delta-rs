extern crate chrono;
extern crate deltalake;
extern crate utime;

use std::path::Path;

use self::chrono::{DateTime, FixedOffset, Utc};

#[tokio::test]
async fn read_simple_table() {
    let table = deltalake::open_table("./tests/data/simple_table")
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
            path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
            deletion_timestamp: 1587968596250,
            data_change: true,
            ..Default::default()
        }
    );
    #[cfg(unix)]
    {
        let paths: Vec<String> = vec![
                "./tests/data/simple_table/part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet".to_string(),
                "./tests/data/simple_table/part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet".to_string(),
                "./tests/data/simple_table/part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet".to_string(),
                "./tests/data/simple_table/part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet".to_string(),
                "./tests/data/simple_table/part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet".to_string(),
            ];
        assert_eq!(table.get_file_uris(), paths);
    }
    #[cfg(windows)]
    {
        let paths: Vec<String> = vec![
                "./tests/data/simple_table\\part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet".to_string(),
                "./tests/data/simple_table\\part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet".to_string(),
                "./tests/data/simple_table\\part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet".to_string(),
                "./tests/data/simple_table\\part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet".to_string(),
                "./tests/data/simple_table\\part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet".to_string(),
            ];
        assert_eq!(table.get_file_uris(), paths);
    }
}

#[tokio::test]
async fn read_simple_table_with_version() {
    let mut table = deltalake::open_table_with_version("./tests/data/simple_table", 0)
        .await
        .unwrap();
    assert_eq!(table.version, 0);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet",
            "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet",
            "part-00003-508ae4aa-801c-4c2c-a923-f6f89930a5c1-c000.snappy.parquet",
            "part-00004-80938522-09c0-420c-861f-5a649e3d9674-c000.snappy.parquet",
            "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet",
            "part-00007-94f725e2-3963-4b00-9e83-e31021a93cf9-c000.snappy.parquet",
        ],
    );

    table = deltalake::open_table_with_version("./tests/data/simple_table", 2)
        .await
        .unwrap();
    assert_eq!(table.version, 2);
    assert_eq!(table.get_min_writer_version(), 2);
    assert_eq!(table.get_min_reader_version(), 1);
    assert_eq!(
        table.get_files(),
        vec![
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00003-53f42606-6cda-4f13-8d07-599a21197296-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00006-46f2ff20-eb5d-4dda-8498-7bfb2940713b-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
        ]
    );

    table = deltalake::open_table_with_version("./tests/data/simple_table", 3)
        .await
        .unwrap();
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
        ],
    );
}

fn ds_to_ts(ds: &str) -> i64 {
    let fixed_dt = DateTime::<FixedOffset>::parse_from_rfc3339(ds).unwrap();
    DateTime::<Utc>::from(fixed_dt).timestamp()
}

#[tokio::test]
async fn time_travel_by_ds() {
    // git does not preserve mtime, so we need to manually set it in the test
    let log_dir = "./tests/data/simple_table/_delta_log";
    let log_mtime_pair = vec![
        ("00000000000000000000.json", "2020-05-01T22:47:31-07:00"),
        ("00000000000000000001.json", "2020-05-02T22:47:31-07:00"),
        ("00000000000000000002.json", "2020-05-03T22:47:31-07:00"),
        ("00000000000000000003.json", "2020-05-04T22:47:31-07:00"),
        ("00000000000000000004.json", "2020-05-05T22:47:31-07:00"),
    ];
    for (fname, ds) in log_mtime_pair {
        let ts = ds_to_ts(ds);
        utime::set_file_times(Path::new(log_dir).join(fname), ts, ts).unwrap();
    }

    let mut table =
        deltalake::open_table_with_ds("./tests/data/simple_table", "2020-05-01T00:47:31-07:00")
            .await
            .unwrap();
    assert_eq!(table.version, 0);

    table = deltalake::open_table_with_ds("./tests/data/simple_table", "2020-05-02T22:47:31-07:00")
        .await
        .unwrap();
    assert_eq!(table.version, 1);

    table = deltalake::open_table_with_ds("./tests/data/simple_table", "2020-05-02T23:47:31-07:00")
        .await
        .unwrap();
    assert_eq!(table.version, 1);

    table = deltalake::open_table_with_ds("./tests/data/simple_table", "2020-05-03T22:47:31-07:00")
        .await
        .unwrap();
    assert_eq!(table.version, 2);

    table = deltalake::open_table_with_ds("./tests/data/simple_table", "2020-05-04T22:47:31-07:00")
        .await
        .unwrap();
    assert_eq!(table.version, 3);

    table = deltalake::open_table_with_ds("./tests/data/simple_table", "2020-05-05T21:47:31-07:00")
        .await
        .unwrap();
    assert_eq!(table.version, 3);

    table = deltalake::open_table_with_ds("./tests/data/simple_table", "2020-05-05T22:47:31-07:00")
        .await
        .unwrap();
    assert_eq!(table.version, 4);

    table = deltalake::open_table_with_ds("./tests/data/simple_table", "2020-05-25T22:47:31-07:00")
        .await
        .unwrap();
    assert_eq!(table.version, 4);
}
