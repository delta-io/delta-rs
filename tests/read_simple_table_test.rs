use delta;

#[test]
fn read_simple_table() {
    let table = delta::open_table("./tests/data/simple_table").unwrap();
    assert_eq!(table.version, 4);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.files,
        vec![
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
            "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
        ]
    );
}

#[test]
fn read_simple_table_with_version() {
    let mut table = delta::open_table_with_version("./tests/data/simple_table", 0).unwrap();
    assert_eq!(table.version, 0);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.files,
        vec![
            "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet",
            "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet",
            "part-00003-508ae4aa-801c-4c2c-a923-f6f89930a5c1-c000.snappy.parquet",
            "part-00004-80938522-09c0-420c-861f-5a649e3d9674-c000.snappy.parquet",
            "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet",
            "part-00007-94f725e2-3963-4b00-9e83-e31021a93cf9-c000.snappy.parquet"
        ],
    );

    table = delta::open_table_with_version("./tests/data/simple_table", 2).unwrap();
    assert_eq!(table.version, 2);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.files,
        vec![
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00003-53f42606-6cda-4f13-8d07-599a21197296-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00006-46f2ff20-eb5d-4dda-8498-7bfb2940713b-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
        ]
    );

    table = delta::open_table_with_version("./tests/data/simple_table", 3).unwrap();
    assert_eq!(table.version, 3);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.files,
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
