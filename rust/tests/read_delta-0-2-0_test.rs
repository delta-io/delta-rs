use delta;

#[test]
fn read_delta_2_0_table_without_version() {
    let table = delta::open_table("./tests/data/delta-0.2.0").unwrap();
    assert_eq!(table.version, 3);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet",
            "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
            "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        ]
    );
    let tombstones = table.get_tombstones();
    assert_eq!(tombstones.len(), 4);
    assert_eq!(
        tombstones[0],
        delta::action::Remove {
            path: "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet".to_string(),
            deletionTimestamp: 1564524298213,
            dataChange: false,
        }
    );
}

#[test]
fn read_delta_2_0_table_with_version() {
    let mut table = delta::open_table_with_version("./tests/data/delta-0.2.0", 0).unwrap();
    assert_eq!(table.version, 0);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
            "part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
        ],
    );

    table = delta::open_table_with_version("./tests/data/delta-0.2.0", 2).unwrap();
    assert_eq!(table.version, 2);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
            "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        ]
    );

    table = delta::open_table_with_version("./tests/data/delta-0.2.0", 3).unwrap();
    assert_eq!(table.version, 3);
    assert_eq!(table.min_writer_version, 2);
    assert_eq!(table.min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        &vec![
            "part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet",
            "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
            "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        ]
    );
}
