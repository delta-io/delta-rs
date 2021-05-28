extern crate deltalake;

#[cfg(feature = "delta-sharing")]
#[tokio::test]
async fn read_simple_delta_sharing_table() {
    let table = deltalake::open_table(
        "https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas/default/tables/COVID_19_NYT"
    )
    .await
    .unwrap();
    assert_eq!(table.version, 0);
    assert_eq!(table.get_min_reader_version(), 1);
    let files = table.get_files();
    assert_eq!(8, files.len());
}
