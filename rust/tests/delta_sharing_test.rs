extern crate deltalake;

#[cfg(feature = "delta-sharing")]
#[tokio::test]
async fn read_simple_table() {
    let table = deltalake::open_table("http://localhost:8000/api/v1/shares/rtyler/schemas/samples/tables/covid19-nyt")
        .await
        .unwrap();
    assert_eq!(table.version, 0);
    assert_eq!(table.get_min_reader_version(), 1);
    let files = table.get_files();
    assert_eq!(8, files.len());
}
