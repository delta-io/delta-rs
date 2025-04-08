#[tokio::test]
async fn test_exotic_tables() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let data_path = std::path::Path::new(dir).join("tests/data/exotic_logs");
    let full = data_path.canonicalize().unwrap();

    let cases = vec![
        // ("table_a", false),
        // ("table_b", false),
        // ("table_c", true),
        // ("table_d", true),
        // ("table_e", true),
        // ("table_f", true),
        ("table_g", false),
        // ("table_h", true),
        // ("table_i", true),
    ];

    for (name, should_error) in cases {
        let table_path = full.join(name);
        println!("{table_path:?}");
        let table = deltalake_core::open_table(&table_path.to_string_lossy()).await;
        println!("table: {:?}", table);
        if should_error {
            assert!(table.is_err());
        } else {
            assert!(table.is_ok());
        }
    }
}
