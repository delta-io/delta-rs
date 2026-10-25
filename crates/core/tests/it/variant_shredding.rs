use deltalake_core::{DeltaResult, kernel::DataType};

/// Loading the Delta log does not imply that an engine can decode shredded files.
/// Keep this coverage independent of the optional DataFusion integration.
#[tokio::test]
async fn load_stable_shredded_variant_snapshot() -> DeltaResult<()> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../test/tests/data/variant-shredded");
    let url = url::Url::from_directory_path(path.canonicalize()?).unwrap();
    let table = deltalake_core::open_table(url).await?;
    let snapshot = table.snapshot()?;
    assert_eq!(snapshot.version(), 0);
    assert!(matches!(
        snapshot.schema().field("body").unwrap().data_type(),
        DataType::Variant(_)
    ));
    let features = snapshot.protocol().reader_features().unwrap();
    assert!(
        features
            .iter()
            .any(|feature| feature.as_ref() == "variantType")
    );
    assert!(
        features
            .iter()
            .any(|feature| feature.as_ref() == "variantShredding")
    );
    Ok(())
}
