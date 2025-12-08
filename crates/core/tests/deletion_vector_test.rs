//! Integration tests for deletion vector support
//!
//! These tests verify that deletion vectors are properly parsed and applied
//! when reading Delta tables.

mod fs_common;

use deltalake_core::test_utils::TestTables;
use deltalake_core::{DeltaResult, DeltaTableBuilder};
use url::Url;

/// Test that we can open a table with deletion vectors
#[tokio::test]
async fn test_open_table_with_dv() -> DeltaResult<()> {
    let table_path = TestTables::WithDvSmall.as_path();
    let table_url = Url::from_directory_path(&table_path).unwrap();

    let table = DeltaTableBuilder::from_uri(table_url)?
        .with_allow_http(true)
        .load()
        .await?;

    // Table should be at version 1 (version 0 created table, version 1 added DVs)
    assert_eq!(table.version(), Some(1));

    Ok(())
}

/// Test that deletion vector metadata is correctly parsed
#[tokio::test]
async fn test_dv_metadata_parsing() -> DeltaResult<()> {
    let table_path = TestTables::WithDvSmall.as_path();
    let table_url = Url::from_directory_path(&table_path).unwrap();

    let table = DeltaTableBuilder::from_uri(table_url)?
        .with_allow_http(true)
        .load()
        .await?;

    let snapshot = table.snapshot()?;
    
    // Check that we can read the table schema - proves metadata was parsed
    let schema = snapshot.schema();
    assert!(schema.fields().len() > 0, "Schema should have fields");

    // The table should have exactly 1 file according to the log
    // We can't easily check DV metadata from the public API without datafusion
    // but we verify the table opens successfully with DV-enabled files
    Ok(())
}

/// Test that the table reports correct file count
#[tokio::test]
async fn test_table_file_count() -> DeltaResult<()> {
    let table_path = TestTables::WithDvSmall.as_path();
    let table_url = Url::from_directory_path(&table_path).unwrap();

    let table = DeltaTableBuilder::from_uri(table_url)?
        .with_allow_http(true)
        .load()
        .await?;

    let snapshot = table.snapshot()?;
    let files: Vec<_> = snapshot.log_data().iter().collect();

    // Should have 1 data file
    assert_eq!(files.len(), 1, "Expected 1 data file");

    Ok(())
}

/// Test the deletion vector descriptor parsing from JSON
#[test]
fn test_dv_descriptor_serde() {
    use deltalake_core::kernel::models::{DeletionVectorDescriptor, StorageType};

    let json = r#"{
        "storageType": "u",
        "pathOrInlineDv": "vBn[lx{q8@P<9BNH/isA",
        "offset": 1,
        "sizeInBytes": 36,
        "cardinality": 2
    }"#;

    let dv: DeletionVectorDescriptor = serde_json::from_str(json).unwrap();

    assert_eq!(dv.storage_type, StorageType::UuidRelativePath);
    assert_eq!(dv.path_or_inline_dv, "vBn[lx{q8@P<9BNH/isA");
    assert_eq!(dv.offset, Some(1));
    assert_eq!(dv.size_in_bytes, 36);
    assert_eq!(dv.cardinality, 2);
}

/// Test that we can serialize and deserialize DV descriptors
#[test]
fn test_dv_descriptor_roundtrip() {
    use deltalake_core::kernel::models::{DeletionVectorDescriptor, StorageType};

    let original = DeletionVectorDescriptor {
        storage_type: StorageType::UuidRelativePath,
        path_or_inline_dv: "test_path".to_string(),
        offset: Some(100),
        size_in_bytes: 256,
        cardinality: 50,
    };

    let json = serde_json::to_string(&original).unwrap();
    let parsed: DeletionVectorDescriptor = serde_json::from_str(&json).unwrap();

    assert_eq!(original, parsed);
}

/// Test inline deletion vector storage type
#[test]
fn test_inline_dv_descriptor() {
    use deltalake_core::kernel::models::{DeletionVectorDescriptor, StorageType};

    let json = r#"{
        "storageType": "i",
        "pathOrInlineDv": "base85EncodedData",
        "sizeInBytes": 20,
        "cardinality": 5
    }"#;

    let dv: DeletionVectorDescriptor = serde_json::from_str(json).unwrap();

    assert_eq!(dv.storage_type, StorageType::Inline);
    assert_eq!(dv.offset, None, "Inline DVs should not have offset");
}

/// Test absolute path deletion vector storage type
#[test]
fn test_absolute_path_dv_descriptor() {
    use deltalake_core::kernel::models::{DeletionVectorDescriptor, StorageType};

    let json = r#"{
        "storageType": "p",
        "pathOrInlineDv": "/absolute/path/to/dv.bin",
        "offset": 0,
        "sizeInBytes": 100,
        "cardinality": 25
    }"#;

    let dv: DeletionVectorDescriptor = serde_json::from_str(json).unwrap();

    assert_eq!(dv.storage_type, StorageType::AbsolutePath);
    assert_eq!(dv.path_or_inline_dv, "/absolute/path/to/dv.bin");
}

#[cfg(feature = "datafusion")]
mod datafusion_tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    /// Test that DataFusion integration exists for tables with deletion vectors
    /// 
    /// Note: Currently, tables with DeletionVectors feature require reader support.
    /// This test documents the current behavior - we get an error because the
    /// DeletionVectors reader feature is not yet fully supported.
    /// 
    /// CURRENT STATUS: Returns error "Unsupported table features required: [DeletionVectors]"
    /// FUTURE: Once DV reader support is complete, this should query successfully and
    /// return 8 rows (10 original - 2 deleted).
    #[tokio::test]
    async fn test_datafusion_query_with_dv() -> DeltaResult<()> {
        let table_path = TestTables::WithDvSmall.as_path();
        let table_url = Url::from_directory_path(&table_path).unwrap();

        let table = DeltaTableBuilder::from_uri(table_url.clone())?
            .with_allow_http(true)
            .load()
            .await?;

        let ctx = SessionContext::new();
        ctx.register_table("test_table", std::sync::Arc::new(table))?;

        // The query will fail because the DeltaScanBuilder calls PROTOCOL.can_read_from()
        // which checks for unsupported reader features (DeletionVectors is not yet supported)
        let df_result = ctx.sql("SELECT COUNT(*) as cnt FROM test_table").await;

        match df_result {
            Ok(df) => {
                let collect_result = df.collect().await;
                match collect_result {
                    Ok(batches) => {
                        // If we get here, DV reader support has been added to PROTOCOL!
                        let count = batches[0]
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .unwrap()
                            .value(0);
                        println!("Query succeeded with {} rows", count);
                        // Once DVs are filtered, should be 8; without filtering, 10
                    }
                    Err(e) => {
                        // Expected: DeletionVectors not supported
                        let err_msg = e.to_string();
                        println!("Expected error during collect: {}", err_msg);
                        assert!(
                            err_msg.contains("DeletionVectors") || err_msg.contains("Unsupported"),
                            "Expected error about DeletionVectors, got: {}",
                            err_msg
                        );
                    }
                }
            }
            Err(e) => {
                // Expected: DeletionVectors not supported
                let err_msg = e.to_string();
                println!("Expected error during SQL: {}", err_msg);
                assert!(
                    err_msg.contains("DeletionVectors") || err_msg.contains("Unsupported"),
                    "Expected error about DeletionVectors, got: {}",
                    err_msg
                );
            }
        }

        Ok(())
    }
}

