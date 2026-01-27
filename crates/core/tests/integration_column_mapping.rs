//! Integration tests for Delta Lake column mapping support
//!
//! These tests verify that delta-rs correctly handles tables with column mapping
//! enabled (both 'name' and 'id' modes), including both read and write operations.

#![cfg(feature = "datafusion")]

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::assert_batches_sorted_eq;
use deltalake_core::delta_datafusion::create_session;
use deltalake_core::operations::write::WriteBuilder;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{ensure_table_uri, open_table};
use delta_kernel::table_features::ColumnMappingMode;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + 'static>>;

fn test_table_uri(path: &str) -> url::Url {
    ensure_table_uri(path).expect("Failed to create table URI")
}

// =============================================================================
// Read Tests
// =============================================================================

/// Test reading a table with column mapping (name mode)
#[tokio::test]
async fn test_read_table_with_column_mapping() -> TestResult {
    // Use the existing test table with column mapping
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;

    // Check that the table has column mapping enabled
    let config = table.snapshot()?.snapshot().table_configuration();
    let mode = config.column_mapping_mode();
    assert!(
        mode != ColumnMappingMode::None,
        "Expected column mapping to be enabled"
    );

    // Get the schema - should have logical column names
    let schema = table.snapshot()?.schema();
    let field_names: Vec<_> = schema.fields().map(|f| f.name().as_str()).collect();

    // The test table should have columns with special characters
    assert!(
        field_names.iter().any(|n| n.contains(' ')),
        "Expected column names with spaces, got: {:?}",
        field_names
    );

    Ok(())
}

/// Test DataFusion query on table with column mapping
#[tokio::test]
async fn test_datafusion_query_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    // Query using logical column names (with special characters)
    let df = ctx
        .sql(r#"SELECT "Company Very Short", "Super Name" FROM test_table ORDER BY "Super Name" LIMIT 3"#)
        .await?;

    let batches = df.collect().await?;

    // Verify we got results
    assert!(!batches.is_empty(), "Expected non-empty result");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Expected at least one row");

    // Verify column names in result schema are logical names
    let schema = batches[0].schema();
    assert!(schema.field_with_name("Company Very Short").is_ok());
    assert!(schema.field_with_name("Super Name").is_ok());

    Ok(())
}

/// Test filtering on partition columns with column mapping
#[tokio::test]
async fn test_partition_filter_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    // Filter on partition column using logical name
    let df = ctx
        .sql(r#"SELECT "Super Name" FROM test_table WHERE "Company Very Short" = 'BMS'"#)
        .await?;

    let batches = df.collect().await?;

    // Verify we got results
    assert!(!batches.is_empty(), "Expected non-empty result");

    let expected = vec![
        "+------------------------+",
        "| Super Name             |",
        "+------------------------+",
        "| Anthony Johnson        |",
        "| Mr. Daniel Ferguson MD |",
        "| Nathan Bennett         |",
        "| Stephanie Mcgrath      |",
        "+------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

/// Test statistics with column mapping
#[tokio::test]
async fn test_statistics_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;

    // Get file count from snapshot
    let snapshot = table.snapshot()?;
    let num_files = snapshot.log_data().num_files();

    assert!(num_files > 0, "Expected at least one file");

    Ok(())
}

/// Test scan with projection and column mapping
#[tokio::test]
async fn test_projection_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    // Select only one column
    let df = ctx
        .sql(r#"SELECT "Super Name" FROM test_table LIMIT 2"#)
        .await?;

    let batches = df.collect().await?;

    // Verify schema only has requested column
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 1);
    assert!(schema.field_with_name("Super Name").is_ok());

    Ok(())
}

/// Test that we can get physical column names from schema
#[tokio::test]
async fn test_physical_name_access() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;

    let config = table.snapshot()?.snapshot().table_configuration();
    let schema = config.schema();
    let mapping_mode = config.column_mapping_mode();

    // Verify physical names are different from logical names
    for field in schema.fields() {
        let logical = field.name();
        let physical = field.physical_name(mapping_mode);

        // For tables with column mapping, physical names should be UUIDs
        if mapping_mode != ColumnMappingMode::None {
            assert_ne!(
                logical, physical,
                "Physical name should differ from logical name with column mapping"
            );
            assert!(
                physical.starts_with("col-"),
                "Physical name should start with 'col-', got: {}",
                physical
            );
        }
    }

    Ok(())
}

/// Test selecting only partition column (metadata-only scan with partition values)
#[tokio::test]
async fn test_partition_column_only_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    // Select only partition column - this is a metadata-only scan
    let df = ctx
        .sql(r#"SELECT DISTINCT "Company Very Short" FROM test_table ORDER BY "Company Very Short""#)
        .await?;

    let batches = df.collect().await?;

    let expected = vec![
        "+--------------------+",
        "| Company Very Short |",
        "+--------------------+",
        "| BME                |",
        "| BMS                |",
        "+--------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

/// Test end-to-end: read full table content
#[tokio::test]
async fn test_full_table_scan_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();

    let batches = ctx.read_table(provider)?.collect().await?;

    let expected = vec![
        "+--------------------+------------------------+",
        "| Company Very Short | Super Name             |",
        "+--------------------+------------------------+",
        "| BME                | Timothy Lamb           |",
        "| BMS                | Anthony Johnson        |",
        "| BMS                | Mr. Daniel Ferguson MD |",
        "| BMS                | Nathan Bennett         |",
        "| BMS                | Stephanie Mcgrath      |",
        "+--------------------+------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

// =============================================================================
// Write Tests
// =============================================================================

/// Test writing to a table with column mapping (append mode)
#[tokio::test]
async fn test_write_append_with_column_mapping() -> TestResult {
    // Create a temp directory and copy the test table
    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().join("table_with_column_mapping");

    // Copy test table to temp location
    let src_path = std::path::Path::new("../test/tests/data/table_with_column_mapping");
    copy_dir_all(src_path, &table_path)?;

    // Open the copied table
    let table_uri = test_table_uri(table_path.to_str().unwrap());
    let mut table = open_table(table_uri).await?;

    // Get the initial row count
    let initial_count = table.snapshot()?.log_data().num_files();

    // Create new data to append with logical column names
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("Company Very Short", ArrowDataType::Utf8, true),
        ArrowField::new("Super Name", ArrowDataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["TST"])),
            Arc::new(StringArray::from(vec!["Test Person"])),
        ],
    )?;

    // Append data to the table
    table = WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Verify more files exist now
    let final_count = table.snapshot()?.log_data().num_files();
    assert!(
        final_count > initial_count,
        "Expected more files after append: initial={}, final={}",
        initial_count,
        final_count
    );

    // Query to verify data was written with correct column names
    let provider = table.table_provider().await?;
    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    let df = ctx
        .sql(r#"SELECT "Super Name" FROM test_table WHERE "Company Very Short" = 'TST'"#)
        .await?;

    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Expected 1 row with company 'TST'");

    Ok(())
}

/// Test that partition values use physical column names in Add actions
#[tokio::test]
async fn test_partition_values_use_physical_names() -> TestResult {
    // Create a temp directory and copy the test table
    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().join("table_with_column_mapping_pv");

    // Copy test table to temp location
    let src_path = std::path::Path::new("../test/tests/data/table_with_column_mapping");
    copy_dir_all(src_path, &table_path)?;

    // Open the copied table
    let table_uri = test_table_uri(table_path.to_str().unwrap());
    let mut table = open_table(table_uri).await?;

    // Get physical column name for partition column
    let config = table.snapshot()?.snapshot().table_configuration();
    let schema = config.schema();
    let mapping_mode = config.column_mapping_mode();

    let partition_field = schema
        .fields()
        .find(|f| f.name() == "Company Very Short")
        .unwrap();
    let physical_partition_name = partition_field.physical_name(mapping_mode).to_string();

    // Create new data to append
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("Company Very Short", ArrowDataType::Utf8, true),
        ArrowField::new("Super Name", ArrowDataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(StringArray::from(vec!["NEW"])),
            Arc::new(StringArray::from(vec!["New Person"])),
        ],
    )?;

    // Append data to the table
    table = WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Read the last commit log to verify partition values use physical names
    let log_store = table.log_store();
    let version = table.snapshot()?.version();
    let commit_uri = deltalake_core::logstore::commit_uri_from_version(version);

    let log_bytes = log_store
        .object_store(None)
        .get(&commit_uri)
        .await?
        .bytes()
        .await?;
    let log_content = String::from_utf8(log_bytes.to_vec())?;

    // Verify the Add action uses physical column name in partitionValues
    assert!(
        log_content.contains(&physical_partition_name),
        "Add action should contain physical column name '{}' in partitionValues. Log content: {}",
        physical_partition_name,
        log_content
    );

    Ok(())
}

/// Helper function to recursively copy a directory
fn copy_dir_all(
    src: impl AsRef<std::path::Path>,
    dst: impl AsRef<std::path::Path>,
) -> std::io::Result<()> {
    std::fs::create_dir_all(&dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

/// Test reading and verifying statistics use physical column names
#[tokio::test]
async fn test_stats_use_physical_names() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";
    let table = open_table(test_table_uri(table_path)).await?;

    // Get physical column name for the non-partition column
    let config = table.snapshot()?.snapshot().table_configuration();
    let schema = config.schema();
    let mapping_mode = config.column_mapping_mode();

    let super_name_field = schema.fields().find(|f| f.name() == "Super Name").unwrap();
    let physical_name = super_name_field.physical_name(mapping_mode);

    // Verify physical name is a UUID-style name
    assert!(
        physical_name.starts_with("col-"),
        "Physical name should start with 'col-', got: {}",
        physical_name
    );

    // Read the log and verify stats contain physical names
    let log_store = table.log_store();
    let commit_uri = deltalake_core::logstore::commit_uri_from_version(0);

    let log_bytes = log_store
        .object_store(None)
        .get(&commit_uri)
        .await?
        .bytes()
        .await?;
    let log_content = String::from_utf8(log_bytes.to_vec())?;

    // Stats should contain physical column names
    assert!(
        log_content.contains(physical_name),
        "Stats should contain physical column name '{}'. Log content snippet: {}",
        physical_name,
        &log_content[..std::cmp::min(2000, log_content.len())]
    );

    Ok(())
}

/// Test full end-to-end: read, append, read back
#[tokio::test]
async fn test_full_roundtrip_with_column_mapping() -> TestResult {
    // Create a temp directory and copy the test table
    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().join("table_roundtrip");

    // Copy test table to temp location
    let src_path = std::path::Path::new("../test/tests/data/table_with_column_mapping");
    copy_dir_all(src_path, &table_path)?;

    // Open the copied table
    let table_uri = test_table_uri(table_path.to_str().unwrap());
    let table = open_table(table_uri.clone()).await?;

    // Read initial data
    let provider = table.table_provider().await?;
    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    let df = ctx.sql(r#"SELECT COUNT(*) as cnt FROM test_table"#).await?;
    let batches = df.collect().await?;
    let initial_count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    // Append new data
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("Company Very Short", ArrowDataType::Utf8, true),
        ArrowField::new("Super Name", ArrowDataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["RND", "RND", "RND"])),
            Arc::new(StringArray::from(vec!["Person A", "Person B", "Person C"])),
        ],
    )?;

    let _ = WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Re-read the table and verify counts
    let table = open_table(table_uri).await?;
    let provider = table.table_provider().await?;
    let ctx = create_session().into_inner();
    ctx.register_table("updated_table", provider)?;

    let df = ctx
        .sql(r#"SELECT COUNT(*) as cnt FROM updated_table"#)
        .await?;
    let batches = df.collect().await?;
    let final_count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(
        final_count,
        initial_count + 3,
        "Expected count to increase by 3"
    );

    // Verify the new data can be queried with column names
    let df = ctx
        .sql(r#"SELECT "Super Name" FROM updated_table WHERE "Company Very Short" = 'RND' ORDER BY "Super Name""#)
        .await?;
    let batches = df.collect().await?;

    let expected = vec![
        "+------------+",
        "| Super Name |",
        "+------------+",
        "| Person A   |",
        "| Person B   |",
        "| Person C   |",
        "+------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

// =============================================================================
// maxColumnId Tracking Tests
// =============================================================================

/// Test that maxColumnId is properly set when creating a table with column mapping
#[tokio::test]
async fn test_max_column_id_on_create() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::kernel::{DataType, StructField};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("value", DataType::DOUBLE, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Verify maxColumnId is set in configuration
    let config = table.snapshot()?.metadata().configuration();
    let max_column_id = config.get("delta.columnMapping.maxColumnId");

    assert!(
        max_column_id.is_some(),
        "maxColumnId should be set in table configuration"
    );

    let max_id: i64 = max_column_id.unwrap().parse()?;
    assert_eq!(
        max_id, 3,
        "maxColumnId should be 3 for a table with 3 columns"
    );

    Ok(())
}

/// Test that schema evolution properly assigns new column IDs
#[tokio::test]
async fn test_schema_evolution_column_id_assignment() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::operations::write::WriteBuilder;
    use deltalake_core::operations::write::SchemaMode;
    use deltalake_core::kernel::{DataType, StructField, StructTypeExt};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Verify initial maxColumnId
    let config = table.snapshot()?.metadata().configuration();
    let initial_max_id: i64 = config
        .get("delta.columnMapping.maxColumnId")
        .unwrap()
        .parse()?;
    assert_eq!(initial_max_id, 2, "Initial maxColumnId should be 2");

    // Write data with a new column (schema evolution)
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("new_column", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )?;

    let table = WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .with_schema_mode(SchemaMode::Merge)
    .await?;

    // Verify maxColumnId is updated
    let config = table.snapshot()?.metadata().configuration();
    let new_max_id: i64 = config
        .get("delta.columnMapping.maxColumnId")
        .unwrap()
        .parse()?;
    assert_eq!(
        new_max_id, 3,
        "maxColumnId should be 3 after adding one new column"
    );

    // Verify the new column has proper column mapping metadata
    let schema = table.snapshot()?.schema();
    let new_field = schema.fields().find(|f| f.name() == "new_column");
    assert!(new_field.is_some(), "new_column should exist in schema");

    let _new_field = new_field.unwrap();
    let id_mappings = schema.get_logical_to_id_mapping();
    let new_column_id = id_mappings.get("new_column");
    assert!(
        new_column_id.is_some(),
        "new_column should have a column ID"
    );
    assert_eq!(
        *new_column_id.unwrap(),
        3,
        "new_column should have ID 3"
    );

    Ok(())
}

/// Test that column mapping metadata is preserved for existing fields during schema evolution
#[tokio::test]
async fn test_schema_evolution_preserves_existing_ids() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::operations::write::WriteBuilder;
    use deltalake_core::operations::write::SchemaMode;
    use deltalake_core::kernel::{DataType, StructField, StructTypeExt};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Get initial column IDs
    let initial_schema = table.snapshot()?.schema();
    let initial_id_map = initial_schema.get_logical_to_id_mapping();
    let id_column_id = *initial_id_map.get("id").unwrap();
    let name_column_id = *initial_id_map.get("name").unwrap();

    // Write data with a new column (schema evolution)
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("new_column", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Test"])),
            Arc::new(Int32Array::from(vec![42])),
        ],
    )?;

    let table = WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .with_schema_mode(SchemaMode::Merge)
    .await?;

    // Verify existing column IDs are preserved
    let new_schema = table.snapshot()?.schema();
    let new_id_map = new_schema.get_logical_to_id_mapping();

    assert_eq!(
        *new_id_map.get("id").unwrap(),
        id_column_id,
        "id column should preserve its column ID"
    );
    assert_eq!(
        *new_id_map.get("name").unwrap(),
        name_column_id,
        "name column should preserve its column ID"
    );

    Ok(())
}

/// Test that reading works from newly created table with column mapping
#[tokio::test]
async fn test_read_from_created_column_mapping_table() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::kernel::{DataType, StructField};
    use arrow::array::{Int32Array, StringArray as ArrowStringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("id", DataType::STRING, false),
            StructField::new("value", DataType::INTEGER, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Write initial data
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "B"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )?;

    let table = deltalake_core::operations::write::WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Now try to read using table provider and DataFusion
    let provider = table.table_provider().await?;
    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    // Try to query data
    let df = ctx.sql("SELECT id, value FROM test_table ORDER BY id").await?;
    let batches = df.collect().await?;

    // Verify results
    assert!(!batches.is_empty(), "Should have results");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should have 2 rows");

    Ok(())
}

/// Test basic MERGE operation (without schema evolution) with column mapping enabled
#[tokio::test]
async fn test_merge_basic_with_column_mapping() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::kernel::{DataType, StructField};
    use arrow::array::{Int32Array, StringArray as ArrowStringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::prelude::{col, SessionContext};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("id", DataType::STRING, false),
            StructField::new("value", DataType::INTEGER, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Write initial data
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "B"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )?;

    let table = deltalake_core::operations::write::WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Create source data for MERGE (same schema, no new columns)
    let source_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
    ]));

    let source_batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "C"])),  // A exists, C is new
            Arc::new(Int32Array::from(vec![150, 300])),  // Update A, insert C
        ],
    )?;

    let ctx = SessionContext::new();
    let source_df = ctx.read_batch(source_batch)?;

    // Perform basic MERGE (no schema evolution)
    let (table, _metrics) = table
        .merge(source_df, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update.update("value", col("source.value"))
        })?
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
        })?
        .await?;

    // Verify merge worked - the table should have 3 rows now (A updated, B unchanged, C inserted)
    let _snapshot = table.snapshot()?;
    // If we get here without error, the merge completed successfully

    Ok(())
}

/// Test that MERGE with schema evolution properly assigns column IDs when column mapping is enabled
#[tokio::test]
async fn test_merge_schema_evolution_with_column_mapping() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::kernel::{DataType, StructField, StructTypeExt};
    use arrow::array::{Int32Array, StringArray as ArrowStringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::prelude::{col, SessionContext};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("id", DataType::STRING, false),
            StructField::new("value", DataType::INTEGER, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Write initial data
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "B"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )?;

    let table = deltalake_core::operations::write::WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Get initial max column ID
    let config = table.snapshot()?.metadata().configuration();
    let initial_max_id: i64 = config
        .get("delta.columnMapping.maxColumnId")
        .unwrap()
        .parse()?;
    assert_eq!(initial_max_id, 2, "Initial maxColumnId should be 2");

    // Get initial column IDs
    let initial_schema = table.snapshot()?.schema();
    let initial_id_map = initial_schema.get_logical_to_id_mapping();
    let id_column_id = *initial_id_map.get("id").unwrap();
    let value_column_id = *initial_id_map.get("value").unwrap();

    // Create source data with a new column for MERGE
    let source_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
        ArrowField::new("new_col", ArrowDataType::Int32, true),
    ]));

    let source_batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(ArrowStringArray::from(vec!["C"])),
            Arc::new(Int32Array::from(vec![300])),
            Arc::new(Int32Array::from(vec![999])),
        ],
    )?;

    let ctx = SessionContext::new();
    let source_df = ctx.read_batch(source_batch)?;

    // Perform MERGE with schema evolution enabled
    let (table, _metrics) = table
        .merge(source_df, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_merge_schema(true)
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
                .set("new_col", col("source.new_col"))
        })?
        .await?;

    // Verify maxColumnId is updated
    let config = table.snapshot()?.metadata().configuration();
    let new_max_id: i64 = config
        .get("delta.columnMapping.maxColumnId")
        .unwrap()
        .parse()?;
    assert_eq!(
        new_max_id, 3,
        "maxColumnId should be 3 after MERGE with schema evolution"
    );

    // Verify the new column has proper column mapping metadata
    let schema = table.snapshot()?.schema();
    let id_map = schema.get_logical_to_id_mapping();

    assert!(
        id_map.contains_key("new_col"),
        "new_col should have a column ID after MERGE"
    );
    assert_eq!(
        *id_map.get("new_col").unwrap(),
        3,
        "new_col should have ID 3"
    );

    // Verify existing column IDs are preserved
    assert_eq!(
        *id_map.get("id").unwrap(),
        id_column_id,
        "id column should preserve its column ID"
    );
    assert_eq!(
        *id_map.get("value").unwrap(),
        value_column_id,
        "value column should preserve its column ID"
    );

    Ok(())
}

/// Test MERGE with column mapping using column names with special characters (spaces)
/// This is a common use case for column mapping - allowing column names that aren't valid in Parquet
#[tokio::test]
async fn test_merge_with_special_column_names_and_column_mapping() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::kernel::{DataType, StructField};
    use arrow::array::{Int32Array, StringArray as ArrowStringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::prelude::{col, SessionContext};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled and column names containing spaces
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("user id", DataType::STRING, false),
            StructField::new("total value", DataType::INTEGER, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Verify column mapping is enabled and columns have physical names
    let schema = table.snapshot()?.schema();
    for field in schema.fields() {
        assert!(
            field.metadata().contains_key("delta.columnMapping.physicalName"),
            "Field {} should have physical name mapping",
            field.name()
        );
        let physical_name = field.metadata().get("delta.columnMapping.physicalName")
            .expect("Should have physical name");
        if let delta_kernel::schema::MetadataValue::String(pn) = physical_name {
            assert!(
                pn.starts_with("col-"),
                "Physical name for '{}' should start with 'col-', got: {}",
                field.name(), pn
            );
        }
    }

    // Write initial data: "A"=100, "B"=200
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("user id", ArrowDataType::Utf8, false),
        ArrowField::new("total value", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "B"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )?;

    let table = deltalake_core::operations::write::WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Verify initial data using query (column names with spaces need quotes)
    let ctx = create_session().into_inner();
    let provider = table.table_provider().await?;
    ctx.register_table("initial_table", provider)?;
    let df = ctx.sql(r#"SELECT "user id", "total value" FROM initial_table ORDER BY "user id""#).await?;
    let initial_batches = df.collect().await?;

    let expected_initial = vec![
        "+---------+-------------+",
        "| user id | total value |",
        "+---------+-------------+",
        "| A       | 100         |",
        "| B       | 200         |",
        "+---------+-------------+",
    ];
    assert_batches_sorted_eq!(expected_initial, &initial_batches);

    // Create source data for MERGE: Update A to 150, Insert C=300
    let source_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("user id", ArrowDataType::Utf8, false),
        ArrowField::new("total value", ArrowDataType::Int32, true),
    ]));

    let source_batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "C"])),
            Arc::new(Int32Array::from(vec![150, 300])),
        ],
    )?;

    let merge_ctx = SessionContext::new();
    let source_df = merge_ctx.read_batch(source_batch)?;

    // Perform MERGE with column names containing spaces
    // Use backticks to quote column names with spaces in the predicate
    let (table, metrics) = table
        .merge(source_df, col("target.`user id`").eq(col("source.`user id`")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update.update("total value", col("source.`total value`"))
        })?
        .when_not_matched_insert(|insert| {
            insert
                .set("user id", col("source.`user id`"))
                .set("total value", col("source.`total value`"))
        })?
        .await?;

    // Verify merge metrics
    assert_eq!(metrics.num_target_rows_updated, 1, "Should update 1 row (A)");
    assert_eq!(metrics.num_target_rows_inserted, 1, "Should insert 1 row (C)");
    assert_eq!(metrics.num_target_rows_copied, 1, "Should copy 1 row (B)");

    // Read back the data and verify it's correct
    let ctx2 = create_session().into_inner();
    let provider2 = table.table_provider().await?;
    ctx2.register_table("merged_table", provider2)?;

    let df2 = ctx2.sql(r#"SELECT "user id", "total value" FROM merged_table ORDER BY "user id""#).await?;
    let result_batches = df2.collect().await?;

    // Expected: A=150 (updated), B=200 (unchanged), C=300 (inserted)
    let expected = vec![
        "+---------+-------------+",
        "| user id | total value |",
        "+---------+-------------+",
        "| A       | 150         |",
        "| B       | 200         |",
        "| C       | 300         |",
        "+---------+-------------+",
    ];
    assert_batches_sorted_eq!(expected, &result_batches);

    Ok(())
}

/// Test that verifies column ordering is maintained correctly after merge with column mapping
/// This test uses distinct values for each column to detect if values get written to wrong columns
#[tokio::test]
async fn test_merge_column_ordering_with_column_mapping() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::kernel::{DataType, StructField};
    use arrow::array::{Int32Array, StringArray as ArrowStringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::prelude::{col, SessionContext};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with 4 columns to better detect ordering issues
    // Use distinct value patterns for each column
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("key", DataType::STRING, false),
            StructField::new("col_a", DataType::INTEGER, true),
            StructField::new("col_b", DataType::INTEGER, true),
            StructField::new("col_c", DataType::INTEGER, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Write initial data with distinct patterns:
    // key "X": col_a=100, col_b=200, col_c=300
    // key "Y": col_a=110, col_b=210, col_c=310
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("key", ArrowDataType::Utf8, false),
        ArrowField::new("col_a", ArrowDataType::Int32, true),
        ArrowField::new("col_b", ArrowDataType::Int32, true),
        ArrowField::new("col_c", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ArrowStringArray::from(vec!["X", "Y"])),
            Arc::new(Int32Array::from(vec![100, 110])),
            Arc::new(Int32Array::from(vec![200, 210])),
            Arc::new(Int32Array::from(vec![300, 310])),
        ],
    )?;

    let table = deltalake_core::operations::write::WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Create source data for MERGE with different value patterns:
    // key "X": update col_a=101, col_b=201, col_c=301
    // key "Z": insert col_a=120, col_b=220, col_c=320
    let source_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("key", ArrowDataType::Utf8, false),
        ArrowField::new("col_a", ArrowDataType::Int32, true),
        ArrowField::new("col_b", ArrowDataType::Int32, true),
        ArrowField::new("col_c", ArrowDataType::Int32, true),
    ]));

    let source_batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(ArrowStringArray::from(vec!["X", "Z"])),
            Arc::new(Int32Array::from(vec![101, 120])),
            Arc::new(Int32Array::from(vec![201, 220])),
            Arc::new(Int32Array::from(vec![301, 320])),
        ],
    )?;

    let merge_ctx = SessionContext::new();
    let source_df = merge_ctx.read_batch(source_batch)?;

    // Perform MERGE with all columns
    let (table, _metrics) = table
        .merge(source_df, col("target.key").eq(col("source.key")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update
                .update("col_a", col("source.col_a"))
                .update("col_b", col("source.col_b"))
                .update("col_c", col("source.col_c"))
        })?
        .when_not_matched_insert(|insert| {
            insert
                .set("key", col("source.key"))
                .set("col_a", col("source.col_a"))
                .set("col_b", col("source.col_b"))
                .set("col_c", col("source.col_c"))
        })?
        .await?;

    // Read back and verify each column has its expected values
    let ctx2 = create_session().into_inner();
    let provider2 = table.table_provider().await?;
    ctx2.register_table("result", provider2)?;

    let df2 = ctx2.sql("SELECT key, col_a, col_b, col_c FROM result ORDER BY key").await?;
    let result_batches = df2.collect().await?;

    // Expected:
    // X: 101, 201, 301 (updated from source)
    // Y: 110, 210, 310 (unchanged from target)
    // Z: 120, 220, 320 (inserted from source)
    let expected = vec![
        "+-----+-------+-------+-------+",
        "| key | col_a | col_b | col_c |",
        "+-----+-------+-------+-------+",
        "| X   | 101   | 201   | 301   |",
        "| Y   | 110   | 210   | 310   |",
        "| Z   | 120   | 220   | 320   |",
        "+-----+-------+-------+-------+",
    ];
    assert_batches_sorted_eq!(expected, &result_batches);

    // Additional verification: ensure values aren't swapped between columns
    // For row X: col_a should be 101 (not 201 or 301)
    // For row Z: col_b should be 220 (not 120 or 320)
    let check_df = ctx2.sql("SELECT col_a, col_b, col_c FROM result WHERE key = 'X'").await?;
    let check_batches = check_df.collect().await?;
    let check_expected = vec![
        "+-------+-------+-------+",
        "| col_a | col_b | col_c |",
        "+-------+-------+-------+",
        "| 101   | 201   | 301   |",
        "+-------+-------+-------+",
    ];
    assert_batches_sorted_eq!(check_expected, &check_batches);

    Ok(())
}

/// Test that MERGE with column mapping writes correct data that can be read back
/// This test verifies the data values are correct after merge, not just that the operation succeeds
#[tokio::test]
async fn test_merge_data_verification_with_column_mapping() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::kernel::{DataType, StructField};
    use arrow::array::{Int32Array, StringArray as ArrowStringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::prelude::{col, SessionContext};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("id", DataType::STRING, false),
            StructField::new("value", DataType::INTEGER, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Write initial data: A=100, B=200
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "B"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )?;

    let table = deltalake_core::operations::write::WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Verify initial data is correct
    let ctx = create_session().into_inner();
    let provider = table.table_provider().await?;
    ctx.register_table("initial_table", provider)?;
    let df = ctx.sql("SELECT id, value FROM initial_table ORDER BY id").await?;
    let initial_batches = df.collect().await?;

    let expected_initial = vec![
        "+----+-------+",
        "| id | value |",
        "+----+-------+",
        "| A  | 100   |",
        "| B  | 200   |",
        "+----+-------+",
    ];
    assert_batches_sorted_eq!(expected_initial, &initial_batches);

    // Create source data for MERGE: Update A to 150, Insert C=300
    let source_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
    ]));

    let source_batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "C"])),  // A exists, C is new
            Arc::new(Int32Array::from(vec![150, 300])),  // Update A to 150, insert C with 300
        ],
    )?;

    let merge_ctx = SessionContext::new();
    let source_df = merge_ctx.read_batch(source_batch)?;

    // Perform MERGE: update matching rows, insert new rows
    let (table, metrics) = table
        .merge(source_df, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update.update("value", col("source.value"))
        })?
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
        })?
        .await?;

    // Verify merge metrics
    assert_eq!(metrics.num_target_rows_updated, 1, "Should update 1 row (A)");
    assert_eq!(metrics.num_target_rows_inserted, 1, "Should insert 1 row (C)");
    assert_eq!(metrics.num_target_rows_copied, 1, "Should copy 1 row (B)");

    // Read back the data and verify it's correct
    let ctx2 = create_session().into_inner();
    let provider2 = table.table_provider().await?;
    ctx2.register_table("merged_table", provider2)?;

    let df2 = ctx2.sql("SELECT id, value FROM merged_table ORDER BY id").await?;
    let result_batches = df2.collect().await?;

    // Expected: A=150 (updated), B=200 (unchanged), C=300 (inserted)
    let expected = vec![
        "+----+-------+",
        "| id | value |",
        "+----+-------+",
        "| A  | 150   |",
        "| B  | 200   |",
        "| C  | 300   |",
        "+----+-------+",
    ];
    assert_batches_sorted_eq!(expected, &result_batches);

    Ok(())
}

/// Test MERGE with schema evolution (autoschema) and column mapping enabled.
/// This tests the scenario where source has new columns that need to be
/// added to the target table during merge - verifying both schema changes and data.
#[tokio::test]
async fn test_autoschema_merge_with_column_mapping() -> TestResult {
    use deltalake_core::operations::create::CreateBuilder;
    use deltalake_core::kernel::{DataType, StructField, StructTypeExt};
    use arrow::array::{Int32Array, StringArray as ArrowStringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::prelude::{col, SessionContext};

    let tmp_dir = tempfile::tempdir()?;
    let table_path = tmp_dir.path().to_str().unwrap();

    // Create a table with column mapping enabled and 2 columns
    let table = CreateBuilder::new()
        .with_location(table_path)
        .with_columns(vec![
            StructField::new("id", DataType::STRING, false),
            StructField::new("value", DataType::INTEGER, true),
        ])
        .with_configuration(vec![
            ("delta.columnMapping.mode".to_string(), Some("name".to_string())),
        ])
        .await?;

    // Write initial data: A=100, B=200
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ArrowStringArray::from(vec!["A", "B"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )?;

    let table = deltalake_core::operations::write::WriteBuilder::new(
        table.log_store(),
        table.snapshot().ok().map(|s| s.snapshot()).cloned(),
    )
    .with_input_batches(vec![batch])
    .with_save_mode(SaveMode::Append)
    .await?;

    // Verify initial maxColumnId is 2
    let config = table.snapshot()?.metadata().configuration();
    let initial_max_id: i64 = config
        .get("delta.columnMapping.maxColumnId")
        .unwrap()
        .parse()?;
    assert_eq!(initial_max_id, 2, "Initial maxColumnId should be 2");

    // Create source data with a NEW column (new_col)
    let source_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, true),
        ArrowField::new("new_col", ArrowDataType::Int32, true),  // NEW column
    ]));

    let source_batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(ArrowStringArray::from(vec!["C", "D"])),  // New rows
            Arc::new(Int32Array::from(vec![300, 400])),
            Arc::new(Int32Array::from(vec![999, 888])),  // Values for new column
        ],
    )?;

    let merge_ctx = SessionContext::new();
    let source_df = merge_ctx.read_batch(source_batch)?;

    // Perform MERGE with schema evolution enabled (autoschema)
    // This should add the new_col to the target schema
    let (table, _metrics) = table
        .merge(source_df, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_merge_schema(true)  // Enable schema evolution
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
                .set("new_col", col("source.new_col"))
        })?
        .await?;

    // Verify maxColumnId was incremented
    let config = table.snapshot()?.metadata().configuration();
    let new_max_id: i64 = config
        .get("delta.columnMapping.maxColumnId")
        .unwrap()
        .parse()?;
    assert_eq!(
        new_max_id, 3,
        "maxColumnId should be 3 after adding new column"
    );

    // Verify the new column has proper column mapping metadata
    let schema = table.snapshot()?.schema();
    let id_map = schema.get_logical_to_id_mapping();

    assert!(
        id_map.contains_key("new_col"),
        "new_col should have a column ID"
    );
    assert_eq!(
        *id_map.get("new_col").unwrap(),
        3,
        "new_col should have ID 3"
    );

    // Verify physical name mapping exists for new column
    let physical_map = schema.get_logical_to_physical_mapping();
    assert!(
        physical_map.contains_key("new_col"),
        "new_col should have a physical name mapping"
    );
    let new_col_physical = physical_map.get("new_col").unwrap();
    assert!(
        new_col_physical.starts_with("col-"),
        "Physical name for new_col should start with 'col-', got: {}",
        new_col_physical
    );

    // Read back the data and verify it's correct
    let ctx = create_session().into_inner();
    let provider = table.table_provider().await?;
    ctx.register_table("result_table", provider)?;

    let df = ctx.sql("SELECT id, value, new_col FROM result_table ORDER BY id").await?;
    let result_batches = df.collect().await?;

    // Expected:
    // A: 100, NULL (existing row, new_col didn't exist)
    // B: 200, NULL (existing row, new_col didn't exist)
    // C: 300, 999 (inserted with new_col value)
    // D: 400, 888 (inserted with new_col value)
    let expected = vec![
        "+----+-------+---------+",
        "| id | value | new_col |",
        "+----+-------+---------+",
        "| A  | 100   |         |",
        "| B  | 200   |         |",
        "| C  | 300   | 999     |",
        "| D  | 400   | 888     |",
        "+----+-------+---------+",
    ];
    assert_batches_sorted_eq!(expected, &result_batches);

    Ok(())
}
