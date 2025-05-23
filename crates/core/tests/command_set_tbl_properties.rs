use deltalake_core::kernel::StructType;
use deltalake_core::operations::DeltaOps;
use serde_json::json;
use std::collections::HashMap;

/// Basic schema for testing
pub fn get_test_schema() -> StructType {
    serde_json::from_value(json!({
      "type": "struct",
      "fields": [
        {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "value", "type": "string", "nullable": true, "metadata": {}},
      ]
    }))
    .unwrap()
}

#[tokio::test]
async fn test_set_table_name_valid() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let name = "test_table_name";
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), name.to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(metadata.name.as_ref().unwrap(), name);
    assert_eq!(updated_table.version(), 1);
}

#[tokio::test]
async fn test_set_table_description_valid() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let description = "This is a test table description";
    let mut properties = HashMap::new();
    properties.insert("description".to_string(), description.to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(metadata.description.as_ref().unwrap(), description);
    assert_eq!(updated_table.version(), 1);
}

#[tokio::test]
async fn test_set_table_name_and_description() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let name = "my_test_table";
    let description = "A comprehensive test table with both name and description";
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), name.to_string());
    properties.insert("description".to_string(), description.to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(metadata.name.as_ref().unwrap(), name);
    assert_eq!(metadata.description.as_ref().unwrap(), description);
    assert_eq!(updated_table.version(), 1);
}

#[tokio::test]
async fn test_set_table_name_character_limit_valid() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    // Test exactly 255 characters (should work)
    let name_255_chars = "x".repeat(255);
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), name_255_chars.clone());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(metadata.name.as_ref().unwrap(), &name_255_chars);
    assert_eq!(updated_table.version(), 1);
}

#[tokio::test]
async fn test_set_table_name_character_limit_exceeded() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    // Test 256 characters (should fail)
    let name_256_chars = "x".repeat(256);
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), name_256_chars);

    let result = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Table name cannot exceed 255 characters"));
    assert!(error_message.contains("256 characters"));
}

#[tokio::test]
async fn test_set_table_description_character_limit_valid() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    // Test exactly 4000 characters (should work)
    let description_4000_chars = "y".repeat(4000);
    let mut properties = HashMap::new();
    properties.insert("description".to_string(), description_4000_chars.clone());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(
        metadata.description.as_ref().unwrap(),
        &description_4000_chars
    );
    assert_eq!(updated_table.version(), 1);
}

#[tokio::test]
async fn test_set_table_description_character_limit_exceeded() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    // Test 4001 characters (should fail)
    let description_4001_chars = "y".repeat(4001);
    let mut properties = HashMap::new();
    properties.insert("description".to_string(), description_4001_chars);

    let result = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Table description cannot exceed 4,000 characters"));
    assert!(error_message.contains("4001 characters"));
}

#[tokio::test]
async fn test_update_existing_table_name() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    // Set initial name
    let initial_name = "initial_table_name";
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), initial_name.to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    assert_eq!(updated_table.version(), 1);
    assert_eq!(
        updated_table.metadata().unwrap().name.as_ref().unwrap(),
        initial_name
    );

    // Update the name
    let new_name = "updated_table_name";
    let mut new_properties = HashMap::new();
    new_properties.insert("name".to_string(), new_name.to_string());

    let final_table = DeltaOps(updated_table)
        .set_tbl_properties()
        .with_properties(new_properties)
        .await
        .unwrap();

    assert_eq!(final_table.version(), 2);
    assert_eq!(
        final_table.metadata().unwrap().name.as_ref().unwrap(),
        new_name
    );
}

#[tokio::test]
async fn test_update_existing_table_description() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    // Set initial description
    let initial_description = "Initial table description";
    let mut properties = HashMap::new();
    properties.insert("description".to_string(), initial_description.to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    assert_eq!(updated_table.version(), 1);
    assert_eq!(
        updated_table
            .metadata()
            .unwrap()
            .description
            .as_ref()
            .unwrap(),
        initial_description
    );

    // Update the description
    let new_description = "Updated table description with more details";
    let mut new_properties = HashMap::new();
    new_properties.insert("description".to_string(), new_description.to_string());

    let final_table = DeltaOps(updated_table)
        .set_tbl_properties()
        .with_properties(new_properties)
        .await
        .unwrap();

    assert_eq!(final_table.version(), 2);
    assert_eq!(
        final_table
            .metadata()
            .unwrap()
            .description
            .as_ref()
            .unwrap(),
        new_description
    );
}

#[tokio::test]
async fn test_set_table_properties_with_other_config() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let name = "test_table";
    let description = "Test table description";
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), name.to_string());
    properties.insert("description".to_string(), description.to_string());
    properties.insert("delta.enableChangeDataFeed".to_string(), "true".to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(metadata.name.as_ref().unwrap(), name);
    assert_eq!(metadata.description.as_ref().unwrap(), description);
    assert_eq!(
        metadata.configuration.get("delta.enableChangeDataFeed"),
        Some(&Some("true".to_string()))
    );
    assert_eq!(updated_table.version(), 1);
}

#[tokio::test]
async fn test_empty_table_name() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let mut properties = HashMap::new();
    properties.insert("name".to_string(), "".to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(metadata.name.as_ref().unwrap(), "");
    assert_eq!(updated_table.version(), 1);
}

#[tokio::test]
async fn test_empty_table_description() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let mut properties = HashMap::new();
    properties.insert("description".to_string(), "".to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(metadata.description.as_ref().unwrap(), "");
    assert_eq!(updated_table.version(), 1);
}
