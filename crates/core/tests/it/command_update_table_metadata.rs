use deltalake_core::DeltaTable;
use deltalake_core::kernel::StructType;
use deltalake_core::kernel::transaction::CommitProperties;
use deltalake_core::operations::update_table_metadata::TableMetadataUpdate;
use serde_json::json;

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
async fn test_update_table_name_valid() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let name = "test_table_name";
    let update = TableMetadataUpdate {
        name: Some(name.to_string()),
        description: None,
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    let metadata = updated_table.snapshot().unwrap().metadata();
    assert_eq!(metadata.name().unwrap(), name);
    assert_eq!(updated_table.version(), Some(1));
}

#[tokio::test]
async fn test_update_table_description_valid() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let description = "This is a test table description";
    let update = TableMetadataUpdate {
        name: None,
        description: Some(description.to_string()),
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    let metadata = updated_table.snapshot().unwrap().metadata();
    assert_eq!(metadata.description().unwrap(), description);
    assert_eq!(updated_table.version(), Some(1));
}

#[tokio::test]
async fn test_update_table_name_character_limit_valid() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let name_255_chars = "x".repeat(255);
    let update = TableMetadataUpdate {
        name: Some(name_255_chars.clone()),
        description: None,
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    let metadata = updated_table.snapshot().unwrap().metadata();
    assert_eq!(metadata.name().unwrap(), &name_255_chars);
    assert_eq!(updated_table.version(), Some(1));
}

#[tokio::test]
async fn test_update_table_name_character_limit_exceeded() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let name_256_chars = "x".repeat(256);
    let update = TableMetadataUpdate {
        name: Some(name_256_chars),
        description: None,
    };
    let result = table.update_table_metadata().with_update(update).await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Table name cannot be empty and cannot exceed 255 characters"));
}

#[tokio::test]
async fn test_update_table_description_character_limit_valid() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let description_4000_chars = "y".repeat(4000);
    let update = TableMetadataUpdate {
        name: None,
        description: Some(description_4000_chars.clone()),
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    let metadata = updated_table.snapshot().unwrap().metadata();
    assert_eq!(metadata.description().unwrap(), &description_4000_chars);
    assert_eq!(updated_table.version(), Some(1));
}

#[tokio::test]
async fn test_update_table_description_character_limit_exceeded() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let description_4001_chars = "y".repeat(4001);
    let update = TableMetadataUpdate {
        name: None,
        description: Some(description_4001_chars),
    };
    let result = table.update_table_metadata().with_update(update).await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Table description cannot exceed 4000 characters"));
}

#[tokio::test]
async fn test_update_existing_table_name() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let initial_name = "initial_table_name";
    let update = TableMetadataUpdate {
        name: Some(initial_name.to_string()),
        description: None,
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    assert_eq!(updated_table.version(), Some(1));
    assert_eq!(
        updated_table.snapshot().unwrap().metadata().name().unwrap(),
        initial_name
    );

    let new_name = "updated_table_name";
    let update = TableMetadataUpdate {
        name: Some(new_name.to_string()),
        description: None,
    };
    let final_table = updated_table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    assert_eq!(final_table.version(), Some(2));
    assert_eq!(
        final_table.snapshot().unwrap().metadata().name().unwrap(),
        new_name
    );
}

#[tokio::test]
async fn test_update_existing_table_description() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let initial_description = "Initial table description";
    let update = TableMetadataUpdate {
        name: None,
        description: Some(initial_description.to_string()),
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    assert_eq!(updated_table.version(), Some(1));
    assert_eq!(
        updated_table
            .snapshot()
            .unwrap()
            .metadata()
            .description()
            .unwrap(),
        initial_description
    );

    let new_description = "Updated table description with more details";
    let update = TableMetadataUpdate {
        name: None,
        description: Some(new_description.to_string()),
    };
    let final_table = updated_table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    assert_eq!(final_table.version(), Some(2));
    assert_eq!(
        final_table
            .snapshot()
            .unwrap()
            .metadata()
            .description()
            .unwrap(),
        new_description
    );
}

#[tokio::test]
async fn test_empty_table_name() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let update = TableMetadataUpdate {
        name: Some("".to_string()),
        description: None,
    };
    let result = table.update_table_metadata().with_update(update).await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Table name cannot be empty"));
}

#[tokio::test]
async fn test_empty_table_description() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let update = TableMetadataUpdate {
        name: None,
        description: Some("".to_string()),
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .await
        .unwrap();

    let metadata = updated_table.snapshot().unwrap().metadata();
    assert_eq!(metadata.description().unwrap(), "");
    assert_eq!(updated_table.version(), Some(1));
}

#[tokio::test]
async fn test_no_update_specified() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let result = table.update_table_metadata().await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("No metadata update specified"));
}

#[tokio::test]
async fn test_with_commit_properties() {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let commit_properties = CommitProperties::default().with_metadata([
        ("app_id".to_string(), json!("test-app")),
        ("test".to_string(), json!("metadata")),
    ]);

    let name = "test_table_with_commit_props";
    let update = TableMetadataUpdate {
        name: Some(name.to_string()),
        description: None,
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .with_commit_properties(commit_properties)
        .await
        .unwrap();

    let metadata = updated_table.snapshot().unwrap().metadata();
    assert_eq!(metadata.name().unwrap(), name);
    assert_eq!(updated_table.version(), Some(1));
}

#[tokio::test]
async fn test_with_custom_execute_handler() {
    use async_trait::async_trait;
    use deltalake_core::DeltaResult;
    use deltalake_core::logstore::LogStoreRef;
    use deltalake_core::operations::CustomExecuteHandler;
    use std::sync::Arc;

    #[derive(Debug)]
    struct MockExecuteHandler {
        pre_execute_called: std::sync::atomic::AtomicBool,
        post_execute_called: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl CustomExecuteHandler for MockExecuteHandler {
        async fn pre_execute(
            &self,
            _log_store: &LogStoreRef,
            _operation_id: uuid::Uuid,
        ) -> DeltaResult<()> {
            self.pre_execute_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn post_execute(
            &self,
            _log_store: &LogStoreRef,
            _operation_id: uuid::Uuid,
        ) -> DeltaResult<()> {
            self.post_execute_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn before_post_commit_hook(
            &self,
            _log_store: &LogStoreRef,
            _file_operation: bool,
            _operation_id: uuid::Uuid,
        ) -> DeltaResult<()> {
            Ok(())
        }

        async fn after_post_commit_hook(
            &self,
            _log_store: &LogStoreRef,
            _file_operation: bool,
            _operation_id: uuid::Uuid,
        ) -> DeltaResult<()> {
            Ok(())
        }
    }

    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let handler = Arc::new(MockExecuteHandler {
        pre_execute_called: std::sync::atomic::AtomicBool::new(false),
        post_execute_called: std::sync::atomic::AtomicBool::new(false),
    });

    let name = "test_table_with_handler";
    let update = TableMetadataUpdate {
        name: Some(name.to_string()),
        description: None,
    };
    let updated_table = table
        .update_table_metadata()
        .with_update(update)
        .with_custom_execute_handler(handler.clone())
        .await
        .unwrap();

    let metadata = updated_table.snapshot().unwrap().metadata();
    assert_eq!(metadata.name().unwrap(), name);
    assert_eq!(updated_table.version(), Some(1));

    assert!(
        handler
            .pre_execute_called
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert!(
        handler
            .post_execute_called
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}
