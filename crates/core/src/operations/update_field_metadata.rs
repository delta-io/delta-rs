//! Update metadata on a field in a schema

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::schema::{MetadataValue, StructType};
use futures::future::BoxFuture;
use itertools::Itertools;

use super::{CustomExecuteHandler, Operation};
use crate::DeltaTable;
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _, SnapshotMetadataRef,
    resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::{DeltaResult, DeltaTableError};

/// Update a field's metadata in a schema. If the key does not exists, the entry is inserted.
pub struct UpdateFieldMetadataBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// The name of the field where the metadata may be updated
    field_name: String,
    /// HashMap of the metadata to upsert
    metadata: HashMap<String, MetadataValue>,
    /// When set, also update the field's `nullable` flag in the schema
    nullable: Option<bool>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for UpdateFieldMetadataBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl UpdateFieldMetadataBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            metadata: HashMap::new(),
            field_name: String::new(),
            nullable: None,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the field you want to update the metadata for
    pub fn with_field_name(mut self, field_name: &str) -> Self {
        self.field_name = field_name.into();
        self
    }

    /// Specify the metadata to be added or modified on a field
    pub fn with_metadata(mut self, metadata: HashMap<String, MetadataValue>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Also update the field's `nullable` flag in the table schema.
    ///
    /// Relaxing (`false` → `true`) is always safe. Tightening (`true` → `false`) is only valid
    /// when the column contains no NULL values — the caller is responsible for verifying this
    /// before committing; this operation does not scan the data.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = Some(nullable);
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

fn plan_update_field_metadata_actions(
    snapshot: SnapshotMetadataRef<'_>,
    field_name: &str,
    metadata_update: HashMap<String, MetadataValue>,
    nullable: Option<bool>,
) -> DeltaResult<(Vec<Action>, DeltaOperation)> {
    let table_schema = snapshot.table_configuration.logical_schema();

    let Some(field) = table_schema.field(field_name) else {
        return Err(DeltaTableError::Generic(
            "No field with the provided name in the schema".to_string(),
        ));
    };
    let mut field = field.clone();

    for key in metadata_update.keys() {
        if key.starts_with("delta.") {
            return Err(DeltaTableError::Generic(
                "Not allowed to modify protected metadata e.g. `delta.columnMapping.id`"
                    .to_string(),
            ));
        }
    }

    metadata_update.into_iter().for_each(|(key, value)| {
        field
            .metadata
            .entry(key)
            .and_modify(|meta| {
                *meta = value.clone();
            })
            .or_insert(value);
    });

    // Apply the nullability change, if requested (see `with_nullable` for the contract)
    if let Some(nullable) = nullable {
        field.nullable = nullable;
    }

    let updated_table_schema =
        StructType::try_new(table_schema.fields().map(|f| match f.name == field.name {
            true => field.clone(),
            false => f.clone(),
        }))?;

    let mut metadata = snapshot.metadata.clone();

    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&updated_table_schema)?
        .move_table_properties_into_features(metadata.configuration());

    let operation = DeltaOperation::UpdateFieldMetadata {
        fields: updated_table_schema.fields().cloned().collect_vec(),
    };

    metadata = metadata.with_schema(&updated_table_schema)?;

    let mut actions = vec![metadata.into()];

    if current_protocol != &new_protocol {
        actions.push(new_protocol.into())
    }

    Ok((actions, operation))
}

impl std::future::IntoFuture for UpdateFieldMetadataBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), false, None).await?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let (actions, operation) = plan_update_field_metadata_actions(
                snapshot.snapshot().metadata_state(),
                &this.field_name,
                this.metadata.clone(),
                this.nullable,
            )?;

            let commit = CommitBuilder::from(this.commit_properties.clone())
                .with_actions(actions)
                .with_operation_id(operation_id)
                .with_post_commit_hook_handler(this.get_custom_execute_handler())
                .build(Some(&snapshot), this.log_store.clone(), operation)
                .await?;

            this.post_execute(operation_id).await?;

            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::{DataType, PrimitiveType, StructField};
    use crate::{DeltaTableConfig, writer::test_utils::TestResult};

    use super::*;

    fn id_field() -> StructField {
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true)
    }

    fn field_metadata() -> HashMap<String, MetadataValue> {
        HashMap::from([(
            "comment".to_string(),
            MetadataValue::String("identifier".to_string()),
        )])
    }

    #[tokio::test]
    async fn update_field_metadata_with_lazy_snapshot_does_not_materialize_files() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field()])
            .await?;
        let log_store = table.log_store().clone();
        let config = DeltaTableConfig {
            require_files: false,
            ..Default::default()
        };
        let snapshot = EagerSnapshot::try_new(log_store.as_ref(), config, None).await?;

        assert!(!snapshot.snapshot().has_materialized_files_for_test());

        UpdateFieldMetadataBuilder::new(log_store, Some(snapshot.clone()))
            .with_field_name("id")
            .with_metadata(field_metadata())
            .await?;

        assert!(!snapshot.snapshot().has_materialized_files_for_test());
        Ok(())
    }
}

#[cfg(feature = "datafusion")]
#[cfg(test)]
mod nullable_tests {
    use delta_kernel::schema::MetadataValue;
    use std::collections::HashMap;

    use crate::DeltaResult;
    use crate::writer::test_utils::{create_bare_table, get_record_batch};

    /// `with_nullable(false)` must flip the schema field's `nullable` flag, not just its
    /// metadata map — reporting and NOT NULL enforcement read the flag.
    #[tokio::test]
    async fn with_nullable_updates_the_schema_flag() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch]).await.unwrap();
        assert!(
            table.snapshot()?.schema().field("value").unwrap().nullable,
            "test premise: 'value' starts nullable"
        );

        let table = table
            .update_field_metadata()
            .with_field_name("value")
            .with_nullable(false)
            .await
            .unwrap();

        let field = table.snapshot()?.schema().field("value").unwrap().clone();
        assert!(!field.nullable, "nullable flag updated in the schema");

        // And back: relaxing is always safe.
        let table = table
            .update_field_metadata()
            .with_field_name("value")
            .with_nullable(true)
            .await
            .unwrap();
        assert!(table.snapshot()?.schema().field("value").unwrap().nullable);
        Ok(())
    }

    /// Without `with_nullable`, the operation keeps its metadata-only behavior.
    #[tokio::test]
    async fn metadata_only_update_leaves_nullable_untouched() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch]).await.unwrap();

        let table = table
            .update_field_metadata()
            .with_field_name("value")
            .with_metadata(HashMap::from([(
                "isUnique".to_string(),
                MetadataValue::Boolean(true),
            )]))
            .await
            .unwrap();

        let field = table.snapshot()?.schema().field("value").unwrap().clone();
        assert!(field.nullable, "nullable flag unchanged");
        assert_eq!(
            field.metadata.get("isUnique"),
            Some(&MetadataValue::Boolean(true)),
            "metadata written"
        );
        Ok(())
    }
}
