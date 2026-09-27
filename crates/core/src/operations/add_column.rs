//! Add a new column to a table

use std::sync::Arc;

use delta_kernel::schema::StructType;
use delta_kernel::table_features::ColumnMappingMode;
use futures::future::BoxFuture;
use itertools::Itertools;

use super::{CustomExecuteHandler, Operation};
use crate::errors::ColumnMappingOperation;
use crate::kernel::schema::merge_delta_struct;
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt, ProtocolExt as _, SnapshotMetadataRef, StructField,
    StructTypeExt, resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Add new columns and/or nested fields to a table
pub struct AddColumnBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// Fields to add/merge into schema
    fields: Option<Vec<StructField>>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl Operation for AddColumnBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl AddColumnBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            fields: None,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the fields to be added
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = StructField> + Clone) -> Self {
        self.fields = Some(fields.into_iter().collect());
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

fn plan_add_column_actions(
    snapshot: SnapshotMetadataRef<'_>,
    fields: Vec<StructField>,
) -> DeltaResult<(Vec<Action>, DeltaOperation)> {
    let fields_right = &StructType::try_new(fields.clone())?;

    if !fields_right
        .get_generated_columns()
        .unwrap_or_default()
        .is_empty()
    {
        return Err(DeltaTableError::Generic(
            "New columns cannot be a generated column".to_string(),
        ));
    }

    let table_schema = snapshot.table_configuration.logical_schema();
    let new_table_schema = merge_delta_struct(table_schema.as_ref(), fields_right)?;

    let operation = DeltaOperation::AddColumn {
        fields: fields.into_iter().collect_vec(),
    };

    Ok((
        schema_evolution_actions(&snapshot, &new_table_schema)?,
        operation,
    ))
}

/// Metadata action for `new_table_schema`, plus a protocol action if the new
/// schema requires table features the current protocol does not have.
fn schema_evolution_actions(
    snapshot: &SnapshotMetadataRef<'_>,
    new_table_schema: &StructType,
) -> DeltaResult<Vec<Action>> {
    let metadata = snapshot.metadata.clone();
    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(new_table_schema)?
        .move_table_properties_into_features(metadata.configuration());

    let mut actions = vec![metadata.with_schema(new_table_schema)?.into()];

    if current_protocol != &new_protocol {
        actions.push(new_protocol.into())
    }

    Ok(actions)
}

/// Actions that merge `schema` into the schema of `snapshot`, for committing
/// alongside file actions that were written with the merged schema.
///
/// New fields are appended and existing fields are widened the same way as
/// [`AddColumnBuilder`]; the protocol is upgraded if the merged schema needs
/// additional table features. Returns no actions if the merged schema equals
/// the current table schema.
///
/// Fails for tables with column mapping enabled, for incompatible field types,
/// and for new fields that are generated columns.
pub fn merge_schema_actions(
    snapshot: &EagerSnapshot,
    schema: &StructType,
) -> DeltaResult<Vec<Action>> {
    let state = snapshot.snapshot().metadata_state();
    if state.table_configuration.column_mapping_mode() != ColumnMappingMode::None {
        return Err(DeltaTableError::unsupported_column_mapping(
            ColumnMappingOperation::Write,
            "schema merge",
        ));
    }

    let table_schema = state.table_configuration.logical_schema();
    let merged = merge_delta_struct(table_schema.as_ref(), schema)?;
    if &merged == table_schema.as_ref() {
        return Ok(Vec::new());
    }

    let new_fields = StructType::try_new(
        merged
            .fields()
            .filter(|field| table_schema.field(field.name()).is_none())
            .cloned(),
    )?;
    if !new_fields
        .get_generated_columns()
        .unwrap_or_default()
        .is_empty()
    {
        return Err(DeltaTableError::Generic(
            "New columns cannot be a generated column".to_string(),
        ));
    }

    schema_evolution_actions(&state, &merged)
}

impl std::future::IntoFuture for AddColumnBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot = resolve_snapshot(&this.log_store, this.snapshot.clone(), None).await?;
            if snapshot
                .snapshot()
                .metadata_state()
                .table_configuration
                .column_mapping_mode()
                != ColumnMappingMode::None
            {
                return Err(DeltaTableError::unsupported_column_mapping(
                    ColumnMappingOperation::Write,
                    "ADD COLUMN",
                ));
            }

            let fields = match this.fields.clone() {
                Some(v) => v,
                None => return Err(DeltaTableError::Generic("No fields provided".to_string())),
            };
            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let (actions, operation) =
                plan_add_column_actions(snapshot.snapshot().metadata_state(), fields)?;

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
    use delta_kernel::schema::DataType;

    use super::*;

    fn base_fields() -> Vec<StructField> {
        vec![
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ]
    }

    async fn base_table() -> DeltaTable {
        DeltaTable::new_in_memory()
            .create()
            .with_columns(base_fields())
            .await
            .unwrap()
    }

    fn merge(table: &DeltaTable, fields: Vec<StructField>) -> DeltaResult<Vec<Action>> {
        merge_schema_actions(
            table.snapshot().unwrap().snapshot(),
            &StructType::try_new(fields).unwrap(),
        )
    }

    #[tokio::test]
    async fn test_merge_schema_actions_adds_new_column() {
        let table = base_table().await;
        let mut fields = base_fields();
        fields.push(StructField::nullable("extra", DataType::STRING));

        let actions = merge(&table, fields).unwrap();

        let [Action::Metadata(metadata)] = actions.as_slice() else {
            panic!("expected a single metadata action, got {actions:?}");
        };
        let schema = metadata.parse_schema().unwrap();
        let names: Vec<_> = schema.fields().map(|field| field.name().as_str()).collect();
        assert_eq!(names, ["id", "name", "extra"]);
    }

    #[tokio::test]
    async fn test_merge_schema_actions_unchanged_schema_is_noop() {
        let table = base_table().await;

        assert!(merge(&table, base_fields()).unwrap().is_empty());
        assert!(
            merge(&table, vec![StructField::nullable("id", DataType::LONG)])
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_merge_schema_actions_upgrades_protocol_for_timestamp_ntz() {
        let table = base_table().await;
        let mut fields = base_fields();
        fields.push(StructField::nullable("ts", DataType::TIMESTAMP_NTZ));

        let actions = merge(&table, fields).unwrap();

        assert!(matches!(
            actions.as_slice(),
            [Action::Metadata(_), Action::Protocol(_)]
        ));
    }

    #[tokio::test]
    async fn test_merge_schema_actions_rejects_incompatible_type() {
        let table = base_table().await;
        let fields = vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("name", DataType::STRING),
        ];

        assert!(matches!(
            merge(&table, fields),
            Err(DeltaTableError::Arrow { .. })
        ));
    }

    #[tokio::test]
    async fn test_merge_schema_actions_rejects_new_generated_column() {
        let table = base_table().await;
        let schema: StructType = serde_json::from_value(serde_json::json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": true, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}},
                {"name": "gc", "type": "long", "nullable": true,
                 "metadata": {"delta.generationExpression": "id * 2"}}
            ]
        }))
        .unwrap();

        let result = merge_schema_actions(table.snapshot().unwrap().snapshot(), &schema);

        assert!(matches!(result, Err(DeltaTableError::Generic(_))));
    }
}
//! Add a new column to a table

use std::sync::Arc;

use delta_kernel::schema::StructType;
use delta_kernel::table_features::ColumnMappingMode;
use futures::future::BoxFuture;
use itertools::Itertools;

use super::{CustomExecuteHandler, Operation};
use crate::errors::ColumnMappingOperation;
use crate::kernel::schema::merge_delta_struct;
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt, ProtocolExt as _, SnapshotMetadataRef, StructField,
    StructTypeExt, resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Add new columns and/or nested fields to a table
pub struct AddColumnBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// Fields to add/merge into schema
    fields: Option<Vec<StructField>>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl Operation for AddColumnBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl AddColumnBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            fields: None,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the fields to be added
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = StructField> + Clone) -> Self {
        self.fields = Some(fields.into_iter().collect());
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

fn plan_add_column_actions(
    snapshot: SnapshotMetadataRef<'_>,
    fields: Vec<StructField>,
) -> DeltaResult<(Vec<Action>, DeltaOperation)> {
    let mut metadata = snapshot.metadata.clone();
    let fields_right = &StructType::try_new(fields.clone())?;

    if !fields_right
        .get_generated_columns()
        .unwrap_or_default()
        .is_empty()
    {
        return Err(DeltaTableError::Generic(
            "New columns cannot be a generated column".to_string(),
        ));
    }

    let table_schema = snapshot.table_configuration.logical_schema();
    let new_table_schema = merge_delta_struct(table_schema.as_ref(), fields_right)?;

    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&new_table_schema)?
        .move_table_properties_into_features(metadata.configuration());

    let operation = DeltaOperation::AddColumn {
        fields: fields.into_iter().collect_vec(),
    };

    metadata = metadata.with_schema(&new_table_schema)?;

    let mut actions = vec![metadata.into()];

    if current_protocol != &new_protocol {
        actions.push(new_protocol.into())
    }

    Ok((actions, operation))
}

impl std::future::IntoFuture for AddColumnBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot = resolve_snapshot(&this.log_store, this.snapshot.clone(), None).await?;
            if snapshot
                .snapshot()
                .metadata_state()
                .table_configuration
                .column_mapping_mode()
                != ColumnMappingMode::None
            {
                return Err(DeltaTableError::unsupported_column_mapping(
                    ColumnMappingOperation::Write,
                    "ADD COLUMN",
                ));
            }

            let fields = match this.fields.clone() {
                Some(v) => v,
                None => return Err(DeltaTableError::Generic("No fields provided".to_string())),
            };
            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let (actions, operation) =
                plan_add_column_actions(snapshot.snapshot().metadata_state(), fields)?;

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
