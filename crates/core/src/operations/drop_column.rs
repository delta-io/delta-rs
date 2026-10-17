//! Drop one or more columns from a table.
//!
//! This implements the equivalent of `ALTER TABLE <table> DROP COLUMNS (<names>)`.
//!
//! Dropping a column is a metadata-only operation: the field is removed from the logical
//! schema while the existing data files keep their (now unreferenced) physical column. That
//! is only sound when [column mapping] is enabled, because column mapping is what decouples
//! the logical column name from the physical name stored in the parquet files. On tables
//! without column mapping the drop is rejected rather than silently rewriting every data
//! file.
//!
//! Only top-level columns are supported; dropping a field nested inside a struct is not
//! implemented yet.
//!
//! [column mapping]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DFSchema, ToDFSchema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::schema::StructType;
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use futures::future::BoxFuture;

use super::{CustomExecuteHandler, Operation};
use crate::delta_datafusion::create_session;
use crate::delta_datafusion::expr::{
    parse_generated_column_expression, parse_predicate_expression,
};
use crate::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _, SnapshotMetadataRef, StructTypeExt,
    resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::config::TablePropertiesExt as _;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Drop one or more top-level columns from a column-mapped table.
pub struct DropColumnsBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// The names of the columns to drop
    column_names: Vec<String>,
    /// Raise if a requested column doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// Datafusion session used to parse constraint / generated column expressions
    session: Option<Arc<dyn Session>>,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for DropColumnsBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl DropColumnsBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            column_names: Vec::new(),
            raise_if_not_exists: true,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
            session: None,
            custom_execute_handler: None,
        }
    }

    /// Specify the columns to drop
    pub fn with_columns(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.column_names = names.into_iter().map(Into::into).collect();
        self
    }

    /// Specify if the operation should error when a requested column does not exist
    pub fn with_raise_if_not_exists(mut self, raise: bool) -> Self {
        self.raise_if_not_exists = raise;
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// The Datafusion session state to use
    pub fn with_session(mut self, session: Arc<dyn Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

/// Return the set of column names referenced by a SQL expression.
///
/// The expression is parsed against the *pre-drop* schema so that parsing always succeeds;
/// the referenced columns are then compared against the columns being dropped. Relying on a
/// parse failure instead would be sensitive to the shape of the expression and would produce
/// a much worse error message.
fn referenced_columns(expr: &datafusion::logical_expr::Expr) -> HashSet<String> {
    expr.column_refs()
        .into_iter()
        .map(|col| col.name.clone())
        .collect()
}

/// Reject the drop if any check constraint, invariant, or generated column still references
/// one of the columns being dropped.
fn ensure_no_dependents(
    snapshot: &SnapshotMetadataRef<'_>,
    dropped: &HashSet<String>,
    df_schema: &DFSchema,
    session: &dyn Session,
) -> DeltaResult<()> {
    let table_configuration = &snapshot.table_configuration;

    let conflict = |kind: &str, name: &str, column: &str| {
        DeltaTableError::Generic(format!(
            "Cannot drop column '{column}': it is referenced by the {kind} '{name}'. \
             Remove or update the {kind} first."
        ))
    };

    if table_configuration.is_feature_enabled(&TableFeature::CheckConstraints) {
        for constraint in table_configuration.table_properties().get_constraints() {
            let expr = parse_predicate_expression(df_schema, &constraint.expr, session)?;
            if let Some(column) = referenced_columns(&expr).intersection(dropped).next() {
                return Err(conflict("check constraint", &constraint.name, column));
            }
        }
    }

    if table_configuration.is_feature_enabled(&TableFeature::Invariants) {
        for invariant in table_configuration.logical_schema().get_invariants()? {
            let expr = parse_predicate_expression(df_schema, &invariant.invariant_sql, session)?;
            // An invariant declared *on* a dropped column disappears with it, but an
            // invariant on a surviving column may not reference one that is going away.
            if dropped.contains(&invariant.field_name) {
                continue;
            }
            if let Some(column) = referenced_columns(&expr).intersection(dropped).next() {
                return Err(conflict("invariant on", &invariant.field_name, column));
            }
        }
    }

    if table_configuration.is_feature_enabled(&TableFeature::GeneratedColumns) {
        for generated in table_configuration
            .logical_schema()
            .get_generated_columns()?
        {
            // Dropping a generated column itself is fine - its expression goes with it.
            if dropped.contains(&generated.name) {
                continue;
            }
            let expr = parse_generated_column_expression(df_schema, &generated, session)?;
            if let Some(column) = referenced_columns(&expr).intersection(dropped).next() {
                return Err(conflict("generated column", &generated.name, column));
            }
        }
    }

    Ok(())
}

/// Plan the actions for dropping `column_names`.
///
/// Returns `Ok(None)` when there is nothing to do, which happens when `raise_if_not_exists`
/// is `false` and none of the requested columns exist.
fn plan_drop_columns_actions(
    snapshot: SnapshotMetadataRef<'_>,
    column_names: &[String],
    raise_if_not_exists: bool,
    session: &dyn Session,
) -> DeltaResult<Option<(Vec<Action>, DeltaOperation)>> {
    let table_schema = snapshot.table_configuration.logical_schema();

    // Resolve the requested names against the schema.
    let mut removed = Vec::with_capacity(column_names.len());
    for name in column_names {
        match table_schema.field(name) {
            Some(field) => removed.push(field.clone()),
            None if raise_if_not_exists => {
                return Err(DeltaTableError::Generic(format!(
                    "No column with the name '{name}' in the schema"
                )));
            }
            None => {}
        }
    }
    if removed.is_empty() {
        return Ok(None);
    }

    let dropped: HashSet<String> = removed.iter().map(|f| f.name.clone()).collect();

    // A table must retain at least one column - `CREATE TABLE` rejects an empty schema, so a
    // drop must not be able to produce one either.
    if dropped.len() == table_schema.fields().count() {
        return Err(DeltaTableError::Generic(
            "Cannot drop all columns of a table; a table must have at least one column".to_string(),
        ));
    }

    // Partition columns are encoded in the data file paths, so they cannot be dropped.
    let metadata = snapshot.metadata.clone();
    for column in metadata.partition_columns() {
        if dropped.contains(column) {
            return Err(DeltaTableError::Generic(format!(
                "Cannot drop column '{column}': it is a partition column"
            )));
        }
    }

    // Stats are collected for an explicit column list when this property is set; dropping a
    // listed column would leave a stale reference behind.
    if let Some(stats_columns) = snapshot
        .table_configuration
        .table_properties()
        .data_skipping_stats_columns
        .as_ref()
    {
        for stats_column in stats_columns {
            // `ColumnName` renders as a (possibly backtick-quoted) dotted path.
            let rendered = stats_column.to_string();
            let column = rendered.trim_matches('`');
            if dropped.contains(column) {
                return Err(DeltaTableError::Generic(format!(
                    "Cannot drop column '{column}': it is listed in \
                     delta.dataSkippingStatsColumns. Update that property first."
                )));
            }
        }
    }

    let arrow_schema: datafusion::arrow::datatypes::Schema =
        table_schema.as_ref().try_into_arrow()?;
    let df_schema = arrow_schema.to_dfschema()?;
    ensure_no_dependents(&snapshot, &dropped, &df_schema, session)?;

    // Keep the surviving fields exactly as they are so their column mapping metadata
    // (physical name and field id) carries over untouched. `delta.columnMapping.maxColumnId`
    // is deliberately left alone: it must never be decremented or reused, so that a future
    // `ADD COLUMN` cannot collide with a dropped column's physical id.
    let updated_table_schema = StructType::try_new(
        table_schema
            .fields()
            .filter(|f| !dropped.contains(&f.name))
            .cloned(),
    )?;

    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&updated_table_schema)?
        .move_table_properties_into_features(metadata.configuration());

    let operation = DeltaOperation::DropColumns { columns: removed };

    let metadata = metadata.with_schema(&updated_table_schema)?;

    let mut actions = vec![metadata.into()];
    if current_protocol != &new_protocol {
        actions.push(new_protocol.into())
    }

    Ok(Some((actions, operation)))
}

impl std::future::IntoFuture for DropColumnsBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), false, None).await?;
            PROTOCOL.can_write_to(&snapshot)?;

            if this.column_names.is_empty() {
                return Err(DeltaTableError::Generic(
                    "No columns provided to drop".to_string(),
                ));
            }
            if let Some(name) = this.column_names.iter().find(|name| name.contains('.')) {
                return Err(DeltaTableError::Generic(format!(
                    "Cannot drop nested column '{name}': only top-level columns can be dropped"
                )));
            }

            if snapshot
                .snapshot()
                .metadata_state()
                .table_configuration
                .column_mapping_mode()
                == ColumnMappingMode::None
            {
                return Err(DeltaTableError::column_mapping_required_for_drop(
                    &this.column_names,
                ));
            }

            let session = this
                .session
                .clone()
                .unwrap_or_else(|| Arc::new(create_session().into_inner().state()));

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let Some((actions, operation)) = plan_drop_columns_actions(
                snapshot.snapshot().metadata_state(),
                &this.column_names,
                this.raise_if_not_exists,
                session.as_ref(),
            )?
            else {
                return Ok(DeltaTable::new_with_state(
                    this.log_store,
                    DeltaTableState::new(snapshot),
                ));
            };

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
    use crate::table::config::TableProperty;
    use crate::writer::test_utils::TestResult;

    use super::*;

    fn id_field() -> StructField {
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true)
    }

    fn value_field() -> StructField {
        StructField::new("value", DataType::Primitive(PrimitiveType::String), true)
    }

    fn legacy_field() -> StructField {
        StructField::new("legacy", DataType::Primitive(PrimitiveType::String), true)
    }

    /// A table with column mapping enabled, which is the only kind that supports dropping.
    async fn column_mapped_table() -> DeltaResult<DeltaTable> {
        DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field(), value_field(), legacy_field()])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await
    }

    #[tokio::test]
    async fn drop_columns_removes_field_and_keeps_the_rest() -> TestResult {
        let table = column_mapped_table().await?;
        let table = table.drop_columns().with_columns(["legacy"]).await?;

        let schema = table.snapshot()?.schema();
        assert!(schema.field("legacy").is_none(), "legacy should be dropped");
        assert!(schema.field("id").is_some());
        assert!(schema.field("value").is_some());

        let history: Vec<_> = table.history(Some(1)).await?.collect();
        assert_eq!(history[0].operation.as_deref(), Some("DROP COLUMNS"));
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_preserves_physical_names_of_survivors() -> TestResult {
        let table = column_mapped_table().await?;
        let before = table.snapshot()?.schema();
        let id_physical = before
            .field("id")
            .unwrap()
            .metadata
            .get("delta.columnMapping.physicalName")
            .cloned();
        assert!(
            id_physical.is_some(),
            "column mapping should have assigned a physical name"
        );
        let max_column_id_before = table
            .snapshot()?
            .metadata()
            .configuration()
            .get("delta.columnMapping.maxColumnId")
            .cloned();

        let table = table.drop_columns().with_columns(["legacy"]).await?;

        let after = table.snapshot()?.schema();
        assert_eq!(
            after
                .field("id")
                .unwrap()
                .metadata
                .get("delta.columnMapping.physicalName")
                .cloned(),
            id_physical,
            "surviving columns must keep their physical name"
        );
        // The high-water mark must never be decremented or reused, so a later ADD COLUMN
        // cannot collide with the dropped column's physical id.
        assert_eq!(
            table
                .snapshot()?
                .metadata()
                .configuration()
                .get("delta.columnMapping.maxColumnId")
                .cloned(),
            max_column_id_before
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_can_remove_several_in_one_commit() -> TestResult {
        let table = column_mapped_table().await?;
        let version_before = table.version();

        let table = table
            .drop_columns()
            .with_columns(["value", "legacy"])
            .await?;

        let schema = table.snapshot()?.schema();
        assert!(schema.field("value").is_none());
        assert!(schema.field("legacy").is_none());
        assert!(schema.field("id").is_some());
        assert_eq!(table.version(), version_before.map(|v| v + 1));
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_requires_column_mapping() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field(), value_field()])
            .await?;

        let err = table
            .drop_columns()
            .with_columns(["value"])
            .await
            .unwrap_err();

        assert!(
            matches!(err, DeltaTableError::ColumnMappingRequiredForDrop { .. }),
            "expected ColumnMappingRequiredForDrop, got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_nested_paths() -> TestResult {
        let table = column_mapped_table().await?;

        let err = table
            .drop_columns()
            .with_columns(["value.nested"])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("only top-level columns"));
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_unknown_column() -> TestResult {
        let table = column_mapped_table().await?;

        let err = table
            .drop_columns()
            .with_columns(["does_not_exist"])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("does_not_exist"));
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_unknown_column_is_noop_when_not_raising() -> TestResult {
        let table = column_mapped_table().await?;
        let version_before = table.version();

        let table = table
            .drop_columns()
            .with_columns(["does_not_exist"])
            .with_raise_if_not_exists(false)
            .await?;

        assert_eq!(table.version(), version_before, "should not have committed");
        assert!(table.snapshot()?.schema().field("id").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_dropping_every_column() -> TestResult {
        let table = column_mapped_table().await?;

        let err = table
            .drop_columns()
            .with_columns(["id", "value", "legacy"])
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("at least one column"),
            "got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_partition_column() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field(), value_field()])
            .with_partition_columns(["value"])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await?;

        let err = table
            .drop_columns()
            .with_columns(["value"])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("partition column"), "got: {err}");
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_column_used_by_constraint() -> TestResult {
        let table = column_mapped_table().await?;
        let table = table
            .add_constraint()
            .with_constraint("value_not_empty", "value != ''")
            .await?;

        let err = table
            .drop_columns()
            .with_columns(["value"])
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("value_not_empty"),
            "error should name the offending constraint, got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_column_in_stats_columns() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field(), value_field()])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .with_configuration_property(TableProperty::DataSkippingStatsColumns, Some("id,value"))
            .await?;

        let err = table
            .drop_columns()
            .with_columns(["value"])
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("dataSkippingStatsColumns"),
            "got: {err}"
        );
        Ok(())
    }
}
