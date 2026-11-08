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
use delta_kernel::column_trie::ColumnTrie;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::StructType;
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use futures::future::BoxFuture;
use itertools::Itertools as _;

use crate::delta_datafusion::create_session;
use crate::delta_datafusion::expr::{
    parse_generated_column_expression, parse_predicate_expression,
};
use crate::kernel::transaction::{CommitProperties, PROTOCOL};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _, SnapshotMetadataRef, StructTypeExt,
    resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::operations::commit_actions_in_scope;
use crate::protocol::DeltaOperation;
use crate::table::config::{TablePropertiesExt as _, TableProperty};
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

/// Reject the drop if any check constraint, invariant, or generated column that survives it
/// still references one of the columns being dropped. The error lists every such reference,
/// one per line, in sorted order.
///
/// Returns the names of the check constraints that only reference dropped columns; they go
/// away together with those columns.
fn check_dependents(
    snapshot: &SnapshotMetadataRef<'_>,
    dropped: &HashSet<String>,
    updated_schema: &StructType,
    df_schema: &DFSchema,
    session: &dyn Session,
) -> DeltaResult<Vec<String>> {
    let table_configuration = &snapshot.table_configuration;

    let mut conflicts = Vec::new();
    let mut add_conflicts = |dependent: String, referenced: &HashSet<String>| {
        conflicts.extend(referenced.intersection(dropped).map(|column| {
            format!("Cannot drop column '{column}': it is referenced by the {dependent}.")
        }));
    };

    let mut dropped_constraints = Vec::new();
    if table_configuration.is_feature_enabled(&TableFeature::CheckConstraints) {
        for constraint in table_configuration.table_properties().get_constraints() {
            let expr = parse_predicate_expression(df_schema, &constraint.expr, session)?;
            let referenced = referenced_columns(&expr);
            // A constraint that only checks dropped columns goes with them, the same as the
            // expression of a dropped generated column.
            if !referenced.is_empty() && referenced.is_subset(dropped) {
                dropped_constraints.push(constraint.name);
                continue;
            }
            add_conflicts(
                format!("check constraint '{}'", constraint.name),
                &referenced,
            );
        }
    }

    if table_configuration.is_feature_enabled(&TableFeature::Invariants) {
        // An invariant declared *on* a dropped column, or on a field nested inside one,
        // disappears with it, so only the invariants of the resulting schema are checked. An
        // invariant on a surviving column may not reference one that is going away.
        for invariant in updated_schema.get_invariants()? {
            let expr = parse_predicate_expression(df_schema, &invariant.invariant_sql, session)?;
            add_conflicts(
                format!("invariant on '{}'", invariant.field_name),
                &referenced_columns(&expr),
            );
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
            add_conflicts(
                format!("generated column '{}'", generated.name),
                &referenced_columns(&expr),
            );
        }
    }

    if !conflicts.is_empty() {
        conflicts.sort();
        return Err(DeltaTableError::Generic(format!(
            "{}\nRemove or update these references first.",
            conflicts.join("\n")
        )));
    }

    Ok(dropped_constraints)
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

    // Resolve the requested names against the schema. A name can be backtick-quoted the way the
    // history records it (`a.b`), so a quoted name that matches no column as is resolves to the
    // single column it names.
    let mut removed: Vec<String> = Vec::with_capacity(column_names.len());
    for name in column_names {
        let field = table_schema.field(name).or_else(|| {
            // Only a name in backticks is parsed: parsing drops the spaces around a name, and
            // `"legacy "` must not match `legacy`.
            if !name.starts_with('`') {
                return None;
            }
            match name.parse::<ColumnName>().ok()?.path() {
                [part] => table_schema.field(part),
                _ => None,
            }
        });
        match field {
            Some(field) if removed.contains(&field.name) => {}
            Some(field) => removed.push(field.name.clone()),
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

    let dropped: HashSet<String> = removed.iter().cloned().collect();

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

    // A table must retain at least one column - `CREATE TABLE` rejects an empty schema, so a
    // drop must not be able to produce one either. Check the resulting schema rather than
    // count the dropped names, so the check holds however the fields are dropped.
    if updated_table_schema.num_fields() == 0 {
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

    // Stats are collected for an explicit column list when this property is set. Like Spark,
    // remove the dropped columns, and any field nested inside one, from that list. The
    // property is only rewritten when it lists a dropped column.
    let stats_columns = snapshot
        .table_configuration
        .table_properties()
        .data_skipping_stats_columns
        .as_ref()
        .and_then(|columns| {
            let dropped_columns = removed
                .iter()
                .map(|name| ColumnName::new([name]))
                .collect_vec();
            let dropped_trie = ColumnTrie::from_columns(&dropped_columns);
            let kept = columns
                .iter()
                .filter(|column| !dropped_trie.contains_prefix_of(column.path()))
                .collect_vec();
            // `ColumnName`'s `Display` backtick-quotes parts where needed, so the joined list
            // parses back with `ColumnName::parse_column_name_list`, as the kernel reads it.
            (kept.len() < columns.len()).then(|| kept.iter().join(","))
        });

    let arrow_schema: datafusion::arrow::datatypes::Schema =
        table_schema.as_ref().try_into_arrow()?;
    let df_schema = arrow_schema.to_dfschema()?;
    let dropped_constraints = check_dependents(
        &snapshot,
        &dropped,
        &updated_table_schema,
        &df_schema,
        session,
    )?;

    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&updated_table_schema)?
        .move_table_properties_into_features(metadata.configuration());

    // Record the names the way Spark does (`UnresolvedAttribute.name`), which backtick-quotes a
    // name that contains a dot.
    let operation = DeltaOperation::DropColumns {
        columns: removed
            .into_iter()
            .map(|name| {
                if name.contains('.') {
                    format!("`{name}`")
                } else {
                    name
                }
            })
            .collect(),
    };

    let mut metadata = metadata.with_schema(&updated_table_schema)?;
    for name in dropped_constraints {
        metadata = metadata.remove_config_key(&format!("delta.constraints.{name}"))?;
    }
    if let Some(stats_columns) = stats_columns {
        metadata = metadata.add_config_key(
            TableProperty::DataSkippingStatsColumns.as_ref().to_string(),
            stats_columns,
        )?;
    }

    // `apply_column_metadata_to_protocol` and `move_table_properties_into_features` keep the
    // features in sets, so they can return them in a different order. Compare the versions and
    // the feature sets, so an unchanged protocol is not committed again.
    let protocol_changed = current_protocol.min_reader_version()
        != new_protocol.min_reader_version()
        || current_protocol.min_writer_version() != new_protocol.min_writer_version()
        || current_protocol.reader_features_set() != new_protocol.reader_features_set()
        || current_protocol.writer_features_set() != new_protocol.writer_features_set();

    let mut actions = vec![metadata.into()];
    if protocol_changed {
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
            let snapshot = resolve_snapshot(&this.log_store, this.snapshot.clone(), None).await?;
            PROTOCOL.can_write_to(&snapshot)?;

            if this.column_names.is_empty() {
                return Err(DeltaTableError::Generic(
                    "No columns provided to drop".to_string(),
                ));
            }
            // A top-level column name may itself contain a dot, so only treat the name as a
            // nested path when no top-level column matches it and the path resolves to a
            // nested field. Any other name is missing and follows `raise_if_not_exists`.
            let schema = snapshot.schema();
            if let Some(name) = this.column_names.iter().find(|name| {
                schema.field(name).is_none()
                    && name.parse::<ColumnName>().is_ok_and(|column| {
                        column.path().len() > 1 && schema.field_at(&column).is_ok()
                    })
            }) {
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
                return Err(DeltaTableError::column_mapping_required("DROP COLUMNS"));
            }

            let session = this
                .session
                .clone()
                .unwrap_or_else(|| Arc::new(create_session().into_inner().state()));

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

            commit_actions_in_scope(
                &this.log_store,
                &snapshot,
                this.commit_properties,
                actions,
                operation,
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt as _;

    use crate::kernel::{DataType, PrimitiveType, StructField};
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

    /// The column names recorded in the `columns` parameter of the last commit.
    async fn last_dropped_columns(table: &DeltaTable) -> Vec<String> {
        let history: Vec<_> = table.history(Some(1)).try_collect().await.unwrap();
        let columns = &history[0].operation_parameters.as_ref().unwrap()["columns"];
        serde_json::from_str(columns.as_str().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn drop_columns_removes_field_and_keeps_the_rest() -> TestResult {
        let table = column_mapped_table().await?;
        let table = table.drop_columns().with_columns(["legacy"]).await?;

        let schema = table.snapshot()?.schema();
        assert!(schema.field("legacy").is_none(), "legacy should be dropped");
        assert!(schema.field("id").is_some());
        assert!(schema.field("value").is_some());

        let history: Vec<_> = table.history(Some(1)).try_collect().await?;
        assert_eq!(history[0].operation.as_deref(), Some("DROP COLUMNS"));
        assert_eq!(last_dropped_columns(&table).await, ["legacy"]);
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
    async fn drop_columns_does_not_commit_an_unchanged_protocol() -> TestResult {
        // With several features, a feature list in a different order is almost certain.
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field(), value_field(), legacy_field()])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
            .await?;

        let table = table.drop_columns().with_columns(["legacy"]).await?;

        let version = table.version().unwrap();
        let commit = table.log_store().read_commit_entry(version).await?.unwrap();
        let commit = std::str::from_utf8(&commit)?;
        assert!(
            !commit
                .lines()
                .any(|line| line.starts_with(r#"{"protocol""#)),
            "{commit}"
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
            matches!(err, DeltaTableError::ColumnMappingRequired { .. }),
            "expected ColumnMappingRequired, got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_nested_paths() -> TestResult {
        let nested = StructField::new(
            "nested",
            DataType::Struct(Box::new(StructType::try_new([StructField::new(
                "inner",
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])?)),
            true,
        );
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field(), nested])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await?;

        // A nested field that exists is rejected even when missing columns are ignored.
        let err = table
            .drop_columns()
            .with_columns(["nested.inner"])
            .with_raise_if_not_exists(false)
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
    async fn drop_columns_rejects_unknown_nested_path() -> TestResult {
        let table = column_mapped_table().await?;

        let err = table
            .drop_columns()
            .with_columns(["value.nested"])
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("No column with the name 'value.nested'"),
            "got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_unknown_nested_path_is_noop_when_not_raising() -> TestResult {
        let table = column_mapped_table().await?;
        let version_before = table.version();

        let table = table
            .drop_columns()
            .with_columns(["does_not.exist"])
            .with_raise_if_not_exists(false)
            .await?;

        assert_eq!(table.version(), version_before, "should not have committed");
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
    async fn drop_columns_drops_constraint_on_dropped_column() -> TestResult {
        let table = column_mapped_table().await?;
        let table = table
            .add_constraint()
            .with_constraint("value_not_empty", "value != ''")
            .with_constraint("id_positive", "id > 0")
            .await?;

        let table = table.drop_columns().with_columns(["value"]).await?;

        let snapshot = table.snapshot()?;
        let configuration = snapshot.metadata().configuration();
        assert!(!configuration.contains_key("delta.constraints.value_not_empty"));
        assert!(configuration.contains_key("delta.constraints.id_positive"));
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_drops_constraint_on_columns_dropped_together() -> TestResult {
        let table = column_mapped_table().await?;
        let table = table
            .add_constraint()
            .with_constraint("value_differs", "value != legacy")
            .await?;

        let table = table
            .drop_columns()
            .with_columns(["value", "legacy"])
            .await?;

        assert!(
            !table
                .snapshot()?
                .metadata()
                .configuration()
                .contains_key("delta.constraints.value_differs")
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_column_used_by_constraint() -> TestResult {
        let table = column_mapped_table().await?;
        let table = table
            .add_constraint()
            .with_constraint("value_not_empty", "value != '' OR id > 0")
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

    /// A column-mapped table where `doubled` is generated from `id`.
    async fn generated_column_table() -> DeltaResult<DeltaTable> {
        DeltaTable::new_in_memory()
            .create()
            .with_columns([
                id_field(),
                value_field(),
                StructField::new("doubled", DataType::Primitive(PrimitiveType::Integer), true)
                    .with_metadata([("delta.generationExpression", "id * 2")]),
            ])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await
    }

    #[tokio::test]
    async fn drop_columns_rejects_column_used_by_generated_column() -> TestResult {
        let table = generated_column_table().await?;

        let err = table.drop_columns().with_columns(["id"]).await.unwrap_err();

        assert!(
            err.to_string().contains("generated column 'doubled'"),
            "got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_lists_every_reference_in_sorted_order() -> TestResult {
        let table = generated_column_table()
            .await?
            .add_constraint()
            .with_constraint("positive", "id > 0 OR value != '' OR doubled > 0")
            .await?;

        let err = table
            .drop_columns()
            .with_columns(["value", "id"])
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Generic DeltaTable error: \
             Cannot drop column 'id': it is referenced by the check constraint 'positive'.\n\
             Cannot drop column 'id': it is referenced by the generated column 'doubled'.\n\
             Cannot drop column 'value': it is referenced by the check constraint 'positive'.\n\
             Remove or update these references first."
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_drops_generated_column() -> TestResult {
        let table = generated_column_table().await?;

        let table = table.drop_columns().with_columns(["doubled"]).await?;

        let schema = table.snapshot()?.schema();
        assert!(schema.field("doubled").is_none());
        assert!(schema.field("id").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_drops_generated_column_with_its_source() -> TestResult {
        let table = generated_column_table().await?;

        let table = table.drop_columns().with_columns(["id", "doubled"]).await?;

        let schema = table.snapshot()?.schema();
        assert!(schema.field("doubled").is_none());
        assert!(schema.field("id").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_removes_column_from_stats_columns() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                id_field(),
                value_field(),
                StructField::new(
                    "legacy name",
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .with_configuration_property(
                TableProperty::DataSkippingStatsColumns,
                Some("id,value,`legacy name`"),
            )
            .await?;

        let table = table.drop_columns().with_columns(["value"]).await?;

        let snapshot = table.snapshot()?;
        let stats_columns = snapshot
            .metadata()
            .configuration()
            .get("delta.dataSkippingStatsColumns")
            .unwrap();
        assert_eq!(stats_columns, "id,`legacy name`");
        assert_eq!(
            ColumnName::parse_column_name_list(stats_columns)?,
            [ColumnName::new(["id"]), ColumnName::new(["legacy name"])]
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_removes_nested_fields_from_stats_columns() -> TestResult {
        let nested = StructField::new(
            "nested",
            DataType::Struct(Box::new(StructType::try_new([StructField::new(
                "inner",
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])?)),
            true,
        );
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field(), nested])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .with_configuration_property(
                TableProperty::DataSkippingStatsColumns,
                Some("id,nested.inner"),
            )
            .await?;

        let table = table.drop_columns().with_columns(["nested"]).await?;

        assert_eq!(
            table
                .snapshot()?
                .metadata()
                .configuration()
                .get("delta.dataSkippingStatsColumns")
                .map(String::as_str),
            Some("id")
        );
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_drops_struct_with_nested_invariant() -> TestResult {
        let nested = StructField::new(
            "nested",
            DataType::Struct(Box::new(StructType::try_new([StructField::new(
                "inner",
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )
            .with_metadata([(
                "delta.invariants",
                r#"{"expression": {"expression": "nested.inner > 0"}}"#,
            )])])?)),
            true,
        );
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([id_field(), nested])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await?;

        let table = table.drop_columns().with_columns(["nested"]).await?;

        assert!(table.snapshot()?.schema().field("nested").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_column_used_by_invariant_on_column_with_a_dot() -> TestResult {
        // The top-level column `a.b` is not nested inside `a`, so dropping `a` keeps its
        // invariant.
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                id_field(),
                StructField::new("a", DataType::Primitive(PrimitiveType::Integer), true),
                StructField::new("a.b", DataType::Primitive(PrimitiveType::Integer), true)
                    .with_metadata([(
                        "delta.invariants",
                        r#"{"expression": {"expression": "a > 0"}}"#,
                    )]),
            ])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await?;

        let err = table.drop_columns().with_columns(["a"]).await.unwrap_err();

        assert!(err.to_string().contains("invariant on 'a.b'"), "got: {err}");
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_rejects_column_with_a_dot_used_by_nested_invariant() -> TestResult {
        // The field `b` of the struct `a` is not the top-level column `a.b`, so dropping `a.b`
        // keeps its invariant.
        let a = StructField::new(
            "a",
            DataType::Struct(Box::new(StructType::try_new([StructField::new(
                "b",
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )
            .with_metadata([(
                "delta.invariants",
                r#"{"expression": {"expression": "`a.b` > 0"}}"#,
            )])])?)),
            true,
        );
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                id_field(),
                a,
                StructField::new("a.b", DataType::Primitive(PrimitiveType::Integer), true),
            ])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await?;

        let err = table
            .drop_columns()
            .with_columns(["a.b"])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("invariant on 'a.b'"), "got: {err}");
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_drops_top_level_column_with_a_dot() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                id_field(),
                StructField::new("a.b", DataType::Primitive(PrimitiveType::String), true),
            ])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await?;

        let table = table.drop_columns().with_columns(["a.b"]).await?;

        assert!(table.snapshot()?.schema().field("a.b").is_none());
        assert_eq!(last_dropped_columns(&table).await, ["`a.b`"]);
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_accepts_the_quoted_name_from_the_history() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                id_field(),
                StructField::new("a.b", DataType::Primitive(PrimitiveType::String), true),
            ])
            .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
            .await?;

        // The quoted and the plain name are the same column, so it is dropped once.
        let table = table.drop_columns().with_columns(["`a.b`", "a.b"]).await?;

        assert!(table.snapshot()?.schema().field("a.b").is_none());
        assert_eq!(last_dropped_columns(&table).await, ["`a.b`"]);
        Ok(())
    }

    #[tokio::test]
    async fn drop_columns_ignores_repeated_names() -> TestResult {
        let table = column_mapped_table().await?;

        let table = table
            .drop_columns()
            .with_columns(["legacy", "legacy"])
            .await?;

        assert_eq!(last_dropped_columns(&table).await, ["legacy"]);
        Ok(())
    }
}
