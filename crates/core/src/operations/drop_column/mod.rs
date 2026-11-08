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
//! A field nested inside a struct is named by its path, such as `s.a`. The element of an
//! array is `element`, and the key and value of a map are `key` and `value`, so
//! `points.element.x` names the field `x` of the structs in the array `points`.
//!
//! [column mapping]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode as _, TreeNodeRecursion};
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::logical_expr::Expr;
use delta_kernel::column_trie::ColumnTrie;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use delta_kernel::transforms::{SchemaTransform, transform_output_type};
use futures::future::BoxFuture;
use itertools::Itertools as _;

use crate::delta_datafusion::create_session;
use crate::delta_datafusion::expr::{
    parse_generated_column_expression, parse_predicate_expression,
};
use crate::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _, SnapshotMetadataRef, StructTypeExt,
    resolve_snapshot, spawn_blocking_with_span,
};
use crate::logstore::{LogStoreRef, with_operation};
use crate::protocol::DeltaOperation;
use crate::table::config::{TablePropertiesExt as _, TableProperty};
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Drop one or more columns, or fields nested inside them, from a column-mapped table.
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

    /// Specify the columns to drop. A nested field is named by its path, such as `s.a`.
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

/// Format a column path for error messages: the parts are joined by dots, and a part that
/// contains a dot is backtick-quoted.
fn display_name(column: &ColumnName) -> String {
    column
        .iter()
        .map(|part| {
            if part.contains('.') {
                format!("`{part}`")
            } else {
                part.clone()
            }
        })
        .join(".")
}

/// Return whether `path` names a field of `schema`, or a field nested in the structs inside
/// one. The element of an array is `element`, and the key and value of a map are `key` and
/// `value`.
fn contains_field(schema: &StructType, path: &[String]) -> bool {
    fn contains_nested_field(data_type: &DataType, path: &[String]) -> bool {
        match (data_type, path) {
            (DataType::Struct(fields), _) => contains_field(fields, path),
            (DataType::Array(array), [part, rest @ ..]) if part == "element" => {
                contains_nested_field(array.element_type(), rest)
            }
            (DataType::Map(map), [part, rest @ ..]) if part == "key" => {
                contains_nested_field(map.key_type(), rest)
            }
            (DataType::Map(map), [part, rest @ ..]) if part == "value" => {
                contains_nested_field(map.value_type(), rest)
            }
            _ => false,
        }
    }

    match path {
        [] => false,
        [name] => schema.field(name).is_some(),
        [name, rest @ ..] => schema
            .field(name)
            .is_some_and(|field| contains_nested_field(field.data_type(), rest)),
    }
}

/// Resolve a requested name to the path of the field it drops, or `None` when it names no
/// field.
///
/// A top-level column whose name matches as is comes first, because a name can contain a dot.
/// Any other name is parsed as a path, such as `s.a` or the backtick-quoted form the history
/// records, so `` a.`b` `` names the field `b` of a struct `a` even when a column `a.b` exists.
fn resolve_column(schema: &StructType, name: &str) -> Option<ColumnName> {
    if schema.field(name).is_some() {
        return Some(ColumnName::new([name]));
    }
    let column = name.parse::<ColumnName>().ok()?;
    // A plain name is not parsed: parsing drops the spaces around a name, and `"legacy "` must
    // not match `legacy`.
    if column.len() == 1 && !name.starts_with('`') {
        return None;
    }
    contains_field(schema, &column).then_some(column)
}

/// Removes the fields that a [`ColumnTrie`] names from a schema, and records the first struct
/// that this leaves without fields.
struct DropFields<'trie> {
    dropped: &'trie ColumnTrie<'trie>,
    /// The path of the field being visited
    path: Vec<String>,
    /// The path of the first struct left without fields
    emptied: Option<ColumnName>,
}

impl DropFields<'_> {
    /// Visit the child `part` of the current field.
    fn visit_child<T>(&mut self, part: &str, visit: impl FnOnce(&mut Self) -> T) -> T {
        self.path.push(part.to_string());
        let result = visit(self);
        self.path.pop();
        result
    }
}

impl<'a> SchemaTransform<'a> for DropFields<'_> {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    fn transform_struct(&mut self, stype: &'a StructType) -> Option<Cow<'a, StructType>> {
        // The transform also removes a struct without fields, so keep one that had none.
        if stype.num_fields() == 0 {
            return Some(Cow::Borrowed(stype));
        }
        let transformed = self.recurse_into_struct(stype);
        if transformed.is_none() && self.emptied.is_none() {
            self.emptied = Some(ColumnName::new(&self.path));
        }
        transformed
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.visit_child(&field.name, |this| {
            if this.dropped.is_terminal(&this.path) {
                None
            } else {
                this.recurse_into_struct_field(field)
            }
        })
    }

    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.visit_child("element", |this| this.transform(etype))
    }

    fn transform_map_key(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.visit_child("key", |this| this.transform(etype))
    }

    fn transform_map_value(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.visit_child("value", |this| this.transform(etype))
    }
}

/// Return the column paths that a SQL expression reads.
///
/// The expression is parsed against the *pre-drop* schema so that parsing always succeeds;
/// the referenced columns are then compared against the columns being dropped. Relying on a
/// parse failure instead would be sensitive to the shape of the expression and would produce
/// a much worse error message.
///
/// A field access on a struct, such as `s.a`, reads only that field. Any other access, such as
/// an array index or a map key, reads the whole column it is applied to.
fn referenced_columns(expr: &Expr, schema: &StructType) -> DeltaResult<HashSet<ColumnName>> {
    /// The path that a column, or a chain of field accesses on a column, reads.
    fn field_path(expr: &Expr) -> Option<Vec<String>> {
        match expr {
            Expr::Column(column) => Some(vec![column.name.clone()]),
            Expr::ScalarFunction(function) if function.name() == "get_field" => {
                let (base, names) = function.args.split_first()?;
                let mut path = field_path(base)?;
                for name in names {
                    let Expr::Literal(name, _) = name else {
                        return None;
                    };
                    path.push(name.try_as_str()??.to_string());
                }
                Some(path)
            }
            _ => None,
        }
    }

    let mut columns = HashSet::new();
    expr.apply(|expr| {
        let Some(mut path) = field_path(expr) else {
            return Ok(TreeNodeRecursion::Continue);
        };
        // `get_field` also reads a map value by its key, so keep only the parts that name a
        // column and the struct fields inside it.
        let mut fields = Some(schema);
        let mut len = 0;
        for part in &path {
            let Some(field) = fields.and_then(|fields| fields.field(part)) else {
                break;
            };
            len += 1;
            fields = match field.data_type() {
                DataType::Struct(inner) => Some(inner),
                _ => None,
            };
        }
        path.truncate(len.max(1));
        columns.insert(ColumnName::new(path));
        Ok(TreeNodeRecursion::Jump)
    })?;
    Ok(columns)
}

/// Reject the drop if any check constraint, invariant, or generated column that survives it
/// still references one of the columns being dropped. The error lists every such reference,
/// one per line, in sorted order.
///
/// A reference conflicts with a dropped column when either path contains the
/// other: `s.a` reads a field of a dropped struct `s`, and `s` reads a dropped field `s.a`.
///
/// Returns the names of the check constraints that only reference dropped columns; they go
/// away together with those columns.
fn check_dependents(
    snapshot: &SnapshotMetadataRef<'_>,
    dropped: &[ColumnName],
    updated_schema: &StructType,
    df_schema: &DFSchema,
    session: &dyn Session,
) -> DeltaResult<Vec<String>> {
    let table_configuration = &snapshot.table_configuration;
    let table_schema = table_configuration.logical_schema();
    let dropped_trie = ColumnTrie::from_columns(dropped);

    let mut conflicts = Vec::new();
    let mut add_conflicts = |dependent: String, referenced: &HashSet<ColumnName>| {
        conflicts.extend(
            dropped
                .iter()
                .filter(|column| {
                    referenced
                        .iter()
                        .any(|reference| reference.iter().zip(column.iter()).all(|(a, b)| a == b))
                })
                .map(|column| {
                    format!(
                        "Cannot drop column '{}': it is referenced by the {dependent}.",
                        display_name(column)
                    )
                }),
        );
    };

    let mut dropped_constraints = Vec::new();
    if table_configuration.is_feature_enabled(&TableFeature::CheckConstraints) {
        for constraint in table_configuration.table_properties().get_constraints() {
            let expr = parse_predicate_expression(df_schema, &constraint.expr, session)?;
            let referenced = referenced_columns(&expr, &table_schema)?;
            // A constraint that only checks dropped columns, or fields inside them, goes with
            // them, the same as the expression of a dropped generated column.
            if !referenced.is_empty()
                && referenced
                    .iter()
                    .all(|column| dropped_trie.contains_prefix_of(column))
            {
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
                &referenced_columns(&expr, &table_schema)?,
            );
        }
    }

    if table_configuration.is_feature_enabled(&TableFeature::GeneratedColumns) {
        for generated in table_schema.get_generated_columns()? {
            // Dropping a generated column itself is fine - its expression goes with it.
            if dropped_trie.contains_prefix_of(std::slice::from_ref(&generated.name)) {
                continue;
            }
            let expr = parse_generated_column_expression(df_schema, &generated, session)?;
            add_conflicts(
                format!("generated column '{}'", generated.name),
                &referenced_columns(&expr, &table_schema)?,
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
    clustering_columns: &[ColumnName],
    session: &dyn Session,
) -> DeltaResult<Option<(Vec<Action>, DeltaOperation)>> {
    let table_schema = snapshot.table_configuration.logical_schema();

    let mut requested: Vec<ColumnName> = Vec::with_capacity(column_names.len());
    for name in column_names {
        match resolve_column(&table_schema, name) {
            Some(column) if requested.contains(&column) => {}
            Some(column) => requested.push(column),
            None if raise_if_not_exists => {
                return Err(DeltaTableError::Generic(format!(
                    "No column with the name '{name}' in the schema"
                )));
            }
            None => {}
        }
    }
    // A field inside another dropped column goes with it, so it is not dropped on its own.
    let removed = requested
        .iter()
        .filter(|column| {
            !requested
                .iter()
                .any(|other| other.len() < column.len() && column.starts_with(other))
        })
        .cloned()
        .collect_vec();
    if removed.is_empty() {
        return Ok(None);
    }
    let dropped_trie = ColumnTrie::from_columns(&removed);

    // Keep the surviving fields exactly as they are so their column mapping metadata
    // (physical name and field id) carries over untouched. `delta.columnMapping.maxColumnId`
    // is deliberately left alone: it must never be decremented or reused, so that a future
    // `ADD COLUMN` cannot collide with a dropped column's physical id.
    let mut drop_fields = DropFields {
        dropped: &dropped_trie,
        path: Vec::new(),
        emptied: None,
    };
    // A table must retain at least one column - `CREATE TABLE` rejects an empty schema, so a
    // drop must not be able to produce one either. In the same way, parquet cannot store a
    // struct without fields. Check the resulting schema rather than count the dropped names,
    // so the check holds however the fields are dropped.
    let updated_table_schema = match (
        drop_fields.transform_struct(&table_schema),
        drop_fields.emptied,
    ) {
        (Some(schema), None) => schema.into_owned(),
        (_, Some(column)) if !column.is_empty() => {
            return Err(DeltaTableError::Generic(format!(
                "Cannot drop every field of '{}'; a struct must have at least one field",
                display_name(&column)
            )));
        }
        _ => {
            return Err(DeltaTableError::Generic(
                "Cannot drop all columns of a table; a table must have at least one column"
                    .to_string(),
            ));
        }
    };

    // Partition columns are encoded in the data file paths, so they cannot be dropped.
    let metadata = snapshot.metadata.clone();
    for column in metadata.partition_columns() {
        if removed.contains(&ColumnName::new([column])) {
            return Err(DeltaTableError::Generic(format!(
                "Cannot drop column '{column}': it is a partition column"
            )));
        }
    }

    // The `delta.clustering` domain metadata names the clustering columns, so they cannot be
    // dropped, and neither can a struct that contains one.
    for column in clustering_columns {
        if let Some(dropped) = removed.iter().find(|dropped| column.starts_with(dropped)) {
            return Err(DeltaTableError::Generic(format!(
                "Cannot drop column '{}': the table is clustered by '{}'",
                display_name(dropped),
                display_name(column)
            )));
        }
    }

    // Stats are collected for an explicit column list when this property is set. Remove the
    // dropped columns, and any field nested inside one, from that list. The property is only
    // rewritten when it lists a dropped column. When no listed column is left, the property
    // stays as an empty list, so stats are collected for no column. Removing it would collect
    // stats for the first `delta.dataSkippingNumIndexedCols` columns instead.
    let stats_columns = snapshot
        .table_configuration
        .table_properties()
        .data_skipping_stats_columns
        .as_ref()
        .and_then(|columns| {
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
        &removed,
        &updated_table_schema,
        &df_schema,
        session,
    )?;

    let current_protocol = snapshot.protocol;
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&updated_table_schema)?
        .move_table_properties_into_features(metadata.configuration());

    let operation = DeltaOperation::DropColumns {
        columns: removed.iter().map(display_name).collect(),
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
            if snapshot
                .snapshot()
                .metadata_state()
                .table_configuration
                .column_mapping_mode()
                == ColumnMappingMode::None
            {
                return Err(DeltaTableError::column_mapping_required("DROP COLUMNS"));
            }

            // The kernel returns no clustering columns unless the table has the clustering
            // feature. Writes to such tables are not supported yet, so `can_write_to` rejects
            // them above and this finds none. It guards the drop once liquid clustering is
            // supported.
            let clustering_columns = {
                let kernel_snapshot = snapshot.snapshot().inner.clone();
                let engine = this.log_store.engine();
                spawn_blocking_with_span(move || {
                    kernel_snapshot.get_clustering_column_infos(engine.as_ref())
                })
                .await
                .map_err(|e| DeltaTableError::GenericError { source: e.into() })??
                .into_iter()
                .flatten()
                .map(|info| info.logical_column)
                .collect_vec()
            };

            let session = this
                .session
                .clone()
                .unwrap_or_else(|| Arc::new(create_session().into_inner().state()));

            let Some((actions, operation)) = plan_drop_columns_actions(
                snapshot.snapshot().metadata_state(),
                &this.column_names,
                this.raise_if_not_exists,
                &clustering_columns,
                session.as_ref(),
            )?
            else {
                return Ok(DeltaTable::new_with_state(
                    this.log_store,
                    DeltaTableState::new(snapshot),
                ));
            };

            // The drop reads no files. Without a list of read files, the conflict checker treats
            // the commit as if it read every file, so a concurrent DELETE would make it fail.
            // TODO(ion): add `DeltaOperation::reads_data()`, return `false` for `DropColumns`, and let
            // the commit path give the conflict checker an empty list for such operations. Then
            // remove `with_read_files` here and use `commit_actions_in_scope` again.
            let commit_properties = this.commit_properties;
            let state = with_operation(&this.log_store, |log_store| async move {
                let commit = CommitBuilder::from(commit_properties)
                    .with_actions(actions)
                    .with_read_files(Vec::new())
                    .build(Some(&snapshot), log_store, operation)
                    .await?;
                Ok(commit.snapshot())
            })
            .await?;
            Ok(DeltaTable::new_with_state(this.log_store, state))
        })
    }
}

#[cfg(test)]
mod tests;
