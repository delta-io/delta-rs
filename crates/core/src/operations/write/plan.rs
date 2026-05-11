//! Write planning has two stages.
//! `prepare_write` normalizes incoming rows into table shaped insert data and resolves exact
//! validations against that prepared schema.
//! `plan_overwrite_rewrite` builds a typed overwrite rewrite plan that keeps matched existing
//! files, rescue planning, and CDC composition explicit until commit assembly.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_schema::Schema;
use datafusion::catalog::Session;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UNNAMED_TABLE, cast, lit, try_cast, when,
};
use datafusion::prelude::col;
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use futures::TryStreamExt as _;
use itertools::Itertools as _;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::configs::WriterStatsConfig;
use super::generated_columns::{gc_is_enabled, with_generated_columns};
use super::metrics::SOURCE_COUNT_ID;
use super::schema_evolution::try_cast_schema;
use super::{SchemaMode, WriteError};
use crate::delta_datafusion::logical::{LogicalPlanBuilderExt as _, MetricObserver};
use crate::delta_datafusion::{
    DataFusionMixins, Expression, analyze_predicate_for_find_files, scan_files_where_matches,
};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::schema::cast::{merge_arrow_schema, normalize_for_delta};
use crate::kernel::{
    Action, Add, DeletionVectorDescriptor, EagerSnapshot, Metadata, ProtocolExt as _, Remove,
    StructType, StructTypeExt,
};
use crate::logstore::LogStoreRef;
use crate::operations::cdc::{CDC_COLUMN_NAME, should_write_cdc};
use crate::operations::{get_num_idx_cols_and_stats_columns, get_target_file_size};
use crate::protocol::SaveMode;

/// Schema and protocol actions required before the sink executes the write.
#[derive(Default)]
pub(super) struct SchemaDelta {
    metadata: Option<Action>,
    protocol: Option<Action>,
}

impl SchemaDelta {
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.metadata.is_none() && self.protocol.is_none()
    }

    pub(super) fn into_actions(self) -> Vec<Action> {
        let mut actions = Vec::with_capacity(2);
        if let Some(metadata) = self.metadata {
            actions.push(metadata);
        }
        if let Some(protocol) = self.protocol {
            actions.push(protocol);
        }
        actions
    }
}

/// Sink specific knobs that must survive planning unchanged.
pub(super) struct WriteExecOptions {
    pub(super) partition_columns: Vec<String>,
    pub(super) target_file_size: Option<NonZeroU64>,
    pub(super) write_batch_size: Option<usize>,
    pub(super) writer_properties: Option<WriterProperties>,
    pub(super) writer_stats_config: WriterStatsConfig,
}

/// Prepared insert input plus the exact validation the sink must enforce.
pub(super) struct PreparedWrite {
    insert_plan: LogicalPlan,
    mode: SaveMode,
    pub(super) schema_delta: SchemaDelta,
    pub(super) exact_validation: Option<Expr>,
    pub(super) exec_options: WriteExecOptions,
}

/// Inputs required to normalize source rows into table shaped insert data.
pub(super) struct WritePreparationInput<'a> {
    pub(super) snapshot: Option<&'a EagerSnapshot>,
    pub(super) session: &'a dyn Session,
    pub(super) source: LogicalPlan,
    pub(super) mode: SaveMode,
    pub(super) schema_mode: Option<SchemaMode>,
    pub(super) safe_cast: bool,
    pub(super) partition_columns: Vec<String>,
    pub(super) predicate: Option<Expression>,
    pub(super) target_file_size: Option<Option<NonZeroU64>>,
    pub(super) write_batch_size: Option<usize>,
    pub(super) writer_properties: Option<WriterProperties>,
    pub(super) configuration: &'a HashMap<String, Option<String>>,
}

/// Prefix for per-operation internal marker columns injected during overwrite rewrite
/// to distinguish insert rows from rescued existing rows.
pub(super) const WRITE_INSERT_MARKER_COLUMN: &str = "__delta_rs_write_insert";

/// High-level rewrite strategy chosen for an overwrite flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RewriteKind {
    Passthrough,
    NoMatch,
    FullTable,
    PartitionOnly,
    DataRescue,
}

/// Planner diagnostics for overwrite / replaceWhere rewrites.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct RewriteDiagnostics {
    pub(super) matched_file_count: usize,
    pub(super) translated_pruning_term_count: usize,
    pub(super) dropped_pruning_term_count: usize,
}

/// A matched existing file kept as a typed removal candidate until commit assembly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MatchedExistingFile {
    path: String,
    partition_values: HashMap<String, Option<String>>,
    size: i64,
    deletion_vector: Option<DeletionVectorDescriptor>,
    base_row_id: Option<i64>,
    default_row_commit_version: Option<i64>,
}

impl From<Add> for MatchedExistingFile {
    fn from(add: Add) -> Self {
        Self {
            path: add.path,
            partition_values: add.partition_values,
            size: add.size,
            deletion_vector: add.deletion_vector,
            base_row_id: add.base_row_id,
            default_row_commit_version: add.default_row_commit_version,
        }
    }
}

impl MatchedExistingFile {
    fn into_remove(self, deletion_timestamp: i64) -> Remove {
        Remove {
            path: self.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(self.partition_values),
            size: Some(self.size),
            deletion_vector: self.deletion_vector,
            tags: None,
            base_row_id: self.base_row_id,
            default_row_commit_version: self.default_row_commit_version,
        }
    }
}

/// The typed set of files that this overwrite will remove if the sink succeeds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct MatchedExistingFiles {
    files: Vec<MatchedExistingFile>,
}

impl MatchedExistingFiles {
    fn new(files: Vec<MatchedExistingFile>) -> Self {
        Self { files }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub(super) fn num_files(&self) -> usize {
        self.files.len()
    }

    pub(super) fn into_actions(self, deletion_timestamp: Option<i64>) -> DeltaResult<Vec<Action>> {
        if self.files.is_empty() {
            return Ok(Vec::new());
        }

        let deletion_timestamp = deletion_timestamp.ok_or_else(|| {
            DeltaTableError::generic(
                "Matched existing files are missing a planner-owned deletion timestamp",
            )
        })?;

        Ok(self
            .files
            .into_iter()
            .map(|file| Action::Remove(file.into_remove(deletion_timestamp)))
            .collect())
    }
}

/// Planner output for overwrite flows before the sink materializes new files.
pub(super) struct MatchedFilesRewritePlan {
    pub(super) kind: RewriteKind,
    pub(super) deletion_timestamp: Option<i64>,
    pub(super) matched_existing: MatchedExistingFiles,
    pub(super) data_plan: LogicalPlan,
    pub(super) cdc_plan: Option<LogicalPlan>,
    insert_marker_column: Option<String>,
    pub(super) diagnostics: RewriteDiagnostics,
}

impl MatchedFilesRewritePlan {
    fn passthrough(insert_plan: LogicalPlan) -> Self {
        Self {
            kind: RewriteKind::Passthrough,
            deletion_timestamp: None,
            matched_existing: MatchedExistingFiles::default(),
            data_plan: insert_plan,
            cdc_plan: None,
            insert_marker_column: None,
            diagnostics: RewriteDiagnostics::default(),
        }
    }

    pub(super) fn num_removed_files(&self) -> usize {
        self.matched_existing.num_files()
    }

    pub(super) fn build_sink_plan(&self) -> DeltaResult<(LogicalPlan, bool, Option<String>)> {
        let data_plan = if let (Some(_), Some(insert_marker_column)) =
            (self.cdc_plan.as_ref(), self.insert_marker_column.as_ref())
        {
            // Empty-string rows are rescued table data: they must bypass CDF output while still
            // flowing through the normal parquet write path.
            LogicalPlanBuilder::new(self.data_plan.clone())
                .with_column(
                    CDC_COLUMN_NAME,
                    when(col(insert_marker_column).eq(lit(true)), lit("insert"))
                        .otherwise(lit(""))?,
                )?
                .build()?
        } else {
            self.data_plan.clone()
        };

        match self.cdc_plan.clone() {
            Some(cdc_plan) => {
                let cdc_plan = align_plan_to_schema(cdc_plan, &data_plan)?;
                Ok((
                    LogicalPlanBuilder::new(data_plan)
                        .union(cdc_plan)?
                        .build()?,
                    true,
                    self.insert_marker_column.clone(),
                ))
            }
            None => Ok((data_plan, false, self.insert_marker_column.clone())),
        }
    }
}

pub(super) fn prepare_write(input: WritePreparationInput<'_>) -> DeltaResult<PreparedWrite> {
    let WritePreparationInput {
        snapshot,
        session,
        mut source,
        mode,
        schema_mode,
        safe_cast,
        partition_columns,
        predicate,
        target_file_size,
        write_batch_size,
        writer_properties,
        configuration,
    } = input;

    let mut schema_drift = false;

    let table_schema = if let Some(snapshot) = snapshot {
        snapshot.arrow_schema()
    } else {
        normalize_for_delta(source.schema().inner())
    };

    if let Some(snapshot) = snapshot
        && gc_is_enabled(snapshot)
    {
        source = with_generated_columns(
            session,
            source,
            &table_schema,
            &snapshot.schema().get_generated_columns()?,
        )?;
    }

    let source_schema: Arc<Schema> = normalize_for_delta(source.schema().inner());
    if !Arc::ptr_eq(&source_schema, source.schema().inner()) {
        let original_schema = source.schema().inner();
        let cast_projection = source_schema
            .fields()
            .iter()
            .zip(original_schema.fields().iter())
            .map(|(target, original)| {
                if target.data_type() != original.data_type() {
                    let cast_fn = if safe_cast { try_cast } else { cast };
                    cast_fn(
                        Expr::Column(Column::from_name(target.name())),
                        target.data_type().clone(),
                    )
                    .alias(target.name())
                } else {
                    Expr::Column(Column::from_name(target.name()))
                }
            })
            .collect_vec();
        source = LogicalPlanBuilder::new(source)
            .project(cast_projection)?
            .build()?;
    }

    let mut new_schema = None;
    if let Some(snapshot) = snapshot {
        let table_schema = snapshot.input_schema();

        if let Err(schema_err) = try_cast_schema(source_schema.fields(), table_schema.fields()) {
            schema_drift = true;
            if mode == SaveMode::Overwrite && schema_mode == Some(SchemaMode::Overwrite) {
                new_schema = None;
            } else if schema_mode == Some(SchemaMode::Merge) {
                new_schema = Some(merge_arrow_schema(
                    table_schema.clone(),
                    source_schema.clone(),
                    schema_drift,
                )?);
            } else {
                return Err(schema_err.into());
            }
        } else if mode == SaveMode::Overwrite && schema_mode == Some(SchemaMode::Overwrite) {
            new_schema = None;
        } else {
            new_schema = Some(merge_arrow_schema(
                table_schema.clone(),
                source_schema.clone(),
                schema_drift,
            )?);
        }
    }

    if let Some(new_schema) = new_schema.as_ref() {
        let mut schema_evolution_projection = Vec::with_capacity(new_schema.fields().len());
        for field in new_schema.fields() {
            if source_schema.index_of(field.name()).is_ok() {
                let cast_fn = if safe_cast { try_cast } else { cast };
                schema_evolution_projection.push(
                    cast_fn(
                        Expr::Column(Column::from_name(field.name())),
                        field.data_type().clone(),
                    )
                    .alias(field.name()),
                );
            } else {
                schema_evolution_projection.push(
                    cast(
                        lit(ScalarValue::Null).alias(field.name()),
                        field.data_type().clone(),
                    )
                    .alias(field.name()),
                );
            }
        }
        source = LogicalPlanBuilder::new(source)
            .project(schema_evolution_projection)?
            .build()?;
    }

    // Validate the effective partition columns against the prepared writer input.
    // Schema merge projects missing table columns before partition values are derived.
    validate_partition_columns_in_schema(&partition_columns, source.schema().inner().as_ref())?;

    let insert_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(MetricObserver {
            id: SOURCE_COUNT_ID.into(),
            input: source,
            enable_pushdown: false,
        }),
    });

    let schema_delta = schema_delta_for_prepared_source(
        snapshot,
        &insert_plan,
        &partition_columns,
        mode,
        schema_mode,
        schema_drift,
        new_schema.as_deref(),
    )?;

    let exact_validation = resolve_exact_validation(session, &insert_plan, predicate)?;

    Ok(PreparedWrite {
        insert_plan,
        mode,
        schema_delta,
        exact_validation,
        exec_options: build_exec_options(
            snapshot,
            partition_columns,
            target_file_size,
            write_batch_size,
            writer_properties,
            configuration,
        ),
    })
}

pub(super) async fn plan_overwrite_rewrite(
    snapshot: Option<&EagerSnapshot>,
    log_store: &LogStoreRef,
    session: &dyn Session,
    mode: SaveMode,
    prepared_write: &PreparedWrite,
    operation_id: Uuid,
) -> DeltaResult<MatchedFilesRewritePlan> {
    let Some(snapshot) = snapshot else {
        return Ok(MatchedFilesRewritePlan::passthrough(
            prepared_write.insert_plan.clone(),
        ));
    };

    debug_assert_eq!(
        mode, prepared_write.mode,
        "plan_overwrite_rewrite mode must match prepared write mode"
    );

    if !matches!(prepared_write.mode, SaveMode::Overwrite) {
        return Ok(MatchedFilesRewritePlan::passthrough(
            prepared_write.insert_plan.clone(),
        ));
    }

    match prepared_write.exact_validation.clone() {
        Some(predicate) => {
            let analysis = analyze_predicate_for_find_files(
                predicate.clone(),
                &prepared_write.exec_options.partition_columns,
            )?;
            let mut diagnostics = RewriteDiagnostics {
                matched_file_count: 0,
                translated_pruning_term_count: analysis.translated_pruning_term_count,
                dropped_pruning_term_count: analysis.dropped_pruning_term_count,
            };

            let Some(files_scan) =
                scan_files_where_matches(session, snapshot, log_store.clone(), predicate).await?
            else {
                return Ok(MatchedFilesRewritePlan {
                    kind: RewriteKind::NoMatch,
                    deletion_timestamp: None,
                    matched_existing: MatchedExistingFiles::default(),
                    data_plan: prepared_write.insert_plan.clone(),
                    cdc_plan: None,
                    insert_marker_column: None,
                    diagnostics,
                });
            };

            let matched_existing =
                collect_matched_existing_files(snapshot, log_store, &files_scan).await?;
            diagnostics.matched_file_count = matched_existing.num_files();

            if matched_existing.is_empty() {
                return Err(DeltaTableError::Generic(
                    "Matched-file scan returned rows but no existing files to rewrite".into(),
                ));
            }

            if analysis.partition_only {
                return Ok(MatchedFilesRewritePlan {
                    kind: RewriteKind::PartitionOnly,
                    deletion_timestamp: Some(planned_deletion_timestamp_ms()?),
                    matched_existing,
                    data_plan: prepared_write.insert_plan.clone(),
                    cdc_plan: None,
                    insert_marker_column: None,
                    diagnostics,
                });
            }

            let insert_marker_column = reserve_internal_write_marker_column(
                &[&prepared_write.insert_plan, files_scan.scan()],
                operation_id,
            );

            let validated_inserts = mark_insert_rows(
                prepared_write.insert_plan.clone(),
                &insert_marker_column,
                true,
            )?;
            let rescued_data = mark_insert_rows(
                LogicalPlanBuilder::new(files_scan.scan().clone())
                    // `IS NOT TRUE` keeps rows where the predicate is FALSE or NULL so nullable
                    // predicate columns are rescued instead of being rewritten.
                    .filter(files_scan.predicate.clone().is_not_true())?
                    .build()?,
                &insert_marker_column,
                false,
            )?;
            let rescued_data = align_plan_to_schema(rescued_data, &validated_inserts)?;
            let data_plan = LogicalPlanBuilder::new(validated_inserts)
                .union(rescued_data)?
                .build()?;

            let cdc_plan = if should_write_cdc(snapshot)? {
                Some(
                    LogicalPlanBuilder::new(files_scan.scan().clone())
                        .filter(files_scan.predicate.clone())?
                        .with_column(insert_marker_column.as_str(), lit(false))?
                        .with_column(CDC_COLUMN_NAME, lit("delete"))?
                        .build()?,
                )
            } else {
                None
            };

            Ok(MatchedFilesRewritePlan {
                kind: RewriteKind::DataRescue,
                deletion_timestamp: Some(planned_deletion_timestamp_ms()?),
                matched_existing,
                data_plan,
                cdc_plan,
                insert_marker_column: Some(insert_marker_column),
                diagnostics,
            })
        }
        None => {
            let matched_existing = collect_all_existing_files(snapshot, log_store).await?;
            let diagnostics = RewriteDiagnostics {
                matched_file_count: matched_existing.num_files(),
                ..RewriteDiagnostics::default()
            };
            let deletion_timestamp = if matched_existing.is_empty() {
                None
            } else {
                Some(planned_deletion_timestamp_ms()?)
            };

            Ok(MatchedFilesRewritePlan {
                kind: RewriteKind::FullTable,
                deletion_timestamp,
                matched_existing,
                data_plan: prepared_write.insert_plan.clone(),
                cdc_plan: None,
                insert_marker_column: None,
                diagnostics,
            })
        }
    }
}

fn planned_deletion_timestamp_ms() -> DeltaResult<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            DeltaTableError::generic(format!(
                "System clock returned a pre-epoch timestamp while planning overwrite deletions: {err}"
            ))
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| DeltaTableError::generic("Overwrite deletion timestamp overflowed i64"))
}

async fn collect_all_existing_files(
    snapshot: &EagerSnapshot,
    log_store: &LogStoreRef,
) -> DeltaResult<MatchedExistingFiles> {
    Ok(MatchedExistingFiles::new(
        snapshot
            .file_views(log_store.as_ref(), None)
            .map_ok(|file| MatchedExistingFile::from(file.to_add()))
            .try_collect()
            .await?,
    ))
}

async fn collect_matched_existing_files(
    snapshot: &EagerSnapshot,
    log_store: &LogStoreRef,
    files_scan: &crate::delta_datafusion::MatchedFilesScan,
) -> DeltaResult<MatchedExistingFiles> {
    let table_root = Arc::new(snapshot.table_configuration().table_root().clone());
    let valid_files = Arc::new(files_scan.files_set());
    let files = snapshot
        .file_views(log_store.as_ref(), Some(files_scan.delta_predicate.clone()))
        .try_filter_map(|file| {
            let table_root = Arc::clone(&table_root);
            let valid_files = Arc::clone(&valid_files);
            async move {
                let file_url = table_root
                    .join(file.path_raw())
                    .map_err(|err| DeltaTableError::Generic(format!("{err}")))?;
                Ok(valid_files
                    .contains(file_url.as_ref())
                    .then(|| MatchedExistingFile::from(file.to_add())))
            }
        })
        .try_collect()
        .await?;

    Ok(MatchedExistingFiles::new(files))
}

fn reserve_internal_write_marker_column(plans: &[&LogicalPlan], operation_id: Uuid) -> String {
    let base = format!("{WRITE_INSERT_MARKER_COLUMN}_{operation_id}");
    let mut candidate = base.clone();
    let mut suffix = 0usize;
    while plans.iter().any(|plan| {
        plan.schema()
            .iter()
            .any(|(_, field)| field.name() == &candidate)
    }) {
        suffix += 1;
        candidate = format!("{base}_{suffix}");
    }
    candidate
}

fn mark_insert_rows(
    plan: LogicalPlan,
    insert_marker_column: &str,
    is_insert: bool,
) -> DeltaResult<LogicalPlan> {
    Ok(LogicalPlanBuilder::new(plan)
        .with_column(insert_marker_column, lit(is_insert))?
        .build()?)
}

fn align_plan_to_schema(plan: LogicalPlan, target_plan: &LogicalPlan) -> DeltaResult<LogicalPlan> {
    let source_schema = plan.schema();
    let target_schema = target_plan.schema();
    let projection = target_schema
        .fields()
        .iter()
        .map(|target_field| {
            let name = target_field.name();
            let expr = match source_schema.qualified_field_with_unqualified_name(name) {
                Ok((qualifier, source_field)) => {
                    // Use a direct column reference here. DataFusion col(name) treats the value
                    // as SQL text and folds unquoted names to lowercase.
                    let source_col =
                        Expr::Column(Column::new(qualifier.cloned(), source_field.name().clone()));
                    if source_field.data_type() != target_field.data_type() {
                        cast(source_col, target_field.data_type().clone()).alias(name)
                    } else {
                        source_col.alias(name)
                    }
                }
                Err(_) => {
                    // Keep alignment behavior for missing or ambiguous fields.
                    cast(lit(ScalarValue::Null), target_field.data_type().clone()).alias(name)
                }
            };
            Ok(expr)
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    Ok(LogicalPlanBuilder::new(plan).project(projection)?.build()?)
}

fn build_exec_options(
    snapshot: Option<&EagerSnapshot>,
    partition_columns: Vec<String>,
    target_file_size: Option<Option<NonZeroU64>>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    configuration: &HashMap<String, Option<String>>,
) -> WriteExecOptions {
    let config = snapshot.map(|snapshot| snapshot.table_properties());
    let target_file_size =
        target_file_size.unwrap_or_else(|| Some(get_target_file_size(config, configuration)));
    let (num_indexed_cols, stats_columns) =
        get_num_idx_cols_and_stats_columns(config, configuration.clone());

    WriteExecOptions {
        partition_columns,
        target_file_size,
        write_batch_size,
        writer_properties,
        writer_stats_config: WriterStatsConfig {
            num_indexed_cols,
            stats_columns,
        },
    }
}

fn resolve_exact_validation(
    session: &dyn Session,
    insert_plan: &LogicalPlan,
    predicate: Option<Expression>,
) -> DeltaResult<Option<Expr>> {
    let df_schema = insert_plan
        .schema()
        .as_ref()
        .clone()
        .replace_qualifier(UNNAMED_TABLE);
    Ok(predicate
        .map(|predicate| predicate.resolve(session, Arc::new(df_schema)))
        .transpose()?)
}

fn validate_partition_columns_in_schema(
    partition_columns: &[String],
    schema: &Schema,
) -> DeltaResult<()> {
    let missing = partition_columns
        .iter()
        .filter(|column| schema.index_of(column.as_str()).is_err())
        .cloned()
        .collect::<Vec<_>>();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(WriteError::MissingPartitionColumns { columns: missing }.into())
    }
}

fn metadata_with_schema_and_partition_columns(
    metadata: &Metadata,
    schema: &StructType,
    partition_columns: &[String],
) -> DeltaResult<Metadata> {
    let mut value = serde_json::to_value(metadata)?;
    value["schemaString"] = serde_json::Value::String(serde_json::to_string(schema)?);
    value["partitionColumns"] = serde_json::Value::Array(
        partition_columns
            .iter()
            .map(|column| serde_json::Value::String(column.clone()))
            .collect(),
    );
    Ok(serde_json::from_value(value)?)
}

fn schema_delta_for_prepared_source(
    snapshot: Option<&EagerSnapshot>,
    insert_plan: &LogicalPlan,
    partition_columns: &[String],
    mode: SaveMode,
    schema_mode: Option<SchemaMode>,
    schema_drift: bool,
    new_schema: Option<&Schema>,
) -> DeltaResult<SchemaDelta> {
    let Some(snapshot) = snapshot else {
        return Ok(SchemaDelta::default());
    };

    let partition_columns_changed = snapshot.metadata().partition_columns() != partition_columns;
    let should_update_metadata = match schema_mode {
        Some(SchemaMode::Merge) if schema_drift => true,
        Some(SchemaMode::Overwrite) if mode == SaveMode::Overwrite => {
            let delta_schema: StructType = insert_plan.schema().as_arrow().try_into_kernel()?;
            &delta_schema != snapshot.schema().as_ref() || partition_columns_changed
        }
        _ => false,
    };

    if !should_update_metadata {
        return Ok(SchemaDelta::default());
    }

    let schema_struct: StructType = match (schema_mode, schema_drift, new_schema) {
        (Some(SchemaMode::Merge), true, Some(schema)) => schema.try_into_kernel()?,
        _ => insert_plan.schema().as_arrow().try_into_kernel()?,
    };

    if &schema_struct == snapshot.schema().as_ref() && !partition_columns_changed {
        return Ok(SchemaDelta::default());
    }

    if partition_columns_changed {
        tracing::info!(
            previous_partition_columns = ?snapshot.metadata().partition_columns(),
            new_partition_columns = ?partition_columns,
            "replacing table partition columns during schema overwrite"
        );
    }

    let current_protocol = snapshot.protocol();
    let configuration = snapshot.metadata().configuration().clone();
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&schema_struct)?
        .move_table_properties_into_features(&configuration);

    let metadata = metadata_with_schema_and_partition_columns(
        snapshot.metadata(),
        &schema_struct,
        partition_columns,
    )?;

    Ok(SchemaDelta {
        metadata: Some(metadata.into()),
        protocol: (current_protocol != &new_protocol).then_some(new_protocol.into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
    use datafusion::prelude::{col, lit};
    use uuid::Uuid;

    use crate::DeltaTable;
    use crate::TableProperty;
    use crate::delta_datafusion::create_session;
    use crate::operations::cdc::CDC_COLUMN_NAME;
    use crate::protocol::SaveMode;
    use crate::writer::test_utils::{
        get_arrow_schema, get_record_batch, setup_table_with_configuration,
    };

    fn source_plan_for_batch(batch: RecordBatch) -> LogicalPlan {
        LogicalPlanBuilder::scan(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(
                MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn assert_passthrough_overwrite_plan(
        overwrite_plan: &MatchedFilesRewritePlan,
        prepared: &PreparedWrite,
    ) {
        assert_eq!(overwrite_plan.kind, RewriteKind::Passthrough);
        assert!(overwrite_plan.matched_existing.is_empty());
        assert!(overwrite_plan.cdc_plan.is_none());
        assert_eq!(
            overwrite_plan.data_plan.schema().as_arrow(),
            prepared.insert_plan.schema().as_arrow()
        );
    }

    #[tokio::test]
    async fn test_prepare_write_emits_schema_delta_for_merge() {
        let table = DeltaTable::new_in_memory()
            .write(vec![get_record_batch(None, false)])
            .await
            .unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("A")])),
                Arc::new(Int32Array::from(vec![Some(100)])),
                Arc::new(StringArray::from(vec![Some("2026-04-16")])),
                Arc::new(StringArray::from(vec![Some("extra")])),
            ],
        )
        .unwrap();

        let session = create_session().state();
        let configuration = HashMap::new();
        let prepared = prepare_write(WritePreparationInput {
            snapshot: Some(table.snapshot().unwrap().snapshot()),
            session: &session,
            source: source_plan_for_batch(batch),
            mode: SaveMode::Append,
            schema_mode: Some(SchemaMode::Merge),
            safe_cast: false,
            partition_columns: vec![],
            predicate: None,
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        assert!(
            prepared
                .schema_delta
                .into_actions()
                .iter()
                .any(|action| matches!(action, Action::Metadata(_)))
        );
        assert!(
            prepared
                .insert_plan
                .schema()
                .iter()
                .any(|(_, field)| field.name() == "extra")
        );
    }

    #[tokio::test]
    async fn test_prepare_write_schema_overwrite_noop_has_no_schema_delta() {
        let batch = get_record_batch(None, false);
        let table = DeltaTable::new_in_memory()
            .write(vec![batch.clone()])
            .await
            .unwrap();

        let session = create_session().state();
        let configuration = HashMap::new();
        let prepared = prepare_write(WritePreparationInput {
            snapshot: Some(table.snapshot().unwrap().snapshot()),
            session: &session,
            source: source_plan_for_batch(batch),
            mode: SaveMode::Overwrite,
            schema_mode: Some(SchemaMode::Overwrite),
            safe_cast: false,
            partition_columns: vec![],
            predicate: None,
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        assert!(prepared.schema_delta.is_empty());
        assert!(prepared.exact_validation.is_none());
    }

    #[tokio::test]
    async fn test_plan_overwrite_rewrite_passthrough_cases() {
        let session = create_session().state();
        let configuration = HashMap::new();
        let table_without_snapshot = DeltaTable::new_in_memory();
        let prepared_without_snapshot = prepare_write(WritePreparationInput {
            snapshot: None,
            session: &session,
            source: source_plan_for_batch(get_record_batch(None, false)),
            mode: SaveMode::Overwrite,
            schema_mode: None,
            safe_cast: false,
            partition_columns: vec![],
            predicate: Some(col("id").eq(lit("A")).into()),
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        let overwrite_plan_without_snapshot = plan_overwrite_rewrite(
            None,
            &table_without_snapshot.log_store(),
            &session,
            SaveMode::Overwrite,
            &prepared_without_snapshot,
            Uuid::new_v4(),
        )
        .await
        .unwrap();
        assert_passthrough_overwrite_plan(
            &overwrite_plan_without_snapshot,
            &prepared_without_snapshot,
        );

        let table_with_snapshot = DeltaTable::new_in_memory()
            .write(vec![get_record_batch(None, false)])
            .await
            .unwrap();

        let prepared_append = prepare_write(WritePreparationInput {
            snapshot: Some(table_with_snapshot.snapshot().unwrap().snapshot()),
            session: &session,
            source: source_plan_for_batch(get_record_batch(None, false)),
            mode: SaveMode::Append,
            schema_mode: None,
            safe_cast: false,
            partition_columns: vec![],
            predicate: Some(col("id").eq(lit("A")).into()),
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        let overwrite_plan_append = plan_overwrite_rewrite(
            Some(table_with_snapshot.snapshot().unwrap().snapshot()),
            &table_with_snapshot.log_store(),
            &session,
            SaveMode::Append,
            &prepared_append,
            Uuid::new_v4(),
        )
        .await
        .unwrap();
        assert_passthrough_overwrite_plan(&overwrite_plan_append, &prepared_append);
    }

    #[tokio::test]
    #[should_panic(expected = "plan_overwrite_rewrite mode must match prepared write mode")]
    async fn test_plan_overwrite_rewrite_panics_on_mode_mismatch() {
        let table = DeltaTable::new_in_memory()
            .write(vec![get_record_batch(None, false)])
            .await
            .unwrap();

        let session = create_session().state();
        let configuration = HashMap::new();
        let prepared = prepare_write(WritePreparationInput {
            snapshot: Some(table.snapshot().unwrap().snapshot()),
            session: &session,
            source: source_plan_for_batch(get_record_batch(None, false)),
            mode: SaveMode::Append,
            schema_mode: None,
            safe_cast: false,
            partition_columns: vec![],
            predicate: None,
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        let _ = plan_overwrite_rewrite(
            Some(table.snapshot().unwrap().snapshot()),
            &table.log_store(),
            &session,
            SaveMode::Overwrite,
            &prepared,
            Uuid::new_v4(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_plan_overwrite_rewrite_no_match_preserves_insert_plan() {
        let table = DeltaTable::new_in_memory()
            .write(vec![get_record_batch(None, false)])
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            get_arrow_schema(&None),
            vec![
                Arc::new(StringArray::from(vec![Some("Z")])),
                Arc::new(Int32Array::from(vec![Some(999)])),
                Arc::new(StringArray::from(vec![Some("2026-04-16")])),
            ],
        )
        .unwrap();

        let session = create_session().state();
        let configuration = HashMap::new();
        let prepared = prepare_write(WritePreparationInput {
            snapshot: Some(table.snapshot().unwrap().snapshot()),
            session: &session,
            source: source_plan_for_batch(batch),
            mode: SaveMode::Overwrite,
            schema_mode: None,
            safe_cast: false,
            partition_columns: vec![],
            predicate: Some(col("id").eq(lit("missing")).into()),
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        let overwrite_plan = plan_overwrite_rewrite(
            Some(table.snapshot().unwrap().snapshot()),
            &table.log_store(),
            &session,
            SaveMode::Overwrite,
            &prepared,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        assert_eq!(overwrite_plan.kind, RewriteKind::NoMatch);
        assert!(overwrite_plan.matched_existing.is_empty());
        assert!(overwrite_plan.cdc_plan.is_none());
        assert_eq!(
            overwrite_plan.data_plan.schema().as_arrow(),
            prepared.insert_plan.schema().as_arrow()
        );
    }

    #[tokio::test]
    async fn test_plan_overwrite_rewrite_partition_only_avoids_rescue_scan() {
        let table = DeltaTable::new_in_memory()
            .write(vec![get_record_batch(None, false)])
            .with_partition_columns(["id"])
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            get_arrow_schema(&None),
            vec![
                Arc::new(StringArray::from(vec![Some("A")])),
                Arc::new(Int32Array::from(vec![Some(999)])),
                Arc::new(StringArray::from(vec![Some("2026-04-16")])),
            ],
        )
        .unwrap();

        let session = create_session().state();
        let configuration = HashMap::new();
        let prepared = prepare_write(WritePreparationInput {
            snapshot: Some(table.snapshot().unwrap().snapshot()),
            session: &session,
            source: source_plan_for_batch(batch),
            mode: SaveMode::Overwrite,
            schema_mode: None,
            safe_cast: false,
            partition_columns: vec!["id".to_string()],
            predicate: Some(col("id").eq(lit("A")).into()),
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        let overwrite_plan = plan_overwrite_rewrite(
            Some(table.snapshot().unwrap().snapshot()),
            &table.log_store(),
            &session,
            SaveMode::Overwrite,
            &prepared,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        assert_eq!(overwrite_plan.kind, RewriteKind::PartitionOnly);
        assert!(overwrite_plan.cdc_plan.is_none());
        let (sink_plan, contains_cdc, insert_marker_column) =
            overwrite_plan.build_sink_plan().unwrap();
        assert!(!contains_cdc);
        assert!(insert_marker_column.is_none());
        assert_eq!(
            sink_plan.schema().as_arrow(),
            prepared.insert_plan.schema().as_arrow()
        );
        assert!(
            sink_plan
                .schema()
                .iter()
                .all(|(_, field)| field.name() != CDC_COLUMN_NAME
                    && field.name() != WRITE_INSERT_MARKER_COLUMN)
        );
        assert!(overwrite_plan.matched_existing.num_files() > 0);
    }

    #[tokio::test]
    async fn test_plan_overwrite_rewrite_full_overwrite_collects_matched_existing_files() {
        let table = DeltaTable::new_in_memory()
            .write(vec![get_record_batch(None, false)])
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            get_arrow_schema(&None),
            vec![
                Arc::new(StringArray::from(vec![Some("Z")])),
                Arc::new(Int32Array::from(vec![Some(999)])),
                Arc::new(StringArray::from(vec![Some("2026-04-16")])),
            ],
        )
        .unwrap();

        let session = create_session().state();
        let configuration = HashMap::new();
        let prepared = prepare_write(WritePreparationInput {
            snapshot: Some(table.snapshot().unwrap().snapshot()),
            session: &session,
            source: source_plan_for_batch(batch),
            mode: SaveMode::Overwrite,
            schema_mode: None,
            safe_cast: false,
            partition_columns: vec![],
            predicate: None,
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        let overwrite_plan = plan_overwrite_rewrite(
            Some(table.snapshot().unwrap().snapshot()),
            &table.log_store(),
            &session,
            SaveMode::Overwrite,
            &prepared,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        assert_eq!(overwrite_plan.kind, RewriteKind::FullTable);
        assert!(overwrite_plan.cdc_plan.is_none());
        assert!(overwrite_plan.deletion_timestamp.is_some());
        assert!(
            overwrite_plan
                .matched_existing
                .clone()
                .into_actions(Some(1_713_398_400_000))
                .unwrap()
                .into_iter()
                .all(|action| matches!(action, Action::Remove(_)))
        );
    }

    #[tokio::test]
    async fn test_plan_overwrite_rewrite_marks_cdc_for_predicate_rewrite() {
        let table =
            setup_table_with_configuration(TableProperty::EnableChangeDataFeed, Some("true"))
                .await
                .write(vec![get_record_batch(None, false)])
                .await
                .unwrap();

        let batch = RecordBatch::try_new(
            get_arrow_schema(&None),
            vec![
                Arc::new(StringArray::from(vec![Some("3")])),
                Arc::new(Int32Array::from(vec![Some(3)])),
                Arc::new(StringArray::from(vec![Some("updated")])),
            ],
        )
        .unwrap();

        let session = create_session().state();
        let configuration = HashMap::new();
        let prepared = prepare_write(WritePreparationInput {
            snapshot: Some(table.snapshot().unwrap().snapshot()),
            session: &session,
            source: source_plan_for_batch(batch),
            mode: SaveMode::Overwrite,
            schema_mode: None,
            safe_cast: false,
            partition_columns: vec![],
            predicate: Some(col("value").eq(lit(3)).into()),
            target_file_size: None,
            write_batch_size: None,
            writer_properties: None,
            configuration: &configuration,
        })
        .unwrap();

        let overwrite_plan = plan_overwrite_rewrite(
            Some(table.snapshot().unwrap().snapshot()),
            &table.log_store(),
            &session,
            SaveMode::Overwrite,
            &prepared,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        assert_eq!(overwrite_plan.kind, RewriteKind::DataRescue);
        assert!(overwrite_plan.matched_existing.num_files() > 0);
        assert!(overwrite_plan.cdc_plan.is_some());
        assert!(
            overwrite_plan
                .data_plan
                .schema()
                .iter()
                .all(|(_, field)| field.name() != CDC_COLUMN_NAME)
        );
    }
}
