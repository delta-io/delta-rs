//! Write planning is split into two stages:
//! - `prepare_write` normalizes incoming rows into table-shaped insert data and resolves exact
//!   validations against that prepared schema.
//! - `plan_overwrite_rewrite` adjusts the sink plan for overwrite flows and returns any
//!   overwrite-side actions the caller must commit before add actions.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_schema::Schema;
use datafusion::catalog::Session;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UNNAMED_TABLE, cast, lit, try_cast,
};
use datafusion::prelude::col;
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use futures::TryStreamExt as _;
use itertools::Itertools as _;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::SchemaMode;
use super::configs::WriterStatsConfig;
use super::execution::prepare_predicate_actions;
use super::generated_columns::{gc_is_enabled, with_generated_columns};
use super::metrics::SOURCE_COUNT_ID;
use super::schema_evolution::try_cast_schema;
use crate::delta_datafusion::DataFusionMixins;
use crate::delta_datafusion::Expression;
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::logical::MetricObserver;
use crate::errors::DeltaResult;
use crate::kernel::schema::cast::{merge_arrow_schema, normalize_for_delta};
use crate::kernel::{
    Action, EagerSnapshot, MetadataExt as _, ProtocolExt as _, StructType, StructTypeExt,
    new_metadata,
};
use crate::logstore::LogStoreRef;
use crate::operations::cdc::CDC_COLUMN_NAME;
use crate::operations::{get_num_idx_cols_and_stats_columns, get_target_file_size};
use crate::protocol::SaveMode;

/// Schema and protocol actions required before the sink executes the write.
#[derive(Clone, Default)]
pub(crate) struct SchemaDelta {
    metadata: Option<Action>,
    protocol: Option<Action>,
}

impl SchemaDelta {
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.metadata.is_none() && self.protocol.is_none()
    }

    pub(crate) fn actions(&self) -> Vec<Action> {
        let mut actions = Vec::with_capacity(2);
        if let Some(metadata) = &self.metadata {
            actions.push(metadata.clone());
        }
        if let Some(protocol) = &self.protocol {
            actions.push(protocol.clone());
        }
        actions
    }
}

/// Sink-only knobs that must survive planning unchanged.
#[derive(Clone)]
pub(crate) struct WriteExecOptions {
    pub partition_columns: Vec<String>,
    pub target_file_size: Option<NonZeroU64>,
    pub write_batch_size: Option<usize>,
    pub writer_properties: Option<WriterProperties>,
    pub writer_stats_config: WriterStatsConfig,
}

/// Prepared insert input plus the exact validations the sink must enforce.
pub(crate) struct PreparedWrite {
    pub insert_plan: LogicalPlan,
    pub schema_delta: SchemaDelta,
    pub exact_validations: Vec<Expr>,
    pub predicate_sql: Option<String>,
    pub exec_options: WriteExecOptions,
}

impl PreparedWrite {
    pub(crate) fn exact_validation_predicate(&self) -> Option<Expr> {
        conjunction(self.exact_validations.clone())
    }
}

/// Inputs required to normalize source rows into table-shaped insert data.
pub(crate) struct WritePreparationInput<'a> {
    pub snapshot: Option<&'a EagerSnapshot>,
    pub session: &'a dyn Session,
    pub source: LogicalPlan,
    pub mode: SaveMode,
    pub schema_mode: Option<SchemaMode>,
    pub safe_cast: bool,
    pub partition_columns: Vec<String>,
    pub predicate: Option<Expression>,
    pub target_file_size: Option<Option<NonZeroU64>>,
    pub write_batch_size: Option<usize>,
    pub writer_properties: Option<WriterProperties>,
    pub configuration: &'a HashMap<String, Option<String>>,
}

/// Planner output for overwrite flows before the sink materializes new files.
pub(crate) struct OverwritePlan {
    pub sink_plan: LogicalPlan,
    pub actions: Vec<Action>,
    pub contains_cdc: bool,
}

impl OverwritePlan {
    fn passthrough(insert_plan: LogicalPlan) -> Self {
        Self {
            sink_plan: insert_plan,
            actions: Vec::new(),
            contains_cdc: false,
        }
    }

    pub(crate) fn num_removed_files(&self) -> usize {
        self.actions
            .iter()
            .filter(|action| matches!(action, Action::Remove(_)))
            .count()
    }
}

pub(crate) fn prepare_write(input: WritePreparationInput<'_>) -> DeltaResult<PreparedWrite> {
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

    let exact_validations = resolve_exact_validations(session, &insert_plan, predicate)?;
    let predicate_sql = exact_validations.first().map(fmt_expr_to_sql).transpose()?;

    Ok(PreparedWrite {
        insert_plan,
        schema_delta,
        exact_validations,
        predicate_sql,
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

pub(crate) async fn plan_overwrite_rewrite(
    snapshot: Option<&EagerSnapshot>,
    log_store: &LogStoreRef,
    session: &dyn Session,
    mode: SaveMode,
    prepared_write: &PreparedWrite,
    operation_id: Uuid,
) -> DeltaResult<OverwritePlan> {
    let Some(snapshot) = snapshot else {
        return Ok(OverwritePlan::passthrough(
            prepared_write.insert_plan.clone(),
        ));
    };

    if !matches!(mode, SaveMode::Overwrite) {
        return Ok(OverwritePlan::passthrough(
            prepared_write.insert_plan.clone(),
        ));
    }

    let mut sink_plan = prepared_write.insert_plan.clone();
    let mut actions = Vec::new();
    let mut contains_cdc = false;

    match prepared_write.exact_validation_predicate() {
        Some(predicate) => {
            let deletion_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let (predicate_actions, cdf_df) = prepare_predicate_actions(
                predicate,
                log_store.clone(),
                snapshot,
                session,
                prepared_write.exec_options.partition_columns.clone(),
                prepared_write.exec_options.writer_properties.clone(),
                deletion_timestamp,
                prepared_write.exec_options.writer_stats_config.clone(),
                operation_id,
            )
            .await?;

            if let Some(cdf_df) = cdf_df {
                contains_cdc = true;
                let mut projection = sink_plan
                    .schema()
                    .iter()
                    .map(|(_, field)| col(field.name()))
                    .collect_vec();
                projection.push(lit("insert").alias(CDC_COLUMN_NAME));
                sink_plan = LogicalPlanBuilder::new(sink_plan)
                    .project(projection)?
                    .union(cdf_df)?
                    .build()?;
            }

            actions.extend(predicate_actions);
        }
        None => {
            let remove_actions = snapshot
                .file_views(log_store, None)
                .map_ok(|file| file.remove_action(true).into())
                .try_collect::<Vec<_>>()
                .await?;
            actions.extend(remove_actions);
        }
    }

    Ok(OverwritePlan {
        sink_plan,
        actions,
        contains_cdc,
    })
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

fn resolve_exact_validations(
    session: &dyn Session,
    insert_plan: &LogicalPlan,
    predicate: Option<Expression>,
) -> DeltaResult<Vec<Expr>> {
    let df_schema = insert_plan
        .schema()
        .as_ref()
        .clone()
        .replace_qualifier(UNNAMED_TABLE);
    Ok(predicate
        .map(|predicate| predicate.resolve(session, Arc::new(df_schema)))
        .transpose()?
        .into_iter()
        .collect())
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

    let should_update_schema = match schema_mode {
        Some(SchemaMode::Merge) if schema_drift => true,
        Some(SchemaMode::Overwrite) if mode == SaveMode::Overwrite => {
            let delta_schema: StructType = insert_plan.schema().as_arrow().try_into_kernel()?;
            &delta_schema != snapshot.schema().as_ref()
        }
        _ => false,
    };

    if !should_update_schema {
        return Ok(SchemaDelta::default());
    }

    let schema_struct: StructType = match (schema_mode, schema_drift, new_schema) {
        (Some(SchemaMode::Merge), true, Some(schema)) => schema.try_into_kernel()?,
        _ => insert_plan.schema().as_arrow().try_into_kernel()?,
    };

    if &schema_struct == snapshot.schema().as_ref() {
        return Ok(SchemaDelta::default());
    }

    let current_protocol = snapshot.protocol();
    let configuration = snapshot.metadata().configuration().clone();
    let new_protocol = current_protocol
        .clone()
        .apply_column_metadata_to_protocol(&schema_struct)?
        .move_table_properties_into_features(&configuration);

    let mut metadata = new_metadata(&schema_struct, partition_columns, configuration.clone())?;
    let existing_metadata_id = snapshot.metadata().id().to_string();
    if !existing_metadata_id.is_empty() {
        metadata = metadata.with_table_id(existing_metadata_id)?;
    }

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
    use delta_kernel::table_properties::DataSkippingNumIndexedCols;
    use uuid::Uuid;

    use crate::DeltaTable;
    use crate::TableProperty;
    use crate::delta_datafusion::create_session;
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

    fn remove_action(path: &str) -> Action {
        Action::Remove(crate::kernel::Remove {
            path: path.to_string(),
            data_change: true,
            deletion_timestamp: Some(1),
            extended_file_metadata: Some(true),
            partition_values: Some(HashMap::new()),
            size: Some(1),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        })
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
                .actions()
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
    }

    #[test]
    fn test_exact_validation_predicate_returns_none_without_predicate() {
        let prepared = PreparedWrite {
            insert_plan: source_plan_for_batch(get_record_batch(None, false)),
            schema_delta: SchemaDelta::default(),
            exact_validations: Vec::new(),
            predicate_sql: None,
            exec_options: WriteExecOptions {
                partition_columns: vec![],
                target_file_size: None,
                write_batch_size: None,
                writer_properties: None,
                writer_stats_config: WriterStatsConfig {
                    num_indexed_cols: DataSkippingNumIndexedCols::NumColumns(32),
                    stats_columns: None,
                },
            },
        };

        assert!(prepared.exact_validation_predicate().is_none());
    }

    #[tokio::test]
    async fn test_schema_delta_actions_preserve_metadata_then_protocol_order() {
        let table = DeltaTable::new_in_memory()
            .write(vec![get_record_batch(None, false)])
            .await
            .unwrap();
        let snapshot = table.snapshot().unwrap().snapshot();

        let schema_delta = SchemaDelta {
            metadata: Some(Action::Metadata(snapshot.metadata().clone())),
            protocol: Some(Action::Protocol(snapshot.protocol().clone())),
        };

        let actions = schema_delta.actions();
        assert!(matches!(actions.first(), Some(Action::Metadata(_))));
        assert!(matches!(actions.get(1), Some(Action::Protocol(_))));
    }

    #[test]
    fn test_overwrite_plan_num_removed_files_counts_only_remove_actions() {
        let overwrite_plan = OverwritePlan {
            sink_plan: source_plan_for_batch(get_record_batch(None, false)),
            actions: vec![
                remove_action("part-000.parquet"),
                Action::Protocol(Default::default()),
                remove_action("part-001.parquet"),
            ],
            contains_cdc: false,
        };

        assert_eq!(overwrite_plan.num_removed_files(), 2);
    }

    #[tokio::test]
    async fn test_plan_overwrite_rewrite_passthrough_without_snapshot() {
        let table = DeltaTable::new_in_memory();
        let session = create_session().state();
        let configuration = HashMap::new();
        let prepared = prepare_write(WritePreparationInput {
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

        let overwrite_plan = plan_overwrite_rewrite(
            None,
            &table.log_store(),
            &session,
            SaveMode::Overwrite,
            &prepared,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        assert!(overwrite_plan.actions.is_empty());
        assert!(!overwrite_plan.contains_cdc);
        assert_eq!(
            overwrite_plan.sink_plan.schema().as_arrow(),
            prepared.insert_plan.schema().as_arrow()
        );
    }

    #[tokio::test]
    async fn test_plan_overwrite_rewrite_passthrough_for_append_mode() {
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
            SaveMode::Append,
            &prepared,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        assert!(overwrite_plan.actions.is_empty());
        assert!(!overwrite_plan.contains_cdc);
        assert_eq!(
            overwrite_plan.sink_plan.schema().as_arrow(),
            prepared.insert_plan.schema().as_arrow()
        );
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

        assert!(overwrite_plan.actions.is_empty());
        assert!(!overwrite_plan.contains_cdc);
        assert_eq!(
            overwrite_plan.sink_plan.schema().as_arrow(),
            prepared.insert_plan.schema().as_arrow()
        );
    }

    #[tokio::test]
    async fn test_plan_overwrite_rewrite_full_overwrite_collects_remove_actions() {
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

        assert!(!overwrite_plan.contains_cdc);
        assert!(
            overwrite_plan
                .actions
                .iter()
                .any(|action| matches!(action, Action::Remove(_)))
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

        assert!(overwrite_plan.contains_cdc);
        assert!(
            overwrite_plan
                .sink_plan
                .schema()
                .iter()
                .any(|(_, field)| field.name() == CDC_COLUMN_NAME)
        );
    }
}
