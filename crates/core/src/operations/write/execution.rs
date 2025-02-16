use std::sync::Arc;
use std::vec;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::prelude::DataFrame;
use datafusion_expr::{lit, Expr, LogicalPlanBuilder};
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::prefix::PrefixStore;
use parquet::file::properties::WriterProperties;
use tracing::log::*;
use uuid::Uuid;

use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::{find_files, DeltaScanConfigBuilder, DeltaTableProvider};
use crate::delta_datafusion::{DataFusionMixins, DeltaDataChecker};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, AddCDCFile, Remove, StructType, StructTypeExt};
use crate::logstore::LogStoreRef;
use crate::operations::cdc::should_write_cdc;
use crate::operations::writer::{DeltaWriter, WriterConfig};
use crate::storage::ObjectStoreRef;
use crate::table::state::DeltaTableState;
use crate::table::Constraint as DeltaConstraint;

use super::configs::WriterStatsConfig;
use super::WriteError;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan_with_predicate(
    predicate: Option<Expr>,
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<Vec<Action>> {
    // We always take the plan Schema since the data may contain Large/View arrow types,
    // the schema and batches were prior constructed with this in mind.
    let schema: ArrowSchemaRef = plan.schema();
    let checker = if let Some(snapshot) = snapshot {
        DeltaDataChecker::new(snapshot)
    } else {
        debug!("Using plan schema to derive generated columns, since no snapshot was provided. Implies first write.");
        let delta_schema: StructType = schema.as_ref().try_into()?;
        DeltaDataChecker::new_with_generated_columns(
            delta_schema.get_generated_columns().unwrap_or_default(),
        )
    };
    let checker = match predicate {
        Some(pred) => {
            // TODO: get the name of the outer-most column? `*` will also work but would it be slower?
            let chk = DeltaConstraint::new("*", &fmt_expr_to_sql(&pred)?);
            checker.with_extra_constraints(vec![chk])
        }
        _ => checker,
    };
    // Write data to disk
    let mut tasks = vec![];
    for i in 0..plan.properties().output_partitioning().partition_count() {
        let inner_plan = plan.clone();
        let inner_schema = schema.clone();
        let task_ctx = Arc::new(TaskContext::from(&state));
        let config = WriterConfig::new(
            inner_schema.clone(),
            partition_columns.clone(),
            writer_properties.clone(),
            target_file_size,
            write_batch_size,
            writer_stats_config.num_indexed_cols,
            writer_stats_config.stats_columns.clone(),
        );
        let mut writer = DeltaWriter::new(object_store.clone(), config);
        let checker_stream = checker.clone();
        let mut stream = inner_plan.execute(i, task_ctx)?;

        let handle: tokio::task::JoinHandle<DeltaResult<Vec<Action>>> =
            tokio::task::spawn(async move {
                while let Some(maybe_batch) = stream.next().await {
                    let batch = maybe_batch?;
                    checker_stream.check_batch(&batch).await?;
                    writer.write(&batch).await?;
                }
                let add_actions = writer.close().await;
                match add_actions {
                    Ok(actions) => Ok(actions.into_iter().map(Action::Add).collect::<Vec<_>>()),
                    Err(err) => Err(err),
                }
            });

        tasks.push(handle);
    }
    let actions = futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| WriteError::WriteTask { source: err })?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .concat()
        .into_iter()
        .collect::<Vec<_>>();
    // Collect add actions to add to commit
    Ok(actions)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan_cdc(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<Vec<Action>> {
    let cdc_store = Arc::new(PrefixStore::new(object_store, "_change_data"));

    Ok(write_execution_plan(
        snapshot,
        state,
        plan,
        partition_columns,
        cdc_store,
        target_file_size,
        write_batch_size,
        writer_properties,
        writer_stats_config,
    )
    .await?
    .into_iter()
    .map(|add| {
        // Modify add actions into CDC actions
        match add {
            Action::Add(add) => {
                Action::Cdc(AddCDCFile {
                    // This is a gnarly hack, but the action needs the nested path, not the
                    // path isnide the prefixed store
                    path: format!("_change_data/{}", add.path),
                    size: add.size,
                    partition_values: add.partition_values,
                    data_change: false,
                    tags: add.tags,
                })
            }
            _ => panic!("Expected Add action"),
        }
    })
    .collect::<Vec<_>>())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<Vec<Action>> {
    write_execution_plan_with_predicate(
        None,
        snapshot,
        state,
        plan,
        partition_columns,
        object_store,
        target_file_size,
        write_batch_size,
        writer_properties,
        writer_stats_config,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_non_empty_expr(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: SessionState,
    partition_columns: Vec<String>,
    expression: &Expr,
    rewrite: &[Add],
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    partition_scan: bool,
    insert_df: DataFrame,
    operation_id: Uuid,
) -> DeltaResult<Vec<Action>> {
    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.
    let mut actions: Vec<Action> = Vec::new();

    // Take the insert plan schema since it might have been schema evolved, if its not
    // it is simply the table schema
    let scan_config = DeltaScanConfigBuilder::new()
        .with_schema(snapshot.input_schema()?)
        .build(snapshot)?;

    let target_provider = Arc::new(
        DeltaTableProvider::try_new(snapshot.clone(), log_store.clone(), scan_config.clone())?
            .with_files(rewrite.to_vec()),
    );

    let target_provider = provider_as_source(target_provider);
    let source = LogicalPlanBuilder::scan("target", target_provider.clone(), None)?.build()?;
    // We don't want to verify the predicate against existing data

    let df = DataFrame::new(state.clone(), source);

    if !partition_scan {
        // Apply the negation of the filter and rewrite files
        let negated_expression = Expr::Not(Box::new(Expr::IsTrue(Box::new(expression.clone()))));

        let filter = df
            .clone()
            .filter(negated_expression)?
            .create_physical_plan()
            .await?;

        let add_actions: Vec<Action> = write_execution_plan(
            Some(snapshot),
            state.clone(),
            filter,
            partition_columns.clone(),
            log_store.object_store(Some(operation_id)),
            Some(snapshot.table_config().target_file_size() as usize),
            None,
            writer_properties.clone(),
            writer_stats_config.clone(),
        )
        .await?;

        actions.extend(add_actions);
    }

    // CDC logic, simply filters data with predicate and adds the _change_type="delete" as literal column
    // Only write when CDC actions when it was not a partition scan, load_cdf can deduce the deletes in that case
    // based on the remove actions if a partition got deleted
    if !partition_scan {
        // We only write deletions when it was not a partition scan
        if let Some(cdc_actions) = execute_non_empty_expr_cdc(
            snapshot,
            log_store,
            state.clone(),
            df,
            expression,
            partition_columns,
            writer_properties,
            writer_stats_config,
            insert_df,
            operation_id,
        )
        .await?
        {
            actions.extend(cdc_actions)
        }
    }
    Ok(actions)
}

/// If CDC is enabled it writes all the deletions based on predicate into _change_data directory
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_non_empty_expr_cdc(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: SessionState,
    scan: DataFrame,
    expression: &Expr,
    table_partition_cols: Vec<String>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    insert_df: DataFrame,
    operation_id: Uuid,
) -> DeltaResult<Option<Vec<Action>>> {
    match should_write_cdc(snapshot) {
        // Create CDC scan
        Ok(true) => {
            let filter = scan.clone().filter(expression.clone())?;

            // Add literal column "_change_type"
            let delete_change_type_expr = lit("delete").alias("_change_type");

            let insert_change_type_expr = lit("insert").alias("_change_type");

            let delete_df = filter.with_column("_change_type", delete_change_type_expr)?;

            let insert_df = insert_df.with_column("_change_type", insert_change_type_expr)?;

            let cdc_df = delete_df.union(insert_df)?;

            let cdc_actions = write_execution_plan_cdc(
                Some(snapshot),
                state.clone(),
                cdc_df.create_physical_plan().await?,
                table_partition_cols.clone(),
                log_store.object_store(Some(operation_id)),
                Some(snapshot.table_config().target_file_size() as usize),
                None,
                writer_properties,
                writer_stats_config,
            )
            .await?;
            Ok(Some(cdc_actions))
        }
        _ => Ok(None),
    }
}

// This should only be called with a valid predicate
#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_predicate_actions(
    predicate: Expr,
    log_store: LogStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    partition_columns: Vec<String>,
    writer_properties: Option<WriterProperties>,
    deletion_timestamp: i64,
    writer_stats_config: WriterStatsConfig,
    insert_df: DataFrame,
    operation_id: Uuid,
) -> DeltaResult<Vec<Action>> {
    let candidates =
        find_files(snapshot, log_store.clone(), &state, Some(predicate.clone())).await?;

    let mut actions = execute_non_empty_expr(
        snapshot,
        log_store,
        state,
        partition_columns,
        &predicate,
        &candidates.candidates,
        writer_properties,
        writer_stats_config,
        candidates.partition_scan,
        insert_df,
        operation_id,
    )
    .await?;

    let remove = candidates.candidates;

    for action in remove {
        actions.push(Action::Remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            deletion_vector: action.deletion_vector,
            tags: None,
            base_row_id: action.base_row_id,
            default_row_commit_version: action.default_row_commit_version,
        }))
    }
    Ok(actions)
}
