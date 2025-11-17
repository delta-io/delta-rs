use std::sync::{Arc, OnceLock};
use std::vec;

use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{col, lit, when, Expr, LogicalPlanBuilder};
use datafusion::physical_plan::{execute_stream_partitioned, ExecutionPlan};
use datafusion::prelude::DataFrame;
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use futures::StreamExt;
use object_store::prefix::PrefixStore;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc;
use tracing::log::*;
use uuid::Uuid;

use super::writer::{DeltaWriter, WriterConfig};
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::{
    find_files, session_state_from_session, DataFusionMixins, DeltaDataChecker,
    DeltaScanConfigBuilder, DeltaTableProvider,
};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, AddCDCFile, EagerSnapshot, Remove, StructType, StructTypeExt};
use crate::logstore::{LogStoreRef, ObjectStoreRef};
use crate::operations::cdc::{should_write_cdc, CDC_COLUMN_NAME};
use crate::operations::write::WriterStatsConfig;
use crate::table::config::TablePropertiesExt as _;
use crate::table::Constraint as DeltaConstraint;
use crate::DeltaTableError;

const DEFAULT_WRITER_BATCH_CHANNEL_SIZE: usize = 10;

fn channel_size() -> usize {
    static CHANNEL_SIZE: OnceLock<usize> = OnceLock::new();
    *CHANNEL_SIZE.get_or_init(|| {
        std::env::var("DELTARS_WRITER_BATCH_CHANNEL_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_WRITER_BATCH_CHANNEL_SIZE)
    })
}

/// Metrics captured from execution
#[derive(Debug, Default)]
pub(crate) struct WriteExecutionPlanMetrics {
    pub scan_time_ms: u64,
    pub write_time_ms: u64,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan_cdc(
    snapshot: Option<&EagerSnapshot>,
    session: &dyn Session,
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
        session,
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
                    // path inside the prefixed store
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
    snapshot: Option<&EagerSnapshot>,
    session: &dyn Session,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<Vec<Action>> {
    let (actions, _) = write_execution_plan_v2(
        snapshot,
        session,
        plan,
        partition_columns,
        object_store,
        target_file_size,
        write_batch_size,
        writer_properties,
        writer_stats_config,
        None,
        false,
    )
    .await?;
    Ok(actions)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_non_empty_expr(
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    session: &dyn Session,
    partition_columns: Vec<String>,
    expression: &Expr,
    rewrite: &[Add],
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    partition_scan: bool,
    operation_id: Uuid,
) -> DeltaResult<(Vec<Action>, Option<DataFrame>)> {
    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.
    let mut actions: Vec<Action> = Vec::new();

    // Take the insert plan schema since it might have been schema evolved, if its not
    // it is simply the table schema
    let scan_config = DeltaScanConfigBuilder::new()
        .with_schema(snapshot.input_schema())
        .build(snapshot)?;

    let target_provider = Arc::new(
        DeltaTableProvider::try_new(snapshot.clone(), log_store.clone(), scan_config.clone())?
            .with_files(rewrite.to_vec()),
    );

    let target_provider = provider_as_source(target_provider);
    let source =
        Arc::new(LogicalPlanBuilder::scan("target", target_provider.clone(), None)?.build()?);

    let cdf_df = if !partition_scan {
        // Apply the negation of the filter and rewrite files
        let negated_expression = Expr::Not(Box::new(Expr::IsTrue(Box::new(expression.clone()))));
        let filter = LogicalPlanBuilder::from(source.clone())
            .filter(negated_expression)?
            .build()?;
        let filter = session.create_physical_plan(&filter).await?;

        let add_actions: Vec<Action> = write_execution_plan(
            Some(snapshot),
            session,
            filter,
            partition_columns.clone(),
            log_store.object_store(Some(operation_id)),
            Some(snapshot.table_properties().target_file_size().get() as usize),
            None,
            writer_properties.clone(),
            writer_stats_config.clone(),
        )
        .await?;

        actions.extend(add_actions);

        // CDC logic, simply filters data with predicate and adds the _change_type="delete" as literal column
        // Only write when CDC actions when it was not a partition scan, load_cdf can deduce the deletes in that case
        // based on the remove actions if a partition got deleted
        if should_write_cdc(snapshot)? {
            let state = session_state_from_session(session)?;
            let df = DataFrame::new(state.clone(), source.as_ref().clone());
            Some(
                df.filter(expression.clone())?
                    .with_column(CDC_COLUMN_NAME, lit("delete"))?,
            )
        } else {
            None
        }
    } else {
        None
    };

    Ok((actions, cdf_df))
}

// This should only be called with a valid predicate
#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_predicate_actions(
    predicate: Expr,
    log_store: LogStoreRef,
    snapshot: &EagerSnapshot,
    session: &dyn Session,
    partition_columns: Vec<String>,
    writer_properties: Option<WriterProperties>,
    deletion_timestamp: i64,
    writer_stats_config: WriterStatsConfig,
    operation_id: Uuid,
) -> DeltaResult<(Vec<Action>, Option<DataFrame>)> {
    let candidates = find_files(
        snapshot,
        log_store.clone(),
        session,
        Some(predicate.clone()),
    )
    .await?;

    let (mut actions, cdf_df) = execute_non_empty_expr(
        snapshot,
        log_store,
        session,
        partition_columns,
        &predicate,
        &candidates.candidates,
        writer_properties,
        writer_stats_config,
        candidates.partition_scan,
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
    Ok((actions, cdf_df))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan_v2(
    snapshot: Option<&EagerSnapshot>,
    session: &dyn Session,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    predicate: Option<Expr>,
    contains_cdc: bool,
) -> DeltaResult<(Vec<Action>, WriteExecutionPlanMetrics)> {
    // We always take the plan Schema since the data may contain Large/View arrow types,
    // the schema and batches were prior constructed with this in mind.
    let schema = plan.schema();
    let mut checker = if let Some(snapshot) = snapshot {
        DeltaDataChecker::new(snapshot)
    } else {
        debug!("Using plan schema to derive generated columns, since no snapshot was provided. Implies first write.");
        let delta_schema: StructType = schema.as_ref().try_into_kernel()?;
        DeltaDataChecker::new_with_generated_columns(
            delta_schema.get_generated_columns().unwrap_or_default(),
        )
    };

    if let Some(mut pred) = predicate {
        if contains_cdc {
            pred = when(col(CDC_COLUMN_NAME).eq(lit("insert")), pred).otherwise(lit(true))?;
        }
        let chk = DeltaConstraint::new("*", &fmt_expr_to_sql(&pred)?);
        checker = checker.with_extra_constraints(vec![chk])
    }

    // Write data to disk
    // We drive partition streams concurrently and centralize writes via an mpsc channel.
    if !contains_cdc {
        let config = WriterConfig::new(
            schema.clone(),
            partition_columns.clone(),
            writer_properties.clone(),
            target_file_size,
            write_batch_size,
            writer_stats_config.num_indexed_cols,
            writer_stats_config.stats_columns.clone(),
        );

        let checker_stream = checker.clone();

        let partition_streams: Vec<SendableRecordBatchStream> =
            execute_stream_partitioned(plan, session.task_ctx())?;

        // sync channel for batches produced by partition stream
        let (tx, mut rx) = mpsc::channel::<RecordBatch>(channel_size());

        let writer_handle = tokio::task::spawn(async move {
            let mut writer = DeltaWriter::new(object_store.clone(), config);
            let mut total_write_ms: u64 = 0;
            while let Some(batch) = rx.recv().await {
                let wstart = std::time::Instant::now();
                writer.write(&batch).await?;
                total_write_ms += wstart.elapsed().as_millis() as u64;
            }
            let adds = writer.close().await?;
            Ok::<(Vec<Add>, u64), DeltaTableError>((adds, total_write_ms))
        });

        // spawn one worker per partition stream to drive DataFusion concurrently
        let mut worker_handles = Vec::with_capacity(partition_streams.len());
        let scan_start = std::time::Instant::now();
        for mut partition_stream in partition_streams {
            let tx_clone = tx.clone();
            let checker_clone = checker_stream.clone();
            let handle = tokio::task::spawn(async move {
                while let Some(maybe_batch) = partition_stream.next().await {
                    let batch = maybe_batch?;
                    checker_clone.check_batch(&batch).await?;
                    tx_clone.send(batch).await.map_err(|_| {
                        DeltaTableError::Generic("Writer task closed unexpectedly".to_string())
                    })?;
                }
                Ok::<(), DeltaTableError>(())
            });
            worker_handles.push(handle);
        }

        drop(tx);

        let join_res = writer_handle
            .await
            .map_err(|e| DeltaTableError::Generic(format!("writer join error: {}", e)))?;
        let (adds, write_time_ms) = join_res?;

        for h in worker_handles {
            let join_res = h.await.map_err(|e| {
                DeltaTableError::Generic(format!("worker join error when driving partition: {}", e))
            })?;
            join_res?;
        }

        let write_elapsed = write_time_ms;
        let scan_time_ms = scan_start.elapsed().as_millis() as u64;
        let scan_time_ms = scan_time_ms.saturating_sub(write_elapsed);

        let metrics = WriteExecutionPlanMetrics {
            scan_time_ms,
            write_time_ms: write_elapsed,
        };

        let actions = adds.into_iter().map(Action::Add).collect::<Vec<_>>();
        return Ok((actions, metrics));
    } else {
        // CDC branch: create two writer tasks (normal + cdf) and drive partition streams concurrently
        let cdf_store = Arc::new(PrefixStore::new(object_store.clone(), "_change_data"));

        let write_schema = Arc::new(Schema::new(
            schema
                .clone()
                .fields()
                .into_iter()
                .filter_map(|f| {
                    if f.name() != CDC_COLUMN_NAME {
                        Some(f.as_ref().clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let cdf_schema = schema.clone();

        let normal_config = WriterConfig::new(
            write_schema.clone(),
            partition_columns.clone(),
            writer_properties.clone(),
            target_file_size,
            write_batch_size,
            writer_stats_config.num_indexed_cols,
            writer_stats_config.stats_columns.clone(),
        );

        let cdf_config = WriterConfig::new(
            cdf_schema.clone(),
            partition_columns.clone(),
            writer_properties.clone(),
            target_file_size,
            write_batch_size,
            writer_stats_config.num_indexed_cols,
            writer_stats_config.stats_columns.clone(),
        );

        let checker_stream = checker.clone();

        // partition streams
        let partition_streams = execute_stream_partitioned(plan, session.task_ctx())?;

        // sync channel for batches produced by partition stream for normal and cdf batches
        let (tx_normal, mut rx_normal) = mpsc::channel::<RecordBatch>(channel_size());
        let (tx_cdf, mut rx_cdf) = mpsc::channel::<RecordBatch>(channel_size());

        let normal_writer_handle = tokio::task::spawn(async move {
            let mut writer = DeltaWriter::new(object_store, normal_config);
            let mut total_write_ms: u64 = 0;
            while let Some(batch) = rx_normal.recv().await {
                let wstart = std::time::Instant::now();
                writer.write(&batch).await?;
                total_write_ms += wstart.elapsed().as_millis() as u64;
            }
            let adds = writer.close().await?;
            Ok::<(Vec<Add>, u64), DeltaTableError>((adds, total_write_ms))
        });

        let cdf_writer_handle = tokio::task::spawn(async move {
            let mut writer = DeltaWriter::new(cdf_store, cdf_config);
            let mut total_write_ms: u64 = 0;
            while let Some(batch) = rx_cdf.recv().await {
                let wstart = std::time::Instant::now();
                writer.write(&batch).await?;
                total_write_ms += wstart.elapsed().as_millis() as u64;
            }
            let adds = writer.close().await?;
            Ok::<(Vec<Add>, u64), DeltaTableError>((adds, total_write_ms))
        });

        // spawn partition workers that split batches and send to appropriate writer channel
        let mut worker_handles = Vec::with_capacity(partition_streams.len());
        let scan_start = std::time::Instant::now();
        for mut partition_stream in partition_streams {
            let txn = tx_normal.clone();
            let txc = tx_cdf.clone();
            let checker_clone = checker_stream.clone();
            let session_ctx = SessionContext::new();
            let cdf_schema_clone = cdf_schema.clone();

            let h = tokio::task::spawn(async move {
                while let Some(maybe_batch) = partition_stream.next().await {
                    let batch = maybe_batch?;

                    // split batch since upstream unioned write and cdf plans
                    let table_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
                        batch.schema(),
                        vec![vec![batch.clone()]],
                    )?);
                    let batch_df = session_ctx.read_table(table_provider).unwrap();

                    let normal_df = batch_df.clone().filter(col(CDC_COLUMN_NAME).in_list(
                        vec![lit("delete"), lit("source_delete"), lit("update_preimage")],
                        true,
                    ))?;

                    let cdf_df = batch_df.filter(col(CDC_COLUMN_NAME).in_list(
                        vec![
                            lit("delete"),
                            lit("insert"),
                            lit("update_preimage"),
                            lit("update_postimage"),
                        ],
                        false,
                    ))?;

                    // Concatenate with the CDF_schema, since we need to keep the _change_type col
                    let mut normal_batch =
                        concat_batches(&cdf_schema_clone, &normal_df.collect().await?)?;
                    checker_clone.check_batch(&normal_batch).await?;

                    // Drop the CDC_COLUMN ("_change_type")
                    let mut idx: Option<usize> = None;
                    for (i_field, field) in normal_batch.schema_ref().fields().iter().enumerate() {
                        if field.name() == CDC_COLUMN_NAME {
                            idx = Some(i_field);
                            break;
                        }
                    }
                    normal_batch.remove_column(idx.ok_or(DeltaTableError::generic(
                        "idx of _change_type col not found. This shouldn't have happened.",
                    ))?);

                    let cdf_batch = concat_batches(&cdf_schema_clone, &cdf_df.collect().await?)?;
                    checker_clone.check_batch(&cdf_batch).await?;

                    // send to writers channels
                    txn.send(normal_batch).await.map_err(|_| {
                        DeltaTableError::Generic("normal writer closed unexpectedly".to_string())
                    })?;
                    txc.send(cdf_batch).await.map_err(|_| {
                        DeltaTableError::Generic("cdf writer closed unexpectedly".to_string())
                    })?;
                }
                Ok::<(), DeltaTableError>(())
            });

            worker_handles.push(h);
        }

        // drop original senders so writer tasks exit after all workers finish
        drop(tx_normal);
        drop(tx_cdf);

        // wait for writer tasks to finish
        let normal_join = normal_writer_handle
            .await
            .map_err(|e| DeltaTableError::Generic(format!("normal writer join error: {}", e)))?;
        let (normal_adds, normal_write_ms) = normal_join?;

        let cdf_join = cdf_writer_handle
            .await
            .map_err(|e| DeltaTableError::Generic(format!("cdf writer join error: {}", e)))?;
        let (cdf_adds, cdf_write_ms) = cdf_join?;

        // check if workers that collect stream didnt fail
        for h in worker_handles {
            let join_res = h.await.map_err(|e| {
                DeltaTableError::Generic(format!("worker join error when driving partition: {}", e))
            })?;
            join_res?;
        }

        let mut actions = normal_adds.into_iter().map(Action::Add).collect::<Vec<_>>();
        let mut cdf_actions = cdf_adds
            .into_iter()
            .map(|add| {
                Action::Cdc(AddCDCFile {
                    path: format!("_change_data/{}", add.path),
                    size: add.size,
                    partition_values: add.partition_values,
                    data_change: false,
                    tags: add.tags,
                })
            })
            .collect::<Vec<_>>();

        actions.append(&mut cdf_actions);

        let write_time_ms = normal_write_ms + cdf_write_ms;
        let scan_elapsed = scan_start.elapsed().as_millis() as u64;
        let scan_time_ms = scan_elapsed.saturating_sub(write_time_ms);

        let metrics = WriteExecutionPlanMetrics {
            scan_time_ms,
            write_time_ms,
        };

        return Ok((actions, metrics));
    }
}
