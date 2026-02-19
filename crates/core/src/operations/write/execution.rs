use std::sync::{Arc, OnceLock};

use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ToDFSchema;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit, when};
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, Partitioning, SendableRecordBatchStream,
    execute_stream_partitioned,
};
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use delta_kernel::table_configuration::TableConfiguration;
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::prefix::PrefixStore;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::log::*;
use uuid::Uuid;

use super::writer::{DeltaWriter, WriterConfig};
use crate::DeltaTableError;
use crate::delta_datafusion::{
    DataFusionMixins, DataValidationExec, DeltaScanConfigBuilder, DeltaTableProvider, find_files,
    generated_columns_to_exprs,
    logical::{LogicalPlanBuilderExt as _, LogicalPlanExt as _},
    validation_predicates,
};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, AddCDCFile, EagerSnapshot, Remove, StructType, StructTypeExt};
use crate::logstore::{LogStore, LogStoreRef, ObjectStoreRef};
use crate::operations::cdc::{CDC_COLUMN_NAME, should_write_cdc};
use crate::operations::write::WriterStatsConfig;
use crate::table::config::TablePropertiesExt as _;

const DEFAULT_WRITER_BATCH_CHANNEL_SIZE: usize = 10;
const WRITER_TASK_CLOSED_UNEXPECTEDLY_MSG: &str = "Writer task closed unexpectedly";

fn parse_channel_size(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .filter(|size| *size > 0)
        .unwrap_or(DEFAULT_WRITER_BATCH_CHANNEL_SIZE)
}

fn channel_size() -> usize {
    static CHANNEL_SIZE: OnceLock<usize> = OnceLock::new();
    *CHANNEL_SIZE.get_or_init(|| {
        parse_channel_size(
            std::env::var("DELTARS_WRITER_BATCH_CHANNEL_SIZE")
                .ok()
                .as_deref(),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::task::{Context, Poll};
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::common::Result as DataFusionResult;
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::{RecordBatchStream, stream::RecordBatchStreamAdapter};
    use delta_kernel::table_properties::DataSkippingNumIndexedCols;
    use futures::{Stream, stream};
    use object_store::memory::InMemory;

    use super::{
        DEFAULT_WRITER_BATCH_CHANNEL_SIZE, ObjectStoreRef, SendableRecordBatchStream,
        WRITER_TASK_CLOSED_UNEXPECTEDLY_MSG, WriterConfig, parse_channel_size, write_streams,
    };

    #[test]
    fn channel_size_zero_falls_back_to_default() {
        assert_eq!(
            parse_channel_size(Some("0")),
            DEFAULT_WRITER_BATCH_CHANNEL_SIZE
        );
    }

    #[test]
    fn channel_size_positive_value_is_used() {
        assert_eq!(parse_channel_size(Some("8")), 8);
    }

    #[test]
    fn channel_size_invalid_value_falls_back_to_default() {
        assert_eq!(
            parse_channel_size(Some("abc")),
            DEFAULT_WRITER_BATCH_CHANNEL_SIZE
        );
    }

    #[test]
    fn channel_size_missing_value_falls_back_to_default() {
        assert_eq!(parse_channel_size(None), DEFAULT_WRITER_BATCH_CHANNEL_SIZE);
    }

    fn write_streams_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]))
    }

    fn write_streams_config(schema: Arc<ArrowSchema>) -> WriterConfig {
        WriterConfig::new(
            schema,
            vec![],
            None,
            Some(1024),
            Some(1024),
            DataSkippingNumIndexedCols::NumColumns(32),
            None,
        )
    }

    fn write_streams_object_store() -> ObjectStoreRef {
        Arc::new(InMemory::new()) as ObjectStoreRef
    }

    struct PendingDropStream {
        schema: Arc<ArrowSchema>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for PendingDropStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl Stream for PendingDropStream {
        type Item = DataFusionResult<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl RecordBatchStream for PendingDropStream {
        fn schema(&self) -> Arc<ArrowSchema> {
            self.schema.clone()
        }
    }

    #[tokio::test]
    async fn test_write_streams_aborts_workers_when_writer_fails() {
        let expected_schema = write_streams_schema();
        let config = write_streams_config(expected_schema.clone());

        let dropped = Arc::new(AtomicBool::new(false));
        let pending_stream: SendableRecordBatchStream = Box::pin(PendingDropStream {
            schema: expected_schema,
            dropped: dropped.clone(),
        });

        let bad_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            true,
        )]));
        let bad_batch = RecordBatch::try_new(
            bad_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let failing_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            bad_schema,
            stream::iter(vec![Ok::<RecordBatch, datafusion::error::DataFusionError>(
                bad_batch,
            )]),
        ));

        let object_store = write_streams_object_store();
        let result =
            write_streams(vec![pending_stream, failing_stream], object_store, config).await;

        let err = result
            .expect_err("expected writer failure when stream schema mismatches writer config");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Unexpected Arrow schema"),
            "expected writer schema mismatch error, got: {err_msg}"
        );
        assert!(
            !err_msg.contains(WRITER_TASK_CLOSED_UNEXPECTEDLY_MSG),
            "expected primary writer failure, got channel-close fallback: {err_msg}"
        );
        assert!(
            dropped.load(Ordering::SeqCst),
            "expected pending worker stream to be dropped when writer fails"
        );
    }

    #[tokio::test]
    async fn test_write_streams_does_not_hang_when_worker_fails() {
        let expected_schema = write_streams_schema();
        let config = write_streams_config(expected_schema.clone());

        let dropped = Arc::new(AtomicBool::new(false));
        let pending_stream: SendableRecordBatchStream = Box::pin(PendingDropStream {
            schema: expected_schema.clone(),
            dropped: dropped.clone(),
        });

        let failing_worker_stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(
                expected_schema,
                stream::iter(vec![Err::<RecordBatch, DataFusionError>(
                    DataFusionError::Execution("worker stream failed".to_string()),
                )]),
            ));

        let object_store = write_streams_object_store();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            write_streams(
                vec![pending_stream, failing_worker_stream],
                object_store,
                config,
            ),
        )
        .await;

        assert!(
            result.is_ok(),
            "write_streams hung waiting for writer completion"
        );
        let result = result.unwrap();
        assert!(result.is_err(), "expected worker stream failure to surface");
        assert!(
            dropped.load(Ordering::SeqCst),
            "expected pending stream to be dropped when a worker fails"
        );
    }
}

/// Cap on concurrent writer tasks. Each writer holds open multipart uploads
/// and in-memory buffers, so more writers means higher memory and FD usage.
/// Defaults to `num_cpus` (matching DataFusion's `target_partitions`),
/// clamped to [1, 128]. Override via `DELTARS_MAX_CONCURRENT_WRITERS`.
fn max_concurrent_writers() -> usize {
    static MAX_WRITERS: OnceLock<usize> = OnceLock::new();
    *MAX_WRITERS.get_or_init(|| {
        std::env::var("DELTARS_MAX_CONCURRENT_WRITERS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or_else(num_cpus::get)
            .clamp(1, 128)
    })
}

/// Metrics captured from execution
#[derive(Debug, Default)]
pub(crate) struct WriteExecutionPlanMetrics {
    pub scan_time_ms: u64,
    pub write_time_ms: u64,
}

/// Metrics captured from draining streams through a writer.
#[derive(Debug, Default)]
pub(crate) struct WriteStreamMetrics {
    pub rows_written: u64,
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
) -> DeltaResult<(Vec<Action>, Option<LogicalPlan>)> {
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

    let source = Arc::new(
        LogicalPlanBuilder::scan("target", provider_as_source(target_provider), None)?.build()?,
    );

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
            Some(
                source
                    .into_builder()
                    .filter(expression.clone())?
                    .with_column(CDC_COLUMN_NAME, lit("delete"))?
                    .build()?,
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
) -> DeltaResult<(Vec<Action>, Option<LogicalPlan>)> {
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
    let mut validations = if let Some(snapshot) = snapshot {
        validation_predicates(
            session,
            &plan.schema().to_dfschema()?,
            snapshot.table_configuration(),
        )?
    } else {
        debug!(
            "Using plan schema to derive generated columns, since no snapshot was provided. Implies first write."
        );
        let delta_schema: StructType = schema.as_ref().try_into_kernel()?;
        let df_schema = schema.clone().to_dfschema()?;
        generated_columns_to_exprs(session, &df_schema, &delta_schema.get_generated_columns()?)?
    };

    if let Some(mut pred) = predicate {
        if contains_cdc {
            pred = when(col(CDC_COLUMN_NAME).eq(lit("insert")), pred).otherwise(lit(true))?;
        }
        validations.push(pred);
    }

    let plan = DataValidationExec::try_new_with_predicates(session, plan, validations)?;

    if !contains_cdc {
        write_data_plan(
            session,
            plan,
            partition_columns,
            object_store,
            target_file_size,
            write_batch_size,
            writer_properties,
            writer_stats_config,
        )
        .await
    } else {
        write_cdc_plan(
            session,
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
}

pub(crate) async fn write_exec_plan(
    session: &dyn Session,
    log_store: &dyn LogStore,
    table_config: &TableConfiguration,
    exec: Arc<dyn ExecutionPlan>,
    operation_id: Option<Uuid>,
    write_as_cdc: bool,
) -> DeltaResult<(Vec<Action>, WriteExecutionPlanMetrics)> {
    let writer_properties = session
        .config_options()
        .execution
        .parquet
        .into_writer_properties_builder()?
        .build();
    let stats_config = WriterStatsConfig::from_config(table_config);
    let object_store = log_store.object_store(operation_id);
    let target_file_size = table_config
        .table_properties()
        .target_file_size
        .map(|v| v.get() as usize);
    let partition_columns = table_config.metadata().partition_columns().clone();

    if write_as_cdc {
        write_cdc_plan(
            session,
            exec,
            partition_columns,
            object_store,
            target_file_size,
            None,
            Some(writer_properties),
            stats_config,
        )
        .await
    } else {
        write_data_plan(
            session,
            exec,
            partition_columns,
            object_store,
            target_file_size,
            None,
            Some(writer_properties),
            stats_config,
        )
        .await
    }
}

/// Drain one or more streams through a single [`DeltaWriter`].
///
/// Each stream is consumed by its own worker task and forwarded over an mpsc channel
/// to a single writer task to preserve backpressure and streaming semantics.
pub(crate) async fn write_streams(
    streams: Vec<SendableRecordBatchStream>,
    object_store: ObjectStoreRef,
    config: WriterConfig,
) -> DeltaResult<(Vec<Add>, WriteStreamMetrics)> {
    let worker_count = streams.len();
    let (tx, mut rx) = mpsc::channel::<RecordBatch>(channel_size());

    let mut writer_handle = tokio::task::spawn(async move {
        let mut writer = DeltaWriter::new(object_store, config);
        let mut total_write_ms: u64 = 0;
        let mut rows_written: u64 = 0;
        while let Some(batch) = rx.recv().await {
            rows_written += batch.num_rows() as u64;
            let wstart = std::time::Instant::now();
            writer.write(&batch).await?;
            total_write_ms += wstart.elapsed().as_millis() as u64;
        }
        let adds = writer.close().await?;
        Ok::<(Vec<Add>, u64, u64), DeltaTableError>((adds, total_write_ms, rows_written))
    });

    let mut worker_set = JoinSet::new();
    for mut stream in streams {
        let tx_clone = tx.clone();
        worker_set.spawn(async move {
            while let Some(maybe_batch) = stream.next().await {
                let batch = maybe_batch?;
                tx_clone.send(batch).await.map_err(|_| {
                    DeltaTableError::Generic(WRITER_TASK_CLOSED_UNEXPECTEDLY_MSG.to_string())
                })?;
            }
            Ok::<(), DeltaTableError>(())
        });
    }

    drop(tx);

    let mut worker_error: Option<DeltaTableError> = None;
    let mut writer_result: Option<DeltaResult<(Vec<Add>, u64, u64)>> = None;
    let mut workers_remaining = worker_count;

    while workers_remaining > 0 || writer_result.is_none() {
        tokio::select! {
            writer_join = &mut writer_handle, if writer_result.is_none() => {
                let result = writer_join
                    .map_err(|e| DeltaTableError::Generic(format!("writer join error: {e}")))
                    .and_then(|join_res| join_res);
                if result.is_err() && workers_remaining > 0 {
                    worker_set.abort_all();
                }
                writer_result = Some(result);
            }
            worker_join = worker_set.join_next(), if workers_remaining > 0 => {
                let Some(worker_join) = worker_join else {
                    workers_remaining = 0;
                    continue;
                };
                workers_remaining -= 1;

                match worker_join {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        let writer_failed = writer_result.as_ref().is_some_and(Result::is_err);
                        if worker_error.is_none()
                            && !(writer_failed && is_writer_task_closed_error(&err))
                        {
                            worker_error = Some(err);
                        }
                        worker_set.abort_all();
                    }
                    Err(join_err) if join_err.is_cancelled() => {
                        let writer_failed = writer_result.as_ref().is_some_and(Result::is_err);
                        if worker_error.is_none() && !writer_failed {
                            worker_error = Some(DeltaTableError::Generic(format!(
                                "worker task unexpectedly cancelled while driving partition: {join_err}"
                            )));
                        }
                    }
                    Err(join_err) => {
                        if worker_error.is_none() {
                            worker_error = Some(DeltaTableError::Generic(format!(
                                "worker join error when driving partition: {join_err}"
                            )));
                        }
                        worker_set.abort_all();
                    }
                }
            }
        }
    }

    while let Some(worker_join) = worker_set.join_next().await {
        match worker_join {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let writer_failed = writer_result.as_ref().is_some_and(Result::is_err);
                if worker_error.is_none() && !(writer_failed && is_writer_task_closed_error(&err)) {
                    worker_error = Some(err);
                }
            }
            Err(join_err) if join_err.is_cancelled() => {}
            Err(join_err) => {
                if worker_error.is_none() {
                    worker_error = Some(DeltaTableError::Generic(format!(
                        "worker join error when driving partition: {join_err}"
                    )));
                }
            }
        }
    }

    let writer_result = writer_result.ok_or_else(|| {
        DeltaTableError::Generic("writer task did not produce a result".to_string())
    })?;
    let (adds, write_time_ms, rows_written) = match writer_result {
        Ok(values) => values,
        Err(err) => return Err(err),
    };

    if let Some(err) = worker_error {
        return Err(err);
    }

    Ok((
        adds,
        WriteStreamMetrics {
            rows_written,
            write_time_ms,
        },
    ))
}

fn is_writer_task_closed_error(err: &DeltaTableError) -> bool {
    matches!(err, DeltaTableError::Generic(msg) if msg == WRITER_TASK_CLOSED_UNEXPECTEDLY_MSG)
}

/// Hash repartitions the plan by partition columns so each stream
/// writes to disjoint Delta partitions.
/// The output partition count is capped by `DELTARS_MAX_CONCURRENT_WRITERS`
/// Returns the plan unchanged if there is only a single stream.
fn repartition_by_partition_columns(
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: &[String],
) -> DeltaResult<Arc<dyn ExecutionPlan>> {
    let original_count = plan.output_partitioning().partition_count();

    if original_count <= 1 {
        return Ok(plan);
    }

    let num_partitions = original_count.min(max_concurrent_writers());

    let schema = plan.schema();
    let hash_exprs = partition_columns
        .iter()
        .map(|name| physical_col(name, &schema).map(|e| e as _))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(RepartitionExec::try_new(
        plan,
        Partitioning::Hash(hash_exprs, num_partitions),
    )?))
}

async fn write_data_plan(
    session: &dyn Session,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<(Vec<Action>, WriteExecutionPlanMetrics)> {
    let config = WriterConfig::new(
        plan.schema().clone(),
        partition_columns.clone(),
        writer_properties.clone(),
        target_file_size,
        write_batch_size,
        writer_stats_config.num_indexed_cols,
        writer_stats_config.stats_columns.clone(),
    );

    // For unpartitioned writes, centralize writer behavior through write_streams.
    if partition_columns.is_empty() {
        let partition_streams = execute_stream_partitioned(plan, session.task_ctx())?;
        let scan_start = std::time::Instant::now();
        let (adds, stream_metrics) = write_streams(partition_streams, object_store, config).await?;

        let scan_time_ms = scan_start
            .elapsed()
            .as_millis()
            .saturating_sub(stream_metrics.write_time_ms as u128) as u64;

        let metrics = WriteExecutionPlanMetrics {
            scan_time_ms,
            write_time_ms: stream_metrics.write_time_ms,
        };

        let actions = adds.into_iter().map(Action::Add).collect::<Vec<_>>();
        return Ok((actions, metrics));
    }

    let plan = repartition_by_partition_columns(plan, &partition_columns)?;
    let partition_streams = execute_stream_partitioned(plan, session.task_ctx())?;
    let scan_start = std::time::Instant::now();

    let mut join_set = JoinSet::new();
    for mut stream in partition_streams {
        let store = object_store.clone();
        let config = config.clone();
        join_set.spawn(async move {
            let mut writer = DeltaWriter::new(store, config);
            let mut write_ms: u64 = 0;
            while let Some(maybe_batch) = stream.next().await {
                let batch = maybe_batch?;
                let wstart = std::time::Instant::now();
                writer.write(&batch).await?;
                write_ms += wstart.elapsed().as_millis() as u64;
            }
            let adds = writer.close().await?;
            Ok::<(Vec<Add>, u64), DeltaTableError>((adds, write_ms))
        });
    }

    let mut all_adds = Vec::new();
    // Writers run in parallel, so wall-clock write time is the slowest task
    let mut max_write_ms: u64 = 0;
    while let Some(join_res) = join_set.join_next().await {
        let result = join_res
            .map_err(|e| DeltaTableError::Generic(format!("writer task join error: {e}")))?;
        match result {
            Ok((adds, write_ms)) => {
                all_adds.extend(adds);
                max_write_ms = max_write_ms.max(write_ms);
            }
            Err(e) => {
                join_set.abort_all();
                return Err(e);
            }
        }
    }

    let scan_elapsed = scan_start.elapsed().as_millis() as u64;
    let scan_time_ms = scan_elapsed.saturating_sub(max_write_ms);

    let metrics = WriteExecutionPlanMetrics {
        scan_time_ms,
        write_time_ms: max_write_ms,
    };

    let actions = all_adds.into_iter().map(Action::Add).collect::<Vec<_>>();
    Ok((actions, metrics))
}

async fn write_cdc_plan(
    session: &dyn Session,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<(Vec<Action>, WriteExecutionPlanMetrics)> {
    let cdf_store = Arc::new(PrefixStore::new(object_store.clone(), "_change_data"));

    let write_schema = Arc::new(Schema::new(
        plan.schema()
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
    let cdf_schema = plan.schema().clone();

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

    // Keep the previous single-writer fan-in path for unpartitioned tables.
    if partition_columns.is_empty() {
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

        let partition_streams = execute_stream_partitioned(plan, session.task_ctx())?;
        let mut worker_handles = Vec::with_capacity(partition_streams.len());
        let scan_start = std::time::Instant::now();

        for mut partition_stream in partition_streams {
            let txn = tx_normal.clone();
            let txc = tx_cdf.clone();
            let session_ctx = SessionContext::new();

            let h = tokio::task::spawn(async move {
                while let Some(maybe_batch) = partition_stream.next().await {
                    let batch = maybe_batch?;

                    // split batch since upstream unioned write and cdf plans
                    let table_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
                        batch.schema(),
                        vec![vec![batch.clone()]],
                    )?);
                    let batch_df = session_ctx
                        .read_table(table_provider)
                        .map_err(|e| DeltaTableError::Generic(format!("read_table failed: {e}")))?;

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

                    let mut normal_stream = normal_df.execute_stream().await?;
                    while let Some(mut normal_batch) = normal_stream.try_next().await? {
                        let mut idx: Option<usize> = None;
                        for (i_field, field) in
                            normal_batch.schema_ref().fields().iter().enumerate()
                        {
                            if field.name() == CDC_COLUMN_NAME {
                                idx = Some(i_field);
                                break;
                            }
                        }
                        normal_batch.remove_column(idx.ok_or(DeltaTableError::generic(
                            "idx of _change_type col not found. This shouldn't have happened.",
                        ))?);

                        txn.send(normal_batch).await.map_err(|_| {
                            DeltaTableError::Generic(
                                "normal writer closed unexpectedly".to_string(),
                            )
                        })?;
                    }

                    let mut cdf_stream = cdf_df.execute_stream().await?;
                    while let Some(cdf_batch) = cdf_stream.try_next().await? {
                        txc.send(cdf_batch).await.map_err(|_| {
                            DeltaTableError::Generic("cdf writer closed unexpectedly".to_string())
                        })?;
                    }
                }
                Ok::<(), DeltaTableError>(())
            });

            worker_handles.push(h);
        }

        drop(tx_normal);
        drop(tx_cdf);

        let normal_join = normal_writer_handle
            .await
            .map_err(|e| DeltaTableError::Generic(format!("normal writer join error: {e}")))?;
        let (normal_adds, normal_write_ms) = match normal_join {
            Ok(ok) => ok,
            Err(e) => {
                cdf_writer_handle.abort();
                for handle in &worker_handles {
                    handle.abort();
                }
                for handle in worker_handles {
                    let _ = handle.await;
                }
                return Err(e);
            }
        };

        let cdf_join = cdf_writer_handle
            .await
            .map_err(|e| DeltaTableError::Generic(format!("cdf writer join error: {e}")))?;
        let (cdf_adds, cdf_write_ms) = match cdf_join {
            Ok(ok) => ok,
            Err(e) => {
                for handle in &worker_handles {
                    handle.abort();
                }
                for handle in worker_handles {
                    let _ = handle.await;
                }
                return Err(e);
            }
        };

        for h in worker_handles {
            let join_res = h.await.map_err(|e| {
                DeltaTableError::Generic(format!("worker join error when driving partition: {e}"))
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

    let plan = repartition_by_partition_columns(plan, &partition_columns)?;
    let partition_streams = execute_stream_partitioned(plan, session.task_ctx())?;
    let scan_start = std::time::Instant::now();
    let mut join_set = JoinSet::new();

    for mut stream in partition_streams {
        let store = object_store.clone();
        let cdf_store: ObjectStoreRef = cdf_store.clone();
        let normal_config = normal_config.clone();
        let cdf_config = cdf_config.clone();

        join_set.spawn(async move {
            let mut normal_writer = DeltaWriter::new(store, normal_config);
            let mut cdf_writer = DeltaWriter::new(cdf_store, cdf_config);
            let session_ctx = SessionContext::new();
            let mut write_ms: u64 = 0;

            while let Some(maybe_batch) = stream.next().await {
                let batch = maybe_batch?;

                // split batch since upstream unioned write and cdf plans
                let table_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
                    batch.schema(),
                    vec![vec![batch.clone()]],
                )?);
                let batch_df = session_ctx
                    .read_table(table_provider)
                    .map_err(|e| DeltaTableError::Generic(format!("read_table failed: {e}")))?;

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

                let mut normal_stream = normal_df.execute_stream().await?;
                while let Some(mut normal_batch) = normal_stream.try_next().await? {
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

                    let wstart = std::time::Instant::now();
                    normal_writer.write(&normal_batch).await?;
                    write_ms += wstart.elapsed().as_millis() as u64;
                }

                let mut cdf_stream = cdf_df.execute_stream().await?;
                while let Some(cdf_batch) = cdf_stream.try_next().await? {
                    let wstart = std::time::Instant::now();
                    cdf_writer.write(&cdf_batch).await?;
                    write_ms += wstart.elapsed().as_millis() as u64;
                }
            }

            let normal_adds = normal_writer.close().await?;
            let cdf_adds = cdf_writer.close().await?;
            Ok::<(Vec<Add>, Vec<Add>, u64), DeltaTableError>((normal_adds, cdf_adds, write_ms))
        });
    }

    let mut all_actions = Vec::new();
    // Writers run in parallel, so wall-clock write time is the slowest task
    let mut max_write_ms: u64 = 0;
    while let Some(join_res) = join_set.join_next().await {
        let result = join_res
            .map_err(|e| DeltaTableError::Generic(format!("writer task join error: {e}")))?;
        match result {
            Ok((normal_adds, cdf_adds, write_ms)) => {
                all_actions.extend(normal_adds.into_iter().map(Action::Add));
                all_actions.extend(cdf_adds.into_iter().map(|add| {
                    Action::Cdc(AddCDCFile {
                        path: format!("_change_data/{}", add.path),
                        size: add.size,
                        partition_values: add.partition_values,
                        data_change: false,
                        tags: add.tags,
                    })
                }));
                max_write_ms = max_write_ms.max(write_ms);
            }
            Err(e) => {
                join_set.abort_all();
                return Err(e);
            }
        }
    }

    let scan_elapsed = scan_start.elapsed().as_millis() as u64;
    let scan_time_ms = scan_elapsed.saturating_sub(max_write_ms);

    let metrics = WriteExecutionPlanMetrics {
        scan_time_ms,
        write_time_ms: max_write_ms,
    };

    Ok((all_actions, metrics))
}
