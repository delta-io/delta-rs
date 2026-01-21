use std::sync::{Arc, OnceLock};
use std::vec;

use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ToDFSchema;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, col, lit, when};
use datafusion::physical_plan::{ExecutionPlan, execute_stream_partitioned};
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use delta_kernel::table_configuration::TableConfiguration;
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::prefix::PrefixStore;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc;
use tracing::log::*;
use uuid::Uuid;

use super::writer::{DeltaWriter, WriterConfig};
use crate::DeltaTableError;
use crate::delta_datafusion::{
    DataValidationExec, generated_columns_to_exprs, validation_predicates,
};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, AddCDCFile, EagerSnapshot, StructType, StructTypeExt};
use crate::logstore::{LogStore, ObjectStoreRef};
use crate::operations::cdc::CDC_COLUMN_NAME;
use crate::operations::write::WriterStatsConfig;

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

// We drive partition streams concurrently and centralize writes via an mpsc channel.
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
        plan.schema(),
        partition_columns.clone(),
        writer_properties.clone(),
        target_file_size,
        write_batch_size,
        writer_stats_config.num_indexed_cols,
        writer_stats_config.stats_columns.clone(),
    );

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
    let partition_streams = execute_stream_partitioned(plan, session.task_ctx())?;
    let mut worker_handles = Vec::with_capacity(partition_streams.len());
    let scan_start = std::time::Instant::now();
    for mut partition_stream in partition_streams {
        let tx_clone = tx.clone();
        let handle = tokio::task::spawn(async move {
            while let Some(maybe_batch) = partition_stream.next().await {
                let batch = maybe_batch?;
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

                let mut normal_stream = normal_df.execute_stream().await?;
                while let Some(mut normal_batch) = normal_stream.try_next().await? {
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

                    txn.send(normal_batch).await.map_err(|_| {
                        DeltaTableError::Generic("normal writer closed unexpectedly".to_string())
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

    Ok((actions, metrics))
}
