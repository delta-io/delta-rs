//! Writer for MERGE operation, can write normal and CDF data at same time

use std::sync::Arc;
use std::vec;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion_expr::{col, lit};
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::prefix::PrefixStore;
use parquet::file::properties::WriterProperties;
use tracing::log::*;

use crate::operations::cdc::CDC_COLUMN_NAME;
use crate::operations::writer::{DeltaWriter, WriterConfig};

use crate::delta_datafusion::DeltaDataChecker;
use crate::errors::DeltaResult;
use crate::kernel::{Action, AddCDCFile, StructType, StructTypeExt};

use crate::operations::write::{WriteError, WriterStatsConfig};
use crate::storage::ObjectStoreRef;
use crate::table::state::DeltaTableState;

use tokio::sync::mpsc::Sender;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan_v2(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    sender: Option<Sender<RecordBatch>>,
    contains_cdc: bool,
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

    // Write data to disk
    let mut tasks = vec![];
    if !contains_cdc {
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
            let sender_stream = sender.clone();
            let mut stream = inner_plan.execute(i, task_ctx)?;

            let handle: tokio::task::JoinHandle<DeltaResult<Vec<Action>>> = tokio::task::spawn(
                async move {
                    let sendable = sender_stream.clone();
                    while let Some(maybe_batch) = stream.next().await {
                        let batch = maybe_batch?;

                        checker_stream.check_batch(&batch).await?;

                        if let Some(s) = sendable.as_ref() {
                            if let Err(e) = s.send(batch.clone()).await {
                                error!("Failed to send data to observer: {e:#?}");
                            }
                        } else {
                            debug!("write_execution_plan_with_predicate did not send any batches, no sender.");
                        }
                        writer.write(&batch).await?;
                    }
                    let add_actions = writer.close().await;
                    match add_actions {
                        Ok(actions) => Ok(actions.into_iter().map(Action::Add).collect::<Vec<_>>()),
                        Err(err) => Err(err),
                    }
                },
            );
            tasks.push(handle);
        }
    } else {
        // Incoming plan contains the normal write_plan unioned with the cdf plan
        // we split these batches during the write
        let cdf_store = Arc::new(PrefixStore::new(object_store.clone(), "_change_data"));
        for i in 0..plan.properties().output_partitioning().partition_count() {
            let inner_plan = plan.clone();
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
            let task_ctx = Arc::new(TaskContext::from(&state));
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

            let mut writer = DeltaWriter::new(object_store.clone(), normal_config);

            let mut cdf_writer = DeltaWriter::new(cdf_store.clone(), cdf_config);

            let checker_stream = checker.clone();
            let sender_stream = sender.clone();
            let mut stream = inner_plan.execute(i, task_ctx)?;

            let session_context = SessionContext::new();

            let handle: tokio::task::JoinHandle<DeltaResult<Vec<Action>>> = tokio::task::spawn(
                async move {
                    let sendable = sender_stream.clone();
                    while let Some(maybe_batch) = stream.next().await {
                        let batch = maybe_batch?;

                        // split batch since we unioned upstream the operation write and cdf plan
                        let table_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
                            batch.schema(),
                            vec![vec![batch.clone()]],
                        )?);
                        let batch_df = session_context.read_table(table_provider).unwrap();

                        let normal_df = batch_df
                            .clone()
                            .filter(col(CDC_COLUMN_NAME).in_list(
                                vec![lit("delete"), lit("source_delete"), lit("update_preimage")],
                                true,
                            ))?
                            .drop_columns(&[CDC_COLUMN_NAME])?;

                        let cdf_df = batch_df.filter(col(CDC_COLUMN_NAME).in_list(
                            vec![
                                lit("delete"),
                                lit("insert"),
                                lit("update_preimage"),
                                lit("update_postimage"),
                            ],
                            false,
                        ))?;

                        let normal_batch =
                            concat_batches(&write_schema, &normal_df.collect().await?)?;
                        checker_stream.check_batch(&normal_batch).await?;

                        let cdf_batch = concat_batches(&cdf_schema, &cdf_df.collect().await?)?;
                        checker_stream.check_batch(&cdf_batch).await?;

                        if let Some(s) = sendable.as_ref() {
                            if let Err(e) = s.send(batch.clone()).await {
                                error!("Failed to send data to observer: {e:#?}");
                            }
                        } else {
                            debug!("write_execution_plan_with_predicate did not send any batches, no sender.");
                        }
                        writer.write(&normal_batch).await?;
                        cdf_writer.write(&cdf_batch).await?;
                    }
                    let mut add_actions = writer
                        .close()
                        .await?
                        .into_iter()
                        .map(Action::Add)
                        .collect::<Vec<_>>();
                    let cdf_actions = cdf_writer.close().await.map(|v| {
                        v.into_iter()
                            .map(|add| {
                                {
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
                            })
                            .collect::<Vec<_>>()
                    })?;
                    add_actions.extend(cdf_actions);
                    Ok(add_actions)
                },
            );
            tasks.push(handle);
        }
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
