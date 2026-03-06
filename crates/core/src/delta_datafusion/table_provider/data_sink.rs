use std::{any::Any, fmt, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType,
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_datasource::sink::DataSink;
use futures::{StreamExt as _, TryStreamExt as _};
use itertools::Itertools as _;
use uuid::Uuid;

use crate::{
    cast_record_batch,
    delta_datafusion::DataFusionMixins as _,
    kernel::{Action, EagerSnapshot, transaction::CommitBuilder},
    logstore::LogStoreRef,
    operations::write::{execution::write_streams, writer::WriterConfig},
    protocol::{DeltaOperation, SaveMode},
    table::config::TablePropertiesExt as _,
};

/// DataSink implementation for delta lake
/// This uses DataSinkExec to handle the insert operation
/// Implements writing streams of RecordBatches to delta.
#[derive(Debug)]
pub struct DeltaDataSink {
    /// The log store
    log_store: LogStoreRef,
    /// The snapshot
    snapshot: EagerSnapshot,
    /// The save mode
    save_mode: SaveMode,
    /// The schema
    schema: SchemaRef,
    /// Metrics for monitoring throughput
    metrics: ExecutionPlanMetricsSet,
}

/// A [`DataSink`] implementation for writing to Delta Lake.
///
/// `DeltaDataSink` is used by [`DataSinkExec`] during query execution to
/// stream [`RecordBatch`]es into a Delta table. It encapsulates everything
/// needed to perform an insert/append/overwrite operation, including
/// transaction log access, snapshot state, and session configuration.
impl DeltaDataSink {
    /// Create a new [`DeltaDataSink`]
    pub fn new(log_store: LogStoreRef, snapshot: EagerSnapshot, save_mode: SaveMode) -> Self {
        Self {
            log_store,
            schema: snapshot.read_schema(),
            snapshot,
            save_mode,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Create a streaming transformed version of the input that converts dictionary columns
    /// This is used to convert dictionary columns to their native types
    fn create_converted_stream(
        &self,
        input: SendableRecordBatchStream,
        target_schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        use futures::StreamExt;

        let schema_for_closure = Arc::clone(&target_schema);
        let converted_stream = input.map(move |batch_result| {
            batch_result.and_then(|batch| {
                cast_record_batch(&batch, Arc::clone(&schema_for_closure), false, true)
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            })
        });

        Box::pin(RecordBatchStreamAdapter::new(
            target_schema,
            converted_stream,
        ))
    }
}

/// Implementation of the `DataSink` trait for `DeltaDataSink`
/// This is used to write the data to the delta table
/// It implements the `DataSink` trait and is used by the `DataSinkExec` node
/// to write the data to the delta table
#[async_trait::async_trait]
impl DataSink for DeltaDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Write the data to the delta table
    /// This is used for insert into operation
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let target_schema = self.snapshot.input_schema();
        let table_props = self.snapshot.table_configuration().table_properties();

        let operation_id = Uuid::new_v4();
        let stream = self.create_converted_stream(data, target_schema.clone());
        let partition_columns = self.snapshot.metadata().partition_columns();
        let object_store = self.log_store.object_store(Some(operation_id));
        let total_rows_metric = MetricBuilder::new(&self.metrics).counter("total_rows", 0);
        let stream = {
            let metric = total_rows_metric.clone();
            Box::pin(RecordBatchStreamAdapter::new(
                target_schema.clone(),
                stream.map(move |batch_result| {
                    if let Ok(ref batch) = batch_result {
                        metric.add(batch.num_rows());
                    }
                    batch_result
                }),
            )) as SendableRecordBatchStream
        };
        let config = WriterConfig::new(
            self.snapshot.read_schema(),
            partition_columns.clone(),
            None,
            Some(table_props.target_file_size().get() as usize),
            None,
            table_props.num_indexed_cols(),
            table_props
                .data_skipping_stats_columns
                .as_ref()
                .map(|c| c.iter().map(|c| c.to_string()).collect_vec()),
        );

        let (adds, write_metrics) = write_streams(vec![stream], object_store, config)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let total_rows = write_metrics.rows_written;

        let mut actions = adds.into_iter().map(Action::Add).collect_vec();

        if self.save_mode == SaveMode::Overwrite {
            actions.extend(
                self.snapshot
                    .file_views(&self.log_store, None)
                    .map_ok(|f| Action::Remove(f.remove_action(true)))
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            );
        };

        let operation = DeltaOperation::Write {
            mode: self.save_mode,
            partition_by: if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns.clone())
            },
            predicate: None,
        };

        CommitBuilder::default()
            .with_actions(actions)
            .with_operation_id(operation_id)
            .build(Some(&self.snapshot), self.log_store.clone(), operation)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(total_rows)
    }
}

/// Implementation of the `DisplayAs` trait for `DeltaDataSink`
impl DisplayAs for DeltaDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaDataSink")
    }
}
