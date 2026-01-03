use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{AsArray, Int64Builder, MapBuilder, StringBuilder};
use arrow::datatypes::SchemaRef;
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::common::{Statistics, exec_datafusion_err, exec_err};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::sink::DataSink;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer, FileSystemCommitter};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult as KernelResult, Engine, FilteredEngineData};
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::ObjectMeta;
use tracing::debug;

use crate::delta_datafusion::engine::{DataFusionEngine, datafusion_scalar_to_scalar};
use crate::delta_datafusion::table_provider::SnapshotWrapper;
use crate::kernel::scalars::ScalarExt as _;
use crate::kernel::spawn_blocking_with_span;
use crate::{DeltaResult, crate_version};

// TODO:
// - validate parquet compression
// - handle column mapping

/// DataSink implementation for delta lake
/// This uses DataSinkExec to handle the insert operation
/// Implements writing streams of RecordBatches to delta.
// #[derive(Debug)]
pub(crate) struct DeltaDataSink {
    /// The snapshot
    snapshot: SnapshotWrapper,
    /// Configuration for the inner parquet file sink
    config: FileSinkConfig,
    /// Full table schema including partition columns
    schema: SchemaRef,
    /// Metrics for monitoring throughput
    metrics: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for DeltaDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaDataSink")
            .field("snapshot", &self.snapshot)
            .field("config", &self.config)
            .field("schema", &self.schema)
            .field("metrics", &self.metrics)
            .finish()
    }
}

/// A [`DataSink`] implementation for writing to Delta Lake.
///
/// `DeltaDataSink` is used by [`DataSinkExec`] during query execution to
/// stream [`RecordBatch`]es into a Delta table. It encapsulates everything
/// needed to perform an insert/append/overwrite operation, including
/// transaction log access, snapshot state, and session configuration.
impl DeltaDataSink {
    /// Create a new [`DeltaDataSink`]
    pub fn new(snapshot: SnapshotWrapper, config: FileSinkConfig) -> Self {
        Self {
            schema: snapshot.snapshot().arrow_schema(),
            snapshot,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    // async fn handle_post_commit(
    //     &self,
    //     _context: &Arc<TaskContext>,
    //     committed: CommitResult,
    // ) -> DeltaResult<()> {
    //     let CommitResult::CommittedTransaction(committed) = committed else {
    //         return Ok(());
    //     };
    //     let stats = committed.post_commit_stats();

    //     todo!()
    // }
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
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        // create a ParquetSink to write the data to parquet files
        let pq_options = TableParquetOptions {
            global: context.session_config().options().execution.parquet.clone(),
            ..Default::default()
        };
        let pq_sink = ParquetSink::new(self.config.clone(), pq_options);
        let num_rows = pq_sink.write_all(data, context).await?;

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;

        // Note: I have yet to find a way to get the object metadata from the parquet sink.
        // As a stop-gap we make some head requests to read the object metadata.
        let written: Vec<_> =
            futures::stream::iter(pq_sink.written().into_iter().map(move |(path, meta)| {
                let store = object_store.clone();
                async move {
                    let object_meta = store.head(&path).await?;
                    Ok::<_, DataFusionError>((
                        object_meta,
                        DFParquetMetadata::statistics_from_parquet_metadata(
                            &meta,
                            &self.config.output_schema,
                        )?,
                    ))
                }
            }))
            .buffer_unordered(10)
            .try_collect()
            .await?;

        // build add action data from written files
        let mut builder = AddDataBuilder::new(self.config.output_schema.clone());
        for (meta, stats) in written.iter() {
            builder.append(meta, stats)?;
        }
        let (actions, stats) = builder.finish()?;

        let committer = Box::new(ExtendedStatsCommitter {
            stats,
            inner: Box::new(FileSystemCommitter::new()),
        });
        let mut txn = self
            .snapshot
            .snapshot()
            .inner
            .clone()
            .transaction(committer)
            .map_err(|e| exec_datafusion_err!("{}", e.to_string()))?
            .with_data_change(true)
            .with_engine_info(format!("delta-rs:{}", crate_version()));

        txn.add_files(Box::new(ArrowEngineData::new(actions)));

        let engine = DataFusionEngine::new_from_context(context.clone());

        if self.config.insert_op == InsertOp::Overwrite {
            let scan = self
                .snapshot
                .snapshot()
                .scan_builder()
                .build()
                .map_err(|e| exec_datafusion_err!("{}", e.to_string()))?;
            let scan_metadata = scan
                .scan_metadata(engine.clone())
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| exec_datafusion_err!("{}", e.to_string()))?;
            for metadata in scan_metadata {
                txn.remove_files(metadata.scan_files);
            }
        };

        // TODO: add operation info to transaction
        // let operation = DeltaOperation::Write {
        //     mode: self.save_mode,
        //     partition_by: if partition_columns.is_empty() {
        //         None
        //     } else {
        //         Some(partition_columns.clone())
        //     },
        //     predicate: None,
        // };

        let result = spawn_blocking_with_span(move || {
            txn.commit(engine.as_ref())
                .map_err(|e| exec_datafusion_err!("{}", e.to_string()))
        })
        .await
        .map_err(|e| exec_datafusion_err!("{}", e.to_string()))??;

        match result {
            CommitResult::CommittedTransaction(committed) => {
                debug!(
                    "Committed transaction with version {}",
                    committed.commit_version()
                );
                // TODO: handle post commit actions
            }
            CommitResult::ConflictedTransaction(conflict) => {
                return exec_err!("Transaction conflict: {:?}", conflict);
            }
            CommitResult::RetryableTransaction(retryable) => {
                return exec_err!("Conflict resolution not yet implemented: {:?}", retryable);
            }
        }

        Ok(num_rows)
    }
}

/// Implementation of the `DisplayAs` trait for `DeltaDataSink`
impl DisplayAs for DeltaDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaDataSink")
    }
}

/// Committer that adds extended statistics to the commit metadata
///
/// Currently delta kernel only supports num_rows as file level statistics.
/// To not break current use cases which rely heavily on file-skipping
/// we process the commit data to add extended statistics such as min/max/null_count
/// for configured stats columns in the written parquet files.
///
/// The actual commit is delegated to an inner committer such that we can use this
/// to commit via the base file system committer or using one that supports external
/// catalogs.
struct ExtendedStatsCommitter {
    stats: ArrayRef,
    inner: Box<dyn Committer>,
}

fn map_batch(batch: RecordBatch, array: ArrayRef) -> KernelResult<RecordBatch> {
    let Ok(add_idx) = batch.schema().index_of("add") else {
        return Ok(batch);
    };

    let add_array = batch.column(add_idx).as_struct().clone();
    let (fields, mut arrays, nulls) = add_array.into_parts();
    let Some((stats_index, _)) = fields.find("stats") else {
        return Ok(batch);
    };
    arrays[stats_index] = array;
    let new_add_array = StructArray::try_new(fields, arrays, nulls)?;

    let (schema, mut columns, _) = batch.into_parts();
    columns[add_idx] = Arc::new(new_add_array);

    Ok(RecordBatch::try_new(schema, columns)?)
}

type ActionData<'a> = Box<dyn Iterator<Item = KernelResult<FilteredEngineData>> + Send + 'a>;
impl ExtendedStatsCommitter {
    fn map_actions<'a>(&self, actions: ActionData<'a>) -> ActionData<'a> {
        let array = self.stats.clone();
        Box::new(actions.map(move |data| {
            let data = ArrowEngineData::try_from_engine_data(data?.apply_selection_vector()?)?;
            let batch = map_batch(data.record_batch().clone(), array.clone())?;
            let mapped_arrow_data = ArrowEngineData::new(batch);
            Ok(FilteredEngineData::with_all_rows_selected(Box::new(
                mapped_arrow_data,
            )))
        }))
    }
}

impl Committer for ExtendedStatsCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: ActionData<'_>,
        commit_metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        self.inner
            .commit(engine, self.map_actions(actions), commit_metadata)
    }
}

/// Builder for Add action data from parquet write metadata
struct AddDataBuilder {
    file_schema: SchemaRef,

    path: StringBuilder,
    partition_values: MapBuilder<StringBuilder, StringBuilder>,
    size: Int64Builder,
    modification_time: Int64Builder,
    num_records: Int64Builder,
    extended_stats: StringBuilder,
}

impl AddDataBuilder {
    fn new(file_schema: SchemaRef) -> Self {
        Self {
            file_schema,
            path: StringBuilder::new(),
            partition_values: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
            size: Int64Builder::new(),
            modification_time: Int64Builder::new(),
            num_records: Int64Builder::new(),
            extended_stats: StringBuilder::new(),
        }
    }

    fn append(&mut self, meta: &ObjectMeta, stats: &Statistics) -> DeltaResult<()> {
        self.path.append_value(meta.location.as_ref());
        self.partition_values.append(true)?;
        self.size.append_value(meta.size as i64);
        self.modification_time
            .append_value(meta.last_modified.timestamp_millis());
        match stats.num_rows {
            Precision::Exact(n) => {
                self.num_records.append_value(n as i64);
            }
            _ => self.num_records.append_null(),
        }

        self.extended_stats
            .append_value(&statistics_to_string(stats, &self.file_schema));

        Ok(())
    }

    fn finish(mut self) -> DeltaResult<(RecordBatch, ArrayRef)> {
        let fields = vec![
            Field::new("path", DataType::Utf8, false),
            Field::new(
                "partitionValues",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
            Field::new("size", DataType::Int64, false),
            Field::new("modificationTime", DataType::Int64, false),
            Field::new(
                "stats",
                DataType::Struct(vec![Field::new("numRecords", DataType::Int64, true)].into()),
                false,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));

        let stats = StructArray::from(vec![(
            Arc::new(Field::new("numRecords", DataType::Int64, true)),
            Arc::new(self.num_records.finish()) as ArrayRef,
        )]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.path.finish()),
            Arc::new(self.partition_values.finish()),
            Arc::new(self.size.finish()),
            Arc::new(self.modification_time.finish()),
            Arc::new(stats),
        ];

        Ok((
            RecordBatch::try_new(schema, columns)?,
            Arc::new(self.extended_stats.finish()),
        ))
    }
}

fn statistics_to_string(stats: &Statistics, file_schema: &Schema) -> String {
    use serde_json::{Map, Value};

    let mut stats_map = Map::new();
    if let Precision::Exact(n) = stats.num_rows {
        stats_map.insert("numRecords".to_string(), Value::Number(n.into()));
    }

    let mut min_values = Map::new();
    let mut max_values = Map::new();
    let mut null_counts = Map::new();

    for (col_stat, field) in stats
        .column_statistics
        .iter()
        .zip(file_schema.fields().iter())
    {
        match col_stat.null_count {
            Precision::Exact(n) => {
                null_counts.insert(field.name().clone(), Value::Number(n.into()));
            }
            _ => {}
        }

        match &col_stat.max_value {
            Precision::Exact(value) | Precision::Inexact(value) => {
                if !value.is_null() {
                    if let Ok(val) = datafusion_scalar_to_scalar(value) {
                        max_values.insert(field.name().clone(), val.to_json());
                    }
                }
            }
            _ => {}
        }

        match &col_stat.min_value {
            Precision::Exact(value) | Precision::Inexact(value) => {
                if !value.is_null() {
                    if let Ok(val) = datafusion_scalar_to_scalar(value) {
                        min_values.insert(field.name().clone(), val.to_json());
                    }
                }
            }
            _ => {}
        }
    }

    stats_map.insert("maxValues".to_string(), Value::Object(max_values));
    stats_map.insert("minValues".to_string(), Value::Object(min_values));
    stats_map.insert("nullCount".to_string(), Value::Object(null_counts));

    Value::Object(stats_map).to_string()
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::collect_partitioned;

    use super::*;
    use crate::test_utils::{TestResult, open_in_memory};

    #[tokio::test]
    async fn test_delta_data_sink() -> TestResult<()> {
        let (mut table, session) =
            open_in_memory("../../dat/v0.0.3/reader_tests/generated/basic_append/delta").await;
        let state = session.state();

        let initial_version = table.snapshot()?.version();

        let provider = table.table_provider().await?;
        let input = provider.scan(&state, None, &[], None).await?;
        let write_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await?;
        let _results = collect_partitioned(write_plan, state.task_ctx()).await?;

        table.update_state().await?;
        let next_version = table.snapshot()?.version();
        assert_eq!(next_version, initial_version + 1);

        let provider = table.table_provider().await?;
        let input = provider.scan(&state, None, &[], None).await?;
        let write_plan = provider
            .insert_into(&state, input, InsertOp::Overwrite)
            .await?;
        let _results = collect_partitioned(write_plan, state.task_ctx()).await?;

        table.update_state().await?;
        let final_version = table.snapshot()?.version();
        assert_eq!(final_version, next_version + 1);

        Ok(())
    }
}
