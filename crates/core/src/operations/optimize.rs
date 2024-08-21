//! Optimize a Delta Table
//!
//! Perform bin-packing on a Delta Table which merges small files into a large
//! file. Bin-packing reduces the number of API calls required for read
//! operations.
//!
//! Optimize will fail if a concurrent write operation removes files from the
//! table (such as in an overwrite). It will always succeed if concurrent writers
//! are only appending.
//!
//! Optimize increments the table's version and creates remove actions for
//! optimized files. Optimize does not delete files from storage. To delete
//! files that were removed, call `vacuum` on [`DeltaTable`].
//!
//! See [`OptimizeBuilder`] for configuration.
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let (table, metrics) = OptimizeBuilder::new(table.object_store(), table.state).await?;
//! ````

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::expressions::Scalar;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use itertools::Itertools;
use num_cpus;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use tracing::debug;

use super::transaction::PROTOCOL;
use super::writer::{PartitionWriter, PartitionWriterConfig};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{scalars::ScalarExt, Action, PartitionsExt, Remove};
use crate::logstore::LogStoreRef;
use crate::operations::transaction::{CommitBuilder, CommitProperties, DEFAULT_RETRIES};
use crate::protocol::DeltaOperation;
use crate::storage::ObjectStoreRef;
use crate::table::state::DeltaTableState;
use crate::writer::utils::arrow_schema_without_partitions;
use crate::{crate_version, DeltaTable, ObjectMeta, PartitionFilter};

/// Metrics from Optimize
#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    /// Number of optimized files added
    pub num_files_added: u64,
    /// Number of unoptimized files removed
    pub num_files_removed: u64,
    /// Detailed metrics for the add operation
    #[serde(
        serialize_with = "serialize_metric_details",
        deserialize_with = "deserialize_metric_details"
    )]
    pub files_added: MetricDetails,
    /// Detailed metrics for the remove operation
    #[serde(
        serialize_with = "serialize_metric_details",
        deserialize_with = "deserialize_metric_details"
    )]
    pub files_removed: MetricDetails,
    /// Number of partitions that had at least one file optimized
    pub partitions_optimized: u64,
    /// The number of batches written
    pub num_batches: u64,
    /// How many files were considered during optimization. Not every file considered is optimized
    pub total_considered_files: usize,
    /// How many files were considered for optimization but were skipped
    pub total_files_skipped: usize,
    /// The order of records from source files is preserved
    pub preserve_insertion_order: bool,
}

// Custom serialization function that serializes metric details as a string
fn serialize_metric_details<S>(value: &MetricDetails, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

// Custom deserialization that parses a JSON string into MetricDetails
fn deserialize_metric_details<'de, D>(deserializer: D) -> Result<MetricDetails, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(DeError::custom)
}

/// Statistics on files for a particular operation
/// Operation can be remove or add
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricDetails {
    /// Average file size of a operation
    pub avg: f64,
    /// Maximum file size of a operation
    pub max: i64,
    /// Minimum file size of a operation
    pub min: i64,
    /// Number of files encountered during operation
    pub total_files: usize,
    /// Sum of file sizes of a operation
    pub total_size: i64,
}

impl MetricDetails {
    /// Add a partial metric to the metrics
    pub fn add(&mut self, partial: &MetricDetails) {
        self.min = std::cmp::min(self.min, partial.min);
        self.max = std::cmp::max(self.max, partial.max);
        self.total_files += partial.total_files;
        self.total_size += partial.total_size;
        self.avg = self.total_size as f64 / self.total_files as f64;
    }
}

impl fmt::Display for MetricDetails {
    /// Display the metric details using serde serialization
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        serde_json::to_string(self).map_err(|_| fmt::Error)?.fmt(f)
    }
}

/// Metrics for a single partition
pub struct PartialMetrics {
    /// Number of optimized files added
    pub num_files_added: u64,
    /// Number of unoptimized files removed
    pub num_files_removed: u64,
    /// Detailed metrics for the add operation
    pub files_added: MetricDetails,
    /// Detailed metrics for the remove operation
    pub files_removed: MetricDetails,
    /// The number of batches written
    pub num_batches: u64,
}

impl Metrics {
    /// Add a partial metric to the metrics
    pub fn add(&mut self, partial: &PartialMetrics) {
        self.num_files_added += partial.num_files_added;
        self.num_files_removed += partial.num_files_removed;
        self.files_added.add(&partial.files_added);
        self.files_removed.add(&partial.files_removed);
        self.num_batches += partial.num_batches;
    }
}

impl Default for MetricDetails {
    fn default() -> Self {
        MetricDetails {
            min: i64::MAX,
            max: 0,
            avg: 0.0,
            total_files: 0,
            total_size: 0,
        }
    }
}

/// Type of optimization to perform.
#[derive(Debug)]
pub enum OptimizeType {
    /// Compact files into pre-determined bins
    Compact,
    /// Z-order files based on provided columns
    ZOrder(Vec<String>),
}

/// Optimize a Delta table with given options
///
/// If a target file size is not provided then `delta.targetFileSize` from the
/// table's configuration is read. Otherwise a default value is used.
#[derive(Debug)]
pub struct OptimizeBuilder<'a> {
    /// A snapshot of the to-be-optimized table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Filters to select specific table partitions to be optimized
    filters: &'a [PartitionFilter],
    /// Desired file size after bin-packing files
    target_size: Option<i64>,
    /// Properties passed to underlying parquet writer
    writer_properties: Option<WriterProperties>,
    /// Commit properties and configuration
    commit_properties: CommitProperties,
    /// Whether to preserve insertion order within files (default false)
    preserve_insertion_order: bool,
    /// Maximum number of concurrent tasks (default is number of cpus)
    max_concurrent_tasks: usize,
    /// Maximum number of bytes allowed in memory before spilling to disk
    max_spill_size: usize,
    /// Optimize type
    optimize_type: OptimizeType,
    min_commit_interval: Option<Duration>,
}

impl super::Operation<()> for OptimizeBuilder<'_> {}

impl<'a> OptimizeBuilder<'a> {
    /// Create a new [`OptimizeBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            filters: &[],
            target_size: None,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
            preserve_insertion_order: false,
            max_concurrent_tasks: num_cpus::get(),
            max_spill_size: 20 * 1024 * 1024 * 1024, // 20 GB.
            optimize_type: OptimizeType::Compact,
            min_commit_interval: None,
        }
    }

    /// Choose the type of optimization to perform. Defaults to [OptimizeType::Compact].
    pub fn with_type(mut self, optimize_type: OptimizeType) -> Self {
        self.optimize_type = optimize_type;
        self
    }

    /// Only optimize files that return true for the specified partition filter
    pub fn with_filters(mut self, filters: &'a [PartitionFilter]) -> Self {
        self.filters = filters;
        self
    }

    /// Set the target file size
    pub fn with_target_size(mut self, target: i64) -> Self {
        self.target_size = Some(target);
        self
    }

    /// Writer properties passed to parquet writer
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Additonal information to write to the commit
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Whether to preserve insertion order within files
    pub fn with_preserve_insertion_order(mut self, preserve_insertion_order: bool) -> Self {
        self.preserve_insertion_order = preserve_insertion_order;
        self
    }

    /// Max number of concurrent tasks
    pub fn with_max_concurrent_tasks(mut self, max_concurrent_tasks: usize) -> Self {
        self.max_concurrent_tasks = max_concurrent_tasks;
        self
    }

    /// Max spill size
    pub fn with_max_spill_size(mut self, max_spill_size: usize) -> Self {
        self.max_spill_size = max_spill_size;
        self
    }

    /// Min commit interval
    pub fn with_min_commit_interval(mut self, min_commit_interval: Duration) -> Self {
        self.min_commit_interval = Some(min_commit_interval);
        self
    }
}

impl<'a> std::future::IntoFuture for OptimizeBuilder<'a> {
    type Output = DeltaResult<(DeltaTable, Metrics)>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            PROTOCOL.can_write_to(&this.snapshot.snapshot)?;
            if !&this.snapshot.load_config().require_files {
                return Err(DeltaTableError::NotInitializedWithFiles("OPTIMIZE".into()));
            }

            let writer_properties = this.writer_properties.unwrap_or_else(|| {
                WriterProperties::builder()
                    .set_compression(Compression::ZSTD(ZstdLevel::try_new(4).unwrap()))
                    .set_created_by(format!("delta-rs version {}", crate_version()))
                    .build()
            });
            let plan = create_merge_plan(
                this.optimize_type,
                &this.snapshot,
                this.filters,
                this.target_size.to_owned(),
                writer_properties,
            )?;
            let metrics = plan
                .execute(
                    this.log_store.clone(),
                    &this.snapshot,
                    this.max_concurrent_tasks,
                    this.max_spill_size,
                    this.min_commit_interval,
                    this.commit_properties,
                )
                .await?;
            let mut table = DeltaTable::new_with_state(this.log_store, this.snapshot);
            table.update().await?;
            Ok((table, metrics))
        })
    }
}

#[derive(Debug, Clone)]
struct OptimizeInput {
    target_size: i64,
    predicate: Option<String>,
}

impl From<OptimizeInput> for DeltaOperation {
    fn from(opt_input: OptimizeInput) -> Self {
        DeltaOperation::Optimize {
            target_size: opt_input.target_size,
            predicate: opt_input.predicate,
        }
    }
}

fn create_remove(
    path: &str,
    partitions: &IndexMap<String, Scalar>,
    size: i64,
) -> Result<Action, DeltaTableError> {
    // NOTE unwrap is safe since UNIX_EPOCH will always be earlier then now.
    let deletion_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let deletion_time = deletion_time.as_millis() as i64;

    Ok(Action::Remove(Remove {
        path: path.to_string(),
        deletion_timestamp: Some(deletion_time),
        data_change: false,
        extended_file_metadata: None,
        partition_values: Some(
            partitions
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        if v.is_null() {
                            None
                        } else {
                            Some(v.serialize())
                        },
                    )
                })
                .collect(),
        ),
        size: Some(size),
        deletion_vector: None,
        tags: None,
        base_row_id: None,
        default_row_commit_version: None,
    }))
}

/// Layout for optimizing a plan
///
/// Within each partition, we identify a set of files that need to be merged
/// together and/or sorted together.
#[derive(Debug)]
enum OptimizeOperations {
    /// Plan to compact files into pre-determined bins
    ///
    /// Bins are determined by the bin-packing algorithm to reach an optimal size.
    /// Files that are large enough already are skipped. Bins of size 1 are dropped.
    Compact(HashMap<String, (IndexMap<String, Scalar>, Vec<MergeBin>)>),
    /// Plan to Z-order each partition
    ZOrder(
        Vec<String>,
        HashMap<String, (IndexMap<String, Scalar>, MergeBin)>,
    ),
    // TODO: Sort
}

impl Default for OptimizeOperations {
    fn default() -> Self {
        OptimizeOperations::Compact(HashMap::new())
    }
}

#[derive(Debug)]
/// Encapsulates the operations required to optimize a Delta Table
pub struct MergePlan {
    operations: OptimizeOperations,
    /// Metrics collected during operation
    metrics: Metrics,
    /// Parameters passed down to merge tasks
    task_parameters: Arc<MergeTaskParameters>,
    /// Version of the table at beginning of optimization. Used for conflict resolution.
    read_table_version: i64,
}

/// Parameters passed to individual merge tasks
#[derive(Debug)]
pub struct MergeTaskParameters {
    /// Parameters passed to optimize operation
    input_parameters: OptimizeInput,
    /// Schema of written files
    file_schema: ArrowSchemaRef,
    /// Properties passed to parquet writer
    writer_properties: WriterProperties,
    /// Num index cols to collect stats for
    num_indexed_cols: i32,
    /// Stats columns, specific columns to collect stats from, takes precedence over num_indexed_cols
    stats_columns: Option<Vec<String>>,
}

/// A stream of record batches, with a ParquetError on failure.
type ParquetReadStream = BoxStream<'static, Result<RecordBatch, ParquetError>>;

impl MergePlan {
    /// Rewrites files in a single partition.
    ///
    /// Returns a vector of add and remove actions, as well as the partial metrics
    /// collected during the operation.
    async fn rewrite_files<F>(
        task_parameters: Arc<MergeTaskParameters>,
        partition_values: IndexMap<String, Scalar>,
        files: MergeBin,
        object_store: ObjectStoreRef,
        read_stream: F,
    ) -> Result<(Vec<Action>, PartialMetrics), DeltaTableError>
    where
        F: Future<Output = Result<ParquetReadStream, DeltaTableError>> + Send + 'static,
    {
        debug!("Rewriting files in partition: {:?}", partition_values);
        // First, initialize metrics
        let mut partial_actions = files
            .iter()
            .map(|file_meta| {
                create_remove(
                    file_meta.location.as_ref(),
                    &partition_values,
                    file_meta.size as i64,
                )
            })
            .collect::<Result<Vec<_>, DeltaTableError>>()?;

        let files_removed = files
            .iter()
            .fold(MetricDetails::default(), |mut curr, file| {
                curr.total_files += 1;
                curr.total_size += file.size as i64;
                curr.max = std::cmp::max(curr.max, file.size as i64);
                curr.min = std::cmp::min(curr.min, file.size as i64);
                curr
            });

        let mut partial_metrics = PartialMetrics {
            num_files_added: 0,
            num_files_removed: files.len() as u64,
            files_added: MetricDetails::default(),
            files_removed,
            num_batches: 0,
        };

        // Next, initialize the writer
        let writer_config = PartitionWriterConfig::try_new(
            task_parameters.file_schema.clone(),
            partition_values.clone(),
            Some(task_parameters.writer_properties.clone()),
            Some(task_parameters.input_parameters.target_size as usize),
            None,
        )?;
        let mut writer = PartitionWriter::try_with_config(
            object_store,
            writer_config,
            task_parameters.num_indexed_cols,
            task_parameters.stats_columns.clone(),
        )?;

        let mut read_stream = read_stream.await?;

        while let Some(maybe_batch) = read_stream.next().await {
            let mut batch = maybe_batch?;

            batch = super::cast::cast_record_batch(
                &batch,
                task_parameters.file_schema.clone(),
                false,
                true,
            )?;
            partial_metrics.num_batches += 1;
            writer.write(&batch).await.map_err(DeltaTableError::from)?;
        }

        let add_actions = writer.close().await?.into_iter().map(|mut add| {
            add.data_change = false;

            let size = add.size;

            partial_metrics.num_files_added += 1;
            partial_metrics.files_added.total_files += 1;
            partial_metrics.files_added.total_size += size;
            partial_metrics.files_added.max = std::cmp::max(partial_metrics.files_added.max, size);
            partial_metrics.files_added.min = std::cmp::min(partial_metrics.files_added.min, size);

            Action::Add(add)
        });
        partial_actions.extend(add_actions);

        debug!(
            "Finished rewriting files in partition: {:?}",
            partition_values
        );

        Ok((partial_actions, partial_metrics))
    }

    /// Creates a stream of batches that are Z-ordered.
    ///
    /// Currently requires loading all the data into memory. This is run for each
    /// partition, so it is not a problem for tables where each partition is small.
    /// But for large unpartitioned tables, this could be a problem.
    #[cfg(not(feature = "datafusion"))]
    async fn read_zorder(
        files: MergeBin,
        context: Arc<zorder::ZOrderExecContext>,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ParquetError>>, DeltaTableError> {
        use arrow_array::cast::as_generic_binary_array;
        use arrow_array::ArrayRef;
        use arrow_schema::ArrowError;

        let object_store_ref = context.object_store.clone();
        // Read all batches into a vec
        let batches = zorder::collect_batches(object_store_ref, files).await?;

        // For each batch, compute the zorder key
        let zorder_keys: Vec<ArrayRef> =
            batches
                .iter()
                .map(|batch| {
                    let mut zorder_columns = Vec::new();
                    for column in context.columns.iter() {
                        let array = batch.column_by_name(column).ok_or(ArrowError::SchemaError(
                            format!("Column not found in data file: {column}"),
                        ))?;
                        zorder_columns.push(array.clone());
                    }
                    zorder::zorder_key(zorder_columns.as_ref())
                })
                .collect::<Result<Vec<_>, ArrowError>>()?;

        let mut indices = zorder_keys
            .iter()
            .enumerate()
            .flat_map(|(batch_i, key)| {
                let key = as_generic_binary_array::<i32>(key);
                key.iter()
                    .enumerate()
                    .map(move |(row_i, key)| (key.unwrap(), batch_i, row_i))
            })
            .collect_vec();
        indices.sort_by_key(|(key, _, _)| *key);
        let indices = indices
            .into_iter()
            .map(|(_, batch_i, row_i)| (batch_i, row_i))
            .collect_vec();

        // Interleave the batches
        Ok(
            util::interleave_batches(batches, indices, 10_000, context.use_inner_threads)
                .await
                .map_err(|err| ParquetError::General(format!("Failed to reorder data: {:?}", err)))
                .boxed(),
        )
    }

    /// Datafusion-based z-order read.
    #[cfg(feature = "datafusion")]
    async fn read_zorder(
        files: MergeBin,
        context: Arc<zorder::ZOrderExecContext>,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ParquetError>>, DeltaTableError> {
        use datafusion::prelude::{col, ParquetReadOptions};
        use datafusion_common::Column;
        use datafusion_expr::expr::ScalarFunction;
        use datafusion_expr::{Expr, ScalarUDF};

        let locations = files
            .iter()
            .map(|file| format!("delta-rs:///{}", file.location))
            .collect_vec();
        let df = context
            .ctx
            .read_parquet(locations, ParquetReadOptions::default())
            .await?;

        let original_columns = df
            .schema()
            .fields()
            .iter()
            .map(|f| Expr::Column(Column::from_qualified_name_ignore_case(f.name())))
            .collect_vec();

        // Add a temporary z-order column we will sort by, and then drop.
        const ZORDER_KEY_COLUMN: &str = "__zorder_key";
        let cols = context
            .columns
            .iter()
            .map(|col| Expr::Column(Column::from_qualified_name_ignore_case(col)))
            .collect_vec();
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::from(zorder::datafusion::ZOrderUDF)),
            cols,
        ));
        let df = df.with_column(ZORDER_KEY_COLUMN, expr)?;

        let df = df.sort(vec![col(ZORDER_KEY_COLUMN).sort(true, true)])?;
        let df = df.select(original_columns)?;

        let stream = df
            .execute_stream()
            .await?
            .map_err(|err| {
                ParquetError::General(format!("Z-order failed while scanning data: {:?}", err))
            })
            .boxed();

        Ok(stream)
    }

    /// Perform the operations outlined in the plan.
    pub async fn execute(
        mut self,
        log_store: LogStoreRef,
        snapshot: &DeltaTableState,
        max_concurrent_tasks: usize,
        #[allow(unused_variables)] // used behind a feature flag
        max_spill_size: usize,
        min_commit_interval: Option<Duration>,
        commit_properties: CommitProperties,
    ) -> Result<Metrics, DeltaTableError> {
        let operations = std::mem::take(&mut self.operations);

        let stream = match operations {
            OptimizeOperations::Compact(bins) => futures::stream::iter(bins)
                .flat_map(|(_, (partition, bins))| {
                    futures::stream::iter(bins).map(move |bin| (partition.clone(), bin))
                })
                .map(|(partition, files)| {
                    debug!(
                        "merging a group of {} files in partition {:?}",
                        files.len(),
                        partition,
                    );
                    for file in files.iter() {
                        debug!("  file {}", file.location);
                    }
                    let object_store_ref = log_store.object_store();
                    let batch_stream = futures::stream::iter(files.clone())
                        .then(move |file| {
                            let object_store_ref = object_store_ref.clone();
                            async move {
                                let file_reader = ParquetObjectReader::new(object_store_ref, file);
                                ParquetRecordBatchStreamBuilder::new(file_reader)
                                    .await?
                                    .build()
                            }
                        })
                        .try_flatten()
                        .boxed();

                    let rewrite_result = tokio::task::spawn(Self::rewrite_files(
                        self.task_parameters.clone(),
                        partition,
                        files,
                        log_store.object_store().clone(),
                        futures::future::ready(Ok(batch_stream)),
                    ));
                    util::flatten_join_error(rewrite_result)
                })
                .boxed(),
            OptimizeOperations::ZOrder(zorder_columns, bins) => {
                #[cfg(not(feature = "datafusion"))]
                let exec_context = Arc::new(zorder::ZOrderExecContext::new(
                    zorder_columns,
                    log_store.object_store(),
                    // If there aren't enough bins to use all threads, then instead
                    // use threads within the bins. This is important for the case where
                    // the table is un-partitioned, in which case the entire table is just
                    // one big bin.
                    bins.len() <= num_cpus::get(),
                ));

                #[cfg(feature = "datafusion")]
                let exec_context = Arc::new(zorder::ZOrderExecContext::new(
                    zorder_columns,
                    log_store.object_store(),
                    max_spill_size,
                )?);
                let task_parameters = self.task_parameters.clone();
                let log_store = log_store.clone();
                futures::stream::iter(bins)
                    .map(move |(_, (partition, files))| {
                        let batch_stream = Self::read_zorder(files.clone(), exec_context.clone());
                        let rewrite_result = tokio::task::spawn(Self::rewrite_files(
                            task_parameters.clone(),
                            partition,
                            files,
                            log_store.object_store(),
                            batch_stream,
                        ));
                        util::flatten_join_error(rewrite_result)
                    })
                    .boxed()
            }
        };

        let mut stream = stream.buffer_unordered(max_concurrent_tasks);

        let mut table = DeltaTable::new_with_state(log_store.clone(), snapshot.clone());

        // Actions buffered so far. These will be flushed either at the end
        // or when we reach the commit interval.
        let mut actions = vec![];

        // Each time we commit, we'll reset buffered_metrics to orig_metrics.
        let orig_metrics = std::mem::take(&mut self.metrics);
        let mut buffered_metrics = orig_metrics.clone();
        let mut total_metrics = orig_metrics.clone();

        let mut last_commit = Instant::now();
        let mut commits_made = 0;
        loop {
            let next = stream.next().await.transpose()?;

            let end = next.is_none();

            if let Some((partial_actions, partial_metrics)) = next {
                debug!("Recording metrics for a completed partition");
                actions.extend(partial_actions);
                buffered_metrics.add(&partial_metrics);
                total_metrics.add(&partial_metrics);
            }

            let now = Instant::now();
            let mature = match min_commit_interval {
                None => false,
                Some(i) => now.duration_since(last_commit) > i,
            };
            if !actions.is_empty() && (mature || end) {
                let actions = std::mem::take(&mut actions);
                last_commit = now;

                buffered_metrics.preserve_insertion_order = true;
                let mut properties = CommitProperties::default();
                properties.app_metadata = commit_properties.app_metadata.clone();
                properties
                    .app_metadata
                    .insert("readVersion".to_owned(), self.read_table_version.into());
                let maybe_map_metrics = serde_json::to_value(std::mem::replace(
                    &mut buffered_metrics,
                    orig_metrics.clone(),
                ));
                if let Ok(map) = maybe_map_metrics {
                    properties
                        .app_metadata
                        .insert("operationMetrics".to_owned(), map);
                }

                debug!("committing {} actions", actions.len());

                CommitBuilder::from(properties)
                    .with_actions(actions)
                    .with_max_retries(DEFAULT_RETRIES + commits_made)
                    .build(
                        Some(snapshot),
                        log_store.clone(),
                        self.task_parameters.input_parameters.clone().into(),
                    )
                    .await?;

                commits_made += 1;
            }

            if end {
                break;
            }
        }

        total_metrics.preserve_insertion_order = true;
        if total_metrics.num_files_added == 0 {
            total_metrics.files_added.min = 0;
        }
        if total_metrics.num_files_removed == 0 {
            total_metrics.files_removed.min = 0;
        }

        table.update().await?;

        Ok(total_metrics)
    }
}

/// Build a Plan on which files to merge together. See [OptimizeBuilder]
pub fn create_merge_plan(
    optimize_type: OptimizeType,
    snapshot: &DeltaTableState,
    filters: &[PartitionFilter],
    target_size: Option<i64>,
    writer_properties: WriterProperties,
) -> Result<MergePlan, DeltaTableError> {
    let target_size = target_size.unwrap_or_else(|| snapshot.table_config().target_file_size());
    let partitions_keys = &snapshot.metadata().partition_columns;

    let (operations, metrics) = match optimize_type {
        OptimizeType::Compact => build_compaction_plan(snapshot, filters, target_size)?,
        OptimizeType::ZOrder(zorder_columns) => {
            build_zorder_plan(zorder_columns, snapshot, partitions_keys, filters)?
        }
    };

    let input_parameters = OptimizeInput {
        target_size,
        predicate: serde_json::to_string(filters).ok(),
    };
    let file_schema =
        arrow_schema_without_partitions(&Arc::new(snapshot.schema().try_into()?), partitions_keys);

    Ok(MergePlan {
        operations,
        metrics,
        task_parameters: Arc::new(MergeTaskParameters {
            input_parameters,
            file_schema,
            writer_properties,
            num_indexed_cols: snapshot.table_config().num_indexed_cols(),
            stats_columns: snapshot
                .table_config()
                .stats_columns()
                .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()),
        }),
        read_table_version: snapshot.version(),
    })
}

/// A collection of bins for a particular partition
#[derive(Debug, Clone)]
struct MergeBin {
    files: Vec<ObjectMeta>,
    size_bytes: i64,
}

impl MergeBin {
    pub fn new() -> Self {
        MergeBin {
            files: Vec::new(),
            size_bytes: 0,
        }
    }

    fn total_file_size(&self) -> i64 {
        self.size_bytes
    }

    fn len(&self) -> usize {
        self.files.len()
    }

    fn add(&mut self, meta: ObjectMeta) {
        self.size_bytes += meta.size as i64;
        self.files.push(meta);
    }
}

impl MergeBin {
    fn iter(&self) -> impl Iterator<Item = &ObjectMeta> {
        self.files.iter()
    }
}

impl IntoIterator for MergeBin {
    type Item = ObjectMeta;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.files.into_iter()
    }
}

fn build_compaction_plan(
    snapshot: &DeltaTableState,
    filters: &[PartitionFilter],
    target_size: i64,
) -> Result<(OptimizeOperations, Metrics), DeltaTableError> {
    let mut metrics = Metrics::default();

    let mut partition_files: HashMap<String, (IndexMap<String, Scalar>, Vec<ObjectMeta>)> =
        HashMap::new();
    for add in snapshot.get_active_add_actions_by_partitions(filters)? {
        let add = add?;
        metrics.total_considered_files += 1;
        let object_meta = ObjectMeta::try_from(&add)?;
        if (object_meta.size as i64) > target_size {
            metrics.total_files_skipped += 1;
            continue;
        }
        let partition_values = add
            .partition_values()?
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<IndexMap<_, _>>();

        partition_files
            .entry(add.partition_values()?.hive_partition_path())
            .or_insert_with(|| (partition_values, vec![]))
            .1
            .push(object_meta);
    }

    for (_, file) in partition_files.values_mut() {
        // Sort files by size: largest to smallest
        file.sort_by(|a, b| b.size.cmp(&a.size));
    }

    let mut operations: HashMap<String, (IndexMap<String, Scalar>, Vec<MergeBin>)> = HashMap::new();
    for (part, (partition, files)) in partition_files {
        let mut merge_bins = vec![MergeBin::new()];

        'files: for file in files {
            for bin in merge_bins.iter_mut() {
                if bin.total_file_size() + file.size as i64 <= target_size {
                    bin.add(file);
                    // Move to next file
                    continue 'files;
                }
            }
            // Didn't find a bin to add to, so create a new one
            let mut new_bin = MergeBin::new();
            new_bin.add(file);
            merge_bins.push(new_bin);
        }

        operations.insert(part, (partition, merge_bins));
    }

    // Prune merge bins with only 1 file, since they have no effect
    for (_, (_, bins)) in operations.iter_mut() {
        bins.retain(|bin| {
            if bin.len() == 1 {
                metrics.total_files_skipped += 1;
                false
            } else {
                true
            }
        })
    }
    operations.retain(|_, (_, files)| !files.is_empty());

    metrics.partitions_optimized = operations.len() as u64;

    Ok((OptimizeOperations::Compact(operations), metrics))
}

fn build_zorder_plan(
    zorder_columns: Vec<String>,
    snapshot: &DeltaTableState,
    partition_keys: &[String],
    filters: &[PartitionFilter],
) -> Result<(OptimizeOperations, Metrics), DeltaTableError> {
    if zorder_columns.is_empty() {
        return Err(DeltaTableError::Generic(
            "Z-order requires at least one column".to_string(),
        ));
    }
    let zorder_partition_cols = zorder_columns
        .iter()
        .filter(|col| partition_keys.contains(col))
        .collect_vec();
    if !zorder_partition_cols.is_empty() {
        return Err(DeltaTableError::Generic(format!(
            "Z-order columns cannot be partition columns. Found: {zorder_partition_cols:?}"
        )));
    }
    let field_names = snapshot
        .schema()
        .fields()
        .map(|field| field.name().to_string())
        .collect_vec();
    let unknown_columns = zorder_columns
        .iter()
        .filter(|col| !field_names.contains(col))
        .collect_vec();
    if !unknown_columns.is_empty() {
        return Err(DeltaTableError::Generic(
            format!("Z-order columns must be present in the table schema. Unknown columns: {unknown_columns:?}"),
        ));
    }

    // For now, just be naive and optimize all files in each selected partition.
    let mut metrics = Metrics::default();

    let mut partition_files: HashMap<String, (IndexMap<String, Scalar>, MergeBin)> = HashMap::new();
    for add in snapshot.get_active_add_actions_by_partitions(filters)? {
        let add = add?;
        let partition_values = add
            .partition_values()?
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<IndexMap<_, _>>();
        metrics.total_considered_files += 1;
        let object_meta = ObjectMeta::try_from(&add)?;

        partition_files
            .entry(partition_values.hive_partition_path())
            .or_insert_with(|| (partition_values, MergeBin::new()))
            .1
            .add(object_meta);
    }

    let operation = OptimizeOperations::ZOrder(zorder_columns, partition_files);
    Ok((operation, metrics))
}

pub(super) mod util {
    use super::*;
    use futures::Future;
    use tokio::task::JoinError;

    /// Interleaves a vector of record batches based on a set of indices
    #[cfg(not(feature = "datafusion"))]
    pub async fn interleave_batches(
        batches: Vec<RecordBatch>,
        indices: Vec<(usize, usize)>,
        batch_size: usize,
        use_threads: bool,
    ) -> BoxStream<'static, Result<RecordBatch, DeltaTableError>> {
        use arrow_array::ArrayRef;
        use arrow_select::interleave::interleave;
        use futures::TryFutureExt;

        if batches.is_empty() {
            return futures::stream::empty().boxed();
        }
        let num_columns = batches[0].num_columns();
        let schema = batches[0].schema();
        let arrays = (0..num_columns)
            .map(move |col_i| {
                Arc::new(
                    batches
                        .iter()
                        .map(|batch| batch.column(col_i).clone())
                        .collect_vec(),
                )
            })
            .collect_vec();
        let arrays = Arc::new(arrays);

        fn interleave_task(
            array_chunks: Arc<Vec<ArrayRef>>,
            indices: Arc<Vec<(usize, usize)>>,
        ) -> impl Future<Output = Result<ArrayRef, DeltaTableError>> + Send + 'static {
            let fut = tokio::task::spawn_blocking(move || {
                let array_refs = array_chunks.iter().map(|arr| arr.as_ref()).collect_vec();
                interleave(&array_refs, &indices)
            });
            flatten_join_error(fut)
        }

        fn interleave_batch(
            arrays: Arc<Vec<Arc<Vec<ArrayRef>>>>,
            chunk: Vec<(usize, usize)>,
            schema: ArrowSchemaRef,
            use_threads: bool,
        ) -> impl Future<Output = Result<RecordBatch, DeltaTableError>> + Send + 'static {
            let num_threads = if use_threads { num_cpus::get() } else { 1 };
            let chunk = Arc::new(chunk);
            futures::stream::iter(0..arrays.len())
                .map(move |i| arrays[i].clone())
                .map(move |array_chunks| interleave_task(array_chunks.clone(), chunk.clone()))
                .buffered(num_threads)
                .try_collect::<Vec<_>>()
                .and_then(move |columns| {
                    futures::future::ready(
                        RecordBatch::try_new(schema, columns).map_err(DeltaTableError::from),
                    )
                })
        }

        futures::stream::iter(indices)
            .chunks(batch_size)
            .map(move |chunk| interleave_batch(arrays.clone(), chunk, schema.clone(), use_threads))
            .buffered(2)
            .boxed()
    }

    pub async fn flatten_join_error<T, E>(
        future: impl Future<Output = Result<Result<T, E>, JoinError>>,
    ) -> Result<T, DeltaTableError>
    where
        E: Into<DeltaTableError>,
    {
        match future.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(error.into()),
            Err(error) => Err(DeltaTableError::GenericError {
                source: Box::new(error),
            }),
        }
    }
}

/// Z-order utilities
pub(super) mod zorder {
    use super::*;

    use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
    use arrow_array::{Array, ArrayRef, BinaryArray};
    use arrow_buffer::bit_util::{get_bit_raw, set_bit_raw, unset_bit_raw};
    use arrow_row::{Row, RowConverter, SortField};
    use arrow_schema::ArrowError;
    // use arrow_schema::Schema as ArrowSchema;

    /// Execution context for Z-order scan
    #[cfg(not(feature = "datafusion"))]
    pub struct ZOrderExecContext {
        /// Columns to z-order by
        pub columns: Arc<[String]>,
        /// Object store to use for reading files
        pub object_store: ObjectStoreRef,
        /// Whether to use threads when interleaving batches
        pub use_inner_threads: bool,
    }

    #[cfg(not(feature = "datafusion"))]
    impl ZOrderExecContext {
        pub fn new(
            columns: Vec<String>,
            object_store: ObjectStoreRef,
            use_inner_threads: bool,
        ) -> Self {
            let columns = columns.into();
            Self {
                columns,
                object_store,
                use_inner_threads,
            }
        }
    }

    /// Read all batches into a vec - is an async function in disguise
    #[cfg(not(feature = "datafusion"))]
    pub(super) fn collect_batches(
        object_store: ObjectStoreRef,
        files: MergeBin,
    ) -> impl Future<Output = Result<Vec<RecordBatch>, ParquetError>> {
        futures::stream::iter(files.clone())
            .then(move |file| {
                let object_store = object_store.clone();
                async move {
                    let file_reader = ParquetObjectReader::new(object_store.clone(), file);
                    ParquetRecordBatchStreamBuilder::new(file_reader)
                        .await?
                        .build()
                }
            })
            .try_flatten()
            .try_collect::<Vec<_>>()
    }

    #[cfg(feature = "datafusion")]
    pub use self::datafusion::ZOrderExecContext;

    #[cfg(feature = "datafusion")]
    pub(super) mod datafusion {
        use super::*;
        use ::datafusion::{
            execution::{
                memory_pool::FairSpillPool,
                runtime_env::{RuntimeConfig, RuntimeEnv},
            },
            prelude::{SessionConfig, SessionContext},
        };
        use arrow_schema::DataType;
        use datafusion_common::DataFusionError;
        use datafusion_expr::{
            ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
        };
        use itertools::Itertools;
        use std::any::Any;

        pub const ZORDER_UDF_NAME: &str = "zorder_key";

        pub struct ZOrderExecContext {
            pub columns: Arc<[String]>,
            pub ctx: SessionContext,
        }

        impl ZOrderExecContext {
            pub fn new(
                columns: Vec<String>,
                object_store: ObjectStoreRef,
                max_spill_size: usize,
            ) -> Result<Self, DataFusionError> {
                let columns = columns.into();

                let memory_pool = FairSpillPool::new(max_spill_size);
                let config = RuntimeConfig::new().with_memory_pool(Arc::new(memory_pool));
                let runtime = Arc::new(RuntimeEnv::new(config)?);
                runtime.register_object_store(&Url::parse("delta-rs://").unwrap(), object_store);

                use url::Url;
                let ctx = SessionContext::new_with_config_rt(SessionConfig::default(), runtime);
                ctx.register_udf(ScalarUDF::from(datafusion::ZOrderUDF));
                Ok(Self { columns, ctx })
            }
        }

        // DataFusion UDF impl for zorder_key
        #[derive(Debug)]
        pub struct ZOrderUDF;

        impl ScalarUDFImpl for ZOrderUDF {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                ZORDER_UDF_NAME
            }

            fn signature(&self) -> &Signature {
                &Signature {
                    type_signature: TypeSignature::VariadicAny,
                    volatility: Volatility::Immutable,
                }
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
                Ok(DataType::Binary)
            }

            fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
                zorder_key_datafusion(args)
            }
        }

        /// Datafusion zorder UDF body
        fn zorder_key_datafusion(
            columns: &[ColumnarValue],
        ) -> Result<ColumnarValue, DataFusionError> {
            let length = columns
                .iter()
                .map(|col| match col {
                    ColumnarValue::Array(array) => array.len(),
                    ColumnarValue::Scalar(_) => 1,
                })
                .max()
                .ok_or(DataFusionError::NotImplemented(
                    "z-order on zero columns.".to_string(),
                ))?;
            let columns: Vec<ArrayRef> = columns
                .iter()
                .map(|col| col.clone().into_array(length))
                .try_collect()?;
            let array = zorder_key(&columns)?;
            Ok(ColumnarValue::Array(array))
        }

        #[cfg(test)]
        mod tests {
            use super::*;
            use ::datafusion::assert_batches_eq;
            use arrow_array::{Int32Array, StringArray};
            use arrow_ord::sort::sort_to_indices;
            use arrow_schema::Field;
            use arrow_select::take::take;
            use rand::Rng;
            #[test]
            fn test_order() {
                let int: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
                let str: ArrayRef = Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("x"),
                    Some("a"),
                    Some("x"),
                    None,
                ]));
                let int_large: ArrayRef = Arc::new(Int32Array::from(vec![10000, 2000, 300, 40, 5]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("int", int),
                    ("str", str),
                    ("int_large", int_large),
                ])
                .unwrap();

                let expected_1 = vec![
                    "+-----+-----+-----------+",
                    "| int | str | int_large |",
                    "+-----+-----+-----------+",
                    "| 1   | a   | 10000     |",
                    "| 2   | x   | 2000      |",
                    "| 3   | a   | 300       |",
                    "| 4   | x   | 40        |",
                    "| 5   |     | 5         |",
                    "+-----+-----+-----------+",
                ];
                let expected_2 = vec![
                    "+-----+-----+-----------+",
                    "| int | str | int_large |",
                    "+-----+-----+-----------+",
                    "| 5   |     | 5         |",
                    "| 1   | a   | 10000     |",
                    "| 3   | a   | 300       |",
                    "| 2   | x   | 2000      |",
                    "| 4   | x   | 40        |",
                    "+-----+-----+-----------+",
                ];
                let expected_3 = vec![
                    "+-----+-----+-----------+",
                    "| int | str | int_large |",
                    "+-----+-----+-----------+",
                    "| 5   |     | 5         |",
                    "| 4   | x   | 40        |",
                    "| 2   | x   | 2000      |",
                    "| 3   | a   | 300       |",
                    "| 1   | a   | 10000     |",
                    "+-----+-----+-----------+",
                ];

                let expected = [expected_1, expected_2, expected_3];

                let indices = Int32Array::from(shuffled_indices().to_vec());
                let shuffled_columns = batch
                    .columns()
                    .iter()
                    .map(|c| take(c, &indices, None).unwrap())
                    .collect::<Vec<_>>();
                let shuffled_batch =
                    RecordBatch::try_new(batch.schema(), shuffled_columns).unwrap();

                for i in 1..=batch.num_columns() {
                    let columns = (0..i)
                        .map(|idx| shuffled_batch.column(idx).clone())
                        .collect::<Vec<_>>();

                    let order_keys = zorder_key(&columns).unwrap();
                    let indices = sort_to_indices(order_keys.as_ref(), None, None).unwrap();
                    let sorted_columns = shuffled_batch
                        .columns()
                        .iter()
                        .map(|c| take(c, &indices, None).unwrap())
                        .collect::<Vec<_>>();
                    let sorted_batch =
                        RecordBatch::try_new(batch.schema(), sorted_columns).unwrap();

                    assert_batches_eq!(expected[i - 1], &[sorted_batch]);
                }
            }
            fn shuffled_indices() -> [i32; 5] {
                let mut rng = rand::thread_rng();
                let mut array = [0, 1, 2, 3, 4];
                for i in (1..array.len()).rev() {
                    let j = rng.gen_range(0..=i);
                    array.swap(i, j);
                }
                array
            }

            #[tokio::test]
            async fn test_zorder_mixed_case() {
                use arrow_schema::Schema as ArrowSchema;
                let schema = Arc::new(ArrowSchema::new(vec![
                    Field::new("moDified", DataType::Utf8, true),
                    Field::new("ID", DataType::Utf8, true),
                    Field::new("vaLue", DataType::Int32, true),
                ]));

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(arrow::array::StringArray::from(vec![
                            "2021-02-01",
                            "2021-02-01",
                            "2021-02-02",
                            "2021-02-02",
                        ])),
                        Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
                        Arc::new(arrow::array::Int32Array::from(vec![1, 10, 20, 100])),
                    ],
                )
                .unwrap();
                // write some data
                let table = crate::DeltaOps::new_in_memory()
                    .write(vec![batch.clone()])
                    .with_save_mode(crate::protocol::SaveMode::Append)
                    .await
                    .unwrap();

                let res = crate::DeltaOps(table)
                    .optimize()
                    .with_type(OptimizeType::ZOrder(vec!["moDified".into()]))
                    .await;
                assert!(res.is_ok());
            }
        }
    }

    /// Creates a new binary array containing the zorder keys for the given columns
    ///
    /// Each value is 16 bytes * number of columns. Each column is converted into
    /// its row binary representation, and then the first 16 bytes are taken.
    /// These truncated values are interleaved in the array values.
    pub fn zorder_key(columns: &[ArrayRef]) -> Result<ArrayRef, ArrowError> {
        if columns.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "Cannot zorder empty columns".to_string(),
            ));
        }

        // length is length of first array or 1 if all scalars
        let out_length = columns[0].len();

        if columns.iter().any(|col| col.len() != out_length) {
            return Err(ArrowError::InvalidArgumentError(
                "All columns must have the same length".to_string(),
            ));
        }

        // We are taking 128 bits (16 bytes) from each value. Shorter values will be padded.
        let value_size: usize = columns.len() * 16;

        // Initialize with zeros
        let mut out: Vec<u8> = vec![0; out_length * value_size];

        for (col_pos, col) in columns.iter().enumerate() {
            set_bits_for_column(col.clone(), col_pos, columns.len(), &mut out)?;
        }

        let offsets = (0..=out_length)
            .map(|i| (i * value_size) as i32)
            .collect::<Vec<i32>>();

        let out_arr = BinaryArray::try_new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(out),
            None,
        )?;

        Ok(Arc::new(out_arr))
    }

    /// Given an input array, will set the bits in the output array
    ///
    /// Arguments:
    /// * `input` - The input array
    /// * `col_pos` - The position of the column. Used to determine position
    ///   when interleaving.
    /// * `num_columns` - The number of columns in the input array. Used to
    ///   determine offset when interleaving.
    fn set_bits_for_column(
        input: ArrayRef,
        col_pos: usize,
        num_columns: usize,
        out: &mut Vec<u8>,
    ) -> Result<(), ArrowError> {
        // Convert array to rows
        let converter = RowConverter::new(vec![SortField::new(input.data_type().clone())])?;
        let rows = converter.convert_columns(&[input])?;

        for (row_i, row) in rows.iter().enumerate() {
            // How many bytes to get to this row's out position
            let row_offset = row_i * num_columns * 16;
            for bit_i in 0..128 {
                let bit = row.get_bit(bit_i);
                // Position of bit within the value. We place a value every
                // `num_columns` bits, offset by `col_pos` when interleaving.
                // So if there are 3 columns, and we are the second column, then
                // we place values at index: 1, 4, 7, 10, etc.
                let bit_pos = (bit_i * num_columns) + col_pos;
                let out_pos = (row_offset * 8) + bit_pos;
                // Safety: we pre-sized the output vector in the outer function
                if bit {
                    unsafe { set_bit_raw(out.as_mut_ptr(), out_pos) };
                } else {
                    unsafe { unset_bit_raw(out.as_mut_ptr(), out_pos) };
                }
            }
        }

        Ok(())
    }

    trait RowBitUtil {
        fn get_bit(&self, bit_i: usize) -> bool;
    }

    impl<'a> RowBitUtil for Row<'a> {
        /// Get the bit at the given index, or just give false if the index is out of bounds
        fn get_bit(&self, bit_i: usize) -> bool {
            let byte_i = bit_i / 8;
            let bytes = self.as_ref();
            if byte_i >= bytes.len() {
                return false;
            }
            // Safety: we just did a bounds check above
            unsafe { get_bit_raw(bytes.as_ptr(), bit_i) }
        }
    }

    #[cfg(test)]
    mod test {
        use arrow_array::{
            cast::as_generic_binary_array, new_empty_array, StringArray, UInt8Array,
        };
        use arrow_schema::DataType;

        use super::*;

        #[test]
        fn test_rejects_no_columns() {
            let columns = vec![];
            let result = zorder_key(&columns);
            assert!(result.is_err());
        }

        #[test]
        fn test_handles_no_rows() {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(new_empty_array(&DataType::Int64)),
                Arc::new(new_empty_array(&DataType::Utf8)),
            ];
            let result = zorder_key(columns.as_slice());
            assert!(result.is_ok());
            let result = result.unwrap();
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_basics() {
            let columns: Vec<ArrayRef> = vec![
                // Small strings
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
                // Strings of various sizes
                Arc::new(StringArray::from(vec![
                    "delta-rs: A native Rust library for Delta Lake, with bindings into Python",
                    "cat",
                    "",
                ])),
                Arc::new(UInt8Array::from(vec![Some(1), Some(4), None])),
            ];
            let result = zorder_key(columns.as_slice()).unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result.data_type(), &DataType::Binary);
            assert_eq!(result.null_count(), 0);

            let data: &BinaryArray = as_generic_binary_array(result.as_ref());
            assert_eq!(data.value_data().len(), 3 * 16 * 3);
            assert!(data.iter().all(|x| x.unwrap().len() == 3 * 16));
        }

        #[tokio::test]
        async fn works_on_spark_table() {
            use crate::DeltaOps;
            use tempfile::TempDir;
            // Create a temporary directory
            let tmp_dir = TempDir::new().expect("Failed to make temp dir");
            let table_name = "delta-1.2.1-only-struct-stats";

            // Copy recursively from the test data directory to the temporary directory
            let source_path = format!("../test/tests/data/{table_name}");
            fs_extra::dir::copy(source_path, tmp_dir.path(), &Default::default()).unwrap();

            // Run optimize
            let (_, metrics) =
                DeltaOps::try_from_uri(tmp_dir.path().join(table_name).to_str().unwrap())
                    .await
                    .unwrap()
                    .optimize()
                    .await
                    .unwrap();

            // Verify it worked
            assert_eq!(metrics.num_files_added, 1);
        }
    }
}
