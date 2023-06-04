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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow_array::cast::as_generic_binary_array;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::ArrowError;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, StreamExt, TryStreamExt};
use itertools::Itertools;
use log::debug;
use num_cpus;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::Map;

use super::transaction::commit;
use super::writer::{PartitionWriter, PartitionWriterConfig};
use crate::action::{self, Action, DeltaOperation};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::storage::ObjectStoreRef;
use crate::table_state::DeltaTableState;
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
    pub files_added: MetricDetails,
    /// Detailed metrics for the remove operation
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

/// Statistics on files for a particular operation
/// Operation can be remove or add
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricDetails {
    /// Minimum file size of a operation
    pub min: i64,
    /// Maximum file size of a operation
    pub max: i64,
    /// Average file size of a operation
    pub avg: f64,
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
    store: ObjectStoreRef,
    /// Filters to select specific table partitions to be optimized
    filters: &'a [PartitionFilter<'a, &'a str>],
    /// Desired file size after bin-packing files
    target_size: Option<i64>,
    /// Properties passed to underlying parquet writer
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<HashMap<String, serde_json::Value>>,
    /// Whether to preserve insertion order within files (default false)
    preserve_insertion_order: bool,
    /// Max number of concurrent tasks (default is number of cpus)
    max_concurrent_tasks: usize,
    /// Optimize type
    optimize_type: OptimizeType,
}

impl<'a> OptimizeBuilder<'a> {
    /// Create a new [`OptimizeBuilder`]
    pub fn new(store: ObjectStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            store,
            filters: &[],
            target_size: None,
            writer_properties: None,
            app_metadata: None,
            preserve_insertion_order: false,
            max_concurrent_tasks: num_cpus::get(),
            optimize_type: OptimizeType::Compact,
        }
    }

    /// Choose the type of optimization to perform. Defaults to [OptimizeType::Compact].
    pub fn with_type(mut self, optimize_type: OptimizeType) -> Self {
        self.optimize_type = optimize_type;
        self
    }

    /// Only optimize files that return true for the specified partition filter
    pub fn with_filters(mut self, filters: &'a [PartitionFilter<'a, &'a str>]) -> Self {
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

    /// Additional metadata to be added to commit info
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.app_metadata = Some(HashMap::from_iter(metadata));
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
}

impl<'a> std::future::IntoFuture for OptimizeBuilder<'a> {
    type Output = DeltaResult<(DeltaTable, Metrics)>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
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
                this.max_concurrent_tasks,
            )?;
            let metrics = plan.execute(this.store.clone(), &this.snapshot).await?;
            let mut table = DeltaTable::new_with_state(this.store, this.snapshot);
            table.update().await?;
            Ok((table, metrics))
        })
    }
}

#[derive(Debug, Clone)]
struct OptimizeInput {
    target_size: i64,
}

impl From<OptimizeInput> for DeltaOperation {
    fn from(opt_input: OptimizeInput) -> Self {
        DeltaOperation::Optimize {
            target_size: opt_input.target_size,
            predicate: None,
        }
    }
}

fn create_remove(
    path: &str,
    partitions: &HashMap<String, Option<String>>,
    size: i64,
) -> Result<Action, DeltaTableError> {
    // NOTE unwrap is safe since UNIX_EPOCH will always be earlier then now.
    let deletion_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let deletion_time = deletion_time.as_millis() as i64;

    Ok(Action::remove(action::Remove {
        path: path.to_string(),
        deletion_timestamp: Some(deletion_time),
        data_change: false,
        extended_file_metadata: None,
        partition_values: Some(partitions.to_owned()),
        size: Some(size),
        tags: None,
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
    Compact(HashMap<PartitionTuples, Vec<MergeBin>>),
    /// Plan to Z-order each partition
    ZOrder(Vec<String>, HashMap<PartitionTuples, MergeBin>),
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
    /// Whether to preserve insertion order within files
    /// Max number of concurrent tasks
    max_concurrent_tasks: usize,
}

/// Parameters passed to individual merge tasks
#[derive(Debug)]
pub struct MergeTaskParameters {
    /// Parameters passed to optimize operation
    input_parameters: OptimizeInput,
    /// Schema of written files
    file_schema: ArrowSchemaRef,
    /// Column names the table is partitioned by.
    partition_columns: Vec<String>,
    /// Properties passed to parquet writer
    writer_properties: WriterProperties,
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
        partition: PartitionTuples,
        files: MergeBin,
        object_store: ObjectStoreRef,
        read_stream: F,
    ) -> Result<(Vec<Action>, PartialMetrics), DeltaTableError>
    where
        F: Future<Output = Result<ParquetReadStream, DeltaTableError>> + Send + 'static,
    {
        debug!("Rewriting files in partition: {:?}", partition);
        // First, initialize metrics
        let partition_values = partition.to_hashmap();
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
            task_parameters.partition_columns.clone(),
            Some(task_parameters.writer_properties.clone()),
            Some(task_parameters.input_parameters.target_size as usize),
            None,
        )?;
        let mut writer = PartitionWriter::try_with_config(object_store.clone(), writer_config)?;

        let mut read_stream = read_stream.await?;

        while let Some(maybe_batch) = read_stream.next().await {
            let batch = maybe_batch?;
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

            Action::add(add)
        });
        partial_actions.extend(add_actions);

        debug!("Finished rewriting files in partition: {:?}", partition);

        Ok((partial_actions, partial_metrics))
    }

    /// Creates a stream of batches that are Z-ordered.
    ///
    /// Currently requires loading all the data into memory. This is run for each
    /// partition, so it is not a problem for tables where each partition is small.
    /// But for large unpartitioned tables, this could be a problem.
    async fn read_zorder(
        columns: Arc<Vec<String>>,
        files: MergeBin,
        object_store: ObjectStoreRef,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ParquetError>>, DeltaTableError> {
        let object_store_ref = object_store.clone();
        // Read all batches into a vec
        let batches: Vec<RecordBatch> = futures::stream::iter(files.clone())
            .then(|file| {
                let object_store_ref = object_store_ref.clone();
                async move {
                    let file_reader = ParquetObjectReader::new(object_store_ref.clone(), file);
                    ParquetRecordBatchStreamBuilder::new(file_reader)
                        .await?
                        .build()
                }
            })
            .try_flatten()
            .try_collect::<Vec<_>>()
            .await?;

        // For each batch, compute the zorder key
        let zorder_keys: Vec<ArrayRef> =
            batches
                .iter()
                .map(|batch| {
                    let mut zorder_columns = Vec::new();
                    for column in columns.iter() {
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
        let out_batches = util::interleave_batches(batches, Arc::new(indices), false).await?;

        Ok(futures::stream::once(futures::future::ready(out_batches))
            .map(Ok)
            .boxed())
    }

    /// Perform the operations outlined in the plan.
    pub async fn execute(
        mut self,
        object_store: ObjectStoreRef,
        snapshot: &DeltaTableState,
    ) -> Result<Metrics, DeltaTableError> {
        let mut actions = vec![];

        // Need to move metrics and operations out of self, so we can use self in the stream
        let mut metrics = std::mem::take(&mut self.metrics);

        let operations = std::mem::take(&mut self.operations);

        match operations {
            OptimizeOperations::Compact(bins) => {
                futures::stream::iter(bins)
                    .flat_map(|(partition, bins)| {
                        futures::stream::iter(bins).map(move |bin| (partition.clone(), bin))
                    })
                    .map(|(partition, files)| {
                        let object_store_ref = object_store.clone();
                        let batch_stream = futures::stream::iter(files.clone())
                            .then(move |file| {
                                let object_store_ref = object_store_ref.clone();
                                async move {
                                    let file_reader =
                                        ParquetObjectReader::new(object_store_ref, file);
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
                            object_store.clone(),
                            futures::future::ready(Ok(batch_stream)),
                        ));
                        util::flatten_join_error(rewrite_result)
                    })
                    .buffer_unordered(self.max_concurrent_tasks)
                    .try_for_each(|(partial_actions, partial_metrics)| {
                        debug!("Recording metrics for a completed partition");
                        actions.extend(partial_actions);
                        metrics.add(&partial_metrics);
                        async { Ok(()) }
                    })
                    .await?;
            }
            OptimizeOperations::ZOrder(zorder_columns, bins) => {
                let zorder_columns = Arc::new(zorder_columns);
                futures::stream::iter(bins)
                    .map(|(partition, files)| {
                        let batch_stream = Self::read_zorder(
                            zorder_columns.clone(),
                            files.clone(),
                            object_store.clone(),
                        );

                        let object_store = object_store.clone();

                        let rewrite_result = tokio::task::spawn(Self::rewrite_files(
                            self.task_parameters.clone(),
                            partition,
                            files,
                            object_store,
                            batch_stream,
                        ));
                        util::flatten_join_error(rewrite_result)
                    })
                    .buffer_unordered(self.max_concurrent_tasks)
                    .try_for_each(|(partial_actions, partial_metrics)| {
                        debug!("Recording metrics for a completed partition");
                        actions.extend(partial_actions);
                        metrics.add(&partial_metrics);
                        async { Ok(()) }
                    })
                    .await?;
            }
        }

        metrics.preserve_insertion_order = true;
        if metrics.num_files_added == 0 {
            metrics.files_added.min = 0;
        }
        if metrics.num_files_removed == 0 {
            metrics.files_removed.min = 0;
        }

        // TODO: Check for remove actions on optimized partitions. If a
        // optimized partition was updated then abort the commit. Requires (#593).
        if !actions.is_empty() {
            let mut metadata = Map::new();
            metadata.insert("readVersion".to_owned(), self.read_table_version.into());
            let maybe_map_metrics = serde_json::to_value(metrics.clone());
            if let Ok(map) = maybe_map_metrics {
                metadata.insert("operationMetrics".to_owned(), map);
            }

            commit(
                object_store.as_ref(),
                &actions,
                self.task_parameters.input_parameters.clone().into(),
                snapshot,
                Some(metadata),
            )
            .await?;
        }

        Ok(metrics)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionTuples(Vec<(String, Option<String>)>);

impl PartitionTuples {
    fn from_hashmap(
        partition_columns: &[String],
        partition_values: &HashMap<String, Option<String>>,
    ) -> Self {
        let mut tuples = Vec::new();
        for column in partition_columns {
            let value = partition_values.get(column).cloned().flatten();
            tuples.push((column.clone(), value));
        }
        Self(tuples)
    }

    fn to_hashmap(&self) -> HashMap<String, Option<String>> {
        self.0.iter().cloned().collect()
    }
}

/// Build a Plan on which files to merge together. See [OptimizeBuilder]
pub fn create_merge_plan(
    optimize_type: OptimizeType,
    snapshot: &DeltaTableState,
    filters: &[PartitionFilter<'_, &str>],
    target_size: Option<i64>,
    writer_properties: WriterProperties,
    max_concurrent_tasks: usize,
) -> Result<MergePlan, DeltaTableError> {
    let target_size = target_size.unwrap_or_else(|| snapshot.table_config().target_file_size());

    let partitions_keys = &snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)?
        .partition_columns;

    let (operations, metrics) = match optimize_type {
        OptimizeType::Compact => {
            build_compaction_plan(snapshot, partitions_keys, filters, target_size)?
        }
        OptimizeType::ZOrder(zorder_columns) => {
            build_zorder_plan(zorder_columns, snapshot, partitions_keys, filters)?
        }
    };

    let input_parameters = OptimizeInput { target_size };
    let file_schema = arrow_schema_without_partitions(
        &Arc::new(<ArrowSchema as TryFrom<&crate::schema::Schema>>::try_from(
            &snapshot
                .current_metadata()
                .ok_or(DeltaTableError::NoMetadata)?
                .schema,
        )?),
        partitions_keys,
    );

    Ok(MergePlan {
        operations,
        metrics,
        task_parameters: Arc::new(MergeTaskParameters {
            input_parameters,
            file_schema,
            partition_columns: partitions_keys.clone(),
            writer_properties,
        }),
        read_table_version: snapshot.version(),
        max_concurrent_tasks,
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
    partition_keys: &[String],
    filters: &[PartitionFilter<'_, &str>],
    target_size: i64,
) -> Result<(OptimizeOperations, Metrics), DeltaTableError> {
    let mut metrics = Metrics::default();

    let mut partition_files: HashMap<PartitionTuples, Vec<ObjectMeta>> = HashMap::new();
    for add in snapshot.get_active_add_actions_by_partitions(filters)? {
        metrics.total_considered_files += 1;
        let object_meta = ObjectMeta::try_from(add)?;
        if (object_meta.size as i64) > target_size {
            metrics.total_files_skipped += 1;
            continue;
        }

        let part = PartitionTuples::from_hashmap(partition_keys, &add.partition_values);

        partition_files
            .entry(part)
            .or_insert_with(Vec::new)
            .push(object_meta);
    }

    for file in partition_files.values_mut() {
        // Sort files by size: largest to smallest
        file.sort_by(|a, b| b.size.cmp(&a.size));
    }

    let mut operations: HashMap<PartitionTuples, Vec<MergeBin>> = HashMap::new();
    for (part, files) in partition_files {
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

        operations.insert(part, merge_bins);
    }

    // Prune merge bins with only 1 file, since they have no effect
    for (_, bins) in operations.iter_mut() {
        if bins.len() == 1 && bins[0].len() == 1 {
            metrics.total_files_skipped += 1;
            bins.clear();
        }
    }
    operations.retain(|_, files| !files.is_empty());

    metrics.partitions_optimized = operations.len() as u64;

    Ok((OptimizeOperations::Compact(operations), metrics))
}

fn build_zorder_plan(
    zorder_columns: Vec<String>,
    snapshot: &DeltaTableState,
    partition_keys: &[String],
    filters: &[PartitionFilter<'_, &str>],
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
        .current_metadata()
        .unwrap()
        .schema
        .get_fields()
        .iter()
        .map(|field| field.get_name().to_string())
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

    let mut partition_files: HashMap<PartitionTuples, MergeBin> = HashMap::new();
    for add in snapshot.get_active_add_actions_by_partitions(filters)? {
        metrics.total_considered_files += 1;
        let object_meta = ObjectMeta::try_from(add)?;
        let part = PartitionTuples::from_hashmap(partition_keys, &add.partition_values);

        partition_files
            .entry(part)
            .or_insert_with(MergeBin::new)
            .add(object_meta);
    }

    let operation = OptimizeOperations::ZOrder(zorder_columns, partition_files);
    Ok((operation, metrics))
}

pub(super) mod util {
    use super::*;
    use arrow_array::ArrayRef;
    use arrow_select::interleave::interleave;
    use futures::Future;
    use itertools::Itertools;
    use tokio::task::JoinError;

    /// Interleaves a vector of record batches based on a set of indices
    pub async fn interleave_batches(
        batches: Vec<RecordBatch>,
        indices: Arc<Vec<(usize, usize)>>,
        use_threads: bool,
    ) -> Result<RecordBatch, DeltaTableError> {
        // It would be nice if upstream provided this. Though TBH they would
        // probably prefer we just use DataFusion to sort.
        let columns: Vec<ArrayRef> = futures::stream::iter(0..batches[0].num_columns())
            .map(|col_i| {
                let arrays: Vec<ArrayRef> = batches
                    .iter()
                    .map(|batch| batch.column(col_i).clone())
                    .collect_vec();
                let indices = indices.clone();
                let task = tokio::task::spawn_blocking(move || {
                    let arrays = arrays.iter().map(|arr| arr.as_ref()).collect_vec();
                    interleave(&arrays, &indices)
                });

                flatten_join_error(task)
            })
            .buffered(if use_threads { num_cpus::get() } else { 1 })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(RecordBatch::try_new(batches[0].schema(), columns)?)
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
    use std::sync::Arc;

    use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
    use arrow_array::{Array, ArrayRef, BinaryArray};
    use arrow_buffer::bit_util::{get_bit_raw, set_bit_raw, unset_bit_raw};
    use arrow_row::{Row, RowConverter, SortField};
    use arrow_schema::ArrowError;

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

        // We are taking 128 bits from each value. Shorter values will be padded.
        let value_size: usize = columns.len() * 16;

        // Initialize with zeros
        let mut out: Vec<u8> = vec![0; out_length * value_size];

        for (col_pos, col) in columns.iter().enumerate() {
            set_bits_for_column(col.clone(), col_pos, columns.len(), &mut out)?;
        }

        let offsets = (0..=out_length)
            .map(|i| (i * value_size) as i32)
            .collect::<Vec<i32>>();

        let out_arr = BinaryArray::new_unchecked(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(out),
            None,
        );

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
        let mut converter = RowConverter::new(vec![SortField::new(input.data_type().clone())])?;
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
            dbg!(data);
            assert_eq!(data.value_data().len(), 3 * 16 * 3);
            assert!(data.iter().all(|x| x.unwrap().len() == 3 * 16));

            // This value is mostly filled in since it has a large string
            assert_eq!(
                data.value(0),
                [
                    28, 0, 0, 133, 128, 13, 130, 0, 9, 128, 4, 9, 128, 32, 9, 2, 0, 9, 130, 4, 1,
                    16, 32, 9, 18, 32, 9, 16, 36, 1, 0, 0, 1, 2, 0, 8, 0, 0, 1, 144, 4, 9, 2, 0, 9,
                    128, 32, 9,
                ]
            );

            // This value only has short strings, so it's largely zeros
            assert_eq!(
                data.value(1)[0..12],
                [28u8, 0u8, 0u8, 26, 129, 13, 2, 0, 9, 128, 32, 9]
            );
            assert_eq!(data.value(1)[12..], [0; (3 * 16) - 12]);

            // Last value is all nulls, so mostly zeros
            assert_eq!(data.value(2)[0..1], [2u8]);
            assert_eq!(data.value(2)[1..], [0; (3 * 16) - 1]);
        }
    }
}
