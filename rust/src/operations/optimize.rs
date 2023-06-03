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

use super::transaction::commit;
use super::writer::{PartitionWriter, PartitionWriterConfig};
use crate::action::{self, Action, DeltaOperation};
use crate::crate_version;
use crate::storage::ObjectStoreRef;
use crate::table_state::DeltaTableState;
use crate::writer::utils::arrow_schema_without_partitions;
use crate::{DeltaResult, DeltaTable, DeltaTableError, ObjectMeta, PartitionFilter};
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use num_cpus;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// Max number of concurrent tasks (defeault 10)
    max_concurrent_tasks: usize,
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
        }
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
    // TODO: ZOrder and Sort
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

impl MergePlan {
    /// Rewrites files in a single partition.
    ///
    /// Returns a vector of add and remove actions, as well as the partial metrics
    /// collected during the operation.
    async fn rewrite_files(
        task_parameters: Arc<MergeTaskParameters>,
        partition: PartitionTuples,
        files: MergeBin,
        object_store: ObjectStoreRef,
    ) -> Result<(Vec<Action>, PartialMetrics), DeltaTableError> {
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

        // Get data as a stream of batches
        let mut batch_stream = futures::stream::iter(files)
            .then(|file| async {
                let file_reader = ParquetObjectReader::new(object_store.clone(), file);
                ParquetRecordBatchStreamBuilder::new(file_reader)
                    .await?
                    .build()
            })
            .try_flatten()
            .boxed();

        while let Some(maybe_batch) = batch_stream.next().await {
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

    /// Peform the operations outlined in the plan.
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
                    .map(|(partition, files)| async {
                        let rewrite_result = tokio::task::spawn(Self::rewrite_files(
                            self.task_parameters.clone(),
                            partition,
                            files,
                            object_store.clone(),
                        ))
                        .await;
                        match rewrite_result {
                            Ok(Ok(data)) => Ok(data),
                            Ok(Err(e)) => Err(e),
                            Err(e) => Err(DeltaTableError::GenericError {
                                source: Box::new(e),
                            }),
                        }
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

    let (operations, metrics) =
        build_compaction_plan(snapshot, partitions_keys, filters, target_size)?;

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
#[derive(Debug)]
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
    // Also prune merge bins where compaction wouldnt provide any benefit
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
