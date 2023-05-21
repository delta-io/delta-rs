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
use crate::storage::ObjectStoreRef;
use crate::table_state::DeltaTableState;
use crate::writer::utils::arrow_schema_without_partitions;
use crate::{DeltaResult, DeltaTable, DeltaTableError, ObjectMeta, PartitionFilter};
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
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
            max_concurrent_tasks: 10,
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
            let writer_properties = this
                .writer_properties
                .unwrap_or_else(|| WriterProperties::builder().build());
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

#[derive(Debug)]
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

#[derive(Debug)]
/// Encapsulates the operations required to optimize a Delta Table
pub struct MergePlan {
    operations: HashMap<PartitionTuples, Vec<ObjectMeta>>,
    /// Metrics collected during operation
    metrics: Metrics,
    /// Parameters passed to optimize operation
    input_parameters: OptimizeInput,
    /// Schema of written files
    file_schema: ArrowSchemaRef,
    /// Column names the table is partitioned by.
    partition_columns: Vec<String>,
    /// Properties passed to parquet writer
    writer_properties: WriterProperties,
    /// Version of the table at beginning of optimization. Used for conflict resolution.
    read_table_version: i64,
    /// Whether to preserve insertion order within files
    /// Max number of concurrent tasks
    max_concurrent_tasks: usize,
}

impl MergePlan {
    async fn rewrite_files(
        &self,
        partition: PartitionTuples,
        files: Vec<ObjectMeta>,
        object_store: ObjectStoreRef,
    ) -> Result<(Vec<Action>, PartialMetrics), DeltaTableError> {
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

        let mut files_removed = MetricDetails::default();

        for file in files.iter() {
            files_removed.total_files += 1;
            files_removed.total_size += file.size as i64;
            files_removed.max = std::cmp::max(files_removed.max, file.size as i64);
            files_removed.min = std::cmp::min(files_removed.min, file.size as i64);
        }

        let mut partial_metrics = PartialMetrics {
            num_files_added: 0,
            num_files_removed: files.len() as u64,
            files_added: MetricDetails::default(),
            files_removed,
            num_batches: 0,
        };

        // Next, initialize the writer
        let writer_config = PartitionWriterConfig::try_new(
            self.file_schema.clone(),
            partition_values.clone(),
            self.partition_columns.clone(),
            Some(self.writer_properties.clone()),
            Some(self.input_parameters.target_size as usize),
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
        let mut metrics = Metrics::default();
        std::mem::swap(&mut self.metrics, &mut metrics);

        let mut operations = HashMap::new();
        std::mem::swap(&mut self.operations, &mut operations);
        // let operations = self.operations.take();

        futures::stream::iter(operations)
            .map(|(partition, files)| self.rewrite_files(partition, files, object_store.clone()))
            .buffer_unordered(self.max_concurrent_tasks)
            .try_for_each(|(partial_actions, partial_metrics)| {
                actions.extend(partial_actions);
                metrics.add(&partial_metrics);
                async { Ok(()) }
            })
            .await?;

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
                self.input_parameters.into(),
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

    let mut operations: HashMap<PartitionTuples, Vec<ObjectMeta>> = HashMap::new();
    let mut metrics = Metrics::default();
    let partitions_keys = &snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)?
        .partition_columns;

    //Place each add action into a bucket determined by the file's partition
    for add in snapshot.get_active_add_actions_by_partitions(filters)? {
        let part = PartitionTuples::from_hashmap(partitions_keys, &add.partition_values);

        metrics.total_considered_files += 1;

        // Skip any files at or over the target size
        if add.size >= target_size {
            metrics.total_files_skipped += 1;
            continue;
        }

        operations
            .entry(part)
            .or_insert_with(Vec::new)
            .push(add.try_into()?);
    }

    // Prune any partitions that only have 1 file, since they can't be merged with anything.
    for (_, files) in operations.iter_mut() {
        if files.len() == 1 {
            metrics.total_files_skipped += 1;
            files.clear();
        }
    }
    operations.retain(|_, files| !files.is_empty());

    metrics.partitions_optimized = operations.len() as u64;

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
        input_parameters,
        writer_properties,
        file_schema,
        partition_columns: partitions_keys.clone(),
        read_table_version: snapshot.version(),
        max_concurrent_tasks,
    })
}
