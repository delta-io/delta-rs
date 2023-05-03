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
use crate::writer::utils::PartitionPath;
use crate::DeltaDataTypeVersion;
use crate::{
    DeltaDataTypeLong, DeltaResult, DeltaTable, DeltaTableError, ObjectMeta, PartitionFilter,
};
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use futures::future::BoxFuture;
use futures::StreamExt;
use log::debug;
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
    /// TODO: The number of batches written
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
    pub min: DeltaDataTypeLong,
    /// Maximum file size of a operation
    pub max: DeltaDataTypeLong,
    /// Average file size of a operation
    pub avg: f64,
    /// Number of files encountered during operation
    pub total_files: usize,
    /// Sum of file sizes of a operation
    pub total_size: DeltaDataTypeLong,
}

impl Default for MetricDetails {
    fn default() -> Self {
        MetricDetails {
            min: DeltaDataTypeLong::MAX,
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
        }
    }

    /// Only optimize files that return true for the specified partition filter
    pub fn with_filters(mut self, filters: &'a [PartitionFilter<'a, &'a str>]) -> Self {
        self.filters = filters;
        self
    }

    /// Set the target file size
    pub fn with_target_size(mut self, target: DeltaDataTypeLong) -> Self {
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
    target_size: DeltaDataTypeLong,
}

impl From<OptimizeInput> for DeltaOperation {
    fn from(opt_input: OptimizeInput) -> Self {
        DeltaOperation::Optimize {
            target_size: opt_input.target_size,
            predicate: None,
        }
    }
}

/// A collection of bins for a particular partition
#[derive(Debug)]
struct MergeBin {
    files: Vec<ObjectMeta>,
    size_bytes: DeltaDataTypeLong,
}

#[derive(Debug)]
struct PartitionMergePlan {
    partition_values: HashMap<String, Option<String>>,
    bins: Vec<MergeBin>,
}

impl MergeBin {
    pub fn new() -> Self {
        MergeBin {
            files: Vec::new(),
            size_bytes: 0,
        }
    }

    fn get_total_file_size(&self) -> i64 {
        self.size_bytes
    }

    fn get_num_files(&self) -> usize {
        self.files.len()
    }

    fn add(&mut self, meta: ObjectMeta) {
        self.size_bytes += meta.size as i64;
        self.files.push(meta);
    }
}

fn create_remove(
    path: &str,
    partitions: &HashMap<String, Option<String>>,
    size: DeltaDataTypeLong,
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
    operations: HashMap<PartitionPath, PartitionMergePlan>,
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
    read_table_version: DeltaDataTypeVersion,
}

impl MergePlan {
    /// Peform the operations outlined in the plan.
    pub async fn execute(
        self,
        object_store: ObjectStoreRef,
        snapshot: &DeltaTableState,
    ) -> Result<Metrics, DeltaTableError> {
        let mut actions = vec![];
        let mut metrics = self.metrics;

        // TODO since we are now async in read and write, should we parallelize this?
        for (_partition_path, merge_partition) in self.operations.iter() {
            let partition_values = &merge_partition.partition_values;
            let bins = &merge_partition.bins;
            debug!("{:?}", bins);
            debug!("{:?}", _partition_path);

            for bin in bins {
                let config = PartitionWriterConfig::try_new(
                    self.file_schema.clone(),
                    partition_values.clone(),
                    self.partition_columns.clone(),
                    Some(self.writer_properties.clone()),
                    Some(self.input_parameters.target_size as usize),
                    None,
                )?;
                let mut writer = PartitionWriter::try_with_config(object_store.clone(), config)?;

                for file_meta in &bin.files {
                    let file_reader =
                        ParquetObjectReader::new(object_store.clone(), file_meta.clone());
                    let mut batch_stream = ParquetRecordBatchStreamBuilder::new(file_reader)
                        .await?
                        .build()?;

                    while let Some(batch) = batch_stream.next().await {
                        let batch = batch?;
                        writer.write(&batch).await?;
                    }

                    let size = file_meta.size as i64;
                    actions.push(create_remove(
                        file_meta.location.as_ref(),
                        partition_values,
                        size,
                    )?);

                    metrics.num_files_removed += 1;
                    metrics.files_removed.total_files += 1;
                    metrics.files_removed.total_size += file_meta.size as i64;
                    metrics.files_removed.max = std::cmp::max(metrics.files_removed.max, size);
                    metrics.files_removed.min = std::cmp::min(metrics.files_removed.min, size);
                }

                // Save the file to storage and create corresponding add and remove actions. Do not commit yet.
                let add_actions = writer.close().await?;
                if add_actions.len() != 1 {
                    // Ensure we don't deviate from the merge plan which may result in idempotency being violated
                    return Err(DeltaTableError::Generic(
                        "Expected writer to return only one add action".to_owned(),
                    ));
                }
                for mut add in add_actions {
                    add.data_change = false;
                    let size = add.size;

                    metrics.num_files_added += 1;
                    metrics.files_added.total_files += 1;
                    metrics.files_added.total_size += size;
                    metrics.files_added.max = std::cmp::max(metrics.files_added.max, size);
                    metrics.files_added.min = std::cmp::min(metrics.files_added.min, size);
                    actions.push(action::Action::add(add));
                }
            }
            metrics.partitions_optimized += 1;
        }

        if metrics.num_files_added == 0 {
            metrics.files_added.min = 0;
            metrics.files_added.avg = 0.0;

            metrics.files_removed.min = 0;
            metrics.files_removed.avg = 0.0;
        } else {
            metrics.files_added.avg =
                (metrics.files_added.total_size as f64) / (metrics.files_added.total_files as f64);
            metrics.files_removed.avg = (metrics.files_removed.total_size as f64)
                / (metrics.files_removed.total_files as f64);
            metrics.num_batches = 1;
        }
        metrics.preserve_insertion_order = true;

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

/// Build a Plan on which files to merge together. See [OptimizeBuilder]
pub fn create_merge_plan(
    snapshot: &DeltaTableState,
    filters: &[PartitionFilter<'_, &str>],
    target_size: Option<DeltaDataTypeLong>,
    writer_properties: WriterProperties,
) -> Result<MergePlan, DeltaTableError> {
    let target_size = target_size.unwrap_or_else(|| snapshot.table_config().target_file_size());
    let mut candidates = HashMap::new();
    let mut operations: HashMap<PartitionPath, PartitionMergePlan> = HashMap::new();
    let mut metrics = Metrics::default();
    let partitions_keys = &snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)?
        .partition_columns;

    //Place each add action into a bucket determined by the file's partition
    for add in snapshot.get_active_add_actions_by_partitions(filters)? {
        let path = PartitionPath::from_hashmap(partitions_keys, &add.partition_values)?;
        let v = candidates
            .entry(path)
            .or_insert_with(|| (add.partition_values.to_owned(), Vec::new()));

        v.1.push(add);
    }

    for mut candidate in candidates {
        let mut bins: Vec<MergeBin> = Vec::new();
        let partition_path = candidate.0;
        let partition_values = &candidate.1 .0;
        let files = &mut candidate.1 .1;
        let mut curr_bin = MergeBin::new();

        files.sort_by(|a, b| b.size.cmp(&a.size));
        metrics.total_considered_files += files.len();

        for file in files {
            if file.size > target_size {
                metrics.total_files_skipped += 1;
                continue;
            }

            if file.size + curr_bin.get_total_file_size() < target_size {
                curr_bin.add((*file).try_into()?);
            } else {
                if curr_bin.get_num_files() > 1 {
                    bins.push(curr_bin);
                } else {
                    metrics.total_files_skipped += curr_bin.get_num_files();
                }
                curr_bin = MergeBin::new();
                curr_bin.add((*file).try_into()?);
            }
        }

        if curr_bin.get_num_files() > 1 {
            bins.push(curr_bin);
        } else {
            metrics.total_files_skipped += curr_bin.get_num_files();
        }

        if !bins.is_empty() {
            operations.insert(
                partition_path.to_owned(),
                PartitionMergePlan {
                    bins,
                    partition_values: partition_values.to_owned(),
                },
            );
        }
    }

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
    })
}
