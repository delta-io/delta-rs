//! Optimize a Delta Table
//!
//! Perform bin-packing on a Delta Table which merges small files into a large
//! file. Bin-packing reduces the number of API calls required for read
//! operations.
//!
//! *WARNING:* Currently Optimize only supports append-only workflows. Use with
//! other workflows may corrupt your table state.
//!
//! Optimize increments the table's version and creates remove actions for
//! optimized files. Optimize does not delete files from storage. To delete
//! files that were removed, call `vacuum` on [`DeltaTable`].
//!
//! See [`Optimize`] for configuration.
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let metrics = Optimize::default().execute(table).await?;
//! ````

use crate::action::DeltaOperation;
use crate::action::{self, Action};
use crate::parquet::file::reader::FileReader;
use crate::writer::utils::PartitionPath;
use crate::writer::{DeltaWriter, DeltaWriterError, RecordBatchWriter};
use crate::{DeltaDataTypeLong, DeltaTable, DeltaTableError, PartitionFilter};
use bytes::Bytes;
use log::debug;
use log::error;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::serialized_reader::SerializedFileReader;
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
#[derive(Default)]
pub struct Optimize<'a> {
    filters: &'a [PartitionFilter<'a, &'a str>],
    target_size: Option<i64>,
}

impl<'a> Optimize<'a> {
    /// Only optimize files that return true for the specified partition filter
    pub fn filter(mut self, filters: &'a [PartitionFilter<'a, &'a str>]) -> Self {
        self.filters = filters;
        self
    }

    /// Set the target file size
    pub fn target_size(mut self, target: DeltaDataTypeLong) -> Self {
        self.target_size = Some(target);
        self
    }

    /// Perform the optimization. On completion, a summary of how many files were added and removed is returned
    pub async fn execute(&self, table: &mut DeltaTable) -> Result<Metrics, DeltaWriterError> {
        let plan = create_merge_plan(table, self.filters, (&self.target_size).to_owned())?;
        let metrics = plan.execute(table).await?;
        Ok(metrics)
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
    files: Vec<String>,
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

    fn add(&mut self, file_path: String, size: i64) {
        self.files.push(file_path);
        self.size_bytes += size;
    }
}

fn create_remove(
    path: &str,
    partitions: &HashMap<String, Option<String>>,
    size: DeltaDataTypeLong,
) -> Result<Action, DeltaTableError> {
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
    metrics: Metrics,
    input_parameters: OptimizeInput,
}

impl MergePlan {
    /// Peform the operations outlined in the plan.
    pub async fn execute(self, table: &mut DeltaTable) -> Result<Metrics, DeltaWriterError> {
        // Read files into memory and write into memory. Once a file is complete write to underlying storage.
        let mut actions = vec![];
        let mut metrics = self.metrics;

        for (_partition_path, merge_partition) in self.operations.iter() {
            let partition_values = &merge_partition.partition_values;
            let bins = &merge_partition.bins;
            debug!("{:?}", bins);
            debug!("{:?}", _partition_path);

            for bin in bins {
                let mut writer = RecordBatchWriter::for_table(table)?;

                for path in &bin.files {
                    //Read the file into memory and append it to the writer.

                    let parquet_uri = table.storage.join_path(&table.table_uri, path);
                    let data = table.storage.get_obj(&parquet_uri).await?;
                    let size: DeltaDataTypeLong = data.len().try_into().unwrap();
                    let data = Bytes::from(data);
                    let reader = SerializedFileReader::new(data)?;
                    let records = reader.metadata().file_metadata().num_rows();

                    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));

                    let batch_reader =
                        arrow_reader.get_record_reader(records.try_into().unwrap())?;
                    for batch in batch_reader {
                        let batch = batch?;
                        writer.write_partition(batch, partition_values).await?;
                    }

                    actions.push(create_remove(path, partition_values, size)?);

                    metrics.num_files_removed += 1;
                    metrics.files_removed.total_files += 1;
                    metrics.files_removed.total_size += size;
                    metrics.files_removed.max = std::cmp::max(metrics.files_removed.max, size);
                    metrics.files_removed.min = std::cmp::min(metrics.files_removed.min, size);
                }

                // Save the file to storage and create corresponding add and remove actions. Do not commit yet.
                let add_actions = writer.flush().await?;
                if add_actions.len() != 1 {
                    // Ensure we don't deviate from the merge plan which may result in idempotency being violated
                    return Err(DeltaTableError::Generic(
                        "Expected writer to return only one add action".to_owned(),
                    )
                    .into());
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
            metadata.insert("readVersion".to_owned(), table.version().into());
            let maybe_map_metrics = serde_json::to_value(metrics.clone());
            if let Ok(map) = maybe_map_metrics {
                metadata.insert("operationMetrics".to_owned(), map);
            }

            let mut dtx = table.create_transaction(None);
            dtx.add_actions(actions);
            // TODO: Add predicate information when we can convert the filter to a string representation
            dtx.commit(Some(self.input_parameters.into()), Some(metadata))
                .await?;
        }

        Ok(metrics)
    }
}

fn get_target_file_size(table: &DeltaTable) -> DeltaDataTypeLong {
    let config = table.get_configurations();
    let mut target_size = 268_435_456;
    if let Ok(config) = config {
        let config_str = config.get("delta.targetFileSize");
        if let Some(s) = config_str {
            if let Some(s) = s {
                let r = s.parse::<i64>();
                if let Ok(size) = r {
                    target_size = size;
                } else {
                    error!("Unable to parse value of 'delta.targetFileSize'. Using default value");
                }
            } else {
                error!("Check your configuration of 'delta.targetFileSize'. Using default value");
            }
        }
    }

    target_size
}

/// Build a Plan on which files to merge together. See [`Optimize`]
pub fn create_merge_plan<'a>(
    table: &mut DeltaTable,
    filters: &[PartitionFilter<'a, &str>],
    target_size: Option<DeltaDataTypeLong>,
) -> Result<MergePlan, DeltaWriterError> {
    let target_size = target_size.unwrap_or_else(|| get_target_file_size(table));
    let mut candidates = HashMap::new();
    let mut operations: HashMap<PartitionPath, PartitionMergePlan> = HashMap::new();
    let mut metrics = Metrics::default();
    let partitions_keys = &table.get_metadata()?.partition_columns;

    //Place each add action into a bucket determined by the file's partition
    for add in table.get_active_add_actions_by_partitions(filters)? {
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
                curr_bin.add(file.path.clone(), file.size);
            } else {
                if curr_bin.get_num_files() > 1 {
                    bins.push(curr_bin);
                } else {
                    metrics.total_files_skipped += curr_bin.get_num_files();
                }
                curr_bin = MergeBin::new();
                curr_bin.add(file.path.clone(), file.size);
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

    Ok(MergePlan {
        operations,
        metrics,
        input_parameters,
    })
}
