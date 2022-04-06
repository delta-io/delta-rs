//! TODO: Optimize

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use log::debug;
use log::error;
use parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::serialized_reader::{SerializedFileReader, SliceableCursor};
use parquet::file::writer::InMemoryWriteableCursor;

use crate::action::{self, Action};
use crate::{
    generate_parquet_filename, DeltaDataTypeLong, DeltaTable, DeltaTableError, DeltaTablePartition,
    PartitionFilter, SchemaTypeStruct,
};

#[derive(Default, Debug)]
///Metrics from Optimize
pub struct Metrics {
    ///Number of optimized files added
    pub num_files_added: u64,
    ///Number of unoptimized files removed
    pub num_files_removed: u64,
    ///Detailed metrics for the add operation
    pub files_added: MetricDetails,
    ///Detailed metrics for the remove operation
    pub files_removed: MetricDetails,
    ///Number of partitions that had at least one file optimized
    pub partitions_optimized: u64,
    ///TODO
    pub num_batches: u64,
    ///How many files were considered during optimization. Not every file considered is optimized
    pub total_considered_files: usize,
    ///How many files were considered for optimization but were skipped
    pub total_files_skipped: usize,
    ///TODO
    pub preserve_insertion_order: bool,
}

#[derive(Debug)]
///Statistics on files for a particular operation
/// Operation can be remove or add
pub struct MetricDetails {
    ///Maximum file size of a operation
    pub min: usize,
    ///Minimum file size of a operation
    pub max: usize,
    ///Average file size of a operation
    pub avg: f64,
    ///Number of files encountered during operation
    pub total_files: usize,
    ///Sum of file sizes of a operation
    pub total_size: usize,
}

impl Default for MetricDetails {
    fn default() -> Self {
        MetricDetails {
            min: usize::MAX,
            max: 0,
            avg: 0.0,
            total_files: 0,
            total_size: 0,
        }
    }
}

///TODO: Optimize
#[derive(Default)]
pub struct Optimize<'a> {
    filters: &'a [PartitionFilter<'a, &'a str>],
}

impl<'a> Optimize<'a> {
    ///Only optimize files that return true for the specified partition filter
    pub fn filter(mut self, filters: &'a [PartitionFilter<'a, &'a str>]) -> Self {
        self.filters = filters;
        self
    }

    ///Perform the optimization
    pub async fn execute(self, table: &mut DeltaTable) -> Result<Metrics, DeltaTableError> {
        let plan = create_merge_plan(table, self.filters)?;
        let metrics = plan.execute(table).await?;
        Ok(metrics)
    }
}

#[derive(std::cmp::Eq, Debug)]
struct PartitionValuesWrapper<'a>(Vec<DeltaTablePartition<'a>>);

impl<'a> From<&DeltaTablePartition<'a>> for Partition {
    fn from(p: &DeltaTablePartition<'a>) -> Self {
        Partition {
            key: p.key.to_owned(),
            value: p.value.to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Partition {
    pub key: String,
    pub value: String,
}

#[derive(std::cmp::Eq, Debug)]
struct Partitions(Vec<Partition>);

impl<'a> From<&PartitionValuesWrapper<'a>> for Partitions {
    fn from(p: &PartitionValuesWrapper<'a>) -> Self {
        let mut vec = Vec::new();
        for v in &p.0 {
            vec.push(Partition::from(v));
        }

        Partitions(vec)
    }
}

impl Hash for Partitions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        //Hashmap does not maintain order and partition values of {a=123, b=234} must match the hash of {b=234, a=123}
        let mut v = Vec::new();
        for p in &self.0 {
            v.push(p);
        }
        v.sort_by(|a, b| a.key.partial_cmp(&b.key).unwrap());

        for p in v {
            p.key.hash(state);
            "=".hash(state);
            p.value.hash(state);
            "/".hash(state);
        }
    }
}

impl<'a> Hash for PartitionValuesWrapper<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        //Hashmap does not maintain order and partition values of {a=123, b=234} must match the hash of {b=234, a=123}
        let mut v = Vec::new();
        for p in &self.0 {
            v.push(p);
        }
        v.sort_by(|a, b| a.key.partial_cmp(b.key).unwrap());

        for p in v {
            p.key.hash(state);
            "=".hash(state);
            p.value.hash(state);
            "/".hash(state);
        }
    }
}

impl<'a> PartialEq for PartitionValuesWrapper<'a> {
    fn eq(&self, rhs: &Self) -> bool {
        self.0.eq(&rhs.0)
    }
}

impl PartialEq for Partitions {
    fn eq(&self, rhs: &Self) -> bool {
        self.0.eq(&rhs.0)
    }
}

type Merge = Vec<Vec<String>>;
#[derive(Debug)]
struct MergePlan {
    operations: HashMap<Partitions, Merge>,
    metrics: Metrics,
}

impl MergePlan {
    fn to_internal_partition_format(
        &self,
        partitions: &Partitions,
    ) -> Option<Vec<(String, String)>> {
        if !partitions.0.is_empty() {
            let mut p = vec![];
            for i in &partitions.0 {
                p.push((i.key.clone(), i.value.clone()));
            }
            Some(p)
        } else {
            None
        }
    }

    fn create_remove(
        &self,
        path: &str,
        partitions: Option<Vec<(String, String)>>,
        size: DeltaDataTypeLong,
    ) -> Result<Action, DeltaTableError> {
        let partition_values = if let Some(partitions) = &partitions {
            let mut partition_values = HashMap::new();
            for (key, value) in partitions {
                partition_values.insert(key.clone(), Some(value.clone()));
            }
            Some(partition_values)
        } else {
            None
        };

        let deletion_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let deletion_time = deletion_time.as_millis() as i64;

        Ok(Action::remove(action::Remove {
            path: path.to_string(),
            deletion_timestamp: Some(deletion_time),
            data_change: false,
            extended_file_metadata: None,
            partition_values,
            size: Some(size),
            tags: None,
        }))
    }

    async fn create_add(
        &self,
        table: &DeltaTable,
        bytes: &[u8],
        partitions: Option<Vec<(String, String)>>,
    ) -> Result<Action, DeltaTableError> {
        let mut partition_values = HashMap::new();
        if let Some(partitions) = &partitions {
            for (key, value) in partitions {
                partition_values.insert(key.clone(), Some(value.clone()));
            }
        }

        let path = generate_parquet_filename(table, partitions);
        let parquet_uri = table.storage.join_path(&table.table_uri, &path);

        debug!("Writing a parquet file to {}", &parquet_uri);

        table.storage.put_obj(&parquet_uri, bytes).await?;

        // Determine the modification timestamp to include in the add action - milliseconds since epoch
        // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
        let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let modification_time = modification_time.as_millis() as i64;

        Ok(Action::add(action::Add {
            path,
            partition_values,
            modification_time,
            size: bytes.len() as i64,
            partition_values_parsed: None,
            data_change: false,
            stats: None,
            stats_parsed: None,
            tags: None,
        }))
    }

    pub async fn execute(mut self, table: &mut DeltaTable) -> Result<Metrics, DeltaTableError> {
        //Read files into memory and write into memory. Once a file is complete write to underlying storage.
        let schema = table.get_metadata()?.clone().schema.clone();

        let columns = &table.get_metadata()?.partition_columns;
        let partition_column_set: HashSet<String> = columns.iter().map(|s| s.to_owned()).collect();

        //Remove partitions from the schema since they don't need to written to the parquet file
        let fields = schema
            .get_fields()
            .iter()
            .filter(|field| !partition_column_set.contains(field.get_name()))
            .map(|e| e.to_owned())
            .collect();
        let schema = SchemaTypeStruct::new(fields);

        let arrow_schema =
            <arrow::datatypes::Schema as TryFrom<&crate::Schema>>::try_from(&schema).unwrap();
        let schema = Arc::new(arrow_schema);
        let mut actions = vec![];

        for (_partitions, merges) in self.operations.iter() {
            debug!("{:?}", merges);
            debug!("{:?}", _partitions);
            for merge in merges {
                let writer_properties = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();
                let writeable_cursor = InMemoryWriteableCursor::default();
                let mut _writer = ArrowWriter::try_new(
                    writeable_cursor.clone(),
                    schema.clone(),
                    Some(writer_properties),
                )?;

                for path in merge {
                    //load the file into memory and append it to the buffer
                    let parquet_uri = table.storage.join_path(&table.table_uri, path);
                    let data = table.storage.get_obj(&parquet_uri).await?;
                    let size = data.len();
                    let data = SliceableCursor::new(data);
                    let reader = SerializedFileReader::new(data)?;

                    //TODO: Can this handle schema changes?
                    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));

                    let mut reader_indicies = vec![];
                    for (i, f) in arrow_reader.get_schema()?.fields().iter().enumerate() {
                        if !partition_column_set.contains(f.name()) {
                            reader_indicies.push(i);
                        }
                    }

                    let batch_reader =
                        arrow_reader.get_record_reader_by_columns(reader_indicies, 2048)?;
                    for batch in batch_reader {
                        let batch = batch?;
                        _writer.write(&batch)?;
                    }
                    actions.push(self.create_remove(
                        path,
                        self.to_internal_partition_format(_partitions),
                        size.try_into().unwrap(),
                    )?);

                    self.metrics.num_files_removed += 1;
                    self.metrics.files_removed.total_files += 1;
                    self.metrics.files_removed.total_size += size;
                    self.metrics.files_removed.max =
                        std::cmp::max(self.metrics.files_removed.max, size);
                    self.metrics.files_removed.min =
                        std::cmp::min(self.metrics.files_removed.min, size);
                }
                //Save the file to storage and create corresponding add and delete actions. Do not commit yet.
                _writer.close()?;
                let size = writeable_cursor.data().len();
                actions.push(
                    self.create_add(
                        table,
                        &writeable_cursor.data(),
                        self.to_internal_partition_format(_partitions),
                    )
                    .await?,
                );

                self.metrics.num_files_added += 1;
                self.metrics.files_added.total_files += 1;
                self.metrics.files_added.total_size += size;
                self.metrics.files_added.max = std::cmp::max(self.metrics.files_added.max, size);
                self.metrics.files_added.min = std::cmp::min(self.metrics.files_added.min, size);
            }
            //Currently a table without any partitions has a count of one partition. Check if that is acceptable
            self.metrics.partitions_optimized += 1;
        }

        //try to commit actions to the delta log.
        //Need to check for conflicts
        let mut dtx = table.create_transaction(None);
        dtx.add_actions(actions);
        dtx.commit(None).await?;

        self.metrics.files_added.avg = (self.metrics.files_added.total_size as f64)
            / (self.metrics.files_added.total_files as f64);
        self.metrics.files_removed.avg = (self.metrics.files_removed.total_size as f64)
            / (self.metrics.files_removed.total_files as f64);
        self.metrics.num_batches = 1;

        Ok(self.metrics)
    }
}

fn get_target_file_size(table: &DeltaTable) -> i64 {
    let config = table.get_configurations();
    let mut target_size = 256000000;
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

fn create_merge_plan<'a>(
    table: &mut DeltaTable,
    filters: &[PartitionFilter<'a, &str>],
) -> Result<MergePlan, DeltaTableError> {
    let target_size = get_target_file_size(table);
    let mut candidates = HashMap::new();
    let mut operations: HashMap<Partitions, Merge> = HashMap::new();
    let mut metrics = Metrics::default();

    //Place each add action into a bucket determined by the file's partition
    for add in table.get_active_add_actions_by_partitions(filters)? {
        let partitions = add
            .partition_values
            .iter()
            .map(|p| DeltaTablePartition::from_partition_value(p, ""))
            .collect::<Vec<DeltaTablePartition>>();

        let partitions = PartitionValuesWrapper(partitions);
        let v = candidates.entry(partitions).or_insert_with(Vec::new);
        v.push(add);
    }

    for candidate in candidates {
        let mut current = Vec::new();
        let mut current_size = 0;
        let mut opt = Vec::new();

        let partition = candidate.0;
        let files = candidate.1;
        metrics.total_considered_files += files.len();

        for f in files {
            if f.size < target_size {
                if f.size < (target_size - current_size) {
                    current.push(f.path.clone());
                    current_size += f.size;
                } else {
                    //create a new bin. Discard old bin if it contains only one member
                    if current.len() > 1 {
                        opt.push(current);
                    } else {
                        metrics.total_files_skipped += 1;
                    }
                    current = Vec::new();
                    current_size = 0;
                    current.push(f.path.clone());
                }
            } else {
                metrics.total_files_skipped += 1;
            }
        }

        if current.len() > 1 {
            opt.push(current);
        } else {
            metrics.total_files_skipped += 1;
        }

        operations.insert(Partitions::from(&partition), opt);
    }

    Ok(MergePlan {
        operations,
        metrics,
    })
}
