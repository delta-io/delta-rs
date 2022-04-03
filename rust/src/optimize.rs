//! TODO: Optimize

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use log::debug;
use parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::serialized_reader::{SerializedFileReader, SliceableCursor};
use parquet::file::writer::InMemoryWriteableCursor;

use crate::action::{self, Action};
use crate::{
    generate_parquet_filename, DeltaDataTypeLong, DeltaTable, DeltaTableError, DeltaTablePartition,
    PartitionFilter,
};

/*
{"numFilesAdded": 13, "numFilesRemoved": 3021, "filesAdded": {"min": 31390063,
"max": 254248103, "avg": 216181665.23076922, "totalFiles": 13, "totalSize":
2810361648}, "filesRemoved": {"min": 8270, "max": 2516341, "avg":
1001047.8202581926, "totalFiles": 3021, "totalSize": 3024165465},
"partitionsOptimized": 2, "zOrderStats": null, "numBatches": 1,
"totalConsideredFiles": 3021, "totalFilesSkipped": 0, "preserveInsertionOrder":
true}
*/
#[derive(Default, Debug)]
///Metrics from Optimize
pub struct Metrics {
    ///TODO
    pub num_files_added: u64,
    ///TODO
    pub num_files_removed: u64,
    ///TODO
    pub partitions_optimized: u64,
    ///TODO
    pub num_batches: u64,
    ///TODO
    pub total_considered_files: u64,
    ///TODO
    pub total_files_skipped: u64,
    ///TODO
    pub preserve_insertion_order: bool,
}

///TODO: Optimize
#[derive(Default)]
pub struct Optimize<'a> {
    filters: Option<&'a [PartitionFilter<'a, &'a str>]>,
}

impl<'a> Optimize<'a> {
    ///Only optimize files that belong to the specified filter
    pub fn _where(&mut self, filters: &'a [PartitionFilter<'a, &'a str>]) -> &Self {
        self.filters = Some(filters);
        return self;
    }

    ///Perform the optimization
    pub async fn execute(self, table: &mut DeltaTable) -> Result<Metrics, DeltaTableError> {
        let filters = Vec::new();
        let plan = if self.filters.is_none() {
            create_merge_plan(table, &filters)?
        } else {
            create_merge_plan(table, self.filters.as_ref().unwrap())?
        };
        print!("Merge Plan: \n {:?}", plan);
        let metrics = plan.execute(table).await?;
        return Ok(metrics);
    }
}

#[derive(std::cmp::Eq, Debug)]
struct PartitionValuesWrapper<'a>(Vec<DeltaTablePartition<'a>>);

impl<'a> From<&DeltaTablePartition<'a>> for Partition {
    fn from(p: &DeltaTablePartition<'a>) -> Self {
        Partition {
            key: p.key.to_owned(),
            value: p.key.to_owned(),
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
        if partitions.0.len() > 0 {
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
            partition_values: partition_values,
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

    pub async fn execute(self, table: &mut DeltaTable) -> Result<Metrics, DeltaTableError> {
        //Read files into memory and write into memory. Once a file is complete write to underlying storage.
        let schema = &table.get_metadata()?.clone().schema;
        let arrow_schema =
            <arrow::datatypes::Schema as TryFrom<&crate::Schema>>::try_from(&schema).unwrap();
        let schema = Arc::new(arrow_schema);
        let mut actions = vec![];

        for (_partitions, merges) in self.operations.iter() {
            println!("{:?}", merges);
            println!("{:?}", _partitions);
            for merge in merges {
                //Open some buffer
                //Extra: track statistics for the file
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

                    let parquet_uri = table.storage.join_path(&table.table_uri, &path);
                    println!("Open {:?}", parquet_uri);
                    let data = table.storage.get_obj(&parquet_uri).await?;
                    let data = SliceableCursor::new(data);
                    let size = data.len();
                    let reader = SerializedFileReader::new(data)?;

                    //TODO: Can this handle schema changes?
                    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
                    let batch_reader = arrow_reader.get_record_reader(2048)?;
                    for batch in batch_reader {
                        let batch = batch?;
                        _writer.write(&batch)?;
                    }
                    actions.push(self.create_remove(
                        &path,
                        self.to_internal_partition_format(_partitions),
                        size.try_into().unwrap(),
                    )?)
                }
                //Save the file to storage and create corresponding add and delete actions. Do not commit yet.
                _writer.close()?;
                actions.push(
                    self.create_add(
                        table,
                        &writeable_cursor.data(),
                        self.to_internal_partition_format(_partitions),
                    )
                    .await?,
                );
            }
        }

        //try to commit actions to the delta log.
        //Need to check for conflicts
        let mut dtx = table.create_transaction(None);
        dtx.add_actions(actions);
        dtx.commit(None).await?;

        Ok(self.metrics)
    }
}

fn create_merge_plan<'a>(
    table: &mut DeltaTable,
    filters: &[PartitionFilter<'a, &str>],
) -> Result<MergePlan, DeltaTableError> {
    let mut candidates = HashMap::new();

    for add in table.get_active_add_actions_by_partitions(filters)? {
        let partitions = add
            .partition_values
            .iter()
            .map(|p| DeltaTablePartition::from_partition_value(p, ""))
            .collect::<Vec<DeltaTablePartition>>();

        let partitions = PartitionValuesWrapper(partitions);
        let v = candidates.entry(partitions).or_insert_with(|| Vec::new());
        v.push(add);
    }

    //Naively try to fit as many files as possible
    let mut operations: HashMap<Partitions, Merge> = HashMap::new();
    let metrics = Metrics::default();
    //TODO: Get this from table config
    let max_size: i64 = 1024 * 1024 * 200;

    for candidate in candidates {
        let mut current = Vec::new();
        let mut current_size = 0;
        let mut opt = Vec::new();

        let partition = candidate.0;
        let files = candidate.1;

        for f in files {
            if f.size < max_size {
                if f.size < (max_size - current_size) {
                    current.push(f.path.clone());
                    current_size += f.size;
                    //Add the file
                } else {
                    //create a new bin. Discard old bin if it contains only one member
                    if current.len() > 1 {
                        opt.push(current);
                    }
                    current = Vec::new();
                    current_size = 0;
                    current.push(f.path.clone());
                }
            }
        }

        if current.len() > 1 {
            opt.push(current);
        }

        operations.insert(Partitions::from(&partition), opt);
    }

    Ok(MergePlan {
        operations,
        metrics,
    })
}
