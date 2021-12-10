//! Abstractions and implementations for writing data to delta tables
// TODO
// - consider file size when writing parquet files
// - handle writer version

use crate::{
    action::{Action, Add, ColumnCountStat, ColumnValueStat, DeltaOperation, Remove, Stats},
    get_backend_for_uri_with_options, schema,
    writer::time_utils::timestamp_to_delta_stats_string,
    DeltaDataTypeVersion, DeltaTable, DeltaTableError, DeltaTableMetaData, Schema, StorageBackend,
    StorageError, UriError,
};
use arrow::{
    array::{as_boolean_array, as_primitive_array, make_array, Array, ArrayData, UInt32Array},
    buffer::MutableBuffer,
    compute::{lexicographical_partition_ranges, lexsort_to_indices, take, SortColumn},
    datatypes::*,
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef},
    error::ArrowError,
    record_batch::*,
};
use arrow_buffer::{stringified_partition_value, DataArrowWriter};
use parquet::{
    basic::{Compression, LogicalType, TimestampType},
    errors::ParquetError,
    file::{metadata::RowGroupMetaData, properties::WriterProperties, statistics::Statistics},
    schema::types::{ColumnDescriptor, SchemaDescriptor},
};
use parquet_format::FileMetaData;
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

mod arrow_buffer;
pub mod handlers;
pub mod streams;

const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

type NullCounts = HashMap<String, ColumnCountStat>;
type MinAndMaxValues = (
    HashMap<String, ColumnValueStat>,
    HashMap<String, ColumnValueStat>,
);

impl TryFrom<Arc<ArrowSchema>> for Schema {
    type Error = DeltaTableError;

    fn try_from(s: Arc<ArrowSchema>) -> Result<Self, DeltaTableError> {
        let fields = s
            .fields()
            .iter()
            .map(<schema::SchemaField as TryFrom<&ArrowField>>::try_from)
            .collect::<Result<Vec<schema::SchemaField>, DeltaTableError>>()?;

        Ok(Schema::new(fields))
    }
}

impl TryFrom<&ArrowField> for schema::SchemaField {
    type Error = DeltaTableError;

    fn try_from(f: &ArrowField) -> Result<Self, DeltaTableError> {
        let field = schema::SchemaField::new(
            f.name().to_string(),
            schema::SchemaDataType::try_from(f.data_type())?,
            f.is_nullable(),
            HashMap::new(),
        );
        Ok(field)
    }
}

impl TryFrom<&ArrowDataType> for schema::SchemaDataType {
    type Error = DeltaTableError;

    fn try_from(t: &ArrowDataType) -> Result<Self, DeltaTableError> {
        match t {
            ArrowDataType::Utf8 => Ok(schema::SchemaDataType::primitive("string".to_string())),
            ArrowDataType::Int64 => Ok(schema::SchemaDataType::primitive("long".to_string())),
            ArrowDataType::Int32 => Ok(schema::SchemaDataType::primitive("integer".to_string())),
            ArrowDataType::Int16 => Ok(schema::SchemaDataType::primitive("short".to_string())),
            ArrowDataType::Int8 => Ok(schema::SchemaDataType::primitive("byte".to_string())),
            ArrowDataType::Float32 => Ok(schema::SchemaDataType::primitive("float".to_string())),
            ArrowDataType::Float64 => Ok(schema::SchemaDataType::primitive("double".to_string())),
            ArrowDataType::Boolean => Ok(schema::SchemaDataType::primitive("boolean".to_string())),
            ArrowDataType::Binary => Ok(schema::SchemaDataType::primitive("binary".to_string())),
            ArrowDataType::Date32 => Ok(schema::SchemaDataType::primitive("date".to_string())),
            // TODO handle missing datatypes, especially struct, array, map
            _ => Err(DeltaTableError::Generic(
                "Error converting Arrow datatype.".to_string(),
            )),
        }
    }
}

/// Enum representing an error when calling [`DataWriter`].
#[derive(thiserror::Error, Debug)]
pub enum DataWriterError {
    /// Partition column is missing in a record written to delta.
    #[error("Missing partition column: {0}")]
    MissingPartitionColumn(String),

    /// The Arrow RecordBatch schema does not match the expected schema.
    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        /// The record batch schema.
        record_batch_schema: SchemaRef,
        /// The schema of the target delta table.
        expected_schema: Arc<arrow::datatypes::Schema>,
    },

    /// An Arrow RecordBatch could not be created from the JSON buffer.
    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,

    /// A record was written that was not a JSON object.
    #[error("Record {0} is not a JSON object")]
    InvalidRecord(String),

    /// Indicates that a partial write was performed and error records were discarded.
    #[error("Failed to write some values to parquet. Sample error: {sample_error}.")]
    PartialParquetWrite {
        /// Vec of tuples where the first element of each tuple is the skipped value and the second element is the [`ParquetError`] associated with it.
        skipped_values: Vec<(Value, ParquetError)>,
        /// A sample [`ParquetError`] representing the overall partial write.
        sample_error: ParquetError,
    },

    // TODO: derive Debug for Stats in delta-rs
    /// Serialization of delta log statistics failed.
    #[error("Serialization of delta log statistics failed")]
    StatsSerializationFailed {
        /// The stats object that failed serialization.
        stats: Stats,
    },

    /// Invalid table paths was specified for the delta table.
    #[error("Invalid table path: {}", .source)]
    UriError {
        /// The wrapped [`UriError`].
        #[from]
        source: UriError,
    },

    /// deltalake storage backend returned an error.
    #[error("Storage interaction failed: {source}")]
    Storage {
        /// The wrapped [`StorageError`]
        #[from]
        source: StorageError,
    },

    /// DeltaTable returned an error.
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// The wrapped [`DeltaTableError`]
        #[from]
        source: DeltaTableError,
    },

    /// Arrow returned an error.
    #[error("Arrow interaction failed: {source}")]
    Arrow {
        /// The wrapped [`ArrowError`]
        #[from]
        source: ArrowError,
    },

    /// Parquet write failed.
    #[error("Parquet write failed: {source}")]
    Parquet {
        /// The wrapped [`ParquetError`]
        #[from]
        source: ParquetError,
    },

    /// Error returned from std::io
    #[error("std::io::Error: {source}")]
    Io {
        /// The wrapped [`std::io::Error`]
        #[from]
        source: std::io::Error,
    },
}

/// Process data messages for handling in data writer
pub trait MessageHandler<M: Clone + Send + Sync> {
    /// Convert a buffer of messages to a RecordBatch
    fn record_batch_from_message(
        &self,
        arrow_schema: Arc<ArrowSchema>,
        message_buffer: &[M],
    ) -> Result<RecordBatch, DataWriterError>;

    /// Split a vector of messages into individual partitions
    fn divide_by_partition_values(
        &self,
        partition_columns: &[String],
        records: Vec<M>,
    ) -> Result<HashMap<String, Vec<M>>, DataWriterError>;
}

/// Writes messages to a delta lake table.
pub struct DataWriter {
    storage: Box<dyn StorageBackend>,
    // message_handler: Arc<dyn MessageHandler<M>>,
    arrow_schema_ref: Arc<arrow::datatypes::Schema>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, DataArrowWriter>,
}

impl DataWriter {
    /// Creates a DataWriter to write to the given table
    pub fn for_table(
        table: &DeltaTable,
        // message_handler: Arc<dyn MessageHandler<M>>,
        options: HashMap<String, String>,
    ) -> Result<DataWriter, DataWriterError> {
        let storage = get_backend_for_uri_with_options(&table.table_uri, options)?;

        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.get_metadata()?;
        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema)?;
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns.clone();

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        Ok(Self {
            storage,
            // message_handler,
            arrow_schema_ref,
            writer_properties,
            partition_columns,
            arrow_writers: HashMap::new(),
        })
    }

    /// Retrieves the latest schema from table, compares to the current and updates if changed.
    /// When schema is updated then `true` is returned which signals the caller that parquet
    /// created file or arrow batch should be revisited.
    pub fn update_schema(
        &mut self,
        metadata: &DeltaTableMetaData,
    ) -> Result<bool, DataWriterError> {
        let schema: ArrowSchema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema)?;

        let schema_updated = self.arrow_schema_ref.as_ref() != &schema
            || self.partition_columns != metadata.partition_columns;

        if schema_updated {
            let _ = std::mem::replace(&mut self.arrow_schema_ref, Arc::new(schema));
            let _ = std::mem::replace(
                &mut self.partition_columns,
                metadata.partition_columns.clone(),
            );
        }

        Ok(schema_updated)
    }

    fn divide_by_partition_values(
        &mut self,
        values: Vec<RecordBatch>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, DataWriterError> {
        let mut partitions = HashMap::new();

        if self.partition_columns.is_empty() {
            partitions.insert(Value::Null.to_string(), values);
            return Ok(partitions);
        }

        for batch in values {
            let parts = self.divide_record_batch_by_partition_values(&batch)?;
            for (key, part_values) in parts {
                match partitions.get_mut(&key) {
                    Some(vec) => vec.push(part_values),
                    None => {
                        partitions.insert(key, vec![part_values]);
                    }
                }
            }
        }
        
        Ok(partitions)
    }

    fn divide_record_batch_by_partition_values(
        &mut self,
        values: &RecordBatch,
    ) -> Result<HashMap<String, RecordBatch>, DataWriterError> {
        let mut partitions = HashMap::new();

        if self.partition_columns.is_empty() {
            partitions.insert(Value::Null.to_string(), values.clone());
            return Ok(partitions);
        }

        let schema = values.schema();
        // let batch_schema = Arc::new(ArrowSchema::new(
        //     schema
        //         .fields()
        //         .iter()
        //         .filter(|f| !self.partition_columns.contains(f.name()))
        //         .map(|f| f.to_owned())
        //         .collect::<Vec<_>>(),
        // ));

        // collect all columns in order relevant for partitioning
        let sort_columns = self
            .partition_columns
            .clone()
            .into_iter()
            .map(|col| SortColumn {
                values: values.column(schema.index_of(&col).unwrap()).clone(),
                options: None,
            })
            .collect::<Vec<_>>();

        let indices = lexsort_to_indices(sort_columns.as_slice(), None).unwrap();
        let sorted_partition_columns = sort_columns
            .iter()
            .map(|c| SortColumn {
                values: take(c.values.as_ref(), &indices, None).unwrap(),
                options: None,
            })
            .collect::<Vec<_>>();

        let ranges = lexicographical_partition_ranges(sorted_partition_columns.as_slice())?;

        for range in ranges {
            // get row indices for current partition
            let idx: UInt32Array = (range.start..range.end)
                .map(|i| Some(indices.value(i)))
                .into_iter()
                .collect();

            let partition_key = sorted_partition_columns
                .iter()
                .map(|c| {
                    stringified_partition_value(
                        &c.values.slice(range.start, range.end - range.start),
                    )
                    .unwrap()
                })
                .zip(self.partition_columns.clone())
                .map(|tuple| format!("{}={}", tuple.1, tuple.0.unwrap_or(Value::Null.to_string())))
                .collect::<Vec<_>>()
                .join("/");

            let batch_data = schema
                .fields()
                .iter()
                .map(|f| values.column(schema.index_of(&f.name()).unwrap()).clone())
                .map(move |col| take(col.as_ref(), &idx, None).unwrap())
                .collect::<Vec<_>>();

            partitions.insert(
                partition_key,
                RecordBatch::try_new(schema.clone(), batch_data).unwrap(),
            );
        }

        Ok(partitions)
    }

    /// Writes the given values to internal parquet buffers for each represented partition.
    pub async fn write_record_batch(
        &mut self,
        values: &RecordBatch,
    ) -> Result<(), DataWriterError> {
        let mut partial_writes: Vec<(RecordBatch, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();

        for (key, batch) in self.divide_record_batch_by_partition_values(values)? {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => DataWriter::collect_partial_write_failure(
                    &mut partial_writes,
                    writer
                        .write_values(&self.partition_columns, arrow_schema.clone(), &batch)
                        .await,
                )?,
                None => {
                    let mut writer = DataArrowWriter::new(
                        arrow_schema.clone(),
                        self.writer_properties.clone(),
                    )?;

                    DataWriter::collect_partial_write_failure(
                        &mut partial_writes,
                        writer
                            .write_values(&self.partition_columns, self.arrow_schema(), &batch)
                            .await,
                    )?;

                    self.arrow_writers.insert(key, writer);
                }
            }
        }

        if !partial_writes.is_empty() {
            let sample = partial_writes.first().map(|t| t.to_owned());
            if let Some((_, e)) = sample {
                return Err(DataWriterError::PartialParquetWrite {
                    // TODO handle error with generic messages
                    skipped_values: vec![],
                    sample_error: e,
                });
            } else {
                unreachable!()
            }
        }

        Ok(())
    }

    /// Writes the given values to internal parquet buffers for each represented partition.
    pub async fn write(&mut self, values: Vec<RecordBatch>) -> Result<(), DataWriterError> {
        let mut partial_writes: Vec<(RecordBatch, ParquetError)> = Vec::new();
        let arrow_schema = self.arrow_schema();

        for (key, partition_values) in self.divide_by_partition_values(values)? {
            match self.arrow_writers.get_mut(&key) {
                Some(writer) => {
                    for batch in partition_values {
                        DataWriter::collect_partial_write_failure(
                            &mut partial_writes,
                            writer
                                .write_values(&self.partition_columns, arrow_schema.clone(), &batch)
                                .await,
                        )?
                    }
                }
                None => {
                    let mut writer = DataArrowWriter::new(
                        arrow_schema.clone(),
                        self.writer_properties.clone(),
                        // self.message_handler.clone(),
                    )?;
                    for batch in partition_values {
                        DataWriter::collect_partial_write_failure(
                            &mut partial_writes,
                            writer
                                .write_values(&self.partition_columns, self.arrow_schema(), &batch)
                                .await,
                        )?;
                    }
                    self.arrow_writers.insert(key, writer);
                }
            }
        }

        if !partial_writes.is_empty() {
            let sample = partial_writes.first().map(|t| t.to_owned());
            if let Some((_, e)) = sample {
                return Err(DataWriterError::PartialParquetWrite {
                    // TODO handle error with generic messages
                    skipped_values: vec![],
                    sample_error: e,
                });
            } else {
                unreachable!()
            }
        }

        Ok(())
    }

    /// Commit data written to buffers to delta table
    pub async fn commit(
        &mut self,
        table: &mut DeltaTable,
        operation: Option<DeltaOperation>,
        removals: Option<Vec<Remove>>,
    ) -> Result<DeltaDataTypeVersion, DataWriterError> {
        let mut adds = self.write_parquet_files(&table.table_uri).await?;
        let mut tx = table.create_transaction(None);
        tx.add_actions(adds.drain(..).map(Action::add).collect());
        if let Some(mut remove_actions) = removals {
            tx.add_actions(remove_actions.drain(..).map(Action::remove).collect());
        }
        let version = tx.commit(operation).await?;
        Ok(version)
    }

    /// Write and immediately commit data
    // TODO trying to pass the table and record batch stream leads to compiler errors.
    // There is a fix by simply wrapping the stream type, but it seems not worth it given the limited
    // gain in convenience...
    // https://github.com/rust-lang/rust/issues/63033#issuecomment-521234696
    // pub async fn write_and_commit(
    //     &mut self,
    //     table: &mut DeltaTable,
    //     values: SendableRecordBatchStream,
    //     // operation: Option<DeltaOperation>,
    // ) -> Result<DeltaDataTypeVersion, DataWriterError> {
    //     self.write(values).await?;
    //     // TODO find a way to handle operation, maybe set on writer?
    //     self.commit(table, None, None).await
    // }

    // TODO provide meaningful implementation
    fn collect_partial_write_failure(
        _partial_writes: &mut Vec<(RecordBatch, ParquetError)>,
        writer_result: Result<(), DataWriterError>,
    ) -> Result<(), DataWriterError> {
        match writer_result {
            Err(DataWriterError::PartialParquetWrite { .. }) => {
                // TODO handle templated type in Error definition
                // partial_writes.extend(skipped_values);
                Ok(())
            }
            other => other,
        }
    }

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    pub fn buffer_len(&self) -> usize {
        self.arrow_writers.values().map(|w| w.cursor.len()).sum()
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    pub async fn write_parquet_files(
        &mut self,
        table_uri: &str,
    ) -> Result<Vec<Add>, DataWriterError> {
        let writers = std::mem::take(&mut self.arrow_writers);
        let mut actions = Vec::new();

        for (_, mut writer) in writers {
            let metadata = writer.arrow_writer.close()?;

            let path = self.next_data_path(&self.partition_columns, &writer.partition_values)?;

            let obj_bytes = writer.cursor.data();
            let file_size = obj_bytes.len() as i64;

            let storage_path = self.storage.join_path(table_uri, path.as_str());

            //
            // TODO: Wrap in retry loop to handle temporary network errors
            //

            self.storage
                .put_obj(&storage_path, obj_bytes.as_slice())
                .await?;

            // Replace self null_counts with an empty map. Use the other for stats.
            let null_counts = std::mem::take(&mut writer.null_counts);

            actions.push(create_add(
                &writer.partition_values,
                null_counts,
                path,
                file_size,
                &metadata,
            )?);
        }
        Ok(actions)
    }

    /// Returns the number of records held in the current buffer.
    pub fn buffered_record_batch_count(&self) -> usize {
        self.arrow_writers
            .values()
            .map(|w| w.buffered_record_batch_count)
            .sum()
    }

    /// Resets internal state.
    pub fn reset(&mut self) {
        self.arrow_writers.clear();
    }

    /// Returns the arrow schema representation of the delta table schema defined for the wrapped
    /// table.
    pub fn arrow_schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.arrow_schema_ref.clone()
    }

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that
    // I have not been able to find documentation for yet.
    fn next_data_path(
        &self,
        partition_cols: &[String],
        partition_values: &HashMap<String, Option<String>>,
    ) -> Result<String, DataWriterError> {
        // TODO: what does 00000 mean?
        let first_part = "00000";
        let uuid_part = Uuid::new_v4();
        // TODO: what does c000 mean?
        let last_part = "c000";

        // NOTE: If we add a non-snappy option, file name must change
        let file_name = format!(
            "part-{}-{}-{}.snappy.parquet",
            first_part, uuid_part, last_part
        );

        let data_path = if !partition_cols.is_empty() {
            let mut path_parts = vec![];

            for k in partition_cols.iter() {
                let partition_value = partition_values
                    .get(k)
                    .ok_or_else(|| DataWriterError::MissingPartitionColumn(k.to_string()))?;

                let partition_value = partition_value
                    .as_deref()
                    .unwrap_or(NULL_PARTITION_VALUE_DATA_PATH);
                let part = format!("{}={}", k, partition_value);

                path_parts.push(part);
            }
            path_parts.push(file_name);
            path_parts.join("/")
        } else {
            file_name
        };

        Ok(data_path)
    }
}

fn create_add(
    partition_values: &HashMap<String, Option<String>>,
    null_counts: NullCounts,
    path: String,
    size: i64,
    file_metadata: &FileMetaData,
) -> Result<Add, DataWriterError> {
    let (min_values, max_values) =
        min_max_values_from_file_metadata(partition_values, file_metadata)?;

    let stats = Stats {
        num_records: file_metadata.num_rows,
        min_values,
        max_values,
        null_count: null_counts,
    };

    let stats_string = serde_json::to_string(&stats)
        .or(Err(DataWriterError::StatsSerializationFailed { stats }))?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
    let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let modification_time = modification_time.as_millis() as i64;

    Ok(Add {
        path,
        size,
        partition_values: partition_values.to_owned(),
        partition_values_parsed: None,
        modification_time,
        data_change: true,
        stats: Some(stats_string),
        stats_parsed: None,
        tags: None,
    })
}

fn min_max_values_from_file_metadata(
    partition_values: &HashMap<String, Option<String>>,
    file_metadata: &FileMetaData,
) -> Result<MinAndMaxValues, ParquetError> {
    let type_ptr = parquet::schema::types::from_thrift(file_metadata.schema.as_slice());
    let schema_descriptor = type_ptr.map(|type_| Arc::new(SchemaDescriptor::new(type_)))?;

    let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();

    let row_group_metadata: Result<Vec<RowGroupMetaData>, ParquetError> = file_metadata
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()))
        .collect();
    let row_group_metadata = row_group_metadata?;

    for i in 0..schema_descriptor.num_columns() {
        let column_descr = schema_descriptor.column(i);

        // If max rep level is > 0, this is an array element or a struct element of an array or something downstream of an array.
        // delta/databricks only computes null counts for arrays - not min max.
        // null counts are tracked at the record batch level, so skip any column with max_rep_level
        // > 0
        if column_descr.max_rep_level() > 0 {
            continue;
        }

        let column_path = column_descr.path();
        let column_path_parts = column_path.parts();

        // Do not include partition columns in statistics
        if partition_values.contains_key(&column_path_parts[0]) {
            continue;
        }

        let statistics: Vec<&Statistics> = row_group_metadata
            .iter()
            .filter_map(|g| g.column(i).statistics())
            .collect();

        let _ = apply_min_max_for_column(
            statistics.as_slice(),
            column_descr.clone(),
            column_path_parts,
            &mut min_values,
            &mut max_values,
        )?;
    }

    Ok((min_values, max_values))
}

fn apply_min_max_for_column(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
    column_path_parts: &[String],
    min_values: &mut HashMap<String, ColumnValueStat>,
    max_values: &mut HashMap<String, ColumnValueStat>,
) -> Result<(), ParquetError> {
    match (column_path_parts.len(), column_path_parts.first()) {
        // Base case - we are at the leaf struct level in the path
        (1, _) => {
            let (min, max) = min_and_max_from_parquet_statistics(statistics, column_descr.clone())?;

            if let Some(min) = min {
                let min = ColumnValueStat::Value(min);
                min_values.insert(column_descr.name().to_string(), min);
            }

            if let Some(max) = max {
                let max = ColumnValueStat::Value(max);
                max_values.insert(column_descr.name().to_string(), max);
            }

            Ok(())
        }
        // Recurse to load value at the appropriate level of HashMap
        (_, Some(key)) => {
            let child_min_values = min_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
            let child_max_values = max_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));

            match (child_min_values, child_max_values) {
                (ColumnValueStat::Column(mins), ColumnValueStat::Column(maxes)) => {
                    let remaining_parts: Vec<String> = column_path_parts
                        .iter()
                        .skip(1)
                        .map(|s| s.to_string())
                        .collect();

                    apply_min_max_for_column(
                        statistics,
                        column_descr,
                        remaining_parts.as_slice(),
                        mins,
                        maxes,
                    )?;

                    Ok(())
                }
                _ => {
                    unreachable!();
                }
            }
        }
        // column path parts will always have at least one element.
        (_, None) => {
            unreachable!();
        }
    }
}

#[inline]
fn is_utf8(opt: Option<LogicalType>) -> bool {
    matches!(opt.as_ref(), Some(LogicalType::STRING(_)))
}

fn min_and_max_from_parquet_statistics(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
) -> Result<(Option<Value>, Option<Value>), ParquetError> {
    let stats_with_min_max: Vec<&Statistics> = statistics
        .iter()
        .filter(|s| s.has_min_max_set())
        .copied()
        .collect();

    if stats_with_min_max.is_empty() {
        return Ok((None, None));
    }

    let (data_size, data_type) = match stats_with_min_max.first() {
        Some(Statistics::Boolean(_)) => (std::mem::size_of::<bool>(), DataType::Boolean),
        Some(Statistics::Int32(_)) => (std::mem::size_of::<i32>(), DataType::Int32),
        Some(Statistics::Int64(_)) => (std::mem::size_of::<i64>(), DataType::Int64),
        Some(Statistics::Float(_)) => (std::mem::size_of::<f32>(), DataType::Float32),
        Some(Statistics::Double(_)) => (std::mem::size_of::<f64>(), DataType::Float64),
        Some(Statistics::ByteArray(_)) if is_utf8(column_descr.logical_type()) => {
            (0, DataType::Utf8)
        }
        _ => {
            // NOTE: Skips
            // Statistics::Int96(_)
            // Statistics::ByteArray(_)
            // Statistics::FixedLenByteArray(_)

            return Ok((None, None));
        }
    };

    if data_type == DataType::Utf8 {
        return Ok(min_max_strings_from_stats(&stats_with_min_max));
    }

    let arrow_buffer_capacity = stats_with_min_max.len() * data_size;

    let min_array = arrow_array_from_bytes(
        data_type.clone(),
        arrow_buffer_capacity,
        stats_with_min_max.iter().map(|s| s.min_bytes()).collect(),
    );

    let max_array = arrow_array_from_bytes(
        data_type.clone(),
        arrow_buffer_capacity,
        stats_with_min_max.iter().map(|s| s.max_bytes()).collect(),
    );

    match data_type {
        DataType::Boolean => {
            let min = arrow::compute::min_boolean(as_boolean_array(&min_array));
            let min = min.map(Value::Bool);

            let max = arrow::compute::max_boolean(as_boolean_array(&max_array));
            let max = max.map(Value::Bool);

            Ok((min, max))
        }
        DataType::Int32 => {
            let min_array = as_primitive_array::<arrow::datatypes::Int32Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min.map(|i| Value::Number(Number::from(i)));

            let max_array = as_primitive_array::<arrow::datatypes::Int32Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.map(|i| Value::Number(Number::from(i)));

            Ok((min, max))
        }
        DataType::Int64 => {
            let min_array = as_primitive_array::<arrow::datatypes::Int64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let max_array = as_primitive_array::<arrow::datatypes::Int64Type>(&max_array);
            let max = arrow::compute::max(max_array);

            match column_descr.logical_type().as_ref() {
                Some(LogicalType::TIMESTAMP(TimestampType { unit, .. })) => {
                    let min = min.map(|n| Value::String(timestamp_to_delta_stats_string(n, unit)));
                    let max = max.map(|n| Value::String(timestamp_to_delta_stats_string(n, unit)));

                    Ok((min, max))
                }
                _ => {
                    let min = min.map(|i| Value::Number(Number::from(i)));
                    let max = max.map(|i| Value::Number(Number::from(i)));

                    Ok((min, max))
                }
            }
        }
        DataType::Float32 => {
            let min_array = as_primitive_array::<arrow::datatypes::Float32Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min
                .map(|f| Number::from_f64(f as f64).map(Value::Number))
                .flatten();

            let max_array = as_primitive_array::<arrow::datatypes::Float32Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max
                .map(|f| Number::from_f64(f as f64).map(Value::Number))
                .flatten();

            Ok((min, max))
        }
        DataType::Float64 => {
            let min_array = as_primitive_array::<arrow::datatypes::Float64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min
                .map(|f| Number::from_f64(f).map(Value::Number))
                .flatten();

            let max_array = as_primitive_array::<arrow::datatypes::Float64Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max
                .map(|f| Number::from_f64(f).map(Value::Number))
                .flatten();

            Ok((min, max))
        }
        _ => Ok((None, None)),
    }
}

fn min_max_strings_from_stats(
    stats_with_min_max: &[&Statistics],
) -> (Option<Value>, Option<Value>) {
    let min_string_candidates = stats_with_min_max
        .iter()
        .filter_map(|s| std::str::from_utf8(s.min_bytes()).ok());

    let min_value = min_string_candidates
        .min()
        .map(|s| Value::String(s.to_string()));

    let max_string_candidates = stats_with_min_max
        .iter()
        .filter_map(|s| std::str::from_utf8(s.max_bytes()).ok());

    let max_value = max_string_candidates
        .max()
        .map(|s| Value::String(s.to_string()));

    (min_value, max_value)
}

fn arrow_array_from_bytes(
    data_type: DataType,
    capacity: usize,
    byte_arrays: Vec<&[u8]>,
) -> Arc<dyn Array> {
    let mut buffer = MutableBuffer::new(capacity);

    for arr in byte_arrays.iter() {
        buffer.extend_from_slice(arr);
    }

    let builder = ArrayData::builder(data_type)
        .len(byte_arrays.len())
        .add_buffer(buffer.into());

    // TODO remove panic
    let data = builder.build().unwrap();

    make_array(data)
}

#[cfg(test)]
mod tests {
    use super::handlers::json::record_batch_from_message;
    use super::*;
    use crate::{
        action::{ColumnCountStat, ColumnValueStat},
        DeltaTable, DeltaTableError, SchemaDataType, SchemaField,
    };
    use lazy_static::lazy_static;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    #[test]
    fn convert_arrow_schema_to_delta() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("modified", DataType::Utf8, true),
        ]);

        let ref_schema = Schema::new(vec![
            SchemaField::new(
                "id".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "value".to_string(),
                SchemaDataType::primitive("integer".to_string()),
                true,
                HashMap::new(),
            ),
            SchemaField::new(
                "modified".to_string(),
                SchemaDataType::primitive("string".to_string()),
                true,
                HashMap::new(),
            ),
        ]);

        let schema = Schema::try_from(Arc::new(arrow_schema)).unwrap();

        assert_eq!(schema, ref_schema);
    }

    #[tokio::test]
    async fn delta_stats_test() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path();
        create_temp_table(table_path);

        let table = load_table(table_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap();

        // let message_handler = Arc::new(JsonHandler {});
        let mut writer = DataWriter::for_table(&table, HashMap::new()).unwrap();

        let arrow_schema = writer.arrow_schema();
        let batch = record_batch_from_message(arrow_schema, JSON_ROWS.clone().as_ref()).unwrap();

        writer.write(vec![batch]).await.unwrap();
        let add = writer.write_parquet_files(&table.table_uri).await.unwrap();
        assert_eq!(add.len(), 1);
        let stats = add[0].get_stats().unwrap().unwrap();

        let min_max_keys = vec!["meta", "some_int", "some_string", "some_bool"];
        let mut null_count_keys = vec!["some_list", "some_nested_list"];
        null_count_keys.extend_from_slice(min_max_keys.as_slice());

        assert_eq!(min_max_keys.len(), stats.min_values.len());
        assert_eq!(min_max_keys.len(), stats.max_values.len());
        assert_eq!(null_count_keys.len(), stats.null_count.len());

        // assert on min values
        for (k, v) in stats.min_values.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnValueStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(0, partition.as_i64().unwrap());

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!("2021-06-22", timestamp.as_str().unwrap());
                }
                ("some_int", ColumnValueStat::Value(v)) => assert_eq!(302, v.as_i64().unwrap()),
                ("some_bool", ColumnValueStat::Value(v)) => assert_eq!(false, v.as_bool().unwrap()),
                ("some_string", ColumnValueStat::Value(v)) => {
                    assert_eq!("GET", v.as_str().unwrap())
                }
                ("date", ColumnValueStat::Value(v)) => {
                    assert_eq!("2021-06-22", v.as_str().unwrap())
                }
                _ => assert!(false, "Key should not be present"),
            }
        }

        // assert on max values
        for (k, v) in stats.max_values.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnValueStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(1, partition.as_i64().unwrap());

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!("2021-06-22", timestamp.as_str().unwrap());
                }
                ("some_int", ColumnValueStat::Value(v)) => assert_eq!(400, v.as_i64().unwrap()),
                ("some_bool", ColumnValueStat::Value(v)) => assert_eq!(true, v.as_bool().unwrap()),
                ("some_string", ColumnValueStat::Value(v)) => {
                    assert_eq!("PUT", v.as_str().unwrap())
                }
                ("date", ColumnValueStat::Value(v)) => {
                    assert_eq!("2021-06-22", v.as_str().unwrap())
                }
                _ => assert!(false, "Key should not be present"),
            }
        }

        // assert on null count
        for (k, v) in stats.null_count.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnCountStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(0, partition);

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!(0, timestamp);
                }
                ("some_int", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_bool", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_string", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_list", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_nested_list", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                ("date", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                _ => assert!(false, "Key should not be present"),
            }
        }
    }

    async fn load_table(
        table_uri: &str,
        options: HashMap<String, String>,
    ) -> Result<DeltaTable, DeltaTableError> {
        let backend = crate::get_backend_for_uri_with_options(table_uri, options)?;
        let mut table = DeltaTable::new(
            table_uri,
            backend,
            crate::DeltaTableConfig {
                require_tombstones: true,
            },
        )?;
        table.load().await?;
        Ok(table)
    }

    fn create_temp_table(table_path: &Path) {
        let log_path = table_path.join("_delta_log");

        let _ = std::fs::create_dir(log_path.as_path()).unwrap();
        let _ = std::fs::write(
            log_path.join("00000000000000000000.json"),
            V0_COMMIT.as_str(),
        )
        .unwrap();
    }

    lazy_static! {
        static ref SCHEMA: Value = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "meta",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "kafka",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "topic",
                                            "type": "string",
                                            "nullable": true, "metadata": {}
                                        },
                                        {
                                            "name": "partition",
                                            "type": "integer",
                                            "nullable": true, "metadata": {}
                                        },
                                        {
                                            "name": "offset",
                                            "type": "long",
                                            "nullable": true, "metadata": {}
                                        }
                                    ],
                                },
                                "nullable": true, "metadata": {}
                            },
                            {
                                "name": "producer",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "timestamp",
                                            "type": "string",
                                            "nullable": true, "metadata": {}
                                        }
                                    ],
                                },
                                "nullable": true, "metadata": {}
                            }
                        ]
                    },
                    "nullable": true, "metadata": {}
                },
                { "name": "some_string", "type": "string", "nullable": true, "metadata": {} },
                { "name": "some_int", "type": "integer", "nullable": true, "metadata": {} },
                { "name": "some_bool", "type": "boolean", "nullable": true, "metadata": {} },
                {
                    "name": "some_list",
                    "type": {
                        "type": "array",
                        "elementType": "string",
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
                },
                {
                    "name": "some_nested_list",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "array",
                            "elementType": "integer",
                            "containsNull": true
                        },
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
               },
               { "name": "date", "type": "string", "nullable": true, "metadata": {} },
            ]
        });
        static ref V0_COMMIT: String = {
            let schema_string = serde_json::to_string(&SCHEMA.clone()).unwrap();
            let jsons = [
                json!({
                    "protocol":{"minReaderVersion":1,"minWriterVersion":2}
                }),
                json!({
                    "metaData": {
                        "id": "22ef18ba-191c-4c36-a606-3dad5cdf3830",
                        "format": {
                            "provider": "parquet", "options": {}
                        },
                        "schemaString": schema_string,
                        "partitionColumns": ["date"], "configuration": {}, "createdTime": 1564524294376i64
                    }
                }),
            ];

            jsons
                .iter()
                .map(|j| serde_json::to_string(j).unwrap())
                .collect::<Vec<String>>()
                .join("\n")
                .to_string()
        };
        static ref JSON_ROWS: Vec<Value> = {
            std::iter::repeat(json!({
                "meta": {
                    "kafka": {
                        "offset": 0,
                        "partition": 0,
                        "topic": "some_topic"
                    },
                    "producer": {
                        "timestamp": "2021-06-22"
                    },
                },
                "some_string": "GET",
                "some_int": 302,
                "some_bool": true,
                "some_list": ["a", "b", "c"],
                "some_nested_list": [[42], [84]],
                "date": "2021-06-22",
            }))
            .take(100)
            .chain(
                std::iter::repeat(json!({
                    "meta": {
                        "kafka": {
                            "offset": 100,
                            "partition": 1,
                            "topic": "another_topic"
                        },
                        "producer": {
                            "timestamp": "2021-06-22"
                        },
                    },
                    "some_string": "PUT",
                    "some_int": 400,
                    "some_bool": false,
                    "some_list": ["x", "y", "z"],
                    "some_nested_list": [[42], [84]],
                    "date": "2021-06-22",
                }))
                .take(100),
            )
            .chain(
                std::iter::repeat(json!({
                    "meta": {
                        "kafka": {
                            "offset": 0,
                            "partition": 0,
                            "topic": "some_topic"
                        },
                        "producer": {
                            "timestamp": "2021-06-22"
                        },
                    },
                    "some_nested_list": [[42], null],
                    "date": "2021-06-22",
                }))
                .take(100),
            )
            .collect()
        };
    }
}
