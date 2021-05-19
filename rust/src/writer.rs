//! The writer module contains the DeltaWriter and DeltaWriterError definitions and provides a
//! higher level API for performing Delta transaction writes to a given Delta Table.
//!
//! Unlike the transaction API on DeltaTable, this higher level writer will also write out the
//! parquet files

use crate::{
    action::{Add, ColumnCountStat, ColumnValueStat, Stats},
    DeltaDataTypeVersion, DeltaTable, DeltaTableError, DeltaTransactionError, Schema,
    StorageBackend, StorageError, UriError,
};
use arrow::{
    array::{as_boolean_array, as_primitive_array, make_array, Array, ArrayData},
    buffer::MutableBuffer,
    datatypes::Schema as ArrowSchema,
    datatypes::*,
    error::ArrowError,
    json::reader::Decoder,
    record_batch::RecordBatch,
};
use log::*;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, LogicalType},
    errors::ParquetError,
    file::{
        metadata::RowGroupMetaData, properties::WriterProperties, statistics::Statistics,
        writer::InMemoryWriteableCursor,
    },
    schema::types::{ColumnDescriptor, SchemaDescriptor},
};
use parquet_format::FileMetaData;
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/**
 * Errors which can be returned when attempting to write a parquet file or Delta transaction
 */
#[derive(thiserror::Error, Debug)]
pub enum DeltaWriterError {
    /// Missing partition column
    #[error("Missing partition column: {col_name}")]
    MissingPartitionColumn {
        /// The name of the column missing the partition
        col_name: String,
    },

    /// The Arrow RecordBatch schema doesn't match the expected schema
    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        /// TODO
        record_batch_schema: SchemaRef,
        /// TODO
        expected_schema: Arc<arrow::datatypes::Schema>,
    },

    /// Missing metadata
    #[error("{0}")]
    MissingMetadata(String),

    /// RecordBatch created from a JSON string turned out to be a None
    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,

    /// Deserialization of the Delta per-file statistics failed
    #[error("Serialization of delta log statistics failed")]
    StatsSerializationFailed {
        /// TODO
        stats: Stats,
    },

    /// Invalid table path
    #[error("Invalid table path: {}", .source)]
    UriError {
        /// The source UriError
        #[from]
        source: UriError,
    },

    /// Delta Lake storage interaction failed
    #[error("Storage interaction failed: {source}")]
    Storage {
        /// Source error
        #[from]
        source: StorageError,
    },

    /// DeltaTable interaction failed
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// The source error from the DeltaTable
        #[from]
        source: DeltaTableError,
    },

    /// Arrow interaction failed
    #[error("Arrow interaction failed: {source}")]
    Arrow {
        /// The source ArrowError from the arrow crate
        #[from]
        source: ArrowError,
    },

    /// Parquet write failed
    #[error("Parquet write failed: {source}")]
    Parquet {
        /// The source ParquetError from the parquet crate
        #[from]
        source: ParquetError,
    },

    /// Delta transaction commit failed
    #[error("Delta transaction commit failed: {source}")]
    DeltaTransactionError {
        /// The source error from the DeltaTable on writing the transaction
        #[from]
        source: DeltaTransactionError,
    },
}

/**
 * The DeltaWriter struct provides a high--level API for writing parquet files and their
 * transactions to the given DeltaTable
 */
pub struct DeltaWriter {
    /// The DeltaTable this writer is affiliated with
    table: DeltaTable,
    /// The DeltaTable's storage backend
    storage: Box<dyn StorageBackend>,
    /// TODD
    arrow_schema_ref: Arc<arrow::datatypes::Schema>,
    /// TODD
    writer_properties: WriterProperties,
    /// TODD
    cursor: InMemoryWriteableCursor,
    /// TODD
    arrow_writer: ArrowWriter<InMemoryWriteableCursor>,
    /// TODD
    partition_columns: Vec<String>,
    /// TODD
    partition_values: HashMap<String, String>,
    /// TODD
    buffered_record_batch_count: usize,
}

impl DeltaWriter {
    /// Initialize the writer from the given table path and delta schema
    pub async fn for_table_path(table_path: &str) -> Result<DeltaWriter, DeltaWriterError> {
        let table = crate::open_table(&table_path).await?;
        let storage = crate::get_backend_for_uri(table_path)?;

        // Initialize an arrow schema ref from the delta table schema
        let metadata = table.get_metadata()?.clone();
        let schema = metadata.schema.clone();
        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&schema).unwrap();
        let arrow_schema_ref = Arc::new(arrow_schema);
        let partition_columns = metadata.partition_columns;

        // Initialize writer properties for the underlying arrow writer
        let writer_properties = WriterProperties::builder()
            // NOTE: Consider extracting config for writer properties and setting more than just compression
            .set_compression(Compression::SNAPPY)
            .build();

        let cursor = InMemoryWriteableCursor::default();
        let arrow_writer = ArrowWriter::try_new(
            cursor.clone(),
            arrow_schema_ref.clone(),
            Some(writer_properties.clone()),
        )?;

        Ok(Self {
            table,
            storage,
            arrow_schema_ref,
            writer_properties,
            cursor,
            arrow_writer,
            partition_columns,
            partition_values: HashMap::new(),
            buffered_record_batch_count: 0,
        })
    }

    /// TODO
    pub async fn last_transaction_version(
        &self,
        app_id: &str,
    ) -> Result<Option<DeltaDataTypeVersion>, DeltaWriterError> {
        let tx_versions = self.table.get_app_transaction_version();

        let v = tx_versions.get(app_id).map(|v| v.to_owned());

        debug!("Transaction version is {:?}", v);

        Ok(v)
    }

    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many record batches and flushed after the appropriate number of bytes has been written.
    pub async fn write_record_batch(
        &mut self,
        record_batch: &RecordBatch,
    ) -> Result<(), DeltaWriterError> {
        let partition_values = extract_partition_values(&self.partition_columns, record_batch)?;

        // Verify record batch schema matches the expected schema
        let schemas_match = record_batch.schema() == self.arrow_schema_ref;

        if !schemas_match {
            return Err(DeltaWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: self.arrow_schema_ref.clone(),
            });
        }

        // TODO: Verify that this RecordBatch contains partition values that agree with previous partition values (if any) for the current writer context
        self.partition_values = partition_values;

        // write the record batch to the held arrow writer
        self.arrow_writer.write(record_batch)?;

        self.buffered_record_batch_count += 1;

        Ok(())
    }

    /// Returns the current byte length of the in memory buffer.
    /// This may be used by the caller to decide when to finalize the file write.
    pub fn buffer_len(&self) -> usize {
        self.cursor.data().len()
    }

    /// Writes the existing parquet bytes to storage and resets internal state to handle another file.
    pub async fn write_file(&mut self) -> Result<DeltaDataTypeVersion, DeltaWriterError> {
        debug!("Writing parquet file.");
        let obj_bytes = self.cursor.data();

        // TODO: support partitions on the write
        let version = self.table.add_file(&obj_bytes, None, None).await?;

        // After data is written, re-initialize internal state to handle another file
        self.reset()?;
        Ok(version)
    }

    /// TODO
    pub fn buffered_record_batch_count(&self) -> usize {
        self.buffered_record_batch_count
    }

    fn reset(&mut self) -> Result<(), DeltaWriterError> {
        // Reset the internal cursor for the next file.
        self.cursor = InMemoryWriteableCursor::default();
        // Reset buffered record batch count to 0.
        self.buffered_record_batch_count = 0;
        // Reset the internal arrow writer for the next file.
        self.arrow_writer = ArrowWriter::try_new(
            self.cursor.clone(),
            self.arrow_schema_ref.clone(),
            Some(self.writer_properties.clone()),
        )?;

        Ok(())
    }

    // TODO: parquet files have a 5 digit zero-padded prefix and a "c\d{3}" suffix that I have not been able to find documentation for yet.
    fn next_data_path(
        &self,
        partition_cols: &Vec<String>,
        partition_values: &HashMap<String, String>,
    ) -> Result<String, DeltaWriterError> {
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

        let data_path = if partition_cols.len() > 0 {
            let mut path_parts = vec![];

            for k in partition_cols.iter() {
                let partition_value =
                    partition_values
                        .get(k)
                        .ok_or(DeltaWriterError::MissingPartitionColumn {
                            col_name: k.to_string(),
                        })?;

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

    /// TODO
    pub fn arrow_schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.arrow_schema_ref.clone()
    }
}

/// TODD
fn delta_stats_from_file_metadata(file_metadata: &FileMetaData) -> Result<Stats, ParquetError> {
    let type_ptr = parquet::schema::types::from_thrift(file_metadata.schema.as_slice());
    let schema_descriptor = type_ptr.map(|type_| Arc::new(SchemaDescriptor::new(type_)))?;

    let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut null_counts: HashMap<String, ColumnCountStat> = HashMap::new();

    let row_group_metadata: Result<Vec<RowGroupMetaData>, ParquetError> = file_metadata
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()))
        .collect();
    let row_group_metadata = row_group_metadata?;

    for i in 0..schema_descriptor.num_columns() {
        let column_descr = schema_descriptor.column(i);
        let column_path = column_descr.path();

        // TODO: skip stats for lists and structs for now as these are missing in parquet crate column chunks
        if column_path.parts().len() > 1 {
            continue;
        }

        let statistics: Vec<&Statistics> = row_group_metadata
            .iter()
            .filter_map(|g| g.column(i).statistics())
            .collect();

        let (min, max) = stats_min_and_max(&statistics, column_descr.clone())?;

        if let Some(min) = min {
            let min = ColumnValueStat::Value(min);
            min_values.insert(column_descr.name().to_string(), min);
        }

        if let Some(max) = max {
            let max = ColumnValueStat::Value(max);
            max_values.insert(column_descr.name().to_string(), max);
        }

        let null_count: u64 = statistics.iter().map(|s| s.null_count()).sum();
        let null_count = ColumnCountStat::Value(null_count as i64);
        null_counts.insert(column_descr.name().to_string(), null_count);
    }

    Ok(Stats {
        numRecords: file_metadata.num_rows,
        maxValues: max_values,
        minValues: min_values,
        nullCount: null_counts,
    })
}

/// TODD
fn stats_min_and_max(
    statistics: &[&Statistics],
    column_descr: Arc<ColumnDescriptor>,
) -> Result<(Option<Value>, Option<Value>), ParquetError> {
    let stats_with_min_max: Vec<&Statistics> = statistics
        .iter()
        .filter(|s| s.has_min_max_set())
        .map(|s| *s)
        .collect();

    if stats_with_min_max.len() == 0 {
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
            let min = min.map(|b| Value::Bool(b));

            let max = arrow::compute::max_boolean(as_boolean_array(&max_array));
            let max = max.map(|b| Value::Bool(b));

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
            let min = min.map(|i| Value::Number(Number::from(i)));

            let max_array = as_primitive_array::<arrow::datatypes::Int64Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max.map(|i| Value::Number(Number::from(i)));

            Ok((min, max))
        }
        DataType::Float32 => {
            let min_array = as_primitive_array::<arrow::datatypes::Float32Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min
                .map(|f| Number::from_f64(f as f64).map(|n| Value::Number(n)))
                .flatten();

            let max_array = as_primitive_array::<arrow::datatypes::Float32Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max
                .map(|f| Number::from_f64(f as f64).map(|n| Value::Number(n)))
                .flatten();

            Ok((min, max))
        }
        DataType::Float64 => {
            let min_array = as_primitive_array::<arrow::datatypes::Float64Type>(&min_array);
            let min = arrow::compute::min(min_array);
            let min = min
                .map(|f| Number::from_f64(f).map(|n| Value::Number(n)))
                .flatten();

            let max_array = as_primitive_array::<arrow::datatypes::Float64Type>(&max_array);
            let max = arrow::compute::max(max_array);
            let max = max
                .map(|f| Number::from_f64(f).map(|n| Value::Number(n)))
                .flatten();

            Ok((min, max))
        }
        _ => Ok((None, None)),
    }
}

/// TODD
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

    let data = builder.build();

    make_array(data)
}

/// TODD
fn min_max_strings_from_stats(
    stats_with_min_max: &Vec<&Statistics>,
) -> (Option<Value>, Option<Value>) {
    let min_string_candidates = stats_with_min_max
        .iter()
        .filter_map(|s| str_opt_from_bytes(s.min_bytes()));

    let min_value = min_string_candidates
        .min()
        .map(|s| Value::String(s.to_string()));

    let max_string_candidates = stats_with_min_max
        .iter()
        .filter_map(|s| str_opt_from_bytes(s.max_bytes()));

    let max_value = max_string_candidates
        .max()
        .map(|s| Value::String(s.to_string()));

    return (min_value, max_value);
}

/// TODD
#[inline]
fn is_utf8(opt: Option<LogicalType>) -> bool {
    match opt.as_ref() {
        Some(LogicalType::STRING(_)) => true,
        _ => false,
    }
}

/// TODD
fn str_opt_from_bytes(bytes: &[u8]) -> Option<&str> {
    std::str::from_utf8(bytes).ok()
}

/// TODD
struct InMemValueIter<'a> {
    /// TODD
    buffer: &'a [Value],
    /// TODD
    current_index: usize,
}

impl<'a> InMemValueIter<'a> {
    fn from_vec(v: &'a [Value]) -> Self {
        Self {
            buffer: v,
            current_index: 0,
        }
    }
}

impl<'a> Iterator for InMemValueIter<'a> {
    type Item = Result<Value, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.buffer.get(self.current_index);

        self.current_index += 1;

        item.map(|v| Ok(v.to_owned()))
    }
}

/// Creates an Arrow RecordBatch from the passed JSON buffer.
pub fn record_batch_from_json(
    arrow_schema_ref: Arc<ArrowSchema>,
    json_buffer: &[Value],
) -> Result<RecordBatch, DeltaWriterError> {
    let row_count = json_buffer.len();
    let mut value_iter = InMemValueIter::from_vec(json_buffer);
    let decoder = Decoder::new(arrow_schema_ref.clone(), row_count, None);

    decoder
        .next_batch(&mut value_iter)?
        .ok_or(DeltaWriterError::EmptyRecordBatch)
}

fn extract_partition_values(
    partition_cols: &Vec<String>,
    record_batch: &RecordBatch,
) -> Result<HashMap<String, String>, DeltaWriterError> {
    let mut partition_values = HashMap::new();

    for col_name in partition_cols.iter() {
        let arrow_schema = record_batch.schema();

        let i = arrow_schema.index_of(col_name)?;
        let col = record_batch.column(i);

        let partition_string = stringified_partition_value(col)?;

        partition_values.insert(col_name.clone(), partition_string);
    }

    Ok(partition_values)
}

fn create_add(
    partition_values: &HashMap<String, String>,
    path: String,
    size: i64,
    file_metadata: &FileMetaData,
) -> Result<Add, DeltaWriterError> {
    let stats = delta_stats_from_file_metadata(file_metadata)?;
    let stats_string = serde_json::to_string(&stats)
        .or(Err(DeltaWriterError::StatsSerializationFailed { stats }))?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
    let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let modification_time = modification_time.as_millis() as i64;

    Ok(Add {
        path,
        size,

        partitionValues: partition_values.to_owned(),
        partitionValues_parsed: None,

        modificationTime: modification_time,
        dataChange: true,

        stats: Some(stats_string),
        stats_parsed: None,
        // ?
        tags: None,
    })
}

// very naive implementation for plucking the partition value from the first element of a column array.
// ideally, we would do some validation to ensure the record batch containing the passed partition column contains only distinct values.
// if we calculate stats _first_, we can avoid the extra iteration by ensuring max and min match for the column.
// however, stats are optional and can be added later with `dataChange` false log entries, and it may be more appropriate to add stats _later_ to speed up the initial write.
// a happy middle-road might be to compute stats for partition columns only on the initial write since we should validate partition values anyway, and compute additional stats later (at checkpoint time perhaps?).
// also this does not currently support nested partition columns and many other data types.
fn stringified_partition_value(arr: &Arc<dyn Array>) -> Result<String, DeltaWriterError> {
    let data_type = arr.data_type();

    let s = match data_type {
        DataType::Int8 => as_primitive_array::<Int8Type>(arr).value(0).to_string(),
        DataType::Int16 => as_primitive_array::<Int16Type>(arr).value(0).to_string(),
        DataType::Int32 => as_primitive_array::<Int32Type>(arr).value(0).to_string(),
        DataType::Int64 => as_primitive_array::<Int64Type>(arr).value(0).to_string(),
        DataType::UInt8 => as_primitive_array::<UInt8Type>(arr).value(0).to_string(),
        DataType::UInt16 => as_primitive_array::<UInt16Type>(arr).value(0).to_string(),
        DataType::UInt32 => as_primitive_array::<UInt32Type>(arr).value(0).to_string(),
        DataType::UInt64 => as_primitive_array::<UInt64Type>(arr).value(0).to_string(),
        DataType::Utf8 => {
            let data = arrow::array::as_string_array(arr);

            data.value(0).to_string()
        }
        // TODO: handle more types
        _ => {
            unimplemented!("Unimplemented data type: {:?}", data_type);
        }
    };

    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_nonexistent_table() {
        let res = DeltaWriter::for_table_path("/nonexistent/table").await;
        assert!(res.is_err());
        if let Err(err) = res {
            match err {
                DeltaWriterError::DeltaTable { source: _ } => {}
                other => {
                    assert!(false, "Expected a DeltaTable error, got `{}`", other);
                }
            }
        }
    }
}
