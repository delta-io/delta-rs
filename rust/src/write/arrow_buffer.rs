//! Arrow writers for writing arrow partitions
#![allow(clippy::type_complexity)]
use super::{DataWriterError, MessageHandler};
use crate::{action::ColumnCountStat, DeltaDataTypeLong};
use arrow::{
    array::{as_primitive_array, as_struct_array, Array, StructArray},
    datatypes::Schema as ArrowSchema,
    datatypes::*,
    record_batch::*,
};
use log::{info, warn};
use parquet::{
    arrow::ArrowWriter,
    errors::ParquetError,
    file::{properties::WriterProperties, writer::InMemoryWriteableCursor},
};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

type NullCounts = HashMap<String, ColumnCountStat>;

pub(super) struct DataArrowWriter<T: Clone> {
    message_handler: Arc<dyn MessageHandler<T>>,
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    pub(super) cursor: InMemoryWriteableCursor,
    pub(super) arrow_writer: ArrowWriter<InMemoryWriteableCursor>,
    pub(super) partition_values: HashMap<String, Option<String>>,
    pub(super) null_counts: NullCounts,
    pub(super) buffered_record_batch_count: usize,
}

impl<T: Clone> DataArrowWriter<T> {
    pub fn new(
        arrow_schema: Arc<ArrowSchema>,
        writer_properties: WriterProperties,
        message_handler: Arc<dyn MessageHandler<T>>,
    ) -> Result<Self, ParquetError> {
        let cursor = InMemoryWriteableCursor::default();
        let arrow_writer = Self::new_underlying_writer(
            cursor.clone(),
            arrow_schema.clone(),
            writer_properties.clone(),
        )?;

        let partition_values = HashMap::new();
        let null_counts = NullCounts::new();
        let buffered_record_batch_count = 0;

        Ok(Self {
            message_handler,
            arrow_schema,
            writer_properties,
            cursor,
            arrow_writer,
            partition_values,
            null_counts,
            buffered_record_batch_count,
        })
    }

    /// Writes the given message buffer and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many
    /// buffers and flushed after the appropriate number of bytes has been written.
    pub async fn write_values(
        &mut self,
        partition_columns: &[String],
        arrow_schema: Arc<ArrowSchema>,
        message_buffer: Vec<T>,
    ) -> Result<(), DataWriterError> {
        let record_batch = self
            .message_handler
            .record_batch_from_message(arrow_schema.clone(), message_buffer.as_slice())?;

        if record_batch.schema() != arrow_schema {
            return Err(DataWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: arrow_schema,
            });
        }

        let result = self
            .write_record_batch(partition_columns, record_batch)
            .await;

        if let Err(DataWriterError::Parquet { source }) = result {
            self.write_partial(partition_columns, arrow_schema, message_buffer, source)
                .await
        } else {
            result
        }
    }

    fn quarantine_failed_parquet_rows(
        &self,
        arrow_schema: Arc<ArrowSchema>,
        values: Vec<T>,
    ) -> Result<(Vec<T>, Vec<(T, ParquetError)>), DataWriterError> {
        let mut good: Vec<T> = Vec::new();
        let mut bad: Vec<(T, ParquetError)> = Vec::new();

        for value in values {
            let record_batch = self
                .message_handler
                .record_batch_from_message(arrow_schema.clone(), &[value.clone()])?;

            let cursor = InMemoryWriteableCursor::default();
            let mut writer = ArrowWriter::try_new(cursor.clone(), arrow_schema.clone(), None)?;

            match writer.write(&record_batch) {
                Ok(_) => good.push(value),
                Err(e) => bad.push((value, e)),
            }
        }

        Ok((good, bad))
    }

    async fn write_partial(
        &mut self,
        partition_columns: &[String],
        arrow_schema: Arc<ArrowSchema>,
        message_buffer: Vec<T>,
        parquet_error: ParquetError,
    ) -> Result<(), DataWriterError> {
        warn!("Failed with parquet error while writing record batch. Attempting quarantine of bad records.");
        let (good, bad) =
            self.quarantine_failed_parquet_rows(arrow_schema.clone(), message_buffer)?;
        let record_batch = self
            .message_handler
            .record_batch_from_message(arrow_schema, good.as_slice())?;
        self.write_record_batch(partition_columns, record_batch)
            .await?;
        info!(
            "Wrote {} good records to record batch and quarantined {} bad records.",
            good.len(),
            bad.len()
        );
        Err(DataWriterError::PartialParquetWrite {
            // TODO handle generic type in Error definition
            // skipped_values: bad,
            skipped_values: vec![],
            sample_error: parquet_error,
        })
    }

    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many
    /// record batches and flushed after the appropriate number of bytes has been written.
    async fn write_record_batch(
        &mut self,
        partition_columns: &[String],
        record_batch: RecordBatch,
    ) -> Result<(), DataWriterError> {
        if record_batch.schema() != self.arrow_schema {
            return Err(DataWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: self.arrow_schema.clone(),
            });
        }

        if !partition_columns.is_empty() && self.partition_values.is_empty() {
            let partition_values = extract_partition_values(partition_columns, &record_batch)?;
            self.partition_values = partition_values;
        }

        // Copy current cursor bytes so we can recover from failures
        let current_cursor_bytes = self.cursor.data();

        let result = self.arrow_writer.write(&record_batch);

        match result {
            Ok(_) => {
                self.buffered_record_batch_count += 1;

                apply_null_counts(
                    partition_columns,
                    &record_batch.into(),
                    &mut self.null_counts,
                    0,
                );
                Ok(())
            }
            // If a write fails we need to reset the state of the DeltaArrowWriter
            Err(e) => {
                let new_cursor = Self::cursor_from_bytes(current_cursor_bytes.as_slice())?;
                let _ = std::mem::replace(&mut self.cursor, new_cursor.clone());
                let arrow_writer = Self::new_underlying_writer(
                    new_cursor,
                    self.arrow_schema.clone(),
                    self.writer_properties.clone(),
                )?;
                let _ = std::mem::replace(&mut self.arrow_writer, arrow_writer);
                self.partition_values.clear();

                Err(e.into())
            }
        }
    }

    fn cursor_from_bytes(bytes: &[u8]) -> Result<InMemoryWriteableCursor, std::io::Error> {
        let mut cursor = InMemoryWriteableCursor::default();
        cursor.write_all(bytes)?;
        Ok(cursor)
    }

    fn new_underlying_writer(
        cursor: InMemoryWriteableCursor,
        arrow_schema: Arc<ArrowSchema>,
        writer_properties: WriterProperties,
    ) -> Result<ArrowWriter<InMemoryWriteableCursor>, ParquetError> {
        ArrowWriter::try_new(cursor, arrow_schema, Some(writer_properties))
    }
}

fn extract_partition_values(
    partition_cols: &[String],
    record_batch: &RecordBatch,
) -> Result<HashMap<String, Option<String>>, DataWriterError> {
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

// very naive implementation for plucking the partition value from the first element of a column array.
// ideally, we would do some validation to ensure the record batch containing the passed partition column contains only distinct values.
// if we calculate stats _first_, we can avoid the extra iteration by ensuring max and min match for the column.
// however, stats are optional and can be added later with `dataChange` false log entries, and it may be more appropriate to add stats _later_ to speed up the initial write.
// a happy middle-road might be to compute stats for partition columns only on the initial write since we should validate partition values anyway, and compute additional stats later (at checkpoint time perhaps?).
// also this does not currently support nested partition columns and many other data types.
fn stringified_partition_value(arr: &Arc<dyn Array>) -> Result<Option<String>, DataWriterError> {
    let data_type = arr.data_type();

    if arr.is_null(0) {
        return Ok(None);
    }

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

    Ok(Some(s))
}

fn apply_null_counts(
    partition_columns: &[String],
    array: &StructArray,
    null_counts: &mut HashMap<String, ColumnCountStat>,
    nest_level: i32,
) {
    let fields = match array.data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!(),
    };

    array
        .columns()
        .iter()
        .zip(fields)
        .for_each(|(column, field)| {
            let key = field.name().to_owned();

            // Do not include partition columns in statistics
            if nest_level == 0 && partition_columns.contains(&key) {
                return;
            }

            match column.data_type() {
                // Recursive case
                DataType::Struct(_) => {
                    let col_struct = null_counts
                        .entry(key)
                        .or_insert_with(|| ColumnCountStat::Column(HashMap::new()));

                    match col_struct {
                        ColumnCountStat::Column(map) => {
                            apply_null_counts(
                                partition_columns,
                                as_struct_array(column),
                                map,
                                nest_level + 1,
                            );
                        }
                        _ => unreachable!(),
                    }
                }
                // Base case
                _ => {
                    let col_struct = null_counts
                        .entry(key.clone())
                        .or_insert_with(|| ColumnCountStat::Value(0));

                    match col_struct {
                        ColumnCountStat::Value(n) => {
                            let null_count = column.null_count() as DeltaDataTypeLong;
                            let n = null_count + *n;
                            null_counts.insert(key, ColumnCountStat::Value(n));
                        }
                        _ => unreachable!(),
                    }
                }
            }
        });
}
