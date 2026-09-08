//! Splits data batches into per-partition batches

use crate::kernel::scalars::ScalarExt;
use crate::writer::DeltaWriterError;
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_ord::partition::partition;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use arrow_select::take::take;
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;

/// Helper container for partitioned record batches
#[derive(Clone, Debug)]
pub struct PartitionResult {
    /// values found in partition columns
    pub partition_values: IndexMap<String, Scalar>,
    /// remaining dataset with partition column values removed
    pub record_batch: RecordBatch,
}

/// Partition a RecordBatch along partition columns
pub(crate) fn divide_by_partition_values(
    arrow_schema: ArrowSchemaRef,
    partition_columns: &[String],
    values: &RecordBatch,
) -> Result<Vec<PartitionResult>, DeltaWriterError> {
    let mut partitions = Vec::new();

    if partition_columns.is_empty() {
        partitions.push(PartitionResult {
            partition_values: IndexMap::new(),
            record_batch: values.clone(),
        });
        return Ok(partitions);
    }

    let schema = values.schema();

    let projection = partition_columns
        .iter()
        .map(|n| Ok(schema.index_of(n)?))
        .collect::<Result<Vec<_>, DeltaWriterError>>()?;
    let sort_columns = values.project(&projection)?;

    let indices = lexsort_to_indices(sort_columns.columns());
    let sorted_partition_columns = partition_columns
        .iter()
        .map(|c| Ok(take(values.column(schema.index_of(c)?), &indices, None)?))
        .collect::<Result<Vec<_>, DeltaWriterError>>()?;

    let partition_ranges = partition(sorted_partition_columns.as_slice())?;

    for range in partition_ranges.ranges().into_iter() {
        // get row indices for current partition
        let idx: UInt32Array = (range.start..range.end)
            .map(|i| Some(indices.value(i)))
            .collect();

        let partition_key_iter = sorted_partition_columns
            .iter()
            .map(|col| {
                Scalar::from_array(&col.slice(range.start, range.end - range.start), 0).ok_or(
                    DeltaWriterError::MissingPartitionColumn("failed to parse".into()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let partition_values = partition_columns
            .iter()
            .cloned()
            .zip(partition_key_iter)
            .collect();
        let batch_data = arrow_schema
            .fields()
            .iter()
            .map(|f| Ok(values.column(schema.index_of(f.name())?).clone()))
            .map(move |col: Result<_, ArrowError>| take(col?.as_ref(), &idx, None))
            .collect::<Result<Vec<_>, _>>()?;

        partitions.push(PartitionResult {
            partition_values,
            record_batch: RecordBatch::try_new(arrow_schema.clone(), batch_data)?,
        });
    }

    Ok(partitions)
}

fn lexsort_to_indices(arrays: &[ArrayRef]) -> UInt32Array {
    let fields = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(arrays).unwrap();
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    sort.sort_unstable_by_key(|(_, a)| *a);
    UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::kernel::PartitionsExt;
    use crate::writer::test_utils::{create_initialized_table, get_record_batch};
    use crate::writer::utils::arrow_schema_without_partitions;
    use crate::writer::{DeltaWriterError, RecordBatchWriter};
    use arrow_array::RecordBatch;
    use arrow_json::ReaderBuilder;
    use arrow_schema::Schema as ArrowSchema;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::schema::StructType;
    use std::sync::Arc;

    /// Partition a record batch through the writer's schema/partition columns
    fn divide_writer_batch(
        writer: &RecordBatchWriter,
        partition_cols: &[String],
        values: &RecordBatch,
    ) -> Result<Vec<PartitionResult>, DeltaWriterError> {
        divide_by_partition_values(
            arrow_schema_without_partitions(&writer.arrow_schema(), partition_cols),
            partition_cols,
            values,
        )
    }

    fn validate_partition_map(partitions: Vec<PartitionResult>, expected_keys: Vec<String>) {
        assert_eq!(partitions.len(), expected_keys.len());
        for result in partitions {
            let partition_key = result.partition_values.hive_partition_path();
            assert!(expected_keys.contains(&partition_key));
            let ref_batch = get_record_batch(Some(partition_key.clone()), false);
            assert_eq!(ref_batch, result.record_batch);
        }
    }

    #[tokio::test]
    async fn test_divide_record_batch_no_partition() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let batch = get_record_batch(None, false);
        let partition_cols = vec![];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = divide_writer_batch(&writer, &partition_cols, &batch).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].record_batch, batch)
    }

    #[tokio::test]
    async fn test_divide_record_batch_single_partition() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();

        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string()];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = divide_writer_batch(&writer, &partition_cols, &batch).unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01"),
            String::from("modified=2021-02-02"),
        ];
        validate_partition_map(partitions, expected_keys)
    }

    /*
     * This test is a little messy but demonstrates a bug when
     * trying to write data to a Delta Table that has a map column and partition columns
     *
     * For readability the schema and data for the write are defined in JSON
     */
    #[tokio::test]
    async fn test_divide_record_batch_with_map_single_partition() {
        let table = crate::writer::test_utils::create_bare_table();
        let partition_cols = ["modified".to_string()];
        let delta_schema = r#"
        {"type" : "struct",
        "fields" : [
            {"name" : "id", "type" : "string", "nullable" : false, "metadata" : {}},
            {"name" : "value", "type" : "integer", "nullable" : false, "metadata" : {}},
            {"name" : "modified", "type" : "string", "nullable" : false, "metadata" : {}},
            {"name" : "metadata", "type" :
                {"type" : "map", "keyType" : "string", "valueType" : "string", "valueContainsNull" : true},
                "nullable" : false, "metadata" : {}}
            ]
        }"#;

        let delta_schema: StructType =
            serde_json::from_str(delta_schema).expect("Failed to parse schema");

        let table = table
            .create()
            .with_partition_columns(partition_cols.to_vec())
            .with_columns(delta_schema.fields().cloned())
            .await
            .unwrap();

        let buf = r#"
            {"id" : "0xdeadbeef", "value" : 42, "modified" : "2021-02-01",
                "metadata" : {"some-key" : "some-value"}}
            {"id" : "0xdeadcaf", "value" : 3, "modified" : "2021-02-02",
                "metadata" : {"some-key" : "some-value"}}"#
            .as_bytes();

        let schema: ArrowSchema = (&delta_schema).try_into_arrow().unwrap();

        // Using a batch size of two since the buf above only has two records
        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(2)
            .build_decoder()
            .expect("Failed to build decoder");

        decoder
            .decode(buf)
            .expect("Failed to deserialize the JSON in the buffer");
        let batch = decoder.flush().expect("Failed to flush").unwrap();

        let writer = RecordBatchWriter::for_table(&table).unwrap();
        let partitions = divide_writer_batch(&writer, &partition_cols, &batch).unwrap();

        let expected_keys = [
            String::from("modified=2021-02-01"),
            String::from("modified=2021-02-02"),
        ];

        assert_eq!(partitions.len(), expected_keys.len());
        for result in partitions {
            let partition_key = result.partition_values.hive_partition_path();
            assert!(expected_keys.contains(&partition_key));
        }
    }

    #[tokio::test]
    async fn test_divide_record_batch_multiple_partitions() {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path().to_str().unwrap();
        let batch = get_record_batch(None, false);
        let partition_cols = vec!["modified".to_string(), "id".to_string()];
        let table = create_initialized_table(table_path, &partition_cols).await;
        let writer = RecordBatchWriter::for_table(&table).unwrap();

        let partitions = divide_writer_batch(&writer, &partition_cols, &batch).unwrap();

        let expected_keys = vec![
            String::from("modified=2021-02-01/id=A"),
            String::from("modified=2021-02-01/id=B"),
            String::from("modified=2021-02-02/id=A"),
            String::from("modified=2021-02-02/id=B"),
        ];
        validate_partition_map(partitions, expected_keys)
    }
}
