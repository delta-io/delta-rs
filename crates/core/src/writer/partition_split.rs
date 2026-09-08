//! Splits data batches into per-partition batches

use crate::kernel::scalars::ScalarExt;
use crate::writer::DeltaWriterError;
use crate::{DeltaResult, DeltaTableError};
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_ord::partition::partition;
use arrow_row::{Row, RowConverter, SortField};
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use arrow_select::take::take;
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;

/// Helper container for partitioned record batches
#[derive(Clone, Debug)]
pub(crate) struct PartitionResult {
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
    if values.num_rows() == 0 {
        return Ok(Vec::new());
    }

    if partition_columns.is_empty() {
        return Ok(vec![PartitionResult {
            partition_values: IndexMap::new(),
            record_batch: values.clone(),
        }]);
    }

    let schema = values.schema();

    let projection = partition_columns
        .iter()
        .map(|n| schema.index_of(n))
        .collect::<Result<Vec<_>, ArrowError>>()
        .map_err(|e| {
            DeltaTableError::generic(format!("partition column missing from batch: {e}"))
        })?;
    let sort_columns = values.project(&projection).map_err(|e| {
        DeltaTableError::generic(format!("failed to project partition columns: {e}"))
    })?;

    let indices = group_by_partition_key(sort_columns.columns())?;

    let sorted_partition_columns = partition_columns
        .iter()
        .map(|c| {
            let idx = schema
                .index_of(c)
                .map_err(|e| DeltaTableError::generic(format!("partition column missing: {e}")))?;
            take(values.column(idx), &indices, None).map_err(|e| {
                DeltaTableError::generic(format!("failed to take partition column: {e}"))
            })
        })
        .collect::<DeltaResult<Vec<ArrayRef>>>()?;

    let partition_ranges = partition(sorted_partition_columns.as_slice()).map_err(|e| {
        DeltaTableError::generic(format!("failed to compute partition ranges: {e}"))
    })?;

    let mut partitions = Vec::new();
    for range in partition_ranges.ranges() {
        // Row indices of the original batch that fall into this partition.
        let idx: UInt32Array = (range.start..range.end)
            .map(|i| Some(indices.value(i)))
            .collect();

        let partition_key_iter = sorted_partition_columns
            .iter()
            .map(|col| {
                Scalar::from_array(&col.slice(range.start, range.end - range.start), 0).ok_or_else(
                    || DeltaTableError::generic("failed to read partition column value as Scalar"),
                )
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let partition_values: IndexMap<String, Scalar> = partition_columns
            .iter()
            .cloned()
            .zip(partition_key_iter)
            .collect();

        let batch_data = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let col_idx = schema.index_of(f.name()).map_err(|e| {
                    DeltaTableError::generic(format!("output column missing from batch: {e}"))
                })?;
                take(values.column(col_idx).as_ref(), &idx, None).map_err(|e| {
                    DeltaTableError::generic(format!("failed to take data column: {e}"))
                })
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let record_batch =
            RecordBatch::try_new(Arc::clone(&arrow_schema), batch_data).map_err(|e| {
                DeltaTableError::generic(format!("failed to build partitioned record batch: {e}"))
            })?;

        partitions.push(PartitionResult {
            partition_values,
            record_batch,
        });
    }

    Ok(partitions)
}

/// Groups row indices by partition key: keys ascending, and rows within a key in input order.
fn group_by_partition_key(arrays: &[ArrayRef]) -> DeltaResult<UInt32Array> {
    let fields = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields)
        .map_err(|e| DeltaTableError::generic(format!("failed to build row converter: {e}")))?;
    let rows = converter
        .convert_columns(arrays)
        .map_err(|e| DeltaTableError::generic(format!("failed to convert columns: {e}")))?;
    let row_count = u32::try_from(rows.num_rows()).map_err(|_| {
        DeltaTableError::generic(format!(
            "cannot partition a batch of {} rows: row indices are u32",
            rows.num_rows()
        ))
    })?;

    let mut rows_by_key: HashMap<Row<'_>, Vec<u32>> = HashMap::with_capacity(rows.num_rows());
    for (row_index, row) in (0..row_count).zip(&rows) {
        rows_by_key.entry(row).or_default().push(row_index);
    }

    // Sort to keep the partition order stable
    let mut grouped: Vec<_> = rows_by_key.into_iter().collect();
    grouped.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    Ok(UInt32Array::from_iter_values(
        grouped.into_iter().flat_map(|(_, row_indices)| row_indices),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::arrow::array::{Int32Array, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use crate::kernel::PartitionsExt;
    use crate::writer::test_utils::{create_initialized_table, get_record_batch};
    use crate::writer::utils::arrow_schema_without_partitions;
    use crate::writer::{DeltaWriterError, RecordBatchWriter};
    use arrow_array::RecordBatch;
    use arrow_json::ReaderBuilder;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::schema::StructType;
    use rstest::rstest;
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

    fn schema_with(cols: &[(&str, DataType)]) -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(
            cols.iter()
                .map(|(n, t)| Field::new(*n, t.clone(), false))
                .collect::<Vec<_>>(),
        ))
    }

    fn build_batch(region: &[&str], year: &[i32], value: &[i32]) -> RecordBatch {
        let schema = schema_with(&[
            ("region", DataType::Utf8),
            ("year", DataType::Int32),
            ("value", DataType::Int32),
        ]);
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(region.to_vec())),
                Arc::new(Int32Array::from(year.to_vec())),
                Arc::new(Int32Array::from(value.to_vec())),
            ],
        )
        .unwrap()
    }

    fn output_schema() -> ArrowSchemaRef {
        schema_with(&[("value", DataType::Int32)])
    }

    fn values_of(partition: &PartitionResult) -> Vec<i32> {
        partition
            .record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[rstest]
    #[case::few_rows(5)]
    #[case::enough_rows_to_expose_an_unstable_sort(4096)]
    fn single_partition_col_groups_by_key_keeping_input_order(#[case] rows: i32) {
        let region: Vec<&str> = (0..rows)
            .map(|i| if i % 2 == 0 { "US" } else { "EU" })
            .collect();
        let year = vec![2024; region.len()];
        let value: Vec<i32> = (0..rows).collect();
        let batch = build_batch(&region, &year, &value);

        let out =
            divide_by_partition_values(output_schema(), &["region".to_owned()], &batch).unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(
            out[0].partition_values.get("region").unwrap(),
            &Scalar::String("EU".to_owned()),
            "keys are ordered lexicographically"
        );
        assert_eq!(
            values_of(&out[0]),
            (0..rows).filter(|v| v % 2 != 0).collect::<Vec<_>>()
        );
        assert_eq!(
            out[1].partition_values.get("region").unwrap(),
            &Scalar::String("US".to_owned())
        );
        assert_eq!(
            values_of(&out[1]),
            (0..rows).filter(|v| v % 2 == 0).collect::<Vec<_>>()
        );
    }

    #[test]
    fn empty_partition_columns_returns_single_result_with_empty_map() {
        let batch = build_batch(&["US"], &[2024], &[1]);
        let out = divide_by_partition_values(batch.schema(), &[], &batch).unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].partition_values.is_empty());
        assert_eq!(out[0].record_batch.num_rows(), 1);
    }

    #[test]
    fn multi_partition_col_produces_cartesian_groups() {
        let batch = build_batch(
            &["US", "EU", "US", "EU"],
            &[2024, 2024, 2025, 2025],
            &[1, 2, 3, 4],
        );
        let out = divide_by_partition_values(
            output_schema(),
            &["region".to_owned(), "year".to_owned()],
            &batch,
        )
        .unwrap();
        assert_eq!(out.len(), 4);
        let keys: Vec<(String, i32)> = out
            .iter()
            .map(|p| {
                let region = match p.partition_values.get("region").unwrap() {
                    Scalar::String(s) => s.clone(),
                    other => panic!("expected String scalar, got {other:?}"),
                };
                let year = match p.partition_values.get("year").unwrap() {
                    Scalar::Integer(v) => *v,
                    other => panic!("expected Integer scalar, got {other:?}"),
                };
                (region, year)
            })
            .collect();
        // Lexicographic on (region, year) -> (EU,2024), (EU,2025), (US,2024), (US,2025).
        assert_eq!(
            keys,
            vec![
                ("EU".to_owned(), 2024),
                ("EU".to_owned(), 2025),
                ("US".to_owned(), 2024),
                ("US".to_owned(), 2025),
            ]
        );
    }

    #[test]
    fn output_schema_strips_partition_columns() {
        // The output `record_batch` must match the provided `arrow_schema` which omits
        // the partition columns.
        let batch = build_batch(&["US", "US"], &[2024, 2024], &[1, 2]);
        let out =
            divide_by_partition_values(output_schema(), &["region".to_owned()], &batch).unwrap();
        assert_eq!(out.len(), 1);
        let schema = out[0].record_batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["value"]);
    }

    #[test]
    fn total_row_count_preserved_across_partitions() {
        let batch = build_batch(
            &["EU", "US", "US", "EU", "APAC"],
            &[2024; 5],
            &[1, 2, 3, 4, 5],
        );
        let out =
            divide_by_partition_values(output_schema(), &["region".to_owned()], &batch).unwrap();
        let total: usize = out.iter().map(|p| p.record_batch.num_rows()).sum();
        assert_eq!(total, batch.num_rows());
    }

    #[test]
    fn empty_batch_with_partition_columns_returns_empty() {
        // Guards against `Scalar::from_array` erroring on a zero-length slice.
        let batch = build_batch(&[], &[], &[]);
        let out =
            divide_by_partition_values(output_schema(), &["region".to_owned()], &batch).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn empty_batch_without_partition_columns_returns_empty() {
        let batch = build_batch(&[], &[], &[]);
        let out = divide_by_partition_values(batch.schema(), &[], &batch).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn missing_partition_column_returns_error() {
        let batch = build_batch(&["US"], &[2024], &[1]);
        let err = divide_by_partition_values(output_schema(), &["nonexistent".to_owned()], &batch)
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent") || msg.contains("partition column"),
            "unexpected error: {msg}",
        );
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
