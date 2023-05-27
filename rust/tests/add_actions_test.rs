#![cfg(feature = "arrow")]

use arrow::array::{self, ArrayRef, StructArray};
use arrow::compute::kernels::cast_utils::Parser;
use arrow::compute::sort_to_indices;
use arrow::datatypes::{DataType, Date32Type, Field, Fields, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn sort_batch_by(batch: &RecordBatch, column: &str) -> arrow::error::Result<RecordBatch> {
    let sort_column = batch.column(batch.schema().column_with_name(column).unwrap().0);
    let sort_indices = sort_to_indices(sort_column, None, None)?;
    let schema = batch.schema();
    let sorted_columns: Vec<(&String, ArrayRef)> = schema
        .fields()
        .iter()
        .zip(batch.columns().iter())
        .map(|(field, column)| {
            Ok((
                field.name(),
                arrow::compute::take(column, &sort_indices, None)?,
            ))
        })
        .collect::<arrow::error::Result<_>>()?;
    RecordBatch::try_from_iter(sorted_columns)
}

#[tokio::test]
async fn test_with_partitions() {
    // test table with partitions
    let path = "./tests/data/delta-0.8.0-null-partition";
    let table = deltalake::open_table(path).await.unwrap();
    let actions = table.get_state().add_actions_table(true).unwrap();
    let actions = sort_batch_by(&actions, "path").unwrap();

    let mut expected_columns: Vec<(&str, ArrayRef)> = vec![
        ("path", Arc::new(array::StringArray::from(vec![
            "k=A/part-00000-b1f1dbbb-70bc-4970-893f-9bb772bf246e.c000.snappy.parquet",
            "k=__HIVE_DEFAULT_PARTITION__/part-00001-8474ac85-360b-4f58-b3ea-23990c71b932.c000.snappy.parquet"
        ]))),
        ("size_bytes", Arc::new(array::Int64Array::from(vec![460, 460]))),
        ("modification_time", Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
            1627990384000, 1627990384000
        ]))),
        ("data_change", Arc::new(array::BooleanArray::from(vec![true, true]))),
        ("partition.k", Arc::new(array::StringArray::from(vec![Some("A"), None]))),
    ];
    let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

    assert_eq!(expected, actions);

    let actions = table.get_state().add_actions_table(false).unwrap();
    let actions = sort_batch_by(&actions, "path").unwrap();

    expected_columns[4] = (
        "partition_values",
        Arc::new(array::StructArray::new(
            Fields::from(vec![Field::new("k", DataType::Utf8, true)]),
            vec![Arc::new(array::StringArray::from(vec![Some("A"), None])) as ArrayRef],
            None,
        )),
    );
    let expected = RecordBatch::try_from_iter(expected_columns).unwrap();

    assert_eq!(expected, actions);
}

#[tokio::test]
async fn test_without_partitions() {
    // test table without partitions
    let path = "./tests/data/simple_table";
    let table = deltalake::open_table(path).await.unwrap();

    let actions = table.get_state().add_actions_table(true).unwrap();
    let actions = sort_batch_by(&actions, "path").unwrap();

    let expected_columns: Vec<(&str, ArrayRef)> = vec![
        (
            "path",
            Arc::new(array::StringArray::from(vec![
                "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
                "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
                "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
                "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
                "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
            ])),
        ),
        (
            "size_bytes",
            Arc::new(array::Int64Array::from(vec![262, 262, 429, 429, 429])),
        ),
        (
            "modification_time",
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                1587968626000,
                1587968602000,
                1587968602000,
                1587968602000,
                1587968602000,
            ])),
        ),
        (
            "data_change",
            Arc::new(array::BooleanArray::from(vec![
                true, true, true, true, true,
            ])),
        ),
    ];
    let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

    assert_eq!(expected, actions);

    let actions = table.get_state().add_actions_table(false).unwrap();
    let actions = sort_batch_by(&actions, "path").unwrap();

    // For now, this column is ignored.
    // expected_columns.push((
    //     "partition_values",
    //     new_null_array(&DataType::Struct(vec![]), 5),
    // ));
    let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

    assert_eq!(expected, actions);
}

#[tokio::test]
async fn test_with_stats() {
    // test table with stats
    let path = "./tests/data/delta-0.8.0";
    let table = deltalake::open_table(path).await.unwrap();
    let actions = table.get_state().add_actions_table(true).unwrap();
    let actions = sort_batch_by(&actions, "path").unwrap();

    let expected_columns: Vec<(&str, ArrayRef)> = vec![
        (
            "path",
            Arc::new(array::StringArray::from(vec![
                "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet",
                "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
            ])),
        ),
        (
            "size_bytes",
            Arc::new(array::Int64Array::from(vec![440, 440])),
        ),
        (
            "modification_time",
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                1615043776000,
                1615043767000,
            ])),
        ),
        (
            "data_change",
            Arc::new(array::BooleanArray::from(vec![true, true])),
        ),
        ("num_records", Arc::new(array::Int64Array::from(vec![2, 2]))),
        (
            "null_count.value",
            Arc::new(array::Int64Array::from(vec![0, 0])),
        ),
        ("min.value", Arc::new(array::Int32Array::from(vec![2, 0]))),
        ("max.value", Arc::new(array::Int32Array::from(vec![4, 2]))),
    ];
    let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

    assert_eq!(expected, actions);
}

#[tokio::test]
async fn test_only_struct_stats() {
    // test table with no json stats
    let path = "./tests/data/delta-1.2.1-only-struct-stats";
    let mut table = deltalake::open_table(path).await.unwrap();
    table.load_version(1).await.unwrap();

    let actions = table.get_state().add_actions_table(true).unwrap();

    let expected_columns: Vec<(&str, ArrayRef)> = vec![
        (
            "path",
            Arc::new(array::StringArray::from(vec![
                "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
            ])),
        ),
        ("size_bytes", Arc::new(array::Int64Array::from(vec![5489]))),
        (
            "modification_time",
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                1666652373000,
            ])),
        ),
        (
            "data_change",
            Arc::new(array::BooleanArray::from(vec![true])),
        ),
        ("num_records", Arc::new(array::Int64Array::from(vec![1]))),
        (
            "null_count.integer",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        ("min.integer", Arc::new(array::Int32Array::from(vec![0]))),
        ("max.integer", Arc::new(array::Int32Array::from(vec![0]))),
        (
            "null_count.null",
            Arc::new(array::Int64Array::from(vec![1])),
        ),
        ("min.null", Arc::new(array::NullArray::new(1))),
        ("max.null", Arc::new(array::NullArray::new(1))),
        (
            "null_count.boolean",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        ("min.boolean", Arc::new(array::NullArray::new(1))),
        ("max.boolean", Arc::new(array::NullArray::new(1))),
        (
            "null_count.double",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "min.double",
            Arc::new(array::Float64Array::from(vec![1.234])),
        ),
        (
            "max.double",
            Arc::new(array::Float64Array::from(vec![1.234])),
        ),
        (
            "null_count.decimal",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "min.decimal",
            Arc::new(
                array::Decimal128Array::from_iter_values([-567800])
                    .with_precision_and_scale(8, 5)
                    .unwrap(),
            ),
        ),
        (
            "max.decimal",
            Arc::new(
                array::Decimal128Array::from_iter_values([-567800])
                    .with_precision_and_scale(8, 5)
                    .unwrap(),
            ),
        ),
        (
            "null_count.string",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "min.string",
            Arc::new(array::StringArray::from(vec!["string"])),
        ),
        (
            "max.string",
            Arc::new(array::StringArray::from(vec!["string"])),
        ),
        (
            "null_count.binary",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        ("min.binary", Arc::new(array::NullArray::new(1))),
        ("max.binary", Arc::new(array::NullArray::new(1))),
        (
            "null_count.date",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "min.date",
            Arc::new(array::Date32Array::from(vec![Date32Type::parse(
                "2022-10-24",
            )])),
        ),
        (
            "max.date",
            Arc::new(array::Date32Array::from(vec![Date32Type::parse(
                "2022-10-24",
            )])),
        ),
        (
            "null_count.timestamp",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "min.timestamp",
            Arc::new(array::TimestampMicrosecondArray::from(vec![
                TimestampMicrosecondType::parse("2022-10-24T22:59:32.846Z"),
            ])),
        ),
        (
            "max.timestamp",
            Arc::new(array::TimestampMicrosecondArray::from(vec![
                TimestampMicrosecondType::parse("2022-10-24T22:59:32.846Z"),
            ])),
        ),
        (
            "null_count.struct.struct_element",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "min.struct.struct_element",
            Arc::new(array::StringArray::from(vec!["struct_value"])),
        ),
        (
            "max.struct.struct_element",
            Arc::new(array::StringArray::from(vec!["struct_value"])),
        ),
        ("null_count.map", Arc::new(array::Int64Array::from(vec![0]))),
        (
            "null_count.array",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "null_count.nested_struct.struct_element.nested_struct_element",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "min.nested_struct.struct_element.nested_struct_element",
            Arc::new(array::StringArray::from(vec!["nested_struct_value"])),
        ),
        (
            "max.nested_struct.struct_element.nested_struct_element",
            Arc::new(array::StringArray::from(vec!["nested_struct_value"])),
        ),
        (
            "null_count.struct_of_array_of_map.struct_element",
            Arc::new(array::Int64Array::from(vec![0])),
        ),
        (
            "tags.INSERTION_TIME",
            Arc::new(array::StringArray::from(vec!["1666652373000000"])),
        ),
        (
            "tags.OPTIMIZE_TARGET_SIZE",
            Arc::new(array::StringArray::from(vec!["268435456"])),
        ),
    ];
    let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

    assert_eq!(
        expected
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<&str>>(),
        actions
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<&str>>()
    );
    assert_eq!(expected, actions);

    let actions = table.get_state().add_actions_table(false).unwrap();
    // For brevity, just checking a few nested columns in stats

    assert_eq!(
        actions
            .get_field_at_path(&[
                "null_count",
                "nested_struct",
                "struct_element",
                "nested_struct_element"
            ])
            .unwrap()
            .as_any()
            .downcast_ref::<array::Int64Array>()
            .unwrap(),
        &array::Int64Array::from(vec![0]),
    );

    assert_eq!(
        actions
            .get_field_at_path(&[
                "min",
                "nested_struct",
                "struct_element",
                "nested_struct_element"
            ])
            .unwrap()
            .as_any()
            .downcast_ref::<array::StringArray>()
            .unwrap(),
        &array::StringArray::from(vec!["nested_struct_value"]),
    );

    assert_eq!(
        actions
            .get_field_at_path(&[
                "max",
                "nested_struct",
                "struct_element",
                "nested_struct_element"
            ])
            .unwrap()
            .as_any()
            .downcast_ref::<array::StringArray>()
            .unwrap(),
        &array::StringArray::from(vec!["nested_struct_value"]),
    );

    assert_eq!(
        actions
            .get_field_at_path(&["null_count", "struct_of_array_of_map", "struct_element"])
            .unwrap()
            .as_any()
            .downcast_ref::<array::Int64Array>()
            .unwrap(),
        &array::Int64Array::from(vec![0])
    );

    assert_eq!(
        actions
            .get_field_at_path(&["tags", "OPTIMIZE_TARGET_SIZE"])
            .unwrap()
            .as_any()
            .downcast_ref::<array::StringArray>()
            .unwrap(),
        &array::StringArray::from(vec!["268435456"])
    );
}

/// Trait to make it easier to access nested fields
trait NestedTabular {
    fn get_field_at_path(&self, path: &[&str]) -> Option<ArrayRef>;
}

impl NestedTabular for RecordBatch {
    fn get_field_at_path(&self, path: &[&str]) -> Option<ArrayRef> {
        // First, get array in the batch
        let (first_key, remainder) = path.split_at(1);
        let mut col = self.column(self.schema().column_with_name(first_key[0])?.0);

        if remainder.is_empty() {
            return Some(Arc::clone(col));
        }

        for segment in remainder {
            col = col
                .as_any()
                .downcast_ref::<StructArray>()?
                .column_by_name(segment)?;
        }

        Some(Arc::clone(col))
    }
}
