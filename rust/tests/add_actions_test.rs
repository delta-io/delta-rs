#![cfg(feature = "arrow")]

use arrow::array::{self, ArrayRef};
use arrow::compute::sort_to_indices;
use arrow::datatypes::{DataType, Field};
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
async fn test_add_action_table() {
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
        ("stats", Arc::new(array::StringArray::from(vec![None, None]))),
        ("partition_k", Arc::new(array::StringArray::from(vec![Some("A"), None]))),
    ];
    let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

    assert_eq!(expected, actions);

    let actions = table.get_state().add_actions_table(false).unwrap();
    let actions = sort_batch_by(&actions, "path").unwrap();

    expected_columns[5] = (
        "partition_values",
        Arc::new(array::StructArray::from(vec![(
            Field::new("k", DataType::Utf8, true),
            Arc::new(array::StringArray::from(vec![Some("A"), None])) as ArrayRef,
        )])),
    );
    let expected = RecordBatch::try_from_iter(expected_columns).unwrap();

    assert_eq!(expected, actions);

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
        (
            "stats",
            Arc::new(array::StringArray::from(vec![None, None, None, None, None])),
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

    // test table with stats
    // let path = "./tests/data/delta-0.8.0";
    // let mut table = deltalake::open_table(path).await.unwrap();

    // test table with no json stats
    // let path = "./tests/data/delta-1.2.1-only-struct-stats";
    // let mut table = deltalake::open_table(path).await.unwrap();
}
