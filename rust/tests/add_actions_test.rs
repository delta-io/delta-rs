#![cfg(feature = "arrow")]

use arrow::array::{self, ArrayRef, StringArray};
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
    let path = "./tests/data/delta-0.8.0";
    let table = deltalake::open_table(path).await.unwrap();
    let actions = table.get_state().add_actions_table(true).unwrap();
    let actions = sort_batch_by(&actions, "path").unwrap();

    let expected_columns: Vec<(&str, ArrayRef)> = vec![
        ("path", Arc::new(array::StringArray::from(vec![
            "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet",
            "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
        ]))),
        ("size_bytes", Arc::new(array::Int64Array::from(vec![440, 440]))),
        ("modification_time", Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
            1615043776000, 1615043767000
        ]))),
        ("data_change", Arc::new(array::BooleanArray::from(vec![true, true]))),
        ("stats", Arc::new(array::StringArray::from(vec![
            "{\"numRecords\":2,\"minValues\":{\"value\":2},\"maxValues\":{\"value\":4},\"nullCount\":{\"value\":0}}",
            "{\"numRecords\":2,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":2},\"nullCount\":{\"value\":0}}",
        ]))),
    ];
    let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

    assert_eq!(expected, actions);

    // test table with no json stats
    let path = "./tests/data/delta-1.2.1-only-struct-stats";
    let table = deltalake::open_table(path).await.unwrap();

    let actions = table.get_state().add_actions_table(true).unwrap();
    let actions = sort_batch_by(&actions, "path").unwrap();

    let stats_col_index = actions.schema().column_with_name("stats").unwrap().0;
    let result_stats: &StringArray = actions
        .column(stats_col_index)
        .as_any()
        .downcast_ref()
        .unwrap();
    let actions = actions
        .project(
            &(0..actions.num_columns())
                .into_iter()
                .filter(|i| i != &stats_col_index)
                .collect::<Vec<usize>>(),
        )
        .unwrap();

    let expected_columns: Vec<(&str, ArrayRef)> = vec![
        (
            "path",
            Arc::new(array::StringArray::from(vec![
                "part-00000-1c2d1a32-02dc-484f-87ff-4328ea56045d-c000.snappy.parquet",
                "part-00000-28925d3a-bdf2-411e-bca9-b067444cbcb0-c000.snappy.parquet",
                "part-00000-6630b7c4-0aca-405b-be86-68a812f2e4c8-c000.snappy.parquet",
                "part-00000-74151571-7ec6-4bd6-9293-b5daab2ce667-c000.snappy.parquet",
                "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
                "part-00000-8e0aefe1-6645-4601-ac29-68cba64023b5-c000.snappy.parquet",
                "part-00000-b26ba634-874c-45b0-a7ff-2f0395a53966-c000.snappy.parquet",
                "part-00000-c4c8caec-299d-42a4-b50c-5a4bf724c037-c000.snappy.parquet",
                "part-00000-ce300400-58ff-4b8f-8ba9-49422fdf9f2e-c000.snappy.parquet",
                "part-00000-e1262b3e-2959-4910-aea9-4eaf92f0c68c-c000.snappy.parquet",
                "part-00000-e8e3753f-e2f6-4c9f-98f9-8f3d346727ba-c000.snappy.parquet",
                "part-00000-f73ff835-0571-4d67-ac43-4fbf948bfb9b-c000.snappy.parquet",
            ])),
        ),
        (
            "size_bytes",
            Arc::new(array::Int64Array::from(vec![
                5488, 5489, 5489, 5489, 5489, 5489, 5489, 5489, 5489, 5489, 5489, 5731,
            ])),
        ),
        (
            "modification_time",
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                1666652376000,
                1666652374000,
                1666652378000,
                1666652377000,
                1666652373000,
                1666652385000,
                1666652375000,
                1666652379000,
                1666652382000,
                1666652386000,
                1666652380000,
                1666652383000,
            ])),
        ),
        (
            "data_change",
            Arc::new(array::BooleanArray::from(vec![
                false, false, false, false, false, true, false, false, false, true, false, false,
            ])),
        ),
    ];
    let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

    assert_eq!(expected, actions);

    // For stats in checkpoints, the serialization order isn't deterministic, so we can't compare with strings
    let expected_stats = vec![
        "{\"numRecords\":1,\"minValues\":{\"struct\":{\"struct_element\":\"struct_value\"},\"double\":1.234,\"decimal\":-5.678,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"timestamp\":\"2022-10-24T22:59:36.177Z\",\"string\":\"string\",\"integer\":3,\"date\":\"2022-10-24\"},\"maxValues\":{\"timestamp\":\"2022-10-24T22:59:36.177Z\",\"integer\":3,\"struct\":{\"struct_element\":\"struct_value\"},\"double\":1.234,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"string\":\"string\",\"date\":\"2022-10-24\",\"decimal\":-5.678},\"nullCount\":{\"struct\":{\"struct_element\":0},\"double\":0,\"array\":0,\"integer\":0,\"date\":0,\"map\":0,\"struct_of_array_of_map\":{\"struct_element\":0},\"boolean\":0,\"null\":1,\"decimal\":0,\"binary\":0,\"timestamp\":0,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"string\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"integer\":1,\"timestamp\":\"2022-10-24T22:59:34.067Z\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"struct\":{\"struct_element\":\"struct_value\"},\"string\":\"string\",\"double\":1.234,\"date\":\"2022-10-24\",\"decimal\":-5.678},\"maxValues\":{\"decimal\":-5.678,\"timestamp\":\"2022-10-24T22:59:34.067Z\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"integer\":1,\"double\":1.234,\"string\":\"string\",\"struct\":{\"struct_element\":\"struct_value\"},\"date\":\"2022-10-24\"},\"nullCount\":{\"boolean\":0,\"string\":0,\"struct\":{\"struct_element\":0},\"map\":0,\"double\":0,\"array\":0,\"null\":1,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"binary\":0,\"decimal\":0,\"date\":0,\"timestamp\":0,\"struct_of_array_of_map\":{\"struct_element\":0},\"integer\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"integer\":5,\"decimal\":-5.678,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"string\":\"string\",\"timestamp\":\"2022-10-24T22:59:38.358Z\",\"double\":1.234,\"date\":\"2022-10-24\",\"struct\":{\"struct_element\":\"struct_value\"}},\"maxValues\":{\"timestamp\":\"2022-10-24T22:59:38.358Z\",\"integer\":5,\"decimal\":-5.678,\"date\":\"2022-10-24\",\"string\":\"string\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"double\":1.234,\"struct\":{\"struct_element\":\"struct_value\"}},\"nullCount\":{\"null\":1,\"struct_of_array_of_map\":{\"struct_element\":0},\"binary\":0,\"string\":0,\"double\":0,\"integer\":0,\"date\":0,\"timestamp\":0,\"map\":0,\"array\":0,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"decimal\":0,\"struct\":{\"struct_element\":0},\"boolean\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"string\":\"string\",\"double\":1.234,\"integer\":4,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"timestamp\":\"2022-10-24T22:59:37.235Z\",\"date\":\"2022-10-24\",\"decimal\":-5.678,\"struct\":{\"struct_element\":\"struct_value\"}},\"maxValues\":{\"timestamp\":\"2022-10-24T22:59:37.235Z\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"date\":\"2022-10-24\",\"string\":\"string\",\"struct\":{\"struct_element\":\"struct_value\"},\"double\":1.234,\"integer\":4,\"decimal\":-5.678},\"nullCount\":{\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"struct_of_array_of_map\":{\"struct_element\":0},\"binary\":0,\"map\":0,\"string\":0,\"double\":0,\"date\":0,\"null\":1,\"integer\":0,\"boolean\":0,\"decimal\":0,\"timestamp\":0,\"struct\":{\"struct_element\":0},\"array\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"struct\":{\"struct_element\":\"struct_value\"},\"decimal\":-5.678,\"timestamp\":\"2022-10-24T22:59:32.846Z\",\"string\":\"string\",\"integer\":0,\"double\":1.234,\"date\":\"2022-10-24\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}}},\"maxValues\":{\"string\":\"string\",\"struct\":{\"struct_element\":\"struct_value\"},\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"date\":\"2022-10-24\",\"decimal\":-5.678,\"integer\":0,\"double\":1.234,\"timestamp\":\"2022-10-24T22:59:32.846Z\"},\"nullCount\":{\"timestamp\":0,\"struct\":{\"struct_element\":0},\"double\":0,\"binary\":0,\"string\":0,\"boolean\":0,\"decimal\":0,\"null\":1,\"integer\":0,\"array\":0,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"struct_of_array_of_map\":{\"struct_element\":0},\"date\":0,\"map\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"struct\":{\"struct_element\":\"struct_value\"},\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"date\":\"2022-10-24\",\"timestamp\":\"2022-10-24T22:59:44.639Z\",\"double\":1.234,\"integer\":10,\"string\":\"string\",\"decimal\":-5.678},\"maxValues\":{\"struct\":{\"struct_element\":\"struct_value\"},\"date\":\"2022-10-24\",\"integer\":10,\"decimal\":-5.678,\"string\":\"string\",\"double\":1.234,\"timestamp\":\"2022-10-24T22:59:44.639Z\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}}},\"nullCount\":{\"binary\":0,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"timestamp\":0,\"boolean\":0,\"string\":0,\"struct_of_array_of_map\":{\"struct_element\":0},\"map\":0,\"null\":1,\"decimal\":0,\"array\":0,\"struct\":{\"struct_element\":0},\"double\":0,\"integer\":0,\"date\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"date\":\"2022-10-24\",\"timestamp\":\"2022-10-24T22:59:35.117Z\",\"integer\":2,\"struct\":{\"struct_element\":\"struct_value\"},\"decimal\":-5.678,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"string\":\"string\",\"double\":1.234},\"maxValues\":{\"integer\":2,\"timestamp\":\"2022-10-24T22:59:35.117Z\",\"struct\":{\"struct_element\":\"struct_value\"},\"decimal\":-5.678,\"double\":1.234,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"date\":\"2022-10-24\",\"string\":\"string\"},\"nullCount\":{\"struct\":{\"struct_element\":0},\"double\":0,\"map\":0,\"array\":0,\"boolean\":0,\"integer\":0,\"date\":0,\"binary\":0,\"decimal\":0,\"string\":0,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"struct_of_array_of_map\":{\"struct_element\":0},\"null\":1,\"timestamp\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"timestamp\":\"2022-10-24T22:59:39.489Z\",\"string\":\"string\",\"struct\":{\"struct_element\":\"struct_value\"},\"date\":\"2022-10-24\",\"integer\":6,\"double\":1.234,\"decimal\":-5.678,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}}},\"maxValues\":{\"timestamp\":\"2022-10-24T22:59:39.489Z\",\"decimal\":-5.678,\"integer\":6,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"string\":\"string\",\"struct\":{\"struct_element\":\"struct_value\"},\"date\":\"2022-10-24\",\"double\":1.234},\"nullCount\":{\"double\":0,\"decimal\":0,\"boolean\":0,\"struct\":{\"struct_element\":0},\"date\":0,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"timestamp\":0,\"array\":0,\"binary\":0,\"map\":0,\"null\":1,\"struct_of_array_of_map\":{\"struct_element\":0},\"string\":0,\"integer\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"timestamp\":\"2022-10-24T22:59:41.637Z\",\"decimal\":-5.678,\"date\":\"2022-10-24\",\"integer\":8,\"string\":\"string\",\"double\":1.234,\"struct\":{\"struct_element\":\"struct_value\"},\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}}},\"maxValues\":{\"date\":\"2022-10-24\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"decimal\":-5.678,\"double\":1.234,\"integer\":8,\"timestamp\":\"2022-10-24T22:59:41.637Z\",\"string\":\"string\",\"struct\":{\"struct_element\":\"struct_value\"}},\"nullCount\":{\"array\":0,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"timestamp\":0,\"string\":0,\"struct_of_array_of_map\":{\"struct_element\":0},\"map\":0,\"integer\":0,\"binary\":0,\"double\":0,\"null\":1,\"date\":0,\"decimal\":0,\"boolean\":0,\"struct\":{\"struct_element\":0}}}",
        "{\"numRecords\":1,\"minValues\":{\"timestamp\":\"2022-10-24T22:59:46.083Z\",\"date\":\"2022-10-24\",\"struct\":{\"struct_element\":\"struct_value\"},\"integer\":11,\"string\":\"string\",\"double\":1.234,\"decimal\":-5.678,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}}},\"maxValues\":{\"decimal\":-5.678,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"date\":\"2022-10-24\",\"timestamp\":\"2022-10-24T22:59:46.083Z\",\"string\":\"string\",\"struct\":{\"struct_element\":\"struct_value\"},\"double\":1.234,\"integer\":11},\"nullCount\":{\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"binary\":0,\"date\":0,\"double\":0,\"map\":0,\"null\":1,\"struct_of_array_of_map\":{\"struct_element\":0},\"string\":0,\"boolean\":0,\"integer\":0,\"struct\":{\"struct_element\":0},\"decimal\":0,\"timestamp\":0,\"array\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"date\":\"2022-10-24\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"decimal\":-5.678,\"timestamp\":\"2022-10-24T22:59:40.572Z\",\"struct\":{\"struct_element\":\"struct_value\"},\"string\":\"string\",\"integer\":7,\"double\":1.234},\"maxValues\":{\"integer\":7,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"struct\":{\"struct_element\":\"struct_value\"},\"double\":1.234,\"decimal\":-5.678,\"string\":\"string\",\"timestamp\":\"2022-10-24T22:59:40.572Z\",\"date\":\"2022-10-24\"},\"nullCount\":{\"double\":0,\"binary\":0,\"boolean\":0,\"timestamp\":0,\"array\":0,\"null\":1,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"struct\":{\"struct_element\":0},\"struct_of_array_of_map\":{\"struct_element\":0},\"map\":0,\"integer\":0,\"date\":0,\"string\":0,\"decimal\":0}}",
        "{\"numRecords\":1,\"minValues\":{\"double\":1.234,\"string\":\"string\",\"date\":\"2022-10-24\",\"struct\":{\"struct_element\":\"struct_value\"},\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"timestamp\":\"2022-10-24T22:59:42.908Z\",\"integer\":9,\"decimal\":-5.678,\"new_column\":0},\"maxValues\":{\"date\":\"2022-10-24\",\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":\"nested_struct_value\"}},\"decimal\":-5.678,\"integer\":9,\"double\":1.234,\"string\":\"string\",\"struct\":{\"struct_element\":\"struct_value\"},\"timestamp\":\"2022-10-24T22:59:42.908Z\",\"new_column\":0},\"nullCount\":{\"double\":0,\"new_column\":0,\"struct_of_array_of_map\":{\"struct_element\":0},\"date\":0,\"map\":0,\"decimal\":0,\"null\":1,\"timestamp\":0,\"struct\":{\"struct_element\":0},\"boolean\":0,\"nested_struct\":{\"struct_element\":{\"nested_struct_element\":0}},\"binary\":0,\"integer\":0,\"array\":0,\"string\":0}}",
    ];

    for (maybe_result_stat, expected_stat) in result_stats.iter().zip(expected_stats.into_iter()) {
        let result_value: serde_json::Value =
            serde_json::from_str(maybe_result_stat.unwrap()).unwrap();
        let expected_value: serde_json::Value = serde_json::from_str(expected_stat).unwrap();
        assert_eq!(result_value, expected_value);
    }
}
