use super::*;

use crate::kernel::{Action, PrimitiveType, StructField, StructType};
use crate::kernel::{DataType as DeltaDataType, ProtocolInner};
use crate::writer::test_utils::datafusion::{get_data, write_batch};
use crate::writer::test_utils::{
    get_arrow_schema, get_delta_schema, get_record_batch, setup_table_with_configuration,
};
use crate::{DeltaTable, TableProperty};
use arrow::array::{Int32Array, ListArray, StringArray};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use datafusion::assert_batches_sorted_eq;
use datafusion::physical_plan::collect;
use datafusion::prelude::*;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use serde_json::json;
use std::sync::Arc;

async fn setup_table(partitions: Option<Vec<&str>>) -> DeltaTable {
    let table_schema = get_delta_schema();

    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(table_schema.fields().cloned())
        .with_partition_columns(partitions.unwrap_or_default())
        .await
        .unwrap();
    assert_eq!(table.version(), Some(0));
    table
}

async fn prepare_values_table() -> DeltaTable {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow::datatypes::DataType::Int32,
        true,
    )]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![
            Some(0),
            None,
            Some(2),
            None,
            Some(4),
        ]))],
    )
    .unwrap();

    DeltaTable::new_in_memory()
        .write(vec![batch])
        .await
        .unwrap()
}

#[tokio::test]
async fn test_update_when_delta_table_is_append_only() {
    let table = setup_table_with_configuration(TableProperty::AppendOnly, Some("true")).await;
    let batch = get_record_batch(None, false);
    // Append
    let table = write_batch(table, batch).await;
    let _err = table
        .update()
        .with_update("modified", lit("2023-05-14"))
        .await
        .expect_err("Remove action is included when Delta table is append-only. Should error");
}

// <https://github.com/delta-io/delta-rs/issues/3414>
#[tokio::test]
async fn test_update_predicate_left_in_data() -> DeltaResult<()> {
    let schema = get_arrow_schema(&None);
    let table = setup_table(None).await;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-02",
                "2021-02-02",
                "2021-02-02",
                "2021-02-02",
            ])),
        ],
    )?;

    let table = write_batch(table, batch).await;
    assert_eq!(table.version(), Some(1));

    let (table, _) = table
        .update()
        .with_update("modified", lit("2023-05-14"))
        .with_predicate(col("value").eq(lit(10)))
        .await?;

    use parquet::arrow::async_reader::ParquetObjectReader;
    use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

    for pq in table.get_files_by_partitions(&[]).await? {
        let store = table.log_store().object_store(None);
        let reader = ParquetObjectReader::new(store, pq);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let schema = builder.schema();

        assert!(
            schema
                .field_with_name("__delta_rs_update_predicate")
                .is_err(),
            "The schema contains __delta_rs_update_predicate which is incorrect!"
        );
        assert_eq!(
            schema.fields.len(),
            3,
            "Expected the Parquet file to only have three fields in the schema, something is amiss!"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_update_string_equality_non_partition() -> DeltaResult<()> {
    // Regression test: DF52 view types can cause predicate evaluation to compare
    // Utf8 against Utf8View for string equality predicates.
    let schema = Arc::new(Schema::new(vec![
        Field::new("utf8", DataType::Utf8, true),
        Field::new("int32", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("0"), Some("1"), Some("2")])),
            Arc::new(Int32Array::from(vec![0, 1, 2])),
        ],
    )?;

    let table = DeltaTable::new_in_memory().write(vec![batch]).await?;

    let (table, _metrics) = table
        .update()
        .with_predicate("utf8 = '1'")
        .with_update(
            "utf8",
            "CASE WHEN utf8 = '1' THEN 'hello world' ELSE utf8 END",
        )
        .await?;

    let expected = [
        "+-------------+-------+",
        "| utf8        | int32 |",
        "+-------------+-------+",
        "| 0           | 0     |",
        "| 2           | 2     |",
        "| hello world | 1     |",
        "+-------------+-------+",
    ];
    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_update_no_predicate() {
    let schema = get_arrow_schema(&None);
    let table = setup_table(None).await;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-02",
                "2021-02-02",
                "2021-02-02",
                "2021-02-02",
            ])),
        ],
    )
    .unwrap();

    let table = write_batch(table, batch).await;
    assert_eq!(table.version(), Some(1));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);

    let (table, metrics) = table
        .update()
        .with_update("modified", lit("2023-05-14"))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(2));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    assert_eq!(metrics.num_added_files, 1);
    assert_eq!(metrics.num_removed_files, 1);
    assert_eq!(metrics.num_updated_rows, 4);
    assert_eq!(metrics.num_copied_rows, 0);

    let expected = vec![
        "+----+-------+------------+",
        "| id | value | modified   |",
        "+----+-------+------------+",
        "| A  | 1     | 2023-05-14 |",
        "| A  | 10    | 2023-05-14 |",
        "| A  | 100   | 2023-05-14 |",
        "| B  | 10    | 2023-05-14 |",
        "+----+-------+------------+",
    ];
    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);
}

#[tokio::test]
async fn test_update_non_partition() {
    let schema = get_arrow_schema(&None);
    let table = setup_table(None).await;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-02",
                "2021-02-02",
                "2021-02-03",
                "2021-02-03",
            ])),
        ],
    )
    .unwrap();

    // Update a partitioned table where the predicate contains only partition column
    // The expectation is that a physical scan of data is not required

    let table = write_batch(table, batch).await;
    assert_eq!(table.version(), Some(1));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);

    let (table, metrics) = table
        .update()
        .with_predicate(col("modified").eq(lit("2021-02-03")))
        .with_update("modified", lit("2023-05-14"))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(2));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    assert_eq!(metrics.num_added_files, 1);
    assert_eq!(metrics.num_removed_files, 1);
    assert_eq!(metrics.num_updated_rows, 2);
    assert_eq!(metrics.num_copied_rows, 2);

    let last_commit = table.last_commit().await.unwrap();
    let parameters = last_commit.operation_parameters.clone().unwrap();
    assert_eq!(parameters["predicate"], json!("modified = '2021-02-03'"));

    let expected = vec![
        "+----+-------+------------+",
        "| id | value | modified   |",
        "+----+-------+------------+",
        "| A  | 1     | 2021-02-02 |",
        "| A  | 10    | 2023-05-14 |",
        "| A  | 100   | 2023-05-14 |",
        "| B  | 10    | 2021-02-02 |",
        "+----+-------+------------+",
    ];
    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);
}

#[tokio::test]
async fn test_update_partitions() {
    let schema = get_arrow_schema(&None);
    let table = setup_table(Some(vec!["modified"])).await;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-02",
                "2021-02-02",
                "2021-02-03",
                "2021-02-03",
            ])),
        ],
    )
    .unwrap();

    let table = write_batch(table, batch.clone()).await;
    assert_eq!(table.version(), Some(1));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 2);

    let (table, metrics) = table
        .update()
        .with_predicate(col("modified").eq(lit("2021-02-03")))
        .with_update("modified", lit("2023-05-14"))
        .with_update("id", lit("C"))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(2));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 2);
    assert_eq!(metrics.num_added_files, 1);
    assert_eq!(metrics.num_removed_files, 1);
    assert_eq!(metrics.num_updated_rows, 2);
    assert_eq!(metrics.num_copied_rows, 0);

    let expected = vec![
        "+----+-------+------------+",
        "| id | value | modified   |",
        "+----+-------+------------+",
        "| A  | 1     | 2021-02-02 |",
        "| C  | 10    | 2023-05-14 |",
        "| C  | 100   | 2023-05-14 |",
        "| B  | 10    | 2021-02-02 |",
        "+----+-------+------------+",
    ];

    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);

    // Update a partitioned table where the predicate contains a partition column and non-partition column
    let table = setup_table(Some(vec!["modified"])).await;
    let table = write_batch(table, batch).await;
    assert_eq!(table.version(), Some(1));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 2);

    let (table, metrics) = table
        .update()
        .with_predicate(
            col("modified")
                .eq(lit("2021-02-03"))
                .and(col("value").eq(lit(100))),
        )
        .with_update("modified", lit("2023-05-14"))
        .with_update("id", lit("C"))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(2));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 3);
    assert_eq!(metrics.num_added_files, 2);
    assert_eq!(metrics.num_removed_files, 1);
    assert_eq!(metrics.num_updated_rows, 1);
    assert_eq!(metrics.num_copied_rows, 1);

    let expected = vec![
        "+----+-------+------------+",
        "| id | value | modified   |",
        "+----+-------+------------+",
        "| A  | 1     | 2021-02-02 |",
        "| A  | 10    | 2021-02-03 |",
        "| B  | 10    | 2021-02-02 |",
        "| C  | 100   | 2023-05-14 |",
        "+----+-------+------------+",
    ];

    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);
}

#[tokio::test]
async fn test_update_case_sensitive() {
    let schema = StructType::try_new(vec![
        StructField::new(
            "Id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "ValUe".to_string(), // spellchecker:disable-line
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "mOdified".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ])
    .unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("Id", DataType::Utf8, true),
        Field::new("ValUe", DataType::Int32, true), // spellchecker:disable-line
        Field::new("mOdified", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-02",
                "2021-02-02",
                "2021-02-03",
                "2021-02-03",
            ])),
        ],
    )
    .unwrap();

    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(schema.fields().cloned())
        .await
        .unwrap();
    let table = write_batch(table, batch).await;

    let (table, _metrics) = table
        .update()
        .with_predicate("mOdified = '2021-02-03'")
        .with_update("mOdified", "'2023-05-14'")
        .with_update("Id", "'C'")
        .await
        .unwrap();

    let expected = vec![
        "+----+-------+------------+",
        "| Id | ValUe | mOdified   |", // spellchecker:disable-line
        "+----+-------+------------+",
        "| A  | 1     | 2021-02-02 |",
        "| B  | 10    | 2021-02-02 |",
        "| C  | 10    | 2023-05-14 |",
        "| C  | 100   | 2023-05-14 |",
        "+----+-------+------------+",
    ];

    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);
}

#[tokio::test]
async fn test_update_null() {
    let table = prepare_values_table().await;
    assert_eq!(table.version(), Some(0));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);

    let (table, metrics) = table
        .update()
        .with_update("value", col("value") + lit(1))
        .await
        .unwrap();
    assert_eq!(table.version(), Some(1));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    assert_eq!(metrics.num_added_files, 1);
    assert_eq!(metrics.num_removed_files, 1);
    assert_eq!(metrics.num_updated_rows, 5);
    assert_eq!(metrics.num_copied_rows, 0);

    let expected = [
        "+-------+",
        "| value |",
        "+-------+",
        "|       |",
        "|       |",
        "| 1     |",
        "| 3     |",
        "| 5     |",
        "+-------+",
    ];

    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);

    // Validate order operators do not include nulls
    let table = prepare_values_table().await;
    let (table, metrics) = table
        .update()
        .with_predicate(col("value").gt(lit(2)).or(col("value").lt(lit(2))))
        .with_update("value", lit(10))
        .await
        .unwrap();
    assert_eq!(table.version(), Some(1));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    assert_eq!(metrics.num_added_files, 1);
    assert_eq!(metrics.num_removed_files, 1);
    assert_eq!(metrics.num_updated_rows, 2);
    assert_eq!(metrics.num_copied_rows, 3);

    let last_commit = table.last_commit().await.unwrap();
    let extra_info = last_commit.info.clone();
    assert_eq!(
        extra_info["operationMetrics"],
        serde_json::to_value(&metrics).unwrap()
    );

    let expected = [
        "+-------+",
        "| value |",
        "+-------+",
        "|       |",
        "|       |",
        "| 2     |",
        "| 10    |",
        "| 10    |",
        "+-------+",
    ];
    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);

    let table = prepare_values_table().await;
    let (table, metrics) = table
        .update()
        .with_predicate("value is null")
        .with_update("value", "10")
        .await
        .unwrap();
    assert_eq!(table.version(), Some(1));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    assert_eq!(metrics.num_added_files, 1);
    assert_eq!(metrics.num_removed_files, 1);
    assert_eq!(metrics.num_updated_rows, 2);
    assert_eq!(metrics.num_copied_rows, 3);

    let expected = [
        "+-------+",
        "| value |",
        "+-------+",
        "| 10    |",
        "| 10    |",
        "| 0     |",
        "| 2     |",
        "| 4     |",
        "+-------+",
    ];
    let actual = get_data(&table).await;
    assert_batches_sorted_eq!(&expected, &actual);
}

#[tokio::test]
async fn test_no_update_operations() {
    // No Update operations are provided
    let table = prepare_values_table().await;
    let (table, metrics) = table.update().await.unwrap();

    assert_eq!(table.version(), Some(0));
    assert_eq!(metrics.num_added_files, 0);
    assert_eq!(metrics.num_removed_files, 0);
    assert_eq!(metrics.num_copied_rows, 0);
    assert_eq!(metrics.num_removed_files, 0);
    assert_eq!(metrics.scan_time_ms, 0);
    assert_eq!(metrics.execution_time_ms, 0);
}

#[tokio::test]
async fn test_no_matching_records() {
    let table = prepare_values_table().await;

    // The predicate does not match any records
    let (table, metrics) = table
        .update()
        .with_predicate(col("value").eq(lit(3)))
        .with_update("value", lit(10))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(0));
    assert_eq!(metrics.num_added_files, 0);
    assert_eq!(metrics.num_removed_files, 0);
    assert_eq!(metrics.num_copied_rows, 0);
    assert_eq!(metrics.num_removed_files, 0);
}

#[tokio::test]
async fn test_expected_failures() {
    // The predicate must be deterministic and expression must be valid

    let table = setup_table(None).await;

    let res = table
        .update()
        .with_predicate(col("value").eq(cast(
            random() * lit(20.0),
            arrow::datatypes::DataType::Int32,
        )))
        .with_update("value", col("value") + lit(20))
        .await;
    assert!(res.is_err());

    // Expression result types must match the table's schema
    let table = prepare_values_table().await;
    let res = table.update().with_update("value", lit("a string")).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_update_with_array() {
    let schema = StructType::try_new(vec![
        StructField::new(
            "id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "temp".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "items".to_string(),
            DeltaDataType::Array(Box::new(crate::kernel::ArrayType::new(
                DeltaDataType::INTEGER,
                false,
            ))),
            true,
        ),
    ])
    .unwrap();
    let arrow_schema: ArrowSchema = (&schema).try_into_arrow().unwrap();

    // Create the first batch
    let arrow_field = Field::new("element", DataType::Int32, false);
    let list_array = ListArray::new_null(arrow_field.clone().into(), 2);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![Some(0), Some(1)])),
            Arc::new(Int32Array::from(vec![Some(30), Some(31)])),
            Arc::new(list_array),
        ],
    )
    .expect("Failed to create record batch");

    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(schema.fields().cloned())
        .await
        .unwrap();
    assert_eq!(table.version(), Some(0));

    let table = table
        .write(vec![batch])
        .await
        .expect("Failed to write first batch");
    assert_eq!(table.version(), Some(1));
    // Completed the first creation/write

    use arrow::array::{Int32Builder, ListBuilder};
    let mut new_items_builder =
        ListBuilder::new(Int32Builder::new()).with_field(arrow_field.clone());
    new_items_builder.append_value([Some(100)]);
    let new_items = ScalarValue::List(Arc::new(new_items_builder.finish()));

    let (table, _metrics) = table
        .update()
        .with_predicate(col("id").eq(lit(1)))
        .with_update("items", lit(new_items))
        .await
        .unwrap();
    assert_eq!(table.version(), Some(2));
}

/// Lists coming in from the Python bindings need to be parsed as SQL expressions by the update
/// and therefore this test emulates their behavior to ensure that the lists are being turned
/// into expressions for the update operation correctly
#[tokio::test]
async fn test_update_with_array_that_must_be_coerced() {
    let _ = pretty_env_logger::try_init();
    let schema = StructType::try_new(vec![
        StructField::new(
            "id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "temp".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "items".to_string(),
            DeltaDataType::Array(Box::new(crate::kernel::ArrayType::new(
                DeltaDataType::LONG,
                true,
            ))),
            true,
        ),
    ])
    .unwrap();
    let arrow_schema: ArrowSchema = (&schema).try_into_arrow().unwrap();

    // Create the first batch
    let arrow_field = Field::new("element", DataType::Int64, true);
    let list_array = ListArray::new_null(arrow_field.clone().into(), 2);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![Some(0), Some(1)])),
            Arc::new(Int32Array::from(vec![Some(30), Some(31)])),
            Arc::new(list_array),
        ],
    )
    .expect("Failed to create record batch");
    let _ = arrow::util::pretty::print_batches(&[batch.clone()]);

    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(schema.fields().cloned())
        .await
        .unwrap();
    assert_eq!(table.version(), Some(0));

    let table = table
        .write(vec![batch])
        .await
        .expect("Failed to write first batch");
    assert_eq!(table.version(), Some(1));
    // Completed the first creation/write

    let (table, _metrics) = table
        .update()
        .with_predicate(col("id").eq(lit(1)))
        .with_update("items", "[100]".to_string())
        .await
        .unwrap();
    assert_eq!(table.version(), Some(2));
}

#[tokio::test]
async fn test_no_cdc_on_older_tables() {
    let table = prepare_values_table().await;
    assert_eq!(table.version(), Some(0));
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow::datatypes::DataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
    )
    .unwrap();
    let table = table
        .write(vec![batch])
        .await
        .expect("Failed to write first batch");
    assert_eq!(table.version(), Some(1));

    let (table, _metrics) = table
        .update()
        .with_predicate(col("value").eq(lit(2)))
        .with_update("value", lit(12))
        .await
        .unwrap();
    assert_eq!(table.version(), Some(2));

    // Too close for missiles, switching to guns. Just checking that the data wasn't actually
    // written instead!
    if let Ok(files) = crate::logstore::tests::flatten_list_stream(
        &table.object_store(),
        Some(&object_store::path::Path::from("_change_data")),
    )
    .await
    {
        assert_eq!(
            0,
            files.len(),
            "This test should not find any written CDC files! {files:#?}"
        );
    }
}

#[tokio::test]
async fn test_update_cdc_enabled() {
    // Currently you cannot pass EnableChangeDataFeed through `with_configuration_property`
    // so the only way to create a truly CDC enabled table is by shoving the Protocol
    // directly into the actions list
    let actions = vec![Action::Protocol(ProtocolInner::new(1, 4).as_kernel())];
    let table: DeltaTable = DeltaTable::new_in_memory()
        .create()
        .with_column(
            "value",
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
            None,
        )
        .with_actions(actions)
        .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
        .await
        .unwrap();
    assert_eq!(table.version(), Some(0));

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow::datatypes::DataType::Int32,
        true,
    )]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
    )
    .unwrap();
    let table = table
        .write(vec![batch])
        .await
        .expect("Failed to write first batch");
    assert_eq!(table.version(), Some(1));

    let (table, _metrics) = table
        .update()
        .with_predicate(col("value").eq(lit(2)))
        .with_update("value", lit(12))
        .await
        .unwrap();
    assert_eq!(table.version(), Some(2));

    let ctx = SessionContext::new();
    let table = table
        .scan_cdf()
        .with_starting_version(0)
        .build(&ctx.state(), None)
        .await
        .expect("Failed to load CDF");

    let mut batches = collect(table, ctx.task_ctx())
        .await
        .expect("Failed to collect batches");

    // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
    let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(3)).collect();

    assert_batches_sorted_eq! {[
    "+-------+------------------+-----------------+",
    "| value | _change_type     | _commit_version |",
    "+-------+------------------+-----------------+",
    "| 1     | insert           | 1               |",
    "| 2     | insert           | 1               |",
    "| 2     | update_preimage  | 2               |",
    "| 12    | update_postimage | 2               |",
    "| 3     | insert           | 1               |",
    "+-------+------------------+-----------------+",
        ], &batches }
}

#[tokio::test]
async fn test_update_cdc_enabled_partitions() {
    // Currently you cannot pass EnableChangeDataFeed through `with_configuration_property`
    // so the only way to create a truly CDC enabled table is by shoving the Protocol
    // directly into the actions list
    let actions = vec![Action::Protocol(ProtocolInner::new(1, 4).as_kernel())];
    let table: DeltaTable = DeltaTable::new_in_memory()
        .create()
        .with_column(
            "year",
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
            None,
        )
        .with_column(
            "value",
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
            None,
        )
        .with_partition_columns(vec!["year"])
        .with_actions(actions)
        .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
        .await
        .unwrap();
    assert_eq!(table.version(), Some(0));

    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("2020"),
                Some("2020"),
                Some("2024"),
            ])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
        ],
    )
    .unwrap();
    let table = table
        .write(vec![batch])
        .await
        .expect("Failed to write first batch");
    assert_eq!(table.version(), Some(1));

    let (table, _metrics): (DeltaTable, _) = table
        .update()
        .with_predicate(col("value").eq(lit(2)))
        .with_update("year", "2024")
        .await
        .unwrap();
    assert_eq!(table.version(), Some(2));

    let ctx = SessionContext::new();
    let table = table
        .scan_cdf()
        .with_starting_version(0)
        .build(&ctx.state(), None)
        .await
        .expect("Failed to load CDF");

    let mut batches = collect(table, ctx.task_ctx())
        .await
        .expect("Failed to collect batches");

    // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
    let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(4)).collect();

    assert_batches_sorted_eq! {[
    "+-------+------+------------------+-----------------+",
    "| value | year | _change_type     | _commit_version |",
    "+-------+------+------------------+-----------------+",
    "| 1     | 2020 | insert           | 1               |",
    "| 2     | 2020 | insert           | 1               |",
    "| 2     | 2020 | update_preimage  | 2               |",
    "| 2     | 2024 | update_postimage | 2               |",
    "| 3     | 2024 | insert           | 1               |",
    "+-------+------+------------------+-----------------+",
    ], &batches }
}
