#![cfg(all(feature = "arrow", feature = "parquet"))]

use std::sync::Arc;

use arrow_array::{types::{UInt8Type, UInt16Type}, Array, DictionaryArray, RecordBatch, StringArray, UInt16Array};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use deltalake::DeltaOps;
use futures::StreamExt;
use tempdir::TempDir;

#[tokio::test]
async fn test_read_write_dictionary() {
    let arrow_schema = ArrowSchema::new(vec![
        // partition columns with datafusion need to be u16
        dict_field("id", ArrowDataType::UInt16, ArrowDataType::Utf8),
        dict_field("str", ArrowDataType::UInt8, ArrowDataType::Utf8),
    ]);

    let arrow_schema = Arc::new(arrow_schema);
    let delta_schema =
        deltalake::Schema::try_from_arrow_with_metadata(arrow_schema.as_ref()).unwrap();

    let temp_dir = TempDir::new("read_write_dictionary").unwrap();
    let table_path = format!(
        "{}/read_write_dictionary.delta",
        temp_dir.path().to_str().unwrap()
    );

    DeltaOps::try_from_uri(table_path.clone())
        .await
        .unwrap()
        .create()
        .with_columns(delta_schema.get_fields().clone())
        .with_partition_columns(vec!["id"])
        .await
        .unwrap();

    DeltaOps::try_from_uri(table_path.clone())
        .await
        .unwrap()
        .write(vec![batch()])
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .with_partition_columns(vec!["id"])
        .await
        .unwrap();

    let (_, mut stream) = DeltaOps::try_from_uri(table_path)
        .await
        .unwrap()
        .load()
        .await
        .unwrap();

    // Datafusion reorders partition cols
    let read_schema = Arc::new(ArrowSchema::new(vec![
        dict_field("str", ArrowDataType::UInt8, ArrowDataType::Utf8),
        dict_field("id", ArrowDataType::UInt16, ArrowDataType::Utf8),
    ]));

    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        assert_eq!(batch.schema(), read_schema);
    }
}

fn batch() -> RecordBatch {
    let keys = UInt16Array::from_iter_values((0..100).map(|i| (i % 2)));
    let values = StringArray::from_iter_values(vec!["0", "1"]);
    let id_array = DictionaryArray::<UInt16Type>::new(keys, Arc::new(values));
    let id_array: Arc<dyn Array> = Arc::new(id_array);

    let strs = vec!["DeltaLake"; 100];
    let str_array: DictionaryArray<UInt8Type> = strs.into_iter().collect();
    let str_array: Arc<dyn Array> = Arc::new(str_array);

    RecordBatch::try_from_iter(vec![("id", id_array), ("str", str_array)]).unwrap()
}

fn dict_field(name: impl Into<String>, key_type: ArrowDataType, value_type: ArrowDataType) -> ArrowField {
    ArrowField::new(
        name,
        ArrowDataType::Dictionary(Box::new(key_type), Box::new(value_type)),
        false,
    )
}
