use std::sync::Arc;

use arrow_array::{types::UInt8Type, Array, DictionaryArray, RecordBatch, StringArray};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use deltalake::DeltaOps;
use futures::StreamExt;

#[tokio::test]
async fn test_read_write_dictionary() {
    let schema = ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        dict_field("str", ArrowDataType::Utf8),
    ]);
    let schema = Arc::new(schema);

    let delta_schema: deltalake::Schema = schema.try_into().unwrap();
    println!("{:?}", delta_schema);

    DeltaOps::try_from_uri("./data/test.delta")
        .await
        .unwrap()
        .create()
        .with_columns(delta_schema.get_fields().clone())
        .with_partition_columns(vec!["id"])
        .await
        .unwrap();

    DeltaOps::try_from_uri("./data/test.delta")
        .await
        .unwrap()
        .write(vec![batch()])
        .with_partition_columns(vec!["id"])
        .with_save_mode(deltalake::action::SaveMode::Append)
        .await
        .unwrap();

    let (_, mut stream) = DeltaOps::try_from_uri("./data/test.delta")
        .await
        .unwrap()
        .load()
        .await
        .unwrap();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        println!("{:?}", batch.schema());
        let display = pretty_format_batches(&vec![batch]).unwrap();
        println!("{}", display);
    }
}

fn batch() -> RecordBatch {
    let id_array: Arc<dyn Array> = Arc::new(StringArray::from_iter_values(
        (0..100).map(|i| (i % 2).to_string()),
    ));

    let strs = vec!["DeltaLake"; 100];
    let str_array: DictionaryArray<UInt8Type> = strs.into_iter().collect();
    let str_array: Arc<dyn Array> = Arc::new(str_array);

    RecordBatch::try_from_iter(vec![("id", id_array), ("str", str_array)]).unwrap()
}

fn dict_field(name: impl Into<String>, value_type: ArrowDataType) -> ArrowField {
    ArrowField::new(
        name,
        ArrowDataType::Dictionary(Box::new(ArrowDataType::UInt8), Box::new(value_type)),
        false,
    )
}
