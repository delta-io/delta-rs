#![cfg(feature = "datafusion")]

use std::sync::Arc;

use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use deltalake_core::kernel::{DataType, StructField, StructType};
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaTable, DeltaTableBuilder, ensure_table_uri};
use parquet::variant::{Variant, VariantArray, VariantArrayBuilder, VariantBuilderExt};
use url::Url;

fn open_fs_path(path: &str) -> DeltaTable {
    let url = Url::from_directory_path(std::path::Path::new(path).canonicalize().unwrap()).unwrap();
    DeltaTableBuilder::from_url(url).unwrap().build().unwrap()
}

fn count_value(batches: &[RecordBatch]) -> i64 {
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    batches[0]
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>()
        .value(0)
}

fn variant_as_i64(variant: Variant<'_, '_>) -> Option<i64> {
    match variant {
        Variant::Int8(value) => Some(i64::from(value)),
        Variant::Int16(value) => Some(i64::from(value)),
        Variant::Int32(value) => Some(i64::from(value)),
        Variant::Int64(value) => Some(value),
        _ => None,
    }
}

fn assert_key_variant(variants: &VariantArray, row: usize, expected_key: i64) {
    assert!(variants.is_valid(row), "variant row should not be null");
    let variant = variants.value(row);
    assert_eq!(
        variant.get_object_field("key").and_then(variant_as_i64),
        Some(expected_key)
    );
}

#[tokio::test]
async fn test_load_spark_variant_checkpoint_table() {
    let path = "../test/tests/data/spark-variant-checkpoint";
    let url = Url::from_directory_path(std::path::Path::new(path).canonicalize().unwrap()).unwrap();
    let table = DeltaTableBuilder::from_url(url)
        .unwrap()
        .load()
        .await
        .expect("Failed to load Spark variant fixture");

    let snapshot = table.snapshot().expect("snapshot should be loaded");
    assert_eq!(snapshot.version(), 2);
    assert_eq!(snapshot.protocol().min_reader_version(), 3);
    assert!(
        snapshot
            .protocol()
            .reader_features()
            .is_some_and(|features| features.iter().any(|f| f.as_ref() == "variantType-preview"))
    );

    let schema = snapshot.schema();
    let variant_fields = schema
        .fields()
        .filter(|field| matches!(field.data_type(), DataType::Variant(_)))
        .count();
    assert_eq!(variant_fields, 1);
    assert_eq!(
        schema.field("array_of_variants").unwrap().name(),
        "array_of_variants"
    );
}

#[tokio::test]
async fn test_load_spark_variant_stable_feature_checkpoint_table() {
    let path = "../test/tests/data/spark-variant-stable-feature-checkpoint";
    let url = Url::from_directory_path(std::path::Path::new(path).canonicalize().unwrap()).unwrap();
    let table = DeltaTableBuilder::from_url(url)
        .unwrap()
        .load()
        .await
        .expect("Failed to load Spark stable variant fixture");

    let snapshot = table.snapshot().expect("snapshot should be loaded");
    assert_eq!(snapshot.version(), 1);
    assert_eq!(snapshot.protocol().min_reader_version(), 3);
    assert!(
        snapshot
            .protocol()
            .reader_features()
            .is_some_and(|features| features.iter().any(|f| f.as_ref() == "variantType"))
    );

    let schema = snapshot.schema();
    assert!(matches!(
        schema.field("v").unwrap().data_type(),
        DataType::Variant(_)
    ));
    assert!(matches!(
        schema.field("struct_of_variants").unwrap().data_type(),
        DataType::Struct(_)
    ));
}

#[tokio::test]
async fn test_datafusion_shredded_variant_fixture() -> Result<()> {
    let table = open_fs_path("../test/tests/data/spark-shredded-variant-preview-delta");
    let err = table.table_provider().await.unwrap_err();
    assert!(
        err.to_string().contains("VariantShreddingPreview"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn test_write_variant_data_end_to_end() -> Result<()> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().unwrap();
    let columns = StructType::try_new([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("payload", DataType::unshredded_variant()),
    ])
    .unwrap();

    let table = DeltaTable::try_from_url(ensure_table_uri(table_uri).unwrap())
        .await?
        .create()
        .with_columns(columns.fields().cloned())
        .await?;

    let mut payload = VariantArrayBuilder::new(2);
    payload.new_object().with_field("key", 1_i64).finish();
    payload.new_object().with_field("key", 2_i64).finish();
    let payload = payload.build();
    let payload_field = payload.field("payload");

    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, true),
            payload_field,
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            ArrayRef::from(payload),
        ],
    )?;

    let table = table
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    let protocol = table.snapshot().unwrap().protocol();
    assert!(
        protocol
            .reader_features()
            .is_some_and(|features| features.iter().any(|f| f.as_ref() == "variantType"))
    );

    let ctx = SessionContext::new();
    ctx.register_table("demo", table.table_provider().await.unwrap())?;

    let total = ctx
        .sql("SELECT COUNT(*) AS cnt FROM demo")
        .await?
        .collect()
        .await?;
    assert_eq!(count_value(&total), 2);

    let rows = ctx
        .sql("SELECT id, payload FROM demo ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(rows.len(), 1);

    let ids = rows[0]
        .column_by_name("id")
        .unwrap()
        .as_primitive::<arrow_array::types::Int64Type>();
    let payloads = VariantArray::try_new(rows[0].column_by_name("payload").unwrap()).unwrap();
    assert_eq!(ids.value(0), 1);
    assert_key_variant(&payloads, 0, 1);
    assert_eq!(ids.value(1), 2);
    assert_key_variant(&payloads, 1, 2);

    Ok(())
}
