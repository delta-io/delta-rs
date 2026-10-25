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

#[tokio::test]
async fn test_read_stable_shredded_variant_values() -> Result<()> {
    // Databricks output after REORG TABLE ... APPLY (SHRED VARIANT),
    // with a minimal Delta log describing the synthetic body column.
    let url = ensure_table_uri("../test/tests/data/variant-shredded")?;
    let table = deltalake_core::open_table(url).await?;
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state())?;
    ctx.register_table("demo", table.table_provider().await?)?;
    let batches = ctx.sql("SELECT body FROM demo").await?.collect().await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
    let variants = VariantArray::try_new(batch.column_by_name("body").unwrap())?;
    let value = variants.value(0);
    assert_eq!(
        value.get_object_field("id").and_then(variant_as_i64),
        Some(1)
    );
    let message = value.get_object_field("message").unwrap();
    assert_eq!(message.as_string(), Some("hello shredded variant"));
    let tags = value.get_object_field("tags").unwrap();
    let tag = tags.as_list().unwrap().get(1).unwrap();
    assert_eq!(tag.as_string(), Some("datafusion"));
    Ok(())
}

#[tokio::test]
async fn test_read_mixed_variant_files_and_nulls() -> Result<()> {
    use parquet::arrow::ArrowWriter;
    use serde_json::{Value, json};

    let fixture = std::path::Path::new("../test/tests/data/variant-shredded");
    let dir = tempfile::tempdir()?;
    std::fs::create_dir(dir.path().join("_delta_log"))?;
    std::fs::copy(
        fixture.join("part-00000.parquet"),
        dir.path().join("part-00000.parquet"),
    )?;

    let mut builder = VariantArrayBuilder::new(3);
    builder
        .new_object()
        .with_field("id", 2_i64)
        .with_field("message", "unshredded")
        .finish();
    builder.append_variant(Variant::Null);
    builder.append_null();
    let values = builder.build();
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![values.field("body")])),
        vec![ArrayRef::from(values)],
    )?;
    let path = dir.path().join("part-00001.parquet");
    let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    let mut log = std::fs::read_to_string(fixture.join("_delta_log/00000000000000000000.json"))?;
    let add: Value = json!({"add": {
        "path": "part-00001.parquet", "partitionValues": {},
        "size": std::fs::metadata(path)?.len(), "modificationTime": 0,
        "dataChange": true, "stats": "{\"numRecords\":3}"
    }});
    log.push_str(&format!("{add}\n"));
    // A third file shreds only id; message must survive in the residual binary.
    let mut partial = VariantArrayBuilder::new(1);
    partial
        .new_object()
        .with_field("id", 3_i64)
        .with_field("message", "partial")
        .finish();
    let schema = parquet::variant::ShreddedSchemaBuilder::new()
        .with_path("id", &ArrowDataType::Int64)?
        .build();
    let partial = parquet::variant::shred_variant(&partial.build(), &schema)?;
    let partial = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![partial.field("body")])),
        vec![ArrayRef::from(partial)],
    )?;
    let path = dir.path().join("part-00002.parquet");
    let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, partial.schema(), None)?;
    writer.write(&partial)?;
    writer.close()?;
    let add = json!({"add": {"path":"part-00002.parquet","partitionValues":{},
        "size":std::fs::metadata(path)?.len(),"modificationTime":0,"dataChange":true,"stats":"{\"numRecords\":1}"}});
    log.push_str(&format!("{add}\n"));
    std::fs::write(dir.path().join("_delta_log/00000000000000000000.json"), log)?;

    let table = deltalake_core::open_table(Url::from_directory_path(dir.path()).unwrap()).await?;
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state())?;
    ctx.register_table("demo", table.table_provider().await?)?;
    assert_eq!(
        count_value(
            &ctx.sql("SELECT COUNT(*) FROM demo")
                .await?
                .collect()
                .await?
        ),
        5
    );
    assert_eq!(
        count_value(
            &ctx.sql("SELECT COUNT(*) FROM demo WHERE body IS NULL")
                .await?
                .collect()
                .await?
        ),
        1
    );

    let batches = ctx
        .sql("SELECT body FROM demo WHERE body IS NOT NULL")
        .await?
        .collect()
        .await?;
    let mut ids = Vec::new();
    let mut variant_nulls = 0;
    for batch in batches {
        let values = VariantArray::try_new(batch.column(0))?;
        for row in 0..values.len() {
            assert!(!values.is_null(row));
            let value = values.value(row);
            match value {
                Variant::Null => variant_nulls += 1,
                _ => {
                    let id = value
                        .get_object_field("id")
                        .and_then(variant_as_i64)
                        .unwrap();
                    if id == 3 {
                        let message = value.get_object_field("message").unwrap();
                        assert_eq!(message.as_string(), Some("partial"));
                    }
                    ids.push(id);
                }
            }
        }
    }
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(variant_nulls, 1);
    Ok(())
}

fn write_variant_table_fixture(
    batch: &RecordBatch,
    fields: serde_json::Value,
) -> Result<tempfile::TempDir> {
    use parquet::arrow::ArrowWriter;
    use serde_json::json;

    let dir = tempfile::tempdir()?;
    std::fs::create_dir(dir.path().join("_delta_log"))?;
    let path = dir.path().join("part.parquet");
    let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    let actions = [
        json!({"protocol": {"minReaderVersion":3,"minWriterVersion":7,
            "readerFeatures":["variantType","variantShredding"],
            "writerFeatures":["variantType","variantShredding"]}}),
        json!({"metaData": {"id":"e6630be2-e604-4653-bbd3-3b6eaf33d670",
            "format":{"provider":"parquet","options":{}},
            "schemaString":json!({"type":"struct","fields":fields}).to_string(),
            "partitionColumns":[],"configuration":{"delta.enableVariantShredding":"true"}}}),
        json!({"add":{"path":"part.parquet","partitionValues":{},
            "size":std::fs::metadata(path)?.len(),"modificationTime":0,"dataChange":true,
            "stats":json!({"numRecords":batch.num_rows()}).to_string()}}),
    ];
    std::fs::write(
        dir.path().join("_delta_log/00000000000000000000.json"),
        actions
            .iter()
            .map(|action| format!("{action}\n"))
            .collect::<String>(),
    )?;
    Ok(dir)
}

#[tokio::test]
async fn test_read_nested_shredded_variants() -> Result<()> {
    use arrow::buffer::OffsetBuffer;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;

    let input = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
        "../test/tests/data/variant-shredded/part-00000.parquet",
    )?)?
    .build()?
    .next()
    .unwrap()?;
    let body = input.column_by_name("body").unwrap().clone();
    let variant_field = input.schema().field_with_name("body")?.clone();
    let nested_fields = vec![Arc::new(variant_field.clone().with_name("v"))].into();
    let nested = StructArray::try_new(nested_fields, vec![body.clone()], None)?;
    let item = Arc::new(variant_field.clone().with_name("item"));
    let list = ListArray::try_new(
        item,
        OffsetBuffer::new(vec![0, 1].into()),
        body.clone(),
        None,
    )?;
    let entries = StructArray::try_new(
        vec![
            Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
            Arc::new(variant_field.with_name("value")),
        ]
        .into(),
        vec![Arc::new(StringArray::from(vec!["a"])), body],
        None,
    )?;
    let map = MapArray::try_new(
        Arc::new(ArrowField::new(
            "entries",
            entries.data_type().clone(),
            false,
        )),
        OffsetBuffer::new(vec![0, 1].into()),
        entries,
        None,
        false,
    )?;
    let batch = RecordBatch::try_from_iter(vec![
        ("nested", Arc::new(nested) as ArrayRef),
        ("list", Arc::new(list) as ArrayRef),
        ("map", Arc::new(map) as ArrayRef),
    ])?;
    let variant_field = json!({"name":"v","type":"variant","nullable":true,"metadata":{}});
    let fields = json!([
        {"name":"nested","type":{"type":"struct","fields":[variant_field]},"nullable":true,"metadata":{}},
        {"name":"list","type":{"type":"array","elementType":"variant","containsNull":true},"nullable":true,"metadata":{}},
        {"name":"map","type":{"type":"map","keyType":"string","valueType":"variant","valueContainsNull":true},"nullable":true,"metadata":{}}
    ]);
    let dir = write_variant_table_fixture(&batch, fields)?;
    let table = deltalake_core::open_table(Url::from_directory_path(dir.path()).unwrap()).await?;
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state())?;
    ctx.register_table("demo", table.table_provider().await?)?;
    let batches = ctx
        .sql("SELECT nested.v AS v, list, map FROM demo WHERE nested IS NOT NULL")
        .await?
        .collect()
        .await?;
    let row = &batches[0];
    let scalar = VariantArray::try_new(row.column(0))?;
    let list = row.column(1).as_list::<i32>();
    let list_value = VariantArray::try_new(list.values())?;
    let map = row.column(2).as_map();
    let map_value = VariantArray::try_new(map.entries().column(1))?;
    for array in [&scalar, &list_value, &map_value] {
        assert_eq!(
            array
                .value(0)
                .get_object_field("id")
                .and_then(variant_as_i64),
            Some(1)
        );
    }
    // Project only the nested Variant; all of its physical typed_value children must survive.
    let projected = ctx
        .sql("SELECT nested.v FROM demo")
        .await?
        .collect()
        .await?;
    let value = VariantArray::try_new(projected[0].column(0))?;
    assert_eq!(
        value
            .value(0)
            .get_object_field("id")
            .and_then(variant_as_i64),
        Some(1)
    );
    Ok(())
}

#[tokio::test]
async fn test_shredded_variant_with_column_mapping_partition_and_deletion_vector() -> Result<()> {
    use arrow::compute::concat;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::{Value, json};

    let input = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
        "../test/tests/data/variant-shredded/part-00000.parquet",
    )?)?
    .build()?
    .next()
    .unwrap()?;
    let body = input.column_by_name("body").unwrap();
    let bodies = concat(&[body.as_ref(), body.as_ref()])?;
    let mut body_field = input
        .schema()
        .field_with_name("body")?
        .clone()
        .with_name("col-body");
    body_field
        .metadata_mut()
        .insert("PARQUET:field_id".into(), "1".into());
    let row_field = ArrowField::new("col-row_id", ArrowDataType::Int64, true).with_metadata(
        std::collections::HashMap::from([("PARQUET:field_id".into(), "2".into())]),
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![body_field, row_field])),
        vec![bodies, Arc::new(Int64Array::from(vec![1, 2]))],
    )?;
    let fields = json!([
        {"name":"body","type":"variant","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-body"}},
        {"name":"row_id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-row_id"}},
        {"name":"p","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-p"}}
    ]);
    let dir = write_variant_table_fixture(&batch, fields)?;
    let path = dir.path().join("_delta_log/00000000000000000000.json");
    let mut actions = std::fs::read_to_string(&path)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| datafusion::common::exec_datafusion_err!("{err}"))?;
    for kind in ["readerFeatures", "writerFeatures"] {
        actions[0]["protocol"][kind]
            .as_array_mut()
            .unwrap()
            .extend([json!("columnMapping"), json!("deletionVectors")]);
    }
    let metadata = &mut actions[1]["metaData"];
    metadata["partitionColumns"] = json!(["p"]);
    metadata["configuration"]["delta.columnMapping.mode"] = json!("name");
    metadata["configuration"]["delta.columnMapping.maxColumnId"] = json!("3");
    metadata["configuration"]["delta.enableDeletionVectors"] = json!("true");
    actions[2]["add"]["partitionValues"] = json!({"col-p":"a"});
    // Portable RoaringTreemap containing only physical row 0 (34 bytes, Z85 padded).
    actions[2]["add"]["deletionVector"] = json!({"storageType":"i",
        "pathOrInlineDv":"^Bg9^0rr910000000000iXQKl0rr91000005c8Xg00000",
        "sizeInBytes":34,"cardinality":1});
    std::fs::write(
        path,
        actions.iter().map(|a| format!("{a}\n")).collect::<String>(),
    )?;

    let table = deltalake_core::open_table(Url::from_directory_path(dir.path()).unwrap()).await?;
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state())?;
    ctx.register_table("demo", table.table_provider().await?)?;
    let rows = ctx
        .sql("SELECT row_id, body, p FROM demo WHERE row_id > 0 AND p = 'a'")
        .await?
        .collect()
        .await?;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let row = rows.iter().find(|batch| batch.num_rows() > 0).unwrap();
    assert_eq!(
        row.column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        2
    );
    let value = VariantArray::try_new(row.column(1))?;
    assert_eq!(
        value
            .value(0)
            .get_object_field("id")
            .and_then(variant_as_i64),
        Some(1)
    );
    let partition = arrow::compute::cast(row.column(2), &ArrowDataType::Utf8)?;
    assert_eq!(partition.as_string::<i32>().value(0), "a");
    Ok(())
}
