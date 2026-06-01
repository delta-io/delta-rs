//! End-to-end test of the HuggingFace adapter against the real Hub.
//!
//! Marked `#[ignore]` and skipped unless `HF_TOKEN` and `HF_TEST_REPO` are set,
//! so it never runs without credentials.
//!
//! Run with credentials:
//!   HF_TOKEN=<token> \
//!   HF_TEST_REPO=hf://datasets/<owner>/<repo>/delta_opendal_test \
//!   cargo test -p deltalake-hf -- --ignored

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake_core::DeltaTableBuilder;
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use url::Url;

#[tokio::test]
#[ignore = "requires HF_TOKEN and HF_TEST_REPO and network access"]
async fn hf_roundtrip() {
    let (Ok(token), Ok(repo)) = (std::env::var("HF_TOKEN"), std::env::var("HF_TEST_REPO")) else {
        eprintln!("HF_TOKEN / HF_TEST_REPO not set; skipping");
        return;
    };

    deltalake_hf::register_handlers(None);

    let storage_options = HashMap::from([("hf.token".to_string(), token)]);

    let mut table = CreateBuilder::new()
        .with_location(&repo)
        .with_storage_options(storage_options.clone())
        .with_columns(vec![
            StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true),
            StructField::new("name", DataType::Primitive(PrimitiveType::String), true),
        ])
        .await
        .expect("create table");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, true),
        Field::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let mut writer = RecordBatchWriter::for_table(&table).expect("writer for table");
    writer.write(batch).await.expect("buffer batch");
    let version = writer
        .flush_and_commit(&mut table)
        .await
        .expect("flush and commit");
    assert_eq!(version, 1);

    let reopened = DeltaTableBuilder::from_url(Url::parse(&repo).unwrap())
        .unwrap()
        .with_storage_options(storage_options)
        .load()
        .await
        .expect("reopen table");
    assert_eq!(reopened.version(), Some(1));
}
