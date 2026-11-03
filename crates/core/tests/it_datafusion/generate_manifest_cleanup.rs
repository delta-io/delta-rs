//! `generate()` removes stale symlink manifests and nothing else under `_symlink_format_manifest/`.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use deltalake_core::DeltaTable;
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::operations::create::CreateBuilder;
use futures::StreamExt as _;
use object_store::ObjectStoreExt as _;
use object_store::path::Path as ObjectStorePath;

fn batch(ids: &[i32], parts: &[&str]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("p", ArrowDataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(parts.to_vec())),
        ],
    )
    .unwrap()
}

async fn partitioned_table(path: &std::path::Path) -> DeltaTable {
    let table = CreateBuilder::new()
        .with_location(path.to_str().unwrap())
        .with_columns(vec![
            StructField::new("id", DataType::Primitive(PrimitiveType::Integer), false),
            StructField::new("p", DataType::Primitive(PrimitiveType::String), false),
        ])
        .with_partition_columns(vec!["p"])
        .await
        .unwrap();
    table
        .write(vec![batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap()
}

/// A partition whose files were all deleted must lose its manifest on the next `generate()`.
#[tokio::test]
async fn generate_removes_manifest_of_emptied_partition() {
    let dir = tempfile::tempdir().unwrap();
    let table = partitioned_table(dir.path()).await;
    let table = table.generate().await.unwrap();
    let table = table
        .delete()
        .with_predicate(datafusion::logical_expr::col("p").eq(datafusion::logical_expr::lit("b")))
        .await
        .unwrap()
        .0;
    let table = table.generate().await.unwrap();

    let store = table.object_store();
    let manifests: Vec<String> = store
        .list(Some(&ObjectStorePath::from("_symlink_format_manifest")))
        .map(|m| m.unwrap().location.to_string())
        .collect()
        .await;
    assert!(
        manifests.iter().all(|m| !m.contains("p=b")),
        "stale manifest for emptied partition must be removed: {manifests:?}"
    );
    assert!(
        manifests.iter().any(|m| m.contains("p=a")),
        "live partition manifest must remain: {manifests:?}"
    );
}

/// Only generated manifests are removed: an unrelated object under the prefix survives.
#[tokio::test]
async fn generate_keeps_non_manifest_objects_under_manifest_root() {
    let dir = tempfile::tempdir().unwrap();
    let table = partitioned_table(dir.path()).await;
    let table = table.generate().await.unwrap();

    let user_file = ObjectStorePath::from("_symlink_format_manifest/notes.txt");
    table
        .object_store()
        .put(&user_file, bytes::Bytes::from_static(b"keep me").into())
        .await
        .unwrap();

    let table = table.generate().await.unwrap();
    assert!(
        table.object_store().head(&user_file).await.is_ok(),
        "non-manifest object under _symlink_format_manifest/ must not be deleted"
    );
}
