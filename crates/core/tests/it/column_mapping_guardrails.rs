use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use deltalake_core::kernel::{
    ColumnMetadataKey, DataType, MetadataValue, StructField, TableFeatures,
};
use deltalake_core::writer::{JsonWriter, RecordBatchWriter};
use deltalake_core::{DeltaTable, DeltaTableError, open_table};
use tempfile::TempDir;
use url::Url;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn assert_unsupported_column_mapping_write(err: &DeltaTableError, operation: &str) {
    let message = err.to_string();
    assert!(
        message.contains("column mapping writes are not supported"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains(operation),
        "expected operation `{operation}` in error: {message}"
    );
}

#[cfg(feature = "datafusion")]
fn column_mapping_batch() -> arrow_array::RecordBatch {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("Company Very Short", ArrowDataType::Utf8, true),
        Field::new("Super Name", ArrowDataType::Utf8, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["BMS"])),
            Arc::new(StringArray::from(vec!["New Customer"])),
        ],
    )
    .unwrap()
}

fn simple_fields() -> Vec<StructField> {
    vec![StructField::nullable("id", DataType::INTEGER)]
}

async fn simple_table() -> TestResult<DeltaTable> {
    Ok(DeltaTable::new_in_memory()
        .create()
        .with_columns(simple_fields())
        .await?)
}

async fn copied_column_mapping_table() -> TestResult<(TempDir, PathBuf, DeltaTable)> {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../test/tests/data/table_with_column_mapping");
    let temp_dir = tempfile::tempdir()?;
    fs_extra::dir::copy(&fixture, temp_dir.path(), &Default::default())?;
    let table_path = temp_dir.path().join("table_with_column_mapping");
    let table_url = Url::from_directory_path(table_path.canonicalize()?).unwrap();
    let table = open_table(table_url).await?;

    Ok((temp_dir, table_path, table))
}

fn collect_data_files(root: &Path) -> TestResult<BTreeSet<PathBuf>> {
    fn visit(root: &Path, dir: &Path, files: &mut BTreeSet<PathBuf>) -> TestResult {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "_delta_log") {
                    continue;
                }
                visit(root, &path, files)?;
            } else {
                files.insert(path.strip_prefix(root)?.to_path_buf());
            }
        }
        Ok(())
    }

    let mut files = BTreeSet::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn column_mapping_guardrails_write_builder_rejects_before_files() -> TestResult {
    use deltalake_core::protocol::SaveMode;

    let (_temp_dir, table_path, table) = copied_column_mapping_table().await?;
    let before = collect_data_files(&table_path)?;

    let err = table
        .clone()
        .write(vec![column_mapping_batch()])
        .await
        .expect_err("append should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "WRITE");
    assert_eq!(before, collect_data_files(&table_path)?);

    let err = table
        .write(vec![column_mapping_batch()])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .expect_err("overwrite should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "WRITE");
    assert_eq!(before, collect_data_files(&table_path)?);

    Ok(())
}

#[tokio::test]
async fn column_mapping_guardrails_legacy_writers_reject() -> TestResult {
    let (_temp_dir, table_path, table) = copied_column_mapping_table().await?;
    let table_url = Url::from_directory_path(table_path.canonicalize()?).unwrap();
    let arrow_schema = table.snapshot()?.snapshot().arrow_schema();

    let err = RecordBatchWriter::for_table(&table)
        .expect_err("record batch writer should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "RecordBatchWriter");

    let err =
        RecordBatchWriter::try_new_checked(table_url.as_str(), arrow_schema.clone(), None, None)
            .await
            .expect_err("checked record batch writer should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "RecordBatchWriter");

    let err =
        JsonWriter::for_table(&table).expect_err("json writer should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "JsonWriter");

    let err = JsonWriter::try_new(table_url, arrow_schema, None, None)
        .await
        .expect_err("json writer by URI should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "JsonWriter");

    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn column_mapping_guardrails_dml_rejects_before_files() -> TestResult {
    use datafusion::prelude::{SessionContext, lit};

    let (_temp_dir, table_path, table) = copied_column_mapping_table().await?;
    let before = collect_data_files(&table_path)?;

    let err = table
        .clone()
        .update()
        .with_update("Super Name", lit("Updated"))
        .await
        .expect_err("update should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "UPDATE");
    assert_eq!(before, collect_data_files(&table_path)?);

    let err = table
        .clone()
        .delete()
        .with_predicate(lit(true))
        .await
        .expect_err("delete should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "DELETE");
    assert_eq!(before, collect_data_files(&table_path)?);

    let ctx = SessionContext::new();
    let source = ctx.read_batch(column_mapping_batch())?;
    let err = table
        .clone()
        .merge(source, lit(true))
        .await
        .expect_err("merge should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "MERGE");
    assert_eq!(before, collect_data_files(&table_path)?);

    let err = table
        .optimize()
        .await
        .expect_err("optimize should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "OPTIMIZE");
    assert_eq!(before, collect_data_files(&table_path)?);

    Ok(())
}

#[tokio::test]
async fn column_mapping_guardrails_metadata_operations() -> TestResult {
    let (_temp_dir, table_path, table) = copied_column_mapping_table().await?;
    let before = collect_data_files(&table_path)?;

    let err = table
        .add_columns()
        .with_fields([StructField::nullable("new_col", DataType::STRING)])
        .await
        .expect_err("add column should reject column-mapped tables");
    assert_unsupported_column_mapping_write(&err, "ADD COLUMN");
    assert_eq!(before, collect_data_files(&table_path)?);

    let table = simple_table().await?;
    let err = table
        .set_tbl_properties()
        .with_properties(HashMap::from([(
            "delta.columnMapping.mode".to_string(),
            "name".to_string(),
        )]))
        .await
        .expect_err("setting column mapping mode should be rejected");
    assert_unsupported_column_mapping_write(&err, "SET TBLPROPERTIES");

    Ok(())
}

#[tokio::test]
async fn column_mapping_guardrails_create_rejects_activation_and_reserved_metadata() -> TestResult {
    let err = DeltaTable::new_in_memory()
        .create()
        .with_columns(simple_fields())
        .with_configuration([("delta.columnMapping.mode", Some("name"))])
        .await
        .expect_err("create should reject column mapping mode");
    assert_unsupported_column_mapping_write(&err, "CREATE TABLE");

    let mapped_field = StructField::nullable("id", DataType::INTEGER).with_metadata([
        (
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::Number(1),
        ),
        (
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
            MetadataValue::String("col-id".to_string()),
        ),
    ]);

    let err = DeltaTable::new_in_memory()
        .create()
        .with_columns([mapped_field])
        .await
        .expect_err("create should reject column mapping metadata");
    assert_unsupported_column_mapping_write(&err, "CREATE TABLE");

    Ok(())
}

#[tokio::test]
async fn column_mapping_guardrails_add_feature_still_allows_column_mapping() -> TestResult {
    simple_table()
        .await?
        .add_feature()
        .with_feature(TableFeatures::ColumnMapping)
        .with_allow_protocol_versions_increase(true)
        .await?;

    Ok(())
}
