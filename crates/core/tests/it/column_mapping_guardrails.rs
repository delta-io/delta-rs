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

/// Physical partition column name baked into the fixture's `_delta_log` metadata.
const PHYSICAL_PARTITION_NAME: &str = "col-173b4db9-b5ad-427f-9e75-516aae37fbbb";

/// Reads the entire table back via DataFusion, returning all row batches.
#[cfg(feature = "datafusion")]
async fn read_all(table: &DeltaTable) -> TestResult<Vec<arrow_array::RecordBatch>> {
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await?)?;
    Ok(ctx.sql("SELECT * FROM t").await?.collect().await?)
}

#[cfg(feature = "datafusion")]
fn count_rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Appending to a column-mapped table writes physical column names, keeps physical partition
/// keys in `partitionValues`, uses random-prefixed (non-Hive) paths, and round-trips the data
/// back under the logical schema.
#[cfg(feature = "datafusion")]
#[tokio::test]
async fn column_mapping_append_roundtrip() -> TestResult {
    let (_temp_dir, table_path, table) = copied_column_mapping_table().await?;
    let before = collect_data_files(&table_path)?;

    let table = table.write(vec![column_mapping_batch()]).await?;

    // A new data file was written...
    let after = collect_data_files(&table_path)?;
    let new_files: Vec<_> = after.difference(&before).collect();
    assert_eq!(new_files.len(), 1, "exactly one new data file expected");
    // ...under a random prefix, not a Hive-style `col=value/` directory.
    let new_file = new_files[0].to_string_lossy();
    assert!(
        !new_file.contains('='),
        "column-mapped writes must not use Hive-style paths, got: {new_file}"
    );

    // The new commit records partition values under the physical column name (the logical name
    // still legitimately appears in commitInfo's operationParameters).
    let commit = std::fs::read_to_string(table_path.join("_delta_log/00000000000000000001.json"))?;
    assert!(
        commit.contains(PHYSICAL_PARTITION_NAME),
        "partitionValues should use the physical column name, commit: {commit}"
    );

    // Data reads back under the logical schema, including the appended row.
    let batches = read_all(&table).await?;
    assert_eq!(count_rows(&batches), 6, "5 fixture rows + 1 appended");

    Ok(())
}

/// Schema evolution on a column-mapped table is rejected for now (handled in a follow-up).
#[cfg(feature = "datafusion")]
#[tokio::test]
async fn column_mapping_schema_evolution_rejected() -> TestResult {
    use deltalake_core::operations::write::SchemaMode;

    let (_temp_dir, table_path, table) = copied_column_mapping_table().await?;
    let before = collect_data_files(&table_path)?;

    let err = table
        .write(vec![column_mapping_batch()])
        .with_schema_mode(SchemaMode::Merge)
        .await
        .expect_err("schema evolution should be rejected on column-mapped tables");
    assert!(
        err.to_string()
            .contains("Schema evolution on column-mapped tables"),
        "unexpected error: {err}"
    );
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

/// UPDATE rewrites a column-mapped table and the change reads back under the logical schema.
#[cfg(feature = "datafusion")]
#[tokio::test]
async fn column_mapping_update_roundtrip() -> TestResult {
    use datafusion::prelude::{SessionContext, lit};

    let (_temp_dir, _table_path, table) = copied_column_mapping_table().await?;
    let (table, _metrics) = table
        .update()
        .with_update("Super Name", lit("Updated"))
        .await?;

    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await?)?;
    let updated = ctx
        .sql("SELECT * FROM t WHERE \"Super Name\" = 'Updated'")
        .await?
        .collect()
        .await?;
    assert_eq!(count_rows(&updated), 5, "every row should be updated");

    Ok(())
}

/// DELETE removes matching rows from a column-mapped table.
#[cfg(feature = "datafusion")]
#[tokio::test]
async fn column_mapping_delete_roundtrip() -> TestResult {
    use datafusion::prelude::{col, lit};

    let (_temp_dir, _table_path, table) = copied_column_mapping_table().await?;
    let (table, _metrics) = table
        .delete()
        .with_predicate(col("Super Name").eq(lit("Timothy Lamb")))
        .await?;

    let batches = read_all(&table).await?;
    assert_eq!(count_rows(&batches), 4, "one of five rows deleted");

    Ok(())
}

/// MERGE inserts a not-matched row into a column-mapped table.
#[cfg(feature = "datafusion")]
#[tokio::test]
async fn column_mapping_merge_roundtrip() -> TestResult {
    use datafusion::common::Column;
    use datafusion::logical_expr::Expr;
    use datafusion::prelude::SessionContext;

    // Build qualified column refs explicitly so the spaces in the column names don't trip up
    // identifier parsing.
    let target_name = Expr::Column(Column::new(Some("target"), "Super Name"));
    let source_name = Expr::Column(Column::new(Some("source"), "Super Name"));
    let source_company = Expr::Column(Column::new(Some("source"), "Company Very Short"));
    let source_super = Expr::Column(Column::new(Some("source"), "Super Name"));

    let (_temp_dir, _table_path, table) = copied_column_mapping_table().await?;
    let ctx = SessionContext::new();
    let source = ctx.read_batch(column_mapping_batch())?;

    let (table, _metrics) = table
        .merge(source, target_name.eq(source_name))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_insert(|insert| {
            insert
                .set("Company Very Short", source_company)
                .set("Super Name", source_super)
        })?
        .await?;

    let batches = read_all(&table).await?;
    assert_eq!(count_rows(&batches), 6, "one row inserted via merge");

    Ok(())
}

/// OPTIMIZE is still rejected on column-mapped tables (handled in a follow-up).
#[cfg(feature = "datafusion")]
#[tokio::test]
async fn column_mapping_optimize_still_rejected() -> TestResult {
    let (_temp_dir, table_path, table) = copied_column_mapping_table().await?;
    let before = collect_data_files(&table_path)?;

    let err = table
        .optimize()
        .await
        .expect_err("optimize should still reject column-mapped tables");
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

/// Writing change data to a column-mapped table is correct: the `_change_data` files use physical
/// column names, and a column-mapping-aware reader (here kernel's `TableChanges`) resolves them
/// back to the logical schema. This is the round-trip delta-rs's own (CM-unaware) `load_cdf`
/// cannot yet do — proving the write side is spec-correct independent of that reader gap.
#[cfg(feature = "datafusion")]
#[tokio::test(flavor = "multi_thread")]
async fn column_mapping_cdf_write_is_kernel_readable() -> TestResult {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::{col, lit};
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
    use delta_kernel::object_store::DynObjectStore;
    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::schema::{DataType as KernelDataType, StructField, StructType};
    use delta_kernel::table_changes::TableChanges;
    use delta_kernel::transaction::create_table::create_table;

    // 1. Create a name-mode column-mapped table with CDF enabled (via kernel, which assigns the
    //    physical names/ids; delta-rs can't create CM tables yet).
    let tmp = tempfile::tempdir()?;
    let table_url = Url::from_directory_path(tmp.path()).unwrap();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );
    let schema = Arc::new(StructType::try_new([
        StructField::nullable("id", KernelDataType::INTEGER),
        StructField::nullable("value", KernelDataType::STRING),
    ])?);
    let _ = create_table(table_url.as_str(), schema, "delta-rs-test/1.0")
        .with_table_properties([
            ("delta.columnMapping.mode", "name"),
            ("delta.enableChangeDataFeed", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // 2. Append two rows, then delete one — through delta-rs's column-mapping write path.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, true),
        Field::new("value", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )?;
    let table = open_table(table_url.clone()).await?;
    let table = table.write(vec![batch]).await?;
    let (_table, _metrics) = table.delete().with_predicate(col("id").eq(lit(2))).await?;

    // 3. Read the change feed back via kernel (column-mapping aware) and assert the delete event
    //    surfaces under the LOGICAL schema with the correct value — proving physical->logical
    //    resolution of the physically-named _change_data file.
    let table_changes = TableChanges::try_new(table_url, engine.as_ref(), 0, None)?;
    let scan = table_changes.into_scan_builder().build()?;

    let mut delete_values = Vec::new();
    for result in scan.execute(engine.clone())? {
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(result?)?.into();
        // Logical column names are present (not the physical col-<uuid> names on disk).
        assert!(
            batch.schema().column_with_name("id").is_some()
                && batch.schema().column_with_name("value").is_some(),
            "change feed should expose logical columns, got {:?}",
            batch.schema()
        );
        let change_type = arrow::compute::cast(
            batch.column_by_name("_change_type").unwrap(),
            &ArrowDataType::Utf8,
        )?;
        let change_type = change_type.as_string::<i32>();
        let values =
            arrow::compute::cast(batch.column_by_name("value").unwrap(), &ArrowDataType::Utf8)?;
        let values = values.as_string::<i32>();
        for i in 0..batch.num_rows() {
            if change_type.value(i) == "delete" {
                delete_values.push(values.value(i).to_string());
            }
        }
    }

    assert_eq!(
        delete_values,
        vec!["b".to_string()],
        "expected exactly one delete change event for the row id=2 (value 'b')"
    );

    Ok(())
}
