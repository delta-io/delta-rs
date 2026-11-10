use std::fs::File;
use std::path::Path as FsPath;

use datafusion::arrow::array::{
    Array as _, ArrayRef, AsArray as _, FixedSizeListArray, Int64Array, LargeListArray, ListArray,
    MapArray, StringArray, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields, Int64Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_kernel::FileMeta;
use delta_kernel::PlanBuilder;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::column_name;
use delta_kernel::plans::ir::nodes::Agg;
use delta_kernel::plans::{Operation, PlanExecutor as _};
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField,
};
use rstest::rstest;
use tokio::runtime::Handle;
use url::Url;

use super::*;
use crate::DataFusionPlanExecutor;

// === Shared helpers ===

fn executor() -> DataFusionPlanExecutor {
    DataFusionPlanExecutor::new(SessionContext::new().task_ctx(), Handle::current())
}

/// A scan file for `path` carrying `constants`.
fn scan_file(path: &FsPath, constants: Vec<delta_kernel::expressions::Scalar>) -> ScanFile {
    ScanFile {
        meta: FileMeta {
            location: Url::from_file_path(path).unwrap(),
            last_modified: 0,
            size: std::fs::metadata(path).unwrap().len(),
        },
        file_constants: constants,
    }
}

fn long_batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter([("x", Arc::new(Int64Array::from(values.to_vec())) as _)]).unwrap()
}

fn write_parquet(dir: &FsPath, name: &str, batch: &RecordBatch) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    path
}

fn write_json(dir: &FsPath, name: &str, lines: &[&str]) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, lines.join("\n")).unwrap();
    path
}

fn schema(fields: impl IntoIterator<Item = StructField>) -> StructType {
    StructType::try_new(fields).unwrap()
}

/// `x LONG` only.
fn x_schema() -> StructType {
    schema([StructField::nullable("x", DataType::LONG)])
}

/// Runs the lowered scan on a session configured like [`crate::query::execute`].
async fn run(
    file_type: FileType,
    files: &[ScanFile],
    constants: &[&str],
    schema: &StructType,
) -> crate::Result<Vec<RecordBatch>> {
    let constants: Vec<String> = constants.iter().map(|c| c.to_string()).collect();
    let plan = lower_scan(file_type, files, &constants, schema)?;
    let config = SessionConfig::new().with_repartition_file_scans(false);
    let session = SessionContext::new_with_config(config);
    Ok(session.execute_logical_plan(plan).await?.collect().await?)
}

fn longs(batches: &[RecordBatch], column: &str) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(column)
                .unwrap()
                .as_primitive::<Int64Type>()
        })
        .collect()
}

fn strings(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| batch.column_by_name(column).unwrap().as_string::<i32>())
        .map(|s| s.map(str::to_string))
        .collect()
}

/// A schema that puts a constant first, a file column second, and the file path last.
fn versioned_schema() -> StructType {
    schema([
        StructField::not_null("version", DataType::LONG),
        StructField::nullable("x", DataType::LONG),
        StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
    ])
}

/// Writes `a` with rows 1, 2 and `b` with row 3 in `file_type`, tagged with versions 7 and 8.
fn two_versioned_files(dir: &FsPath, file_type: FileType) -> Vec<ScanFile> {
    let (a, b) = match file_type {
        FileType::Parquet => (
            write_parquet(dir, "a.parquet", &long_batch(&[1, 2])),
            write_parquet(dir, "b.parquet", &long_batch(&[3])),
        ),
        FileType::Json => (
            write_json(dir, "a.json", &[r#"{"x": 1}"#, r#"{"x": 2}"#]),
            write_json(dir, "b.json", &[r#"{"x": 3}"#]),
        ),
    };
    vec![
        scan_file(&a, vec![7i64.into()]),
        scan_file(&b, vec![8i64.into()]),
    ]
}

/// [`two_versioned_files`] without their constants.
fn unversioned_files(dir: &FsPath, file_type: FileType) -> Vec<ScanFile> {
    let mut files = two_versioned_files(dir, file_type);
    files
        .iter_mut()
        .for_each(|file| file.file_constants.clear());
    files
}

// === Both formats ===

#[rstest]
#[case::parquet(FileType::Parquet)]
#[case::json(FileType::Json)]
#[tokio::test]
async fn reads_columns_constants_and_file_path_in_declared_order(#[case] file_type: FileType) {
    let dir = tempfile::tempdir().unwrap();
    let files = two_versioned_files(dir.path(), file_type);
    let batches = run(file_type, &files, &["version"], &versioned_schema())
        .await
        .unwrap();

    let names: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["version", "x", "_file"]);

    let mut rows: Vec<_> = longs(&batches, "version")
        .into_iter()
        .zip(longs(&batches, "x"))
        .zip(strings(&batches, "_file"))
        .map(|((version, x), file)| (x.unwrap(), version.unwrap(), file.unwrap()))
        .collect();
    rows.sort();
    let (a, b) = (
        files[0].meta.location.to_string(),
        files[1].meta.location.to_string(),
    );
    assert_eq!(
        rows,
        [(1, 7, a.clone()), (2, 7, a), (3, 8, b)],
        "constants and the file path are broadcast per file"
    );
}

#[rstest]
#[case::parquet(FileType::Parquet)]
#[case::json(FileType::Json)]
#[tokio::test]
async fn missing_nullable_column_is_null_filled(#[case] file_type: FileType) {
    let dir = tempfile::tempdir().unwrap();
    let files = unversioned_files(dir.path(), file_type);
    let schema = schema([
        StructField::nullable("x", DataType::LONG),
        StructField::nullable("missing", DataType::STRING),
    ]);
    let batches = run(file_type, &files, &[], &schema).await.unwrap();
    assert_eq!(longs(&batches, "x").len(), 3);
    assert!(strings(&batches, "missing").iter().all(Option::is_none));
}

#[rstest]
#[case::parquet(FileType::Parquet)]
#[case::json(FileType::Json)]
#[tokio::test]
async fn missing_non_nullable_column_errors(#[case] file_type: FileType) {
    let dir = tempfile::tempdir().unwrap();
    let files = unversioned_files(dir.path(), file_type);
    let schema = schema([
        StructField::nullable("x", DataType::LONG),
        StructField::not_null("required", DataType::STRING),
    ]);
    let err = run(file_type, &files, &[], &schema).await.unwrap_err();
    assert!(err.to_string().contains("required"), "{err}");
}

/// A struct `s` with one `other` column, and the declared schema that adds `path` to it.
fn struct_with_other(dir: &FsPath, file_type: FileType) -> std::path::PathBuf {
    match file_type {
        FileType::Parquet => {
            let inner = Arc::new(Int64Array::from(vec![1, 2]));
            let s = StructArray::from(vec![(
                Arc::new(ArrowField::new("other", ArrowDataType::Int64, true)),
                inner as _,
            )]);
            let batch = RecordBatch::try_from_iter([("s", Arc::new(s) as _)]).unwrap();
            write_parquet(dir, "s.parquet", &batch)
        }
        FileType::Json => write_json(
            dir,
            "s.json",
            &[r#"{"s": {"other": 1}}"#, r#"{"s": {"other": 2}}"#],
        ),
    }
}

fn struct_with_other_and(path: StructField) -> StructType {
    schema([StructField::nullable(
        "s",
        DataType::Struct(Box::new(schema([
            StructField::nullable("other", DataType::LONG),
            path,
        ]))),
    )])
}

/// A nested field missing from the file is NULL-filled when nullable, and an error when not,
/// as in the kernel's default engine.
#[rstest]
#[case::parquet(FileType::Parquet)]
#[case::json(FileType::Json)]
#[tokio::test]
async fn missing_nested_field(#[case] file_type: FileType) {
    let dir = tempfile::tempdir().unwrap();
    let files = [scan_file(&struct_with_other(dir.path(), file_type), vec![])];

    let nullable = struct_with_other_and(StructField::nullable("path", DataType::STRING));
    let batches = run(file_type, &files, &[], &nullable).await.unwrap();
    let s = batches[0].column_by_name("s").unwrap().as_struct();
    assert_eq!(s.len(), 2);
    assert_eq!(s.column_by_name("path").unwrap().null_count(), 2);

    let not_null = struct_with_other_and(StructField::not_null("path", DataType::STRING));
    let err = run(file_type, &files, &[], &not_null).await.unwrap_err();
    assert!(err.to_string().contains("path"), "{err}");
}

/// Spark writes nested leaves as `optional`. One reads into a non-nullable declared field as
/// long as it holds no NULLs, and the rows come back with the declared type.
#[tokio::test]
async fn parquet_optional_nested_leaf_reads_into_non_nullable_field() {
    let dir = tempfile::tempdir().unwrap();
    let x = Arc::new(ArrowField::new("x", ArrowDataType::Int64, true));
    let write = |name: &str, values: Vec<Option<i64>>| {
        let s = StructArray::from(vec![(x.clone(), Arc::new(Int64Array::from(values)) as _)]);
        let batch = RecordBatch::try_from_iter([("s", Arc::new(s) as _)]).unwrap();
        write_parquet(dir.path(), name, &batch)
    };
    let declared = schema([StructField::nullable(
        "s",
        DataType::Struct(Box::new(schema([StructField::not_null(
            "x",
            DataType::LONG,
        )]))),
    )]);

    let file = write("no_nulls.parquet", vec![Some(1), Some(2)]);
    let batches = run(
        FileType::Parquet,
        &[scan_file(&file, vec![])],
        &[],
        &declared,
    )
    .await
    .unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ArrowDataType::Struct(vec![ArrowField::new("x", ArrowDataType::Int64, false)].into()),
        "declared nullability is restored above the read"
    );
    let s = batches[0].column_by_name("s").unwrap().as_struct();
    assert_eq!(s.column(0).as_primitive::<Int64Type>().values(), &[1, 2]);

    let file = write("nulls.parquet", vec![Some(1), None]);
    let err = run(
        FileType::Parquet,
        &[scan_file(&file, vec![])],
        &[],
        &declared,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("non-nullable"), "{err}");
}

/// A checkpoint's shape: `add` is NULL on non-add rows, and the parquet reader leaves its
/// non-nullable `path` non-null there. The scan unions the struct's NULLs into its children,
/// as the kernel's default engine does, whatever the declared nullability of the child.
#[rstest]
#[case::declared_not_null(StructField::not_null("path", DataType::STRING))]
#[case::declared_nullable(StructField::nullable("path", DataType::STRING))]
#[tokio::test]
async fn parquet_null_struct_has_null_children(#[case] path: StructField) {
    let dir = tempfile::tempdir().unwrap();
    let add = StructArray::new(
        vec![ArrowField::new("path", ArrowDataType::Utf8, false)].into(),
        vec![Arc::new(StringArray::from(vec!["a.parquet", ""])) as _],
        Some(NullBuffer::from(vec![true, false])),
    );
    let batch = RecordBatch::try_from_iter([("add", Arc::new(add) as _)]).unwrap();
    let file = write_parquet(dir.path(), "checkpoint.parquet", &batch);
    let declared = schema([StructField::nullable(
        "add",
        DataType::Struct(Box::new(schema([path]))),
    )]);

    let batches = run(
        FileType::Parquet,
        &[scan_file(&file, vec![])],
        &[],
        &declared,
    )
    .await
    .unwrap();
    let add = batches[0].column_by_name("add").unwrap().as_struct();
    assert_eq!(
        add.column(0).as_string::<i32>().iter().collect::<Vec<_>>(),
        [Some("a.parquet"), None]
    );
}

/// Spark escapes a `:` in a partition value as `%3A`, so the directory name holds a `%`.
#[rstest]
#[case::parquet(FileType::Parquet)]
#[case::json(FileType::Json)]
#[tokio::test]
async fn reads_a_file_with_percent_in_its_path(#[case] file_type: FileType) {
    let dir = tempfile::tempdir().unwrap();
    let partition = dir.path().join("ts=12%3A30");
    std::fs::create_dir(&partition).unwrap();
    let file = match file_type {
        FileType::Parquet => write_parquet(&partition, "a.parquet", &long_batch(&[1])),
        FileType::Json => write_json(&partition, "a.json", &[r#"{"x": 1}"#]),
    };
    let batches = run(file_type, &[scan_file(&file, vec![])], &[], &x_schema())
        .await
        .unwrap();
    assert_eq!(longs(&batches, "x"), [Some(1)]);
}

#[rstest]
#[case::parquet(FileType::Parquet)]
#[case::json(FileType::Json)]
#[tokio::test]
async fn no_files_is_an_empty_relation(#[case] file_type: FileType) {
    let batches = run(file_type, &[], &["version"], &versioned_schema())
        .await
        .unwrap();
    assert!(batches.iter().all(|b| b.num_rows() == 0));
}

#[rstest]
#[case::parquet(FileType::Parquet)]
#[case::json(FileType::Json)]
#[tokio::test]
async fn row_index_metadata_column_is_refused(#[case] file_type: FileType) {
    let schema = schema([
        StructField::nullable("x", DataType::LONG),
        StructField::create_metadata_column("ridx", MetadataColumnSpec::RowIndex),
    ]);
    let err = lower_scan(file_type, &[], &[], &schema).unwrap_err();
    assert!(matches!(err, ExecutorError::Unsupported(_)), "{err}");
    assert!(err.to_string().contains("RowIndex"), "{err}");
}

#[rstest]
#[case::parquet(FileType::Parquet)]
#[case::json(FileType::Json)]
#[tokio::test]
async fn wrong_number_of_constants_errors(#[case] file_type: FileType) {
    let dir = tempfile::tempdir().unwrap();
    let mut files = two_versioned_files(dir.path(), file_type);
    files[1].file_constants.clear();
    let err = lower_scan(
        file_type,
        &files,
        &["version".to_string()],
        &versioned_schema(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("carries 0 constant(s)"), "{err}");
}

#[tokio::test]
async fn mistyped_constant_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut files = two_versioned_files(dir.path(), FileType::Json);
    files[0].file_constants = vec!["seven".into()];
    let err = lower_scan(
        FileType::Json,
        &files,
        &["version".to_string()],
        &versioned_schema(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("expected Int64"), "{err}");
}

#[tokio::test]
async fn unknown_constant_column_errors() {
    let err = lower_scan(FileType::Json, &[], &["nope".to_string()], &x_schema()).unwrap_err();
    assert!(err.to_string().contains("`nope`"), "{err}");
}

// === Parquet ===

fn field_id_schema() -> StructType {
    schema([with_id(StructField::nullable("x", DataType::LONG), 5)])
}

/// `field` with the parquet field id `id`, as parquet-rs reads and writes it.
fn arrow_with_id(field: ArrowField, id: i64) -> ArrowField {
    field.with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())].into())
}

/// `field` with the kernel's `parquet.field.id`.
fn with_id(field: StructField, id: i64) -> StructField {
    field.with_metadata([(
        ColumnMetadataKey::ParquetFieldId.as_ref(),
        MetadataValue::Number(id),
    )])
}

/// Writes one row group of `columns`, each `(field, array)`.
fn write_columns(
    dir: &FsPath,
    name: &str,
    columns: Vec<(ArrowField, ArrayRef)>,
) -> std::path::PathBuf {
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays).unwrap();
    write_parquet(dir, name, &batch)
}

async fn run_parquet(file: &FsPath, schema: &StructType) -> crate::Result<Vec<RecordBatch>> {
    run(FileType::Parquet, &[scan_file(file, vec![])], &[], schema).await
}

/// The requested `x` reads the file column with its id, not the file's `x`.
#[tokio::test]
async fn parquet_matches_top_level_columns_by_field_id() {
    let dir = tempfile::tempdir().unwrap();
    let long = |name: &str, id, values: Vec<i64>| {
        (
            arrow_with_id(ArrowField::new(name, ArrowDataType::Int64, true), id),
            Arc::new(Int64Array::from(values)) as ArrayRef,
        )
    };
    let file = write_columns(
        dir.path(),
        "a.parquet",
        vec![long("old", 1, vec![1, 2]), long("x", 2, vec![10, 20])],
    );
    let declared = schema([
        with_id(StructField::nullable("x", DataType::LONG), 1),
        with_id(StructField::nullable("y", DataType::LONG), 2),
    ]);
    let batches = run_parquet(&file, &declared).await.unwrap();
    assert_eq!(longs(&batches, "x"), [Some(1), Some(2)]);
    assert_eq!(longs(&batches, "y"), [Some(10), Some(20)]);
}

/// The file's `x` has the id of the requested `other`, so the requested `x` is missing.
#[tokio::test]
async fn parquet_field_id_miss_is_missing_even_when_the_name_matches() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_columns(
        dir.path(),
        "a.parquet",
        vec![(
            arrow_with_id(ArrowField::new("x", ArrowDataType::Int64, true), 2),
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        )],
    );
    let declared = |x: StructField| {
        schema([
            with_id(StructField::nullable("other", DataType::LONG), 2),
            with_id(x, 3),
        ])
    };
    let batches = run_parquet(&file, &declared(StructField::nullable("x", DataType::LONG)))
        .await
        .unwrap();
    assert_eq!(longs(&batches, "other"), [Some(10), Some(20)]);
    assert_eq!(longs(&batches, "x"), [None, None]);

    let err = run_parquet(&file, &declared(StructField::not_null("x", DataType::LONG)))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("x"), "{err}");
}

/// A file column matches by name when it has no id, or an id no requested field has.
#[rstest]
#[case::no_file_id(None)]
#[case::unrequested_file_id(Some(9))]
#[tokio::test]
async fn parquet_falls_back_to_the_name(#[case] file_id: Option<i64>) {
    let dir = tempfile::tempdir().unwrap();
    let mut field = ArrowField::new("x", ArrowDataType::Int64, true);
    if let Some(id) = file_id {
        field = arrow_with_id(field, id);
    }
    let file = write_columns(
        dir.path(),
        "a.parquet",
        vec![(field, Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)],
    );
    let batches = run_parquet(&file, &field_id_schema()).await.unwrap();
    assert_eq!(longs(&batches, "x"), [Some(1), Some(2)]);
}

/// `Struct { old }` with `old` (id 10) holding 1 and 2.
fn old_child_array() -> ArrayRef {
    Arc::new(StructArray::new(
        Fields::from(vec![arrow_with_id(
            ArrowField::new("old", ArrowDataType::Int64, true),
            10,
        )]),
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        None,
    ))
}

/// The declared `Struct { y }`, with `y` taking id 10.
fn declared_y() -> DataType {
    DataType::Struct(Box::new(schema([with_id(
        StructField::nullable("y", DataType::LONG),
        10,
    )])))
}

fn y_values(array: &dyn datafusion::arrow::array::Array) -> Vec<i64> {
    let array = array.as_struct();
    assert_eq!(array.fields()[0].name(), "y");
    array
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

/// Parquet returns a list as the type it was written with. The kernel declares `List`.
#[rstest]
#[case::large_list(true)]
#[case::fixed_size_list(false)]
#[tokio::test]
async fn parquet_matches_ids_inside_any_list_type(#[case] large: bool) {
    let dir = tempfile::tempdir().unwrap();
    let values = old_child_array();
    let element = Arc::new(ArrowField::new("element", values.data_type().clone(), true));
    let list: ArrayRef = if large {
        Arc::new(LargeListArray::new(
            element,
            OffsetBuffer::from_lengths([1, 1]),
            values,
            None,
        ))
    } else {
        Arc::new(FixedSizeListArray::new(element, 1, values, None))
    };
    let written = list.data_type().clone();
    let file = write_columns(
        dir.path(),
        "a.parquet",
        vec![(
            arrow_with_id(ArrowField::new("l_old", written.clone(), true), 2),
            list,
        )],
    );
    let read_back = ParquetRecordBatchReaderBuilder::try_new(File::open(&file).unwrap())
        .unwrap()
        .schema()
        .field(0)
        .data_type()
        .clone();
    assert_eq!(
        std::mem::discriminant(&read_back),
        std::mem::discriminant(&written),
        "parquet reads the list back as {read_back}"
    );
    let declared = schema([with_id(
        StructField::nullable(
            "l",
            DataType::Array(Box::new(ArrayType::new(declared_y(), true))),
        ),
        2,
    )]);
    let batches = run_parquet(&file, &declared).await.unwrap();
    let l = batches[0].column_by_name("l").unwrap().as_list::<i32>();
    assert_eq!(y_values(l.values().as_ref()), [1, 2]);
}

/// `d.items[].value.y`: a struct child under a struct, a list, and a map, all matched by id.
#[tokio::test]
async fn parquet_matches_ids_at_depth() {
    let dir = tempfile::tempdir().unwrap();
    let values = old_child_array();
    let entries = Fields::from(vec![
        ArrowField::new("key", ArrowDataType::Utf8, false),
        ArrowField::new("value", values.data_type().clone(), true),
    ]);
    let map = MapArray::new(
        Arc::new(ArrowField::new(
            "key_value",
            ArrowDataType::Struct(entries.clone()),
            false,
        )),
        OffsetBuffer::from_lengths([1, 1]),
        StructArray::new(
            entries,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                values,
            ],
            None,
        ),
        None,
        false,
    );
    let items = ListArray::new(
        Arc::new(ArrowField::new("element", map.data_type().clone(), true)),
        OffsetBuffer::from_lengths([1, 1]),
        Arc::new(map),
        None,
    );
    let d = StructArray::new(
        Fields::from(vec![arrow_with_id(
            ArrowField::new("items_old", items.data_type().clone(), true),
            5,
        )]),
        vec![Arc::new(items) as ArrayRef],
        None,
    );
    let file = write_columns(
        dir.path(),
        "a.parquet",
        vec![(
            arrow_with_id(ArrowField::new("d_old", d.data_type().clone(), true), 1),
            Arc::new(d) as ArrayRef,
        )],
    );

    let map_type = DataType::Map(Box::new(MapType::new(DataType::STRING, declared_y(), true)));
    let items_type = DataType::Array(Box::new(ArrayType::new(map_type, true)));
    let declared = schema([with_id(
        StructField::nullable(
            "d",
            DataType::Struct(Box::new(schema([with_id(
                StructField::nullable("items", items_type),
                5,
            )]))),
        ),
        1,
    )]);
    let batches = run_parquet(&file, &declared).await.unwrap();
    let d = batches[0].column_by_name("d").unwrap().as_struct();
    let items = d.column_by_name("items").unwrap().as_list::<i32>();
    let map = items.values().as_map();
    assert_eq!(y_values(map.values().as_ref()), [1, 2]);
}

/// A map whose value struct differs from the file's: the file has an extra child, or the table
/// added one. No field ids are involved.
///
/// DataFusion 55 cannot read either: its nested cast (`datafusion_common::nested_struct`) has
/// no `Map` arm, so the plan fails with "Cannot cast struct field 'm'". DataFusion 56.0.0 adds
/// it (apache/datafusion#23914): remove the `ignore` when we upgrade.
#[rstest]
#[case::file_has_an_extra_child(&["a", "b"], &["a"])]
#[case::table_added_a_child(&["a"], &["a", "c"])]
#[ignore = "DataFusion 55 cannot cast a map value struct to different children; fixed in 56.0.0"]
#[tokio::test]
async fn parquet_reads_a_map_whose_value_struct_changed(
    #[case] file_children: &[&str],
    #[case] declared_children: &[&str],
) {
    let dir = tempfile::tempdir().unwrap();
    let value_fields: Fields = file_children
        .iter()
        .map(|name| ArrowField::new(*name, ArrowDataType::Int64, true))
        .collect();
    let value = StructArray::new(
        value_fields.clone(),
        file_children
            .iter()
            .map(|_| Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)
            .collect(),
        None,
    );
    let entries = Fields::from(vec![
        ArrowField::new("key", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Struct(value_fields), true),
    ]);
    let map = MapArray::new(
        Arc::new(ArrowField::new(
            "key_value",
            ArrowDataType::Struct(entries.clone()),
            false,
        )),
        OffsetBuffer::from_lengths([1, 1]),
        StructArray::new(
            entries,
            vec![
                Arc::new(StringArray::from(vec!["k", "k"])) as ArrayRef,
                Arc::new(value) as ArrayRef,
            ],
            None,
        ),
        None,
        false,
    );
    let file = write_columns(
        dir.path(),
        "a.parquet",
        vec![(
            ArrowField::new("m", map.data_type().clone(), true),
            Arc::new(map) as ArrayRef,
        )],
    );
    let declared_value = DataType::Struct(Box::new(schema(
        declared_children
            .iter()
            .map(|name| StructField::nullable(*name, DataType::LONG)),
    )));
    let declared = schema([StructField::nullable(
        "m",
        DataType::Map(Box::new(MapType::new(
            DataType::STRING,
            declared_value,
            true,
        ))),
    )]);

    let batches = run_parquet(&file, &declared).await.unwrap();
    let m = batches[0].column_by_name("m").unwrap().as_map();
    let value = m.values().as_struct();
    assert_eq!(value.num_columns(), declared_children.len());
    assert_eq!(
        value.column(0).as_primitive::<Int64Type>().values(),
        &[1, 2],
        "`a` is read from the file"
    );
    if let Some(added) = value.column_by_name("c") {
        assert_eq!(added.null_count(), 2, "the added child is NULL");
    }
}

/// Struct children match by id in a struct, a list, and a map value, and come back with the
/// declared names.
#[tokio::test]
async fn parquet_matches_nested_fields_by_field_id() {
    let dir = tempfile::tempdir().unwrap();
    let child = || {
        Fields::from(vec![
            arrow_with_id(ArrowField::new("y", ArrowDataType::Int64, true), 11),
            arrow_with_id(ArrowField::new("old", ArrowDataType::Int64, true), 10),
        ])
    };
    let child_array = || {
        StructArray::new(
            child(),
            vec![
                Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
            None,
        )
    };
    let struct_type = ArrowDataType::Struct(child());
    let element = Arc::new(ArrowField::new("element", struct_type.clone(), true));
    let list = ListArray::new(
        Arc::clone(&element),
        OffsetBuffer::from_lengths([1, 1]),
        Arc::new(child_array()),
        None,
    );
    // DataFusion cannot cast a map value struct to fewer children (ids or not), so the map
    // value holds only `old`.
    let value_child = Fields::from(vec![arrow_with_id(
        ArrowField::new("old", ArrowDataType::Int64, true),
        10,
    )]);
    let value = StructArray::new(
        value_child.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        None,
    );
    let entries = Fields::from(vec![
        ArrowField::new("key", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Struct(value_child), true),
    ]);
    let map = MapArray::new(
        Arc::new(ArrowField::new(
            "key_value",
            ArrowDataType::Struct(entries.clone()),
            false,
        )),
        OffsetBuffer::from_lengths([1, 1]),
        StructArray::new(
            entries,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(value) as ArrayRef,
            ],
            None,
        ),
        None,
        false,
    );
    let file = write_columns(
        dir.path(),
        "a.parquet",
        vec![
            (
                arrow_with_id(ArrowField::new("s_old", struct_type, true), 1),
                Arc::new(child_array()) as ArrayRef,
            ),
            (
                arrow_with_id(ArrowField::new("l_old", list.data_type().clone(), true), 2),
                Arc::new(list) as ArrayRef,
            ),
            (
                arrow_with_id(ArrowField::new("m_old", map.data_type().clone(), true), 3),
                Arc::new(map) as ArrayRef,
            ),
        ],
    );

    // The declared `y` has the file's `old` id, so `y` reads `old`.
    let declared_child = || {
        DataType::Struct(Box::new(schema([with_id(
            StructField::nullable("y", DataType::LONG),
            10,
        )])))
    };
    let declared = schema([
        with_id(StructField::nullable("s", declared_child()), 1),
        with_id(
            StructField::nullable(
                "l",
                DataType::Array(Box::new(ArrayType::new(declared_child(), true))),
            ),
            2,
        ),
        with_id(
            StructField::nullable(
                "m",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    declared_child(),
                    true,
                ))),
            ),
            3,
        ),
    ]);
    let batches = run_parquet(&file, &declared).await.unwrap();
    let batch = &batches[0];

    let y_of = |array: &StructArray| {
        assert_eq!(array.fields().len(), 1);
        assert_eq!(array.fields()[0].name(), "y");
        array
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec()
    };
    assert_eq!(y_of(batch.column_by_name("s").unwrap().as_struct()), [1, 2]);
    let l = batch.column_by_name("l").unwrap().as_list::<i32>();
    assert_eq!(y_of(l.values().as_struct()), [1, 2]);
    let m = batch.column_by_name("m").unwrap().as_map();
    assert_eq!(y_of(m.values().as_struct()), [1, 2]);
}

#[tokio::test]
async fn json_ignores_field_ids() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_json(dir.path(), "a.json", &[r#"{"x": 1}"#]);
    let batches = run(
        FileType::Json,
        &[scan_file(&file, vec![])],
        &[],
        &field_id_schema(),
    )
    .await
    .unwrap();
    assert_eq!(longs(&batches, "x"), [Some(1)]);
}

// === JSON ===

#[tokio::test]
async fn json_coerces_primitives_into_string_columns() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_json(dir.path(), "a.json", &[r#"{"s": false}"#, r#"{"s": 3}"#]);
    let schema = schema([StructField::nullable("s", DataType::STRING)]);
    let batches = run(FileType::Json, &[scan_file(&file, vec![])], &[], &schema)
        .await
        .unwrap();
    assert_eq!(
        strings(&batches, "s"),
        [Some("false".to_string()), Some("3".to_string())]
    );
}

#[tokio::test]
async fn json_empty_file_yields_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_json(dir.path(), "empty.json", &[]);
    let batches = run(
        FileType::Json,
        &[scan_file(&file, vec![])],
        &[],
        &x_schema(),
    )
    .await
    .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

/// Through the executor itself: one partition per file keeps a file's rows in order, and a
/// batch never spans files.
#[tokio::test]
async fn executor_keeps_rows_in_file_order() {
    let dir = tempfile::tempdir().unwrap();
    let lines: Vec<String> = (0..5000).map(|i| format!(r#"{{"x": {i}}}"#)).collect();
    let line_refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let a = write_json(dir.path(), "a.json", &line_refs);
    let b = write_json(dir.path(), "b.json", &[r#"{"x": -1}"#]);
    let plan = PlanBuilder::scan_json(
        [scan_file(&a, vec![]), scan_file(&b, vec![])],
        &[],
        Arc::new(x_schema()),
    )
    .unwrap()
    .build()
    .unwrap();

    let executor = executor();
    let batches: Vec<RecordBatch> = tokio::task::spawn_blocking(move || {
        executor
            .execute_op(Operation::QueryPlan(plan))
            .unwrap()
            .into_data()
            .unwrap()
            .map(|batch| {
                ArrowEngineData::try_from_engine_data(batch.unwrap())
                    .unwrap()
                    .record_batch()
                    .clone()
            })
            .collect()
    })
    .await
    .unwrap();

    let per_batch: Vec<Vec<i64>> = batches
        .iter()
        .map(|batch| {
            longs(std::slice::from_ref(batch), "x")
                .into_iter()
                .flatten()
                .collect()
        })
        .collect();
    for rows in &per_batch {
        assert!(
            rows.iter().all(|&x| x == -1) || rows.windows(2).all(|w| w[0] + 1 == w[1]),
            "a batch holds one file's rows, in file order: {rows:?}"
        );
    }
    let a_rows: Vec<i64> = per_batch
        .iter()
        .flatten()
        .copied()
        .filter(|&x| x >= 0)
        .collect();
    assert_eq!(a_rows, (0..5000).collect::<Vec<_>>());
}

// === Composition ===

/// The shape of the kernel's P&M replay: a JSON scan and a parquet scan with the same schema,
/// unioned and aggregated by the version constant.
#[tokio::test]
async fn union_of_json_and_parquet_scans_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(schema([
        StructField::not_null("version", DataType::LONG),
        StructField::nullable("x", DataType::LONG),
    ]));
    let commits = two_versioned_files(dir.path(), FileType::Json);
    let checkpoint = vec![scan_file(
        &write_parquet(dir.path(), "cp.parquet", &long_batch(&[10, 20])),
        vec![5i64.into()],
    )];
    let plan = PlanBuilder::union_all([
        PlanBuilder::scan_json(commits, &["version"], schema.clone()).unwrap(),
        PlanBuilder::scan_parquet(checkpoint, &["version"], schema).unwrap(),
    ])
    .unwrap()
    .aggregate_ungrouped(|a| {
        a.max_non_null_by(
            column_name!("x"),
            column_name!("x"),
            column_name!("version"),
        )
        .aggregate_as(Agg::max(column_name!("version")), "newest")
    })
    .unwrap()
    .build()
    .unwrap();

    let executor = executor();
    let logical = crate::query::to_logical_plan(&executor, &plan).unwrap();
    let batches = SessionContext::new()
        .execute_logical_plan(logical)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(longs(&batches, "x"), [Some(3)], "x from the newest version");
    assert_eq!(longs(&batches, "newest"), [Some(8)]);
}
