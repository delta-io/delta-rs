use arrow::array::{Int32Array, StringArray, StructArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::assert_batches_sorted_eq;
use futures::TryStreamExt as _;

use crate::kernel::{ArrayType, DataType, MapType, PrimitiveType, StructField};
use crate::writer::test_utils::TestResult;
use crate::writer::test_utils::datafusion::get_data;

use super::*;

fn id_field() -> StructField {
    StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true)
}

fn value_field() -> StructField {
    StructField::new("value", DataType::Primitive(PrimitiveType::String), true)
}

fn legacy_field() -> StructField {
    StructField::new("legacy", DataType::Primitive(PrimitiveType::String), true)
}

/// A table with column mapping enabled, which is the only kind that supports dropping.
async fn column_mapped_table() -> DeltaResult<DeltaTable> {
    DeltaTable::new_in_memory()
        .create()
        .with_columns([id_field(), value_field(), legacy_field()])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await
}

/// The column names recorded in the `columns` parameter of the last commit.
async fn last_dropped_columns(table: &DeltaTable) -> Vec<String> {
    let history: Vec<_> = table.history(Some(1)).try_collect().await.unwrap();
    let columns = &history[0].operation_parameters.as_ref().unwrap()["columns"];
    serde_json::from_str(columns.as_str().unwrap()).unwrap()
}

#[test]
fn display_name_quotes_only_parts_with_a_dot() {
    let cases = [
        (vec!["id"], "id"),
        (vec!["s", "inner", "x"], "s.inner.x"),
        (vec!["points", "element", "x"], "points.element.x"),
        (vec!["a.b"], "`a.b`"),
        (vec!["a.b", "c"], "`a.b`.c"),
        (vec!["s", "c.d"], "s.`c.d`"),
        // Unlike `ColumnName`'s `Display`, other special characters are not quoted.
        (vec!["legacy name"], "legacy name"),
        (vec!["a`b"], "a`b"),
    ];
    for (path, expected) in cases {
        assert_eq!(display_name(&ColumnName::new(path)), expected);
    }
}

#[tokio::test]
async fn drop_columns_removes_field_and_keeps_the_rest() -> TestResult {
    let table = column_mapped_table().await?;
    let table = table.drop_columns().with_columns(["legacy"]).await?;

    let schema = table.snapshot()?.schema();
    assert!(schema.field("legacy").is_none(), "legacy should be dropped");
    assert!(schema.field("id").is_some());
    assert!(schema.field("value").is_some());

    let history: Vec<_> = table.history(Some(1)).try_collect().await?;
    assert_eq!(history[0].operation.as_deref(), Some("DROP COLUMNS"));
    assert_eq!(last_dropped_columns(&table).await, ["legacy"]);
    Ok(())
}

#[tokio::test]
async fn drop_columns_preserves_physical_names_of_survivors() -> TestResult {
    let table = column_mapped_table().await?;
    let before = table.snapshot()?.schema();
    let id_physical = before
        .field("id")
        .unwrap()
        .metadata
        .get("delta.columnMapping.physicalName")
        .cloned();
    assert!(
        id_physical.is_some(),
        "column mapping should have assigned a physical name"
    );
    let max_column_id_before = table
        .snapshot()?
        .metadata()
        .configuration()
        .get("delta.columnMapping.maxColumnId")
        .cloned();

    let table = table.drop_columns().with_columns(["legacy"]).await?;

    let after = table.snapshot()?.schema();
    assert_eq!(
        after
            .field("id")
            .unwrap()
            .metadata
            .get("delta.columnMapping.physicalName")
            .cloned(),
        id_physical,
        "surviving columns must keep their physical name"
    );
    // The high-water mark must never be decremented or reused, so a later ADD COLUMN
    // cannot collide with the dropped column's physical id.
    assert_eq!(
        table
            .snapshot()?
            .metadata()
            .configuration()
            .get("delta.columnMapping.maxColumnId")
            .cloned(),
        max_column_id_before
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_does_not_conflict_with_a_concurrent_delete() -> TestResult {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, true),
        Field::new("value", ArrowDataType::Utf8, true),
        Field::new("legacy", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )?;
    let table = column_mapped_table().await?.write(vec![batch]).await?;
    let stale = table.clone();
    table.delete().with_predicate("id = 1").await?;

    // The drop reads no files, so the DELETE that removed a file must not conflict with it.
    let table = stale.drop_columns().with_columns(["legacy"]).await?;

    assert!(table.snapshot()?.schema().field("legacy").is_none());
    Ok(())
}

#[tokio::test]
async fn drop_columns_does_not_commit_an_unchanged_protocol() -> TestResult {
    // With several features, a feature list in a different order is almost certain.
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([id_field(), value_field(), legacy_field()])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .await?;

    let table = table.drop_columns().with_columns(["legacy"]).await?;

    let version = table.version().unwrap();
    let commit = table.log_store().read_commit_entry(version).await?.unwrap();
    let commit = std::str::from_utf8(&commit)?;
    assert!(
        !commit
            .lines()
            .any(|line| line.starts_with(r#"{"protocol""#)),
        "{commit}"
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_can_remove_several_in_one_commit() -> TestResult {
    let table = column_mapped_table().await?;
    let version_before = table.version();

    let table = table
        .drop_columns()
        .with_columns(["value", "legacy"])
        .await?;

    let schema = table.snapshot()?.schema();
    assert!(schema.field("value").is_none());
    assert!(schema.field("legacy").is_none());
    assert!(schema.field("id").is_some());
    assert_eq!(table.version(), version_before.map(|v| v + 1));
    Ok(())
}

#[tokio::test]
async fn drop_columns_requires_column_mapping() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([id_field(), value_field()])
        .await?;

    let err = table
        .drop_columns()
        .with_columns(["value"])
        .await
        .unwrap_err();

    assert!(
        matches!(err, DeltaTableError::ColumnMappingRequired { .. }),
        "expected ColumnMappingRequired, got: {err}"
    );
    Ok(())
}

fn int_field(name: &str) -> StructField {
    StructField::new(name, DataType::Primitive(PrimitiveType::Integer), true)
}

fn struct_type(fields: impl IntoIterator<Item = StructField>) -> DataType {
    DataType::Struct(Box::new(StructType::try_new(fields).unwrap()))
}

/// A column-mapped table with the struct `s` of the fields `a` and `b`.
async fn struct_table() -> DeltaResult<DeltaTable> {
    DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            StructField::new("s", struct_type([int_field("a"), int_field("b")]), true),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await
}

/// A column-mapped table with a struct in the struct `s`, the array of structs `points`, and
/// the map `tags` whose values are structs. Each of these structs has the fields `x` and `y`.
async fn nested_types_table() -> DeltaResult<DeltaTable> {
    let xy = || struct_type([int_field("x"), int_field("y")]);
    DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            StructField::new(
                "s",
                struct_type([StructField::new("inner", xy(), true), int_field("b")]),
                true,
            ),
            StructField::new(
                "points",
                DataType::Array(Box::new(ArrayType::new(xy(), true))),
                true,
            ),
            StructField::new(
                "tags",
                DataType::Map(Box::new(MapType::new(DataType::STRING, xy(), true))),
                true,
            ),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await
}

#[tokio::test]
async fn drop_columns_drops_nested_field() -> TestResult {
    let a_and_b = Fields::from(vec![
        Field::new("a", ArrowDataType::Int32, true),
        Field::new("b", ArrowDataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int32, true),
            Field::new("s", ArrowDataType::Struct(a_and_b.clone()), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StructArray::new(
                a_and_b,
                vec![
                    Arc::new(Int32Array::from(vec![10, 20])),
                    Arc::new(Int32Array::from(vec![11, 21])),
                ],
                None,
            )),
        ],
    )?;
    let table = struct_table().await?.write(vec![batch]).await?;

    let table = table.drop_columns().with_columns(["s.a"]).await?;

    let schema = table.snapshot()?.schema();
    assert!(!contains_field(&schema, &"s.a".parse::<ColumnName>()?));
    assert!(contains_field(&schema, &"s.b".parse::<ColumnName>()?));
    assert_eq!(last_dropped_columns(&table).await, ["s.a"]);

    // The data files still hold `s.a`; reads skip it, and new files are written without it.
    let only_b = Fields::from(vec![Field::new("b", ArrowDataType::Int32, true)]);
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int32, true),
            Field::new("s", ArrowDataType::Struct(only_b.clone()), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StructArray::new(
                only_b,
                vec![Arc::new(Int32Array::from(vec![31]))],
                None,
            )),
        ],
    )?;
    let table = table.write(vec![batch]).await?;
    assert_batches_sorted_eq!(
        [
            "+----+---------+",
            "| id | s       |",
            "+----+---------+",
            "| 1  | {b: 11} |",
            "| 2  | {b: 21} |",
            "| 3  | {b: 31} |",
            "+----+---------+",
        ],
        &get_data(&table).await
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_fields_nested_in_structs_arrays_and_maps() -> TestResult {
    let table = nested_types_table().await?;

    let table = table
        .drop_columns()
        .with_columns(["s.inner.x", "points.element.x", "tags.value.y"])
        .await?;

    let schema = table.snapshot()?.schema();
    let has = |path: &str| contains_field(&schema, &path.parse::<ColumnName>().unwrap());
    assert!(!has("s.inner.x") && has("s.inner.y") && has("s.b"));
    assert!(!has("points.element.x") && has("points.element.y"));
    assert!(has("tags.value.x") && !has("tags.value.y"));
    assert_eq!(
        last_dropped_columns(&table).await,
        ["s.inner.x", "points.element.x", "tags.value.y"]
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_paths_that_name_no_struct_field() -> TestResult {
    let table = nested_types_table().await?;

    // An array element or a map key is not a struct field, and an array has no named fields.
    for name in ["points.element", "tags.key", "points.x", "s.missing"] {
        let err = table
            .clone()
            .drop_columns()
            .with_columns([name])
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("No column with the name '{name}'")),
            "got: {err}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_dropping_every_field_of_a_struct() -> TestResult {
    let err = struct_table()
        .await?
        .drop_columns()
        .with_columns(["s.a", "s.b"])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Cannot drop every field of 's'"),
        "got: {err}"
    );

    let err = nested_types_table()
        .await?
        .drop_columns()
        .with_columns(["points.element.x", "points.element.y"])
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Cannot drop every field of 'points.element'"),
        "got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_a_struct_and_its_field_once() -> TestResult {
    let table = struct_table().await?;

    let table = table.drop_columns().with_columns(["s.a", "s"]).await?;

    assert!(table.snapshot()?.schema().field("s").is_none());
    assert_eq!(last_dropped_columns(&table).await, ["s"]);
    Ok(())
}

#[tokio::test]
async fn drop_columns_accepts_a_quoted_nested_path() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            StructField::new("a.b", struct_type([int_field("c"), int_field("d")]), true),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await?;

    let table = table.drop_columns().with_columns(["`a.b`.c"]).await?;

    let schema = table.snapshot()?.schema();
    assert!(!contains_field(&schema, &ColumnName::new(["a.b", "c"])));
    assert!(contains_field(&schema, &ColumnName::new(["a.b", "d"])));
    assert_eq!(last_dropped_columns(&table).await, ["`a.b`.c"]);
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_constraint_on_dropped_nested_field() -> TestResult {
    let table = struct_table()
        .await?
        .add_constraint()
        .with_constraint("a_positive", "s.a > 0")
        .with_constraint("b_positive", "s.b > 0")
        .await?;

    let table = table.drop_columns().with_columns(["s.a"]).await?;

    let snapshot = table.snapshot()?;
    let configuration = snapshot.metadata().configuration();
    assert!(!configuration.contains_key("delta.constraints.a_positive"));
    assert!(configuration.contains_key("delta.constraints.b_positive"));
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_nested_field_used_by_its_struct_or_siblings() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            StructField::new(
                "s",
                struct_type([
                    int_field("a"),
                    int_field("b").with_metadata([(
                        "delta.invariants",
                        r#"{"expression": {"expression": "s.b > s.a"}}"#,
                    )]),
                ]),
                true,
            ),
            int_field("doubled").with_metadata([("delta.generationExpression", "s.a * 2")]),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await?
        .add_constraint()
        .with_constraint("s_present", "s IS NOT NULL")
        .await?;

    let err = table
        .drop_columns()
        .with_columns(["s.a"])
        .await
        .unwrap_err();

    // `s IS NOT NULL` reads the whole struct, so it reads `s.a` too.
    assert_eq!(
        err.to_string(),
        "Generic DeltaTable error: \
         Cannot drop column 's.a': it is referenced by the check constraint 's_present'.\n\
         Cannot drop column 's.a': it is referenced by the generated column 'doubled'.\n\
         Cannot drop column 's.a': it is referenced by the invariant on 's.b'.\n\
         Remove or update these references first."
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_treats_a_map_lookup_as_a_read_of_the_whole_value() -> TestResult {
    let table = nested_types_table()
        .await?
        .add_constraint()
        .with_constraint("k_present", "tags['k'] IS NOT NULL")
        .await?;

    let err = table
        .drop_columns()
        .with_columns(["tags.value.x"])
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains(
            "Cannot drop column 'tags.value.x': it is referenced by the check constraint \
             'k_present'."
        ),
        "got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_unknown_column() -> TestResult {
    let table = column_mapped_table().await?;

    let err = table
        .drop_columns()
        .with_columns(["does_not_exist"])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("does_not_exist"));
    Ok(())
}

#[tokio::test]
async fn drop_columns_unknown_column_is_noop_when_not_raising() -> TestResult {
    let table = column_mapped_table().await?;
    let version_before = table.version();

    let table = table
        .drop_columns()
        .with_columns(["does_not_exist"])
        .with_raise_if_not_exists(false)
        .await?;

    assert_eq!(table.version(), version_before, "should not have committed");
    assert!(table.snapshot()?.schema().field("id").is_some());
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_unknown_nested_path() -> TestResult {
    let table = column_mapped_table().await?;

    let err = table
        .drop_columns()
        .with_columns(["value.nested"])
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("No column with the name 'value.nested'"),
        "got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_unknown_nested_path_is_noop_when_not_raising() -> TestResult {
    let table = column_mapped_table().await?;
    let version_before = table.version();

    let table = table
        .drop_columns()
        .with_columns(["does_not.exist"])
        .with_raise_if_not_exists(false)
        .await?;

    assert_eq!(table.version(), version_before, "should not have committed");
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_dropping_every_column() -> TestResult {
    let table = column_mapped_table().await?;

    let err = table
        .drop_columns()
        .with_columns(["id", "value", "legacy"])
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("at least one column"),
        "got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_partition_column() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([id_field(), value_field()])
        .with_partition_columns(["value"])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await?;

    let err = table
        .drop_columns()
        .with_columns(["value"])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("partition column"), "got: {err}");
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_constraint_on_dropped_column() -> TestResult {
    let table = column_mapped_table().await?;
    let table = table
        .add_constraint()
        .with_constraint("value_not_empty", "value != ''")
        .with_constraint("id_positive", "id > 0")
        .await?;

    let table = table.drop_columns().with_columns(["value"]).await?;

    let snapshot = table.snapshot()?;
    let configuration = snapshot.metadata().configuration();
    assert!(!configuration.contains_key("delta.constraints.value_not_empty"));
    assert!(configuration.contains_key("delta.constraints.id_positive"));
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_constraint_on_columns_dropped_together() -> TestResult {
    let table = column_mapped_table().await?;
    let table = table
        .add_constraint()
        .with_constraint("value_differs", "value != legacy")
        .await?;

    let table = table
        .drop_columns()
        .with_columns(["value", "legacy"])
        .await?;

    assert!(
        !table
            .snapshot()?
            .metadata()
            .configuration()
            .contains_key("delta.constraints.value_differs")
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_column_used_by_constraint() -> TestResult {
    let table = column_mapped_table().await?;
    let table = table
        .add_constraint()
        .with_constraint("value_not_empty", "value != '' OR id > 0")
        .await?;

    let err = table
        .drop_columns()
        .with_columns(["value"])
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("value_not_empty"),
        "error should name the offending constraint, got: {err}"
    );
    Ok(())
}

/// A column-mapped table where `doubled` is generated from `id`.
async fn generated_column_table() -> DeltaResult<DeltaTable> {
    DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            value_field(),
            StructField::new("doubled", DataType::Primitive(PrimitiveType::Integer), true)
                .with_metadata([("delta.generationExpression", "id * 2")]),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await
}

#[tokio::test]
async fn drop_columns_rejects_column_used_by_generated_column() -> TestResult {
    let table = generated_column_table().await?;

    let err = table.drop_columns().with_columns(["id"]).await.unwrap_err();

    assert!(
        err.to_string().contains("generated column 'doubled'"),
        "got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_lists_every_reference_in_sorted_order() -> TestResult {
    let table = generated_column_table()
        .await?
        .add_constraint()
        .with_constraint("positive", "id > 0 OR value != '' OR doubled > 0")
        .await?;

    let err = table
        .drop_columns()
        .with_columns(["value", "id"])
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Generic DeltaTable error: \
         Cannot drop column 'id': it is referenced by the check constraint 'positive'.\n\
         Cannot drop column 'id': it is referenced by the generated column 'doubled'.\n\
         Cannot drop column 'value': it is referenced by the check constraint 'positive'.\n\
         Remove or update these references first."
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_generated_column() -> TestResult {
    let table = generated_column_table().await?;

    let table = table.drop_columns().with_columns(["doubled"]).await?;

    let schema = table.snapshot()?.schema();
    assert!(schema.field("doubled").is_none());
    assert!(schema.field("id").is_some());
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_generated_column_with_its_source() -> TestResult {
    let table = generated_column_table().await?;

    let table = table.drop_columns().with_columns(["id", "doubled"]).await?;

    let schema = table.snapshot()?.schema();
    assert!(schema.field("doubled").is_none());
    assert!(schema.field("id").is_none());
    Ok(())
}

#[tokio::test]
async fn drop_columns_removes_column_from_stats_columns() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            value_field(),
            StructField::new(
                "legacy name",
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .with_configuration_property(
            TableProperty::DataSkippingStatsColumns,
            Some("id,value,`legacy name`"),
        )
        .await?;

    let table = table.drop_columns().with_columns(["value"]).await?;

    let snapshot = table.snapshot()?;
    let stats_columns = snapshot
        .metadata()
        .configuration()
        .get("delta.dataSkippingStatsColumns")
        .unwrap();
    assert_eq!(stats_columns, "id,`legacy name`");
    assert_eq!(
        ColumnName::parse_column_name_list(stats_columns)?,
        [ColumnName::new(["id"]), ColumnName::new(["legacy name"])]
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_removes_nested_fields_from_stats_columns() -> TestResult {
    let nested = StructField::new(
        "nested",
        DataType::Struct(Box::new(StructType::try_new([StructField::new(
            "inner",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )])?)),
        true,
    );
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([id_field(), nested])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .with_configuration_property(
            TableProperty::DataSkippingStatsColumns,
            Some("id,nested.inner"),
        )
        .await?;

    let table = table.drop_columns().with_columns(["nested"]).await?;

    assert_eq!(
        table
            .snapshot()?
            .metadata()
            .configuration()
            .get("delta.dataSkippingStatsColumns")
            .map(String::as_str),
        Some("id")
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_removes_dropped_nested_field_from_stats_columns() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            StructField::new("s", struct_type([int_field("a"), int_field("b")]), true),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .with_configuration_property(TableProperty::DataSkippingStatsColumns, Some("id,s.a,s.b"))
        .await?;

    let table = table.drop_columns().with_columns(["s.a"]).await?;

    assert_eq!(
        table
            .snapshot()?
            .metadata()
            .configuration()
            .get("delta.dataSkippingStatsColumns")
            .map(String::as_str),
        Some("id,s.b")
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_leaves_empty_stats_columns_when_none_is_left() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([id_field(), value_field(), legacy_field()])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .with_configuration_property(
            TableProperty::DataSkippingStatsColumns,
            Some("value,legacy"),
        )
        .await?;

    let table = table
        .drop_columns()
        .with_columns(["value", "legacy"])
        .await?;

    // An empty list collects stats for no column, as the dropped columns were the only ones.
    let snapshot = table.snapshot()?;
    assert_eq!(
        snapshot
            .metadata()
            .configuration()
            .get("delta.dataSkippingStatsColumns")
            .map(String::as_str),
        Some("")
    );
    assert_eq!(
        snapshot
            .snapshot()
            .table_configuration()
            .table_properties()
            .data_skipping_stats_columns,
        Some(vec![])
    );
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_struct_with_nested_invariant() -> TestResult {
    let nested = StructField::new(
        "nested",
        DataType::Struct(Box::new(StructType::try_new([StructField::new(
            "inner",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )
        .with_metadata([(
            "delta.invariants",
            r#"{"expression": {"expression": "nested.inner > 0"}}"#,
        )])])?)),
        true,
    );
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([id_field(), nested])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await?;

    let table = table.drop_columns().with_columns(["nested"]).await?;

    assert!(table.snapshot()?.schema().field("nested").is_none());
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_column_used_by_invariant_on_column_with_a_dot() -> TestResult {
    // The top-level column `a.b` is not nested inside `a`, so dropping `a` keeps its
    // invariant.
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            StructField::new("a", DataType::Primitive(PrimitiveType::Integer), true),
            StructField::new("a.b", DataType::Primitive(PrimitiveType::Integer), true)
                .with_metadata([(
                    "delta.invariants",
                    r#"{"expression": {"expression": "a > 0"}}"#,
                )]),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await?;

    let err = table.drop_columns().with_columns(["a"]).await.unwrap_err();

    assert!(err.to_string().contains("invariant on 'a.b'"), "got: {err}");
    Ok(())
}

#[tokio::test]
async fn drop_columns_rejects_column_with_a_dot_used_by_nested_invariant() -> TestResult {
    // The field `b` of the struct `a` is not the top-level column `a.b`, so dropping `a.b`
    // keeps its invariant.
    let a = StructField::new(
        "a",
        DataType::Struct(Box::new(StructType::try_new([StructField::new(
            "b",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )
        .with_metadata([(
            "delta.invariants",
            r#"{"expression": {"expression": "`a.b` > 0"}}"#,
        )])])?)),
        true,
    );
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            a,
            StructField::new("a.b", DataType::Primitive(PrimitiveType::Integer), true),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await?;

    let err = table
        .drop_columns()
        .with_columns(["a.b"])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("invariant on 'a.b'"), "got: {err}");
    Ok(())
}

#[tokio::test]
async fn drop_columns_drops_top_level_column_with_a_dot() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            StructField::new("a.b", DataType::Primitive(PrimitiveType::String), true),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await?;

    let table = table.drop_columns().with_columns(["a.b"]).await?;

    assert!(table.snapshot()?.schema().field("a.b").is_none());
    assert_eq!(last_dropped_columns(&table).await, ["`a.b`"]);
    Ok(())
}

#[tokio::test]
async fn drop_columns_accepts_the_quoted_name_from_the_history() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns([
            id_field(),
            StructField::new("a.b", DataType::Primitive(PrimitiveType::String), true),
        ])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await?;

    // The quoted and the plain name are the same column, so it is dropped once.
    let table = table.drop_columns().with_columns(["`a.b`", "a.b"]).await?;

    assert!(table.snapshot()?.schema().field("a.b").is_none());
    assert_eq!(last_dropped_columns(&table).await, ["`a.b`"]);
    Ok(())
}

#[tokio::test]
async fn drop_columns_ignores_repeated_names() -> TestResult {
    let table = column_mapped_table().await?;

    let table = table
        .drop_columns()
        .with_columns(["legacy", "legacy"])
        .await?;

    assert_eq!(last_dropped_columns(&table).await, ["legacy"]);
    Ok(())
}

/// Tests of [`DropFields`] on a schema, without a table.
mod drop_fields {
    use delta_kernel::schema::{ColumnMetadataKey, MetadataValue};
    use rstest::rstest;

    use super::*;

    /// The column `id`, the struct `s` of the struct `inner` and the field `b`, the array of
    /// structs `points`, the map `tags` with struct values, and the map `keys` with struct
    /// keys. Each of the inner structs has the fields `x` and `y`.
    fn schema() -> StructType {
        let xy = || struct_type([int_field("x"), int_field("y")]);
        StructType::try_new([
            int_field("id"),
            StructField::new(
                "s",
                struct_type([StructField::new("inner", xy(), true), int_field("b")]),
                true,
            ),
            StructField::new(
                "points",
                DataType::Array(Box::new(ArrayType::new(xy(), true))),
                true,
            ),
            StructField::new(
                "tags",
                DataType::Map(Box::new(MapType::new(DataType::STRING, xy(), true))),
                true,
            ),
            StructField::new(
                "keys",
                DataType::Map(Box::new(MapType::new(xy(), DataType::STRING, true))),
                true,
            ),
        ])
        .unwrap()
    }

    /// Run [`DropFields`] over `schema` with the `dropped` paths. Returns the new schema, or
    /// `None` when no field is left, and the first struct left without fields.
    fn drop_fields(
        schema: &StructType,
        dropped: &[&str],
    ) -> (Option<StructType>, Option<ColumnName>) {
        let columns = dropped
            .iter()
            .map(|path| path.parse::<ColumnName>().unwrap())
            .collect_vec();
        let trie = ColumnTrie::from_columns(&columns);
        let mut transform = DropFields {
            dropped: &trie,
            path: Vec::new(),
            emptied: None,
        };
        let schema = transform.transform_struct(schema).map(Cow::into_owned);
        (schema, transform.emptied)
    }

    #[test]
    fn removes_the_named_fields_at_every_depth() {
        let (schema, emptied) = drop_fields(
            &schema(),
            &[
                "id",
                "s.inner.x",
                "points.element.y",
                "tags.value.x",
                "keys.key.y",
            ],
        );

        let expected = StructType::try_new([
            StructField::new(
                "s",
                struct_type([
                    StructField::new("inner", struct_type([int_field("y")]), true),
                    int_field("b"),
                ]),
                true,
            ),
            StructField::new(
                "points",
                DataType::Array(Box::new(ArrayType::new(
                    struct_type([int_field("x")]),
                    true,
                ))),
                true,
            ),
            StructField::new(
                "tags",
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    struct_type([int_field("y")]),
                    true,
                ))),
                true,
            ),
            StructField::new(
                "keys",
                DataType::Map(Box::new(MapType::new(
                    struct_type([int_field("x")]),
                    DataType::STRING,
                    true,
                ))),
                true,
            ),
        ])
        .unwrap();
        assert_eq!(schema, Some(expected));
        assert_eq!(emptied, None);
    }

    #[test]
    fn removes_a_struct_with_all_its_fields() {
        let (schema, emptied) = drop_fields(&schema(), &["s"]);

        let schema = schema.unwrap();
        assert!(schema.field("s").is_none());
        assert_eq!(schema.num_fields(), 4);
        assert_eq!(emptied, None);
    }

    #[test]
    fn borrows_the_schema_when_no_field_is_dropped() {
        let schema = schema();
        let columns = ["missing".parse::<ColumnName>().unwrap()];
        let trie = ColumnTrie::from_columns(&columns);
        let mut transform = DropFields {
            dropped: &trie,
            path: Vec::new(),
            emptied: None,
        };

        assert!(matches!(
            transform.transform_struct(&schema),
            Some(Cow::Borrowed(_))
        ));
    }

    #[test]
    fn keeps_the_metadata_of_the_remaining_fields() {
        let mapped = |name: &str, id: i64, data_type: DataType| {
            StructField::new(name, data_type, true).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(id),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String(format!("col-{id}")),
                ),
            ])
        };
        let int = DataType::Primitive(PrimitiveType::Integer);
        let schema = StructType::try_new([mapped(
            "s",
            1,
            struct_type([mapped("a", 2, int.clone()), mapped("b", 3, int.clone())]),
        )])
        .unwrap();

        let (schema, _) = drop_fields(&schema, &["s.a"]);

        let expected =
            StructType::try_new([mapped("s", 1, struct_type([mapped("b", 3, int)]))]).unwrap();
        assert_eq!(schema, Some(expected));
    }

    #[rstest]
    #[case::nested_struct(&["s.inner.x", "s.inner.y"], "s.inner")]
    #[case::struct_of_a_struct(&["s.inner", "s.b"], "s")]
    #[case::array_element(&["points.element.x", "points.element.y"], "points.element")]
    #[case::map_value(&["tags.value.x", "tags.value.y"], "tags.value")]
    #[case::map_key(&["keys.key.x", "keys.key.y"], "keys.key")]
    fn records_the_struct_left_without_fields(#[case] dropped: &[&str], #[case] expected: &str) {
        let (_, emptied) = drop_fields(&schema(), dropped);

        assert_eq!(emptied, Some(expected.parse().unwrap()));
    }

    #[test]
    fn records_only_the_first_struct_left_without_fields() {
        let (_, emptied) = drop_fields(
            &schema(),
            &[
                "s.inner.x",
                "s.inner.y",
                "points.element.x",
                "points.element.y",
            ],
        );

        assert_eq!(emptied, Some("s.inner".parse().unwrap()));
    }

    #[test]
    fn records_the_root_when_every_column_is_dropped() {
        let (schema, emptied) = drop_fields(&schema(), &["id", "s", "points", "tags", "keys"]);

        assert_eq!(schema, None);
        assert!(emptied.is_some_and(|column| column.is_empty()));
    }

    #[test]
    fn keeps_a_struct_that_had_no_fields() {
        let empty = || StructField::new("e", struct_type([]), true);
        let schema = StructType::try_new([int_field("id"), int_field("v"), empty()]).unwrap();

        let (schema, emptied) = drop_fields(&schema, &["v"]);

        let expected = StructType::try_new([int_field("id"), empty()]).unwrap();
        assert_eq!(schema, Some(expected));
        assert_eq!(emptied, None);
    }
}
