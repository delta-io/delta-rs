use datafusion::arrow::array::{
    AsArray as _, DictionaryArray, FixedSizeListArray, Int32Array, Int64Array, LargeListArray,
    LargeListViewArray, ListArray, ListViewArray, MapArray, StringArray, StructArray,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::config::ConfigOptions;
use datafusion::functions::core::get_field;
use datafusion::functions_nested::extract::array_element_udf;
use datafusion::physical_expr::expressions::lit;
use rstest::rstest;

use super::*;

fn with_id(field: Field, id: i64) -> Field {
    field.with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())].into())
}

fn long(name: &str, id: Option<i64>) -> Field {
    let field = Field::new(name, DataType::Int64, true);
    match id {
        Some(id) => with_id(field, id),
        None => field,
    }
}

fn struct_field(name: &str, id: i64, children: Vec<Field>) -> Field {
    with_id(
        Field::new(name, DataType::Struct(children.into()), true),
        id,
    )
}

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

/// `Struct { <child>: Int64 }`, the child with id 10.
fn one_child(child: &str) -> DataType {
    DataType::Struct(vec![long(child, Some(10))].into())
}

fn element(data_type: DataType) -> FieldRef {
    Arc::new(Field::new("element", data_type, true))
}

/// A map from strings to `value`.
fn map_of(value: DataType) -> DataType {
    let entries = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", value, true),
    ]);
    DataType::Map(
        Arc::new(Field::new("key_value", DataType::Struct(entries), false)),
        false,
    )
}

/// Every nested type, with `Struct { <child> }` inside.
fn nested_types(child: &str) -> Vec<DataType> {
    let inner = one_child(child);
    vec![
        inner.clone(),
        DataType::List(element(inner.clone())),
        DataType::LargeList(element(inner.clone())),
        DataType::ListView(element(inner.clone())),
        DataType::LargeListView(element(inner.clone())),
        DataType::FixedSizeList(element(inner.clone()), 1),
        map_of(inner.clone()),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(inner)),
    ]
}

/// A rewrite of `expr`, built against `logical`, for a file that stores `s` as `s_old`, `l` as
/// `l_old`, and `m` as `m_old`, each with its struct child `y` stored as `old`.
fn rewrite_against_old_names(build: impl Fn(&Schema) -> Arc<dyn PhysicalExpr>) -> String {
    let fields = |prefix: &str, child: &str| {
        vec![
            with_id(Field::new(format!("s{prefix}"), one_child(child), true), 1),
            with_id(
                Field::new(
                    format!("l{prefix}"),
                    DataType::List(element(one_child(child))),
                    true,
                ),
                2,
            ),
            with_id(
                Field::new(format!("m{prefix}"), map_of(one_child(child)), true),
                3,
            ),
        ]
    };
    let logical = schema(fields("", "y"));
    let adapter =
        FieldIdAdapter::try_new(Arc::clone(&logical), schema(fields("_old", "old"))).unwrap();
    adapter.rewrite(build(&logical)).unwrap().to_string()
}

fn function(
    udf: Arc<datafusion::logical_expr::ScalarUDF>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(
        ScalarFunctionExpr::try_new(udf, args, schema, Arc::new(ConfigOptions::default())).unwrap(),
    )
}

#[test]
fn no_adapter_without_ids_or_name_changes() {
    let file = schema(vec![long("x", Some(1))]);
    assert!(FieldIdAdapter::try_new(schema(vec![long("x", None)]), Arc::clone(&file)).is_none());
    assert!(FieldIdAdapter::try_new(schema(vec![long("x", Some(1))]), file).is_none());
}

#[test]
fn unmatched_field_does_not_take_a_file_name() {
    // The file's `x` belongs to `other` by id, so the requested `x` must not read it.
    let renamed = rename_fields(
        schema(vec![long("other", Some(2)), long("x", Some(3))]).fields(),
        schema(vec![long("x", Some(2))]).fields(),
    )
    .unwrap();
    assert_eq!(renamed[0].name(), "x");
    assert_eq!(renamed[1].name(), "x (not in file)");
}

/// A predicate on a nested field names the file's column and child.
#[test]
fn get_field_keys_follow_the_renamed_children() {
    let logical = schema(vec![struct_field("s", 1, vec![long("y", Some(10))])]);
    let physical = schema(vec![struct_field(
        "s_old",
        1,
        vec![long("y", Some(11)), long("old", Some(10))],
    )]);
    let adapter = FieldIdAdapter::try_new(Arc::clone(&logical), physical).unwrap();
    let access = ScalarFunctionExpr::try_new(
        get_field(),
        vec![Arc::new(Column::new("s", 0)), lit("y")],
        &logical,
        Arc::new(ConfigOptions::default()),
    )
    .unwrap();
    let rewritten = adapter.rewrite(Arc::new(access)).unwrap();
    assert_eq!(rewritten.to_string(), "get_field(s_old@0, old)");
}

/// The file's child `old` has the requested child's id, inside every nested type.
#[rstest]
#[case::structure(0)]
#[case::list(1)]
#[case::large_list(2)]
#[case::list_view(3)]
#[case::large_list_view(4)]
#[case::fixed_size_list(5)]
#[case::map(6)]
#[case::dictionary(7)]
fn every_nested_type_matches_children_by_id(#[case] index: usize) {
    let requested = Fields::from(vec![with_id(
        Field::new("c", nested_types("y")[index].clone(), true),
        1,
    )]);
    let file = Fields::from(vec![with_id(
        Field::new("c", nested_types("old")[index].clone(), true),
        1,
    )]);
    let renamed = rename_fields(&requested, &file).unwrap();
    assert_eq!(renamed[0].data_type(), file[0].data_type());
}

/// The kernel asks for `List`; parquet can give back any list type.
#[test]
fn list_matches_another_list_type_in_the_file() {
    let requested = DataType::List(element(one_child("y")));
    let file = DataType::LargeList(element(one_child("old")));
    assert_eq!(
        rename_type(&requested, &file),
        Some(DataType::List(element(one_child("old"))))
    );
}

/// Relabels every nested type without touching the values.
#[rstest]
#[case::structure(0)]
#[case::list(1)]
#[case::large_list(2)]
#[case::list_view(3)]
#[case::large_list_view(4)]
#[case::fixed_size_list(5)]
#[case::map(6)]
#[case::dictionary(7)]
fn rename_array_relabels_every_nested_type(#[case] index: usize) {
    let inner = Arc::new(StructArray::new(
        vec![long("old", Some(10))].into(),
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        None,
    )) as ArrayRef;
    let file_element = element(one_child("old"));
    let entries = || {
        StructArray::new(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", one_child("old"), true),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::clone(&inner),
            ],
            None,
        )
    };
    let arrays: Vec<ArrayRef> = vec![
        Arc::clone(&inner),
        Arc::new(ListArray::new(
            Arc::clone(&file_element),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::clone(&inner),
            None,
        )),
        Arc::new(LargeListArray::new(
            Arc::clone(&file_element),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::clone(&inner),
            None,
        )),
        Arc::new(ListViewArray::new(
            Arc::clone(&file_element),
            ScalarBuffer::from(vec![0, 1]),
            ScalarBuffer::from(vec![1, 1]),
            Arc::clone(&inner),
            None,
        )),
        Arc::new(LargeListViewArray::new(
            Arc::clone(&file_element),
            ScalarBuffer::from(vec![0i64, 1]),
            ScalarBuffer::from(vec![1i64, 1]),
            Arc::clone(&inner),
            None,
        )),
        Arc::new(FixedSizeListArray::new(
            Arc::clone(&file_element),
            1,
            Arc::clone(&inner),
            None,
        )),
        Arc::new(MapArray::new(
            Arc::new(Field::new(
                "key_value",
                entries().data_type().clone(),
                false,
            )),
            OffsetBuffer::from_lengths([1, 1]),
            entries(),
            None,
            false,
        )),
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1]), Arc::clone(&inner))
                .unwrap(),
        ),
    ];
    assert_eq!(arrays[index].data_type(), &nested_types("old")[index]);

    let target = &nested_types("y")[index];
    let renamed = rename_array(Arc::clone(&arrays[index]), target).unwrap();
    assert_eq!(renamed.data_type(), target);
    let leaves = match renamed.data_type() {
        DataType::Struct(_) => renamed.as_struct().column(0).clone(),
        DataType::Map(..) => renamed.as_map().values().as_struct().column(0).clone(),
        DataType::Dictionary(..) => renamed
            .as_dictionary::<Int32Type>()
            .values()
            .as_struct()
            .column(0)
            .clone(),
        _ => make_array(renamed.to_data().child_data()[0].clone())
            .as_struct()
            .column(0)
            .clone(),
    };
    assert_eq!(leaves.as_primitive::<Int64Type>().values(), &[1, 2]);
}

#[test]
fn rename_array_refuses_a_different_shape() {
    let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    assert!(rename_array(array, &one_child("y")).is_err());
}

/// `l[1].y` reads `l_old[1].old`.
#[test]
fn get_field_keys_follow_a_list_element() {
    let shown = rewrite_against_old_names(|logical| {
        let item = function(
            array_element_udf(),
            vec![Arc::new(Column::new("l", 1)), lit(1i64)],
            logical,
        );
        function(get_field(), vec![item, lit("y")], logical)
    });
    assert_eq!(shown, "get_field(array_element(l_old@1, 1), old)");
}

/// `m['k'].y` reads `m_old['k'].old`: the map key is a value and stays.
#[test]
fn get_field_keys_follow_a_map_value() {
    let shown = rewrite_against_old_names(|logical| {
        function(
            get_field(),
            vec![Arc::new(Column::new("m", 2)), lit("k"), lit("y")],
            logical,
        )
    });
    assert_eq!(shown, "get_field(m_old@2, k, old)");
}

/// A `get_field` inside another function is renamed too.
#[test]
fn get_field_keys_are_renamed_under_other_functions() {
    let shown = rewrite_against_old_names(|logical| {
        let item = function(
            array_element_udf(),
            vec![Arc::new(Column::new("l", 1)), lit(1i64)],
            logical,
        );
        let wrapped = function(get_field(), vec![item, lit("y")], logical);
        function(
            datafusion::functions::core::coalesce(),
            vec![wrapped, lit(0i64)],
            logical,
        )
    });
    assert_eq!(
        shown,
        "coalesce(get_field(array_element(l_old@1, 1), old), 0)"
    );
}
