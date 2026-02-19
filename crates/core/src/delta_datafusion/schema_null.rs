use std::sync::{Arc, Mutex};
use arrow_schema::{DataType, Field, FieldRef, Fields, SchemaRef, UnionFields};
use arrow_schema::DataType::{Dictionary, FixedSizeList, LargeList, LargeListView, List, ListView, Map, RunEndEncoded, Struct, Union};

static DELTA_FIELD_PATHS_TO_MAKE_NULLABLE: Mutex<Vec<String>> = Mutex::new(Vec::new());

#[ctor::ctor]
fn init() {
    if let Ok(var) = std::env::var("DELTA_FIELD_PATHS_TO_MAKE_NULLABLE") {
        let splits = var
            .split(",")
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        *DELTA_FIELD_PATHS_TO_MAKE_NULLABLE.lock().unwrap() = splits;
    }
}

pub fn rewrite_schema_with_nullable_fields(input: SchemaRef) -> SchemaRef {
    let paths = DELTA_FIELD_PATHS_TO_MAKE_NULLABLE.lock().unwrap();
    rewrite_schema_with_nullable_fields_inner(input, &paths)
}

fn rewrite_schema_with_nullable_fields_inner(input: SchemaRef, paths: &[String]) -> SchemaRef {
    if paths.is_empty() {
        return input;
    }

    let new_fields = rewrite_fields(input.fields(), paths, "");
    Arc::new(arrow_schema::Schema::new_with_metadata(new_fields, input.metadata().clone()))
}

fn rewrite_fields(fields: &Fields, paths: &[String], parent_path: &str) -> Vec<FieldRef> {
    fields.iter().map(|field| {
        let current_path = if parent_path.is_empty() {
            field.name().to_string()
        } else {
            format!("{}.{}", parent_path, field.name())
        };

        let make_nullable = !field.is_nullable() && paths.iter().any(|p| *p == current_path);
        let new_field = rewrite_field_type(field, paths, &current_path);

        if make_nullable {
            Arc::new(new_field.with_nullable(true))
        } else {
            Arc::new(new_field)
        }
    }).collect()
}

fn rewrite_field_type(field: &Field, paths: &[String], current_path: &str) -> Field {
    let new_dt = rewrite_data_type(field.data_type(), paths, current_path);
    Field::new(field.name(), new_dt, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

fn rewrite_data_type(dt: &DataType, paths: &[String], current_path: &str) -> DataType {
    match dt {
        Struct(fields) => {
            Struct(Fields::from(rewrite_fields(fields, paths, current_path)))
        },
        List(inner) => {
            List(Arc::new(rewrite_field_type(inner, paths, current_path)))
        },
        LargeList(inner) => {
            LargeList(Arc::new(rewrite_field_type(inner, paths, current_path)))
        },
        FixedSizeList(inner, n) => {
            FixedSizeList(Arc::new(rewrite_field_type(inner, paths, current_path)), *n)
        },
        ListView(inner) => {
            ListView(Arc::new(rewrite_field_type(inner, paths, current_path)))
        },
        LargeListView(inner) => {
            LargeListView(Arc::new(rewrite_field_type(inner, paths, current_path)))
        },
        Map(inner, sorted) => {
            Map(Arc::new(rewrite_field_type(inner, paths, current_path)), *sorted)
        },
        Dictionary(key, value) => {
            Dictionary(key.clone(), Box::new(rewrite_data_type(value, paths, current_path)))
        },
        RunEndEncoded(run_ends, values) => {
            RunEndEncoded(
                Arc::clone(run_ends),
                Arc::new(rewrite_field_type(values, paths, current_path)),
            )
        },
        Union(union_fields, mode) => {
            let type_ids: Vec<i8> = union_fields.iter().map(|(id, _)| id).collect();
            let fields: Vec<FieldRef> = union_fields.iter().map(|(_, field)| {
                let field_path = if current_path.is_empty() {
                    field.name().to_string()
                } else {
                    format!("{}.{}", current_path, field.name())
                };
                let make_nullable = !field.is_nullable() && paths.iter().any(|p| *p == field_path);
                let mut new_field = rewrite_field_type(field, paths, &field_path);
                if make_nullable {
                    new_field = new_field.with_nullable(true);
                }
                Arc::new(new_field)
            }).collect();
            Union(UnionFields::new(type_ids, fields), *mode)
        }
        _ => dt.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn make_schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn paths(strs: &[&str]) -> Vec<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_schema_null_empty_paths_returns_input() {
        let schema = make_schema(vec![
            Field::new("a", DataType::Int32, false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema.clone(), &[]);
        assert_eq!(schema, result);
    }

    #[test]
    fn test_schema_null_top_level_field_made_nullable() {
        let schema = make_schema(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a"]));
        assert!(result.field_with_name("a").unwrap().is_nullable());
        assert!(!result.field_with_name("b").unwrap().is_nullable());
    }

    #[test]
    fn test_schema_null_already_nullable_unchanged() {
        let schema = make_schema(vec![
            Field::new("a", DataType::Int32, true),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a"]));
        assert!(result.field_with_name("a").unwrap().is_nullable());
    }

    #[test]
    fn test_schema_null_path_not_in_schema_unchanged() {
        let schema = make_schema(vec![
            Field::new("a", DataType::Int32, false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema.clone(), &paths(&["z"]));
        assert_eq!(schema, result);
    }

    #[test]
    fn test_schema_null_nested_struct_field() {
        let schema = make_schema(vec![
            Field::new("a", Struct(Fields::from(vec![
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Utf8, false),
            ])), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a.b"]));
        let a = result.field_with_name("a").unwrap();
        assert!(!a.is_nullable());
        if let Struct(fields) = a.data_type() {
            assert!(fields.find("b").unwrap().1.is_nullable());
            assert!(!fields.find("c").unwrap().1.is_nullable());
        } else {
            assert_eq!(true, false, "expected struct");
        }
    }

    #[test]
    fn test_schema_null_deeply_nested_struct() {
        let schema = make_schema(vec![
            Field::new("a", Struct(Fields::from(vec![
                Field::new("b", Struct(Fields::from(vec![
                    Field::new("c", Struct(Fields::from(vec![
                        Field::new("d", DataType::Int32, false),
                    ])), false),
                ])), false),
            ])), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a.b.c.d"]));
        let a = result.field_with_name("a").unwrap();
        assert!(!a.is_nullable());
        let b = match a.data_type() { Struct(f) => f.find("b").unwrap().1, _ => panic!() };
        assert!(!b.is_nullable());
        let c = match b.data_type() { Struct(f) => f.find("c").unwrap().1, _ => panic!() };
        assert!(!c.is_nullable());
        let d = match c.data_type() { Struct(f) => f.find("d").unwrap().1, _ => panic!() };
        assert!(d.is_nullable());
    }

    #[test]
    fn test_schema_null_struct_itself_made_nullable() {
        let schema = make_schema(vec![
            Field::new("a", Struct(Fields::from(vec![
                Field::new("b", DataType::Int32, false),
            ])), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a"]));
        let a = result.field_with_name("a").unwrap();
        assert!(a.is_nullable());
        if let Struct(fields) = a.data_type() {
            assert!(!fields.find("b").unwrap().1.is_nullable());
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_schema_null_struct_and_child_both_made_nullable() {
        let schema = make_schema(vec![
            Field::new("a", Struct(Fields::from(vec![
                Field::new("b", DataType::Int32, false),
            ])), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a", "a.b"]));
        let a = result.field_with_name("a").unwrap();
        assert!(a.is_nullable());
        if let Struct(fields) = a.data_type() {
            assert!(fields.find("b").unwrap().1.is_nullable());
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_schema_null_list_containing_struct() {
        let schema = make_schema(vec![
            Field::new("a", List(Arc::new(
                Field::new("item", Struct(Fields::from(vec![
                    Field::new("b", DataType::Int32, false),
                ])), false),
            )), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a.b"]));
        let a = result.field_with_name("a").unwrap();
        assert!(!a.is_nullable());
        if let List(inner) = a.data_type() {
            if let Struct(fields) = inner.data_type() {
                assert!(fields.find("b").unwrap().1.is_nullable());
            } else {
                panic!("expected struct inside list");
            }
        } else {
            panic!("expected list");
        }
    }

    #[test]
    fn test_schema_null_list_itself_made_nullable() {
        let schema = make_schema(vec![
            Field::new("a", List(Arc::new(
                Field::new("item", DataType::Int32, false),
            )), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a"]));
        assert!(result.field_with_name("a").unwrap().is_nullable());
    }

    #[test]
    fn test_schema_null_dictionary_with_nested_value() {
        let schema = make_schema(vec![
            Field::new("a", Dictionary(
                Box::new(DataType::Int32),
                Box::new(Struct(Fields::from(vec![
                    Field::new("b", DataType::Utf8, false),
                ]))),
            ), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a.b"]));
        let a = result.field_with_name("a").unwrap();
        if let Dictionary(_, value) = a.data_type() {
            if let Struct(fields) = value.as_ref() {
                assert!(fields.find("b").unwrap().1.is_nullable());
            } else {
                panic!("expected struct in dict value");
            }
        } else {
            panic!("expected dictionary");
        }
    }

    #[test]
    fn test_schema_null_map_with_nested_value() {
        let schema = make_schema(vec![
            Field::new("a", Map(Arc::new(
                Field::new("entries", Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", Struct(Fields::from(vec![
                        Field::new("b", DataType::Int32, false),
                    ])), false),
                ])), false),
            ), false), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a.value.b"]));
        let a = result.field_with_name("a").unwrap();
        if let Map(inner, _) = a.data_type() {
            if let Struct(entries) = inner.data_type() {
                let value_field = entries.find("value").unwrap().1;
                if let Struct(vf) = value_field.data_type() {
                    assert!(vf.find("b").unwrap().1.is_nullable());
                } else {
                    panic!("expected struct in value");
                }
            } else {
                panic!("expected struct in map entries");
            }
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn test_schema_null_multiple_paths_across_tree() {
        let schema = make_schema(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", Struct(Fields::from(vec![
                Field::new("z", DataType::Float64, false),
                Field::new("w", DataType::Boolean, false),
            ])), false),
        ]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["x", "y.z"]));
        assert!(result.field_with_name("x").unwrap().is_nullable());
        let y = result.field_with_name("y").unwrap();
        assert!(!y.is_nullable());
        if let Struct(fields) = y.data_type() {
            assert!(fields.find("z").unwrap().1.is_nullable());
            assert!(!fields.find("w").unwrap().1.is_nullable());
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_schema_null_metadata_preserved() {
        let mut field = Field::new("a", DataType::Int32, false);
        field.set_metadata([("key".to_string(), "val".to_string())].into());
        let schema = make_schema(vec![field]);
        let result = rewrite_schema_with_nullable_fields_inner(schema, &paths(&["a"]));
        let a = result.field_with_name("a").unwrap();
        assert!(a.is_nullable());
        assert_eq!(a.metadata().get("key").unwrap(), "val");
    }
}
