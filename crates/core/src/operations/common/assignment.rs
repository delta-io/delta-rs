//! Assignments of `update` and `merge`: the column or struct field that a clause sets, and the
//! value that a struct column gets when a clause sets some of its fields.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::{cast as cast_array, is_null, nullif};
use arrow_array::{Array as _, ArrayRef, StructArray, cast::AsArray as _};
use arrow_schema::{DataType, Field, Fields, Schema};
use datafusion::catalog::Session;
use datafusion::common::{
    DFSchema, Result as DataFusionResult, ScalarValue, exec_err, internal_err, plan_err,
};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, cast,
    lit,
};
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::StructType;
use deltalake_kernel_executor::query::scan::expr_adapter::relax_schema_nested_nullability;

use super::cast_expr;
use crate::delta_datafusion::DeltaColumn;
use crate::delta_datafusion::expr::parse_column_name;
use crate::{DeltaResult, DeltaTableError};

/// An assignment key, resolved to the path of a column or struct field of the table
#[derive(Debug, Clone)]
pub(crate) struct AssignmentKey {
    /// The key as the user wrote it. Error messages show this text.
    written: String,
    path: ColumnName,
}

impl AssignmentKey {
    pub(crate) fn new(written: impl Into<String>, path: ColumnName) -> Self {
        let written = written.into();
        Self { written, path }
    }

    /// The names of the column and of the struct fields under it. The table alias is not part
    /// of the path.
    pub(crate) fn path(&self) -> &ColumnName {
        &self.path
    }
}

/// Resolves an assignment key to the path of a column or struct field of the table
///
/// `schema` holds the columns that a key can name. The DataFusion SQL planner reads the key
/// against it, so the key can start with the table alias. The alias is not part of the path.
pub(crate) fn resolve_assignment_key(
    key: &DeltaColumn,
    schema: &DFSchema,
    table_schema: &StructType,
    session: &dyn Session,
) -> DeltaResult<AssignmentKey> {
    let expr = parse_column_name(schema, key.name(), session)?;
    let Some(path) = column_path(&expr) else {
        // Not expected: for a column name, the planner returns a column or `get_field` calls
        return Err(DeltaTableError::generic(format!(
            "Assignment key '{}' gave the unexpected expression '{expr}'",
            key.name()
        )));
    };
    if path.len() > 1 {
        // The planner checks only the column name. Check here that the names after it are struct
        // fields: without this, `s.no_such_field` passes, and so does `m.key` on a map column.
        table_schema.field_at(&path)?;
    }
    Ok(AssignmentKey::new(key.name(), path))
}

/// The path of a column reference, or of struct field accesses on one
fn column_path(expr: &Expr) -> Option<ColumnName> {
    match expr {
        Expr::Column(column) => Some(ColumnName::new([column.name()])),
        Expr::ScalarFunction(function) if function.func.inner().is::<GetFieldFunc>() => {
            let (base, names) = function.args.split_first()?;
            let names = names
                .iter()
                .map(|name| match name {
                    Expr::Literal(name, _) => name.try_as_str().flatten(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            Some(column_path(base)?.join(&ColumnName::new(names)))
        }
        _ => None,
    }
}

/// The assignments of one clause: the `SET` of an `update`, or one `WHEN` clause of a `merge`
///
/// An assignment is a key with an expression, such as `s.a = 1`. Each key is resolved to a path
/// in the table schema.
#[derive(Debug, Default)]
pub(crate) struct Assignments {
    /// The assignments to a column or to fields of it, by the name of the column
    columns: HashMap<String, Vec<(AssignmentKey, Expr)>>,
}

impl Assignments {
    /// Fails if two keys set the same field, such as `s` and `s.a`
    pub(crate) fn try_new(assignments: Vec<(AssignmentKey, Expr)>) -> DeltaResult<Self> {
        let mut keys: Vec<_> = assignments.iter().map(|(key, _)| key).collect();
        keys.sort_by_key(|key| key.path());
        if let Some(keys) = keys
            .windows(2)
            .find(|keys| keys[1].path().path().starts_with(keys[0].path().path()))
        {
            return Err(DeltaTableError::generic(format!(
                "Assignment keys '{}' and '{}' set the same field",
                keys[0].written, keys[1].written
            )));
        }

        let mut columns: HashMap<_, Vec<_>> = HashMap::new();
        for (key, expr) in assignments {
            let column = key.path()[0].clone();
            columns.entry(column).or_default().push((key, expr));
        }
        Ok(Self { columns })
    }

    /// The paths of the columns and struct fields that the assignments set
    pub(crate) fn paths(&self) -> impl Iterator<Item = &ColumnName> {
        self.columns.values().flatten().map(|(key, _)| key.path())
    }

    /// Returns the new value of the column `name`, which has the type `data_type`
    ///
    /// - No assignment sets the column: `None`.
    /// - An assignment sets the column: its expression, unchanged.
    /// - Assignments set fields of a struct column: an expression that builds a struct of the
    ///   type `data_type`. A field with an assignment gets that expression, cast to the type of
    ///   the field. The other fields keep their value.
    ///
    /// `current` is the column in the existing row, with its type. That type differs from
    /// `data_type` when a merge adds fields to the struct: a field that the existing struct does
    /// not have is NULL unless an assignment sets it. If the struct is NULL in the existing row,
    /// the struct is built and the other fields are NULL, as in Delta Spark. An insert has no
    /// existing row and passes `None`; the other fields are then NULL too.
    pub(crate) fn column_value(
        &self,
        name: &str,
        data_type: &DataType,
        current: Option<(Expr, &DataType)>,
        safe_cast: bool,
    ) -> DataFusionResult<Option<Expr>> {
        let Some(assignments) = self.columns.get(name) else {
            return Ok(None);
        };
        match (assignments.as_slice(), data_type) {
            ([(key, expr)], _) if key.path().len() == 1 => Ok(Some(expr.clone())),
            (assignments, DataType::Struct(fields)) => {
                let assignments: Vec<_> = assignments
                    .iter()
                    .map(|(key, expr)| (key.path().path(), expr))
                    .collect();
                let current = current.map(struct_fields).transpose()?;
                struct_value(fields, current, &assignments, safe_cast).map(Some)
            }
            _ => internal_err!("Assignment to a field of column '{name}', which is no struct"),
        }
    }
}

/// The fields of the existing struct `current`, next to the struct
fn struct_fields((current, data_type): (Expr, &DataType)) -> DataFusionResult<(Expr, &Fields)> {
    match data_type {
        DataType::Struct(fields) => Ok((current, fields)),
        _ => internal_err!("Assignment to a field of '{current}', which is no struct"),
    }
}

/// The value of a struct with the type `fields`, from the assignments to its fields. Each path
/// starts at the struct and names a field in it. `current` is the existing struct with its
/// fields.
fn struct_value(
    fields: &Fields,
    current: Option<(Expr, &Fields)>,
    struct_assignments: &[(&[String], &Expr)],
    safe_cast: bool,
) -> DataFusionResult<Expr> {
    let mut args = Vec::with_capacity(fields.len());
    for field in fields {
        // The assignments to this field or to fields in it. Each path now starts at this field.
        let field_assignments: Vec<_> = struct_assignments
            .iter()
            .map(|(struct_path, expr)| (&struct_path[1..], *expr))
            .filter(|(field_path, _)| field_path[0] == *field.name())
            .collect();
        // The field in the existing struct. A field that the merge adds is not there.
        let current = current.as_ref().and_then(|(current, current_fields)| {
            let (_, current_field) = current_fields.find(field.name())?;
            let value = field_or_null(current.clone(), field.name());
            Some((value, current_field.data_type()))
        });
        // DataFusion does not cast to a type with a NOT NULL nested field. `StructWithFields`
        // gives the value the type of the field.
        let cast_type = with_nullable_nested_fields(field);

        let value = match (field_assignments.as_slice(), field.data_type()) {
            // The field keeps its value. That value is NULL if the struct is NULL.
            ([], data_type) => match current {
                Some((current, current_type)) if current_type == data_type => current,
                // The merge changes the type of the field, for example it adds a nested field
                Some((current, _)) => cast(current, cast_type),
                None => lit(ScalarValue::try_new_null(data_type)?),
            },
            ([(field_path, expr)], _) if field_path.len() == 1 => {
                cast_expr((*expr).clone(), cast_type, safe_cast)
            }
            (field_assignments, DataType::Struct(fields)) => {
                let current = current.map(struct_fields).transpose()?;
                struct_value(fields, current, field_assignments, safe_cast)?
            }
            _ => {
                return internal_err!(
                    "Assignment to a field of '{}', which is no struct",
                    field.name()
                );
            }
        };
        args.push(value);
    }
    Ok(ScalarUDF::from(StructWithFields::new(fields.clone())).call(args))
}

/// The type of `field`, with each nested field nullable
fn with_nullable_nested_fields(field: &Field) -> DataType {
    let schema = relax_schema_nested_nullability(&Schema::new(vec![field.clone()]));
    schema.field(0).data_type().clone()
}

/// The value of the field `name` in the existing struct `current`, or NULL if that struct is NULL
fn field_or_null(current: Expr, name: &str) -> Expr {
    ScalarUDF::from(FieldOrNull::new(name)).call(vec![current])
}

/// Returns one field of a struct, or NULL where the struct is NULL
///
/// `get_field` is not enough: where the struct is NULL it returns the value that the field has
/// in storage, which can be any value. A `CASE` around `get_field` holds the struct expression
/// two times, so its size doubles for each level of a nested struct.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FieldOrNull {
    name: String,
    signature: Signature,
}

impl FieldOrNull {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FieldOrNull {
    fn name(&self) -> &str {
        "field_or_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        match arg_types {
            [DataType::Struct(fields)] => match fields.find(&self.name) {
                Some((_, field)) => Ok(field.data_type().clone()),
                None => plan_err!("The struct has no field '{}'", self.name),
            },
            _ => plan_err!(
                "The field '{}' is read from a value that is no struct",
                self.name
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return internal_err!("field_or_null takes one argument");
        };
        let value = value.to_array(args.number_rows)?;
        let Some(value) = value.as_struct_opt() else {
            return exec_err!(
                "The field '{}' is read from a value that is no struct",
                self.name
            );
        };
        let Some(field) = value.column_by_name(&self.name) else {
            return exec_err!("The struct has no field '{}'", self.name);
        };
        let field = match value.null_count() {
            0 => Arc::clone(field),
            _ => nullif(field, &is_null(value)?)?,
        };
        Ok(ColumnarValue::Array(field))
    }
}

/// Builds a struct that has the exact type of a table column
///
/// `named_struct` makes each field nullable, and DataFusion does not cast such a struct to a
/// type with a NOT NULL field. The arguments are the values of the fields, in the order of the
/// fields. The struct itself is never NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StructWithFields {
    fields: Fields,
    signature: Signature,
}

impl StructWithFields {
    fn new(fields: Fields) -> Self {
        let signature = Signature::any(fields.len(), Volatility::Immutable);
        Self { fields, signature }
    }

    /// The value of `field` from an argument. The argument can have nullable nested fields
    /// where the type of `field` has NOT NULL fields.
    fn field_value(field: &Field, array: ArrayRef) -> DataFusionResult<ArrayRef> {
        if !field.is_nullable() && array.logical_null_count() > 0 {
            return exec_err!("NULL value in the NOT NULL field '{}'", field.name());
        }
        if array.data_type() == field.data_type() {
            return Ok(array);
        }
        // Fails if a NOT NULL nested field holds a NULL
        Ok(cast_array(&array, field.data_type())?)
    }
}

impl ScalarUDFImpl for StructWithFields {
    fn name(&self) -> &str {
        "struct_with_fields"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Struct(self.fields.clone()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = self
            .fields
            .iter()
            .zip(&args.args)
            .map(|(field, arg)| Self::field_value(field, arg.to_array(args.number_rows)?))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let value = StructArray::try_new(self.fields.clone(), arrays, None)?;
        Ok(ColumnarValue::Array(Arc::new(value)))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion::common::Column;
    use datafusion::functions::core::expr_fn::{coalesce, get_field, get_field_path};
    use datafusion::prelude::{SessionContext, col};
    use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
    use rstest::rstest;

    use super::*;
    use crate::DeltaTable;
    use crate::delta_datafusion::create_session;

    /// The columns that the keys in the tests name. Each struct column has the field `a` and the
    /// struct `inner`, which has the field `x`.
    fn key_columns() -> Schema {
        let inner = Fields::from(vec![Field::new("x", DataType::Int32, true)]);
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("inner", DataType::Struct(inner), true),
        ]);
        Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("Value", DataType::Int32, true),
            Field::new("s", DataType::Struct(fields.clone()), true),
            Field::new("my.col", DataType::Struct(fields.clone()), true),
            Field::new("my col", DataType::Struct(fields), true),
        ])
    }

    #[test]
    fn test_resolve_assignment_key() {
        let schema = key_columns();
        let table_schema: StructType = (&schema).try_into_kernel().unwrap();
        let key_schema = DFSchema::try_from_qualified_schema("t", &schema).unwrap();
        let session = create_session().state();

        let keys: [(DeltaColumn, &[&str]); 11] = [
            ("id".into(), &["id"]),
            ("t.id".into(), &["id"]),
            ("Value".into(), &["Value"]),
            ("s.a".into(), &["s", "a"]),
            ("t.s.a".into(), &["s", "a"]),
            ("`s`.`a`".into(), &["s", "a"]),
            ("`my.col`.a".into(), &["my.col", "a"]),
            ("t.`my col`.a".into(), &["my col", "a"]),
            // Text that is not a list of identifiers is one column name
            ("my col".into(), &["my col"]),
            (Column::new(Some("t"), "Value").into(), &["Value"]),
            (Column::from_name("my.col").into(), &["my.col"]),
        ];
        for (key, path) in keys {
            let resolved = resolve_assignment_key(&key, &key_schema, &table_schema, &session);
            assert_eq!(
                resolved.unwrap().path(),
                &ColumnName::new(path.iter().copied()),
                "{key:?}"
            );
        }

        for key in [
            "value", "other.id", "s.b", "id.a", "my.col", "s['a']", "id + 1",
        ] {
            let key = DeltaColumn::from(key);
            let resolved = resolve_assignment_key(&key, &key_schema, &table_schema, &session);
            assert!(resolved.is_err(), "{key:?}");
        }
    }

    /// The column `name` of the table that has the alias `table`
    fn column(table: &str, name: &str) -> Expr {
        Expr::Column(Column::new(Some(table), name))
    }

    #[rstest]
    #[case::column(column("t", "id"), &["id"])]
    #[case::column_without_alias(Expr::Column(Column::from_name("id")), &["id"])]
    #[case::dot_in_column_name(column("t", "my.col"), &["my.col"])]
    #[case::struct_field(get_field(column("t", "s"), "a"), &["s", "a"])]
    #[case::nested_struct_field(
        get_field(get_field(column("t", "s"), "inner"), "x"),
        &["s", "inner", "x"]
    )]
    #[case::two_names_in_one_call(
        get_field_path(column("t", "s"), vec![lit("inner"), lit("x")]),
        &["s", "inner", "x"]
    )]
    fn test_column_path(#[case] expr: Expr, #[case] expected: &[&str]) {
        let expected = ColumnName::new(expected.iter().copied());
        assert_eq!(column_path(&expr), Some(expected));
    }

    #[rstest]
    #[case::literal(lit(1))]
    #[case::binary_expression(column("t", "id") + lit(1))]
    #[case::cast(cast(column("t", "id"), DataType::Int64))]
    #[case::other_function(coalesce(vec![column("t", "id")]))]
    #[case::field_of_other_expression(get_field(coalesce(vec![column("t", "s")]), "a"))]
    #[case::field_name_from_column(get_field_path(column("t", "s"), vec![column("t", "name")]))]
    #[case::field_name_is_number(get_field(column("t", "s"), 1))]
    fn test_column_path_is_none_for_other_expression(#[case] expr: Expr) {
        assert_eq!(column_path(&expr), None);
    }

    /// Reads the path from the expression that the planner returns for a column name
    #[rstest]
    #[case::column("id", &["id"])]
    #[case::column_with_alias("t.id", &["id"])]
    #[case::struct_field("s.a", &["s", "a"])]
    #[case::struct_field_with_alias("t.s.a", &["s", "a"])]
    #[case::nested_struct_field("s.inner.x", &["s", "inner", "x"])]
    #[case::nested_struct_field_with_alias("t.s.inner.x", &["s", "inner", "x"])]
    #[case::dot_in_quotes("`my.col`.a", &["my.col", "a"])]
    #[case::space("my col", &["my col"])]
    fn test_column_path_of_parsed_column_name(#[case] name: &str, #[case] expected: &[&str]) {
        let schema = DFSchema::try_from_qualified_schema("t", &key_columns()).unwrap();
        let session = create_session().state();

        let expr = parse_column_name(&schema, name, &session).unwrap();

        let expected = ColumnName::new(expected.iter().copied());
        assert_eq!(column_path(&expr), Some(expected));
    }

    /// The key `written`, resolved to `path`
    fn key(written: &str, path: &[&str]) -> (AssignmentKey, Expr) {
        let path = ColumnName::new(path.iter().copied());
        (AssignmentKey::new(written, path), lit(1))
    }

    #[test]
    fn test_paths() {
        let keys = vec![key("id", &["id"]), key("t.s.a", &["s", "a"])];
        let assignments = Assignments::try_new(keys).unwrap();

        let mut paths: Vec<_> = assignments.paths().map(ToString::to_string).collect();
        paths.sort();
        assert_eq!(paths, ["id", "s.a"]);
    }

    #[rstest]
    #[case::same_column(key("value", &["value"]), key("t.value", &["value"]))]
    #[case::column_and_field(key("s", &["s"]), key("t.s.a", &["s", "a"]))]
    #[case::field_and_nested_field(key("s.inner.x", &["s", "inner", "x"]), key("s.inner", &["s", "inner"]))]
    fn test_keys_that_set_the_same_field(
        #[case] first: (AssignmentKey, Expr),
        #[case] second: (AssignmentKey, Expr),
    ) {
        let (first_key, second_key) = (first.0.written.clone(), second.0.written.clone());

        let err = Assignments::try_new(vec![first, second])
            .unwrap_err()
            .to_string();

        // The message shows the keys as written
        assert!(err.contains(&format!("'{first_key}'")), "{err}");
        assert!(err.contains(&format!("'{second_key}'")), "{err}");
    }

    /// Builds the new value of the column `s` for the rows of `struct_batch`. `current` is `None`
    /// for an insert. Where `s` or `s.inner` is NULL, the struct is built and the fields with no
    /// assignment are NULL.
    #[rstest]
    #[case::field(
        &[("s.a", 7)],
        Some(col("s")),
        r#"{"s": {"a": 7, "b": "one", "inner": {"x": 10, "y": 11}}}
           {"s": {"a": 7, "b": "two", "inner": null}}
           {"s": {"a": 7, "b": null, "inner": null}}"#
    )]
    #[case::nested_field(
        &[("s.inner.x", 8)],
        Some(col("s")),
        r#"{"s": {"a": 1, "b": "one", "inner": {"x": 8, "y": 11}}}
           {"s": {"a": 2, "b": "two", "inner": {"x": 8, "y": null}}}
           {"s": {"a": null, "b": null, "inner": {"x": 8, "y": null}}}"#
    )]
    #[case::two_fields(
        &[("s.a", 7), ("s.inner.y", 9)],
        Some(col("s")),
        r#"{"s": {"a": 7, "b": "one", "inner": {"x": 10, "y": 9}}}
           {"s": {"a": 7, "b": "two", "inner": {"x": null, "y": 9}}}
           {"s": {"a": 7, "b": null, "inner": {"x": null, "y": 9}}}"#
    )]
    #[case::insert(
        &[("s.a", 7)],
        None,
        r#"{"s": {"a": 7, "b": null, "inner": null}}
           {"s": {"a": 7, "b": null, "inner": null}}
           {"s": {"a": 7, "b": null, "inner": null}}"#
    )]
    #[case::insert_nested_field(
        &[("s.inner.x", 8)],
        None,
        r#"{"s": {"a": null, "b": null, "inner": {"x": 8, "y": null}}}
           {"s": {"a": null, "b": null, "inner": {"x": 8, "y": null}}}
           {"s": {"a": null, "b": null, "inner": {"x": 8, "y": null}}}"#
    )]
    #[tokio::test]
    async fn test_struct_value(
        #[case] assignments: &[(&str, i32)],
        #[case] current: Option<Expr>,
        #[case] expected: &str,
    ) {
        let batch = struct_batch();
        let column = batch.schema().field_with_name("s").unwrap().clone();
        let DataType::Struct(fields) = column.data_type() else {
            panic!("`s` is not a struct");
        };
        let assignments: Vec<_> = assignments
            .iter()
            .map(|(key, value)| (ColumnName::new(key.split('.')), lit(*value)))
            .collect();
        let assignments: Vec<_> = assignments
            .iter()
            .map(|(key, expr)| (key.path(), expr))
            .collect();

        let current = current.map(|current| (current, fields));
        let value = struct_value(fields, current, &assignments, false).unwrap();

        let rows = SessionContext::new()
            .read_batch(batch)
            .unwrap()
            .select(vec![value.alias("s")])
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows = arrow::compute::concat_batches(&rows[0].schema(), &rows).unwrap();
        let expected = batch_from_json(Schema::new(vec![column.clone()]), expected);
        assert_eq!(rows.column(0), expected.column(0));
    }

    #[tokio::test]
    async fn test_field_or_null_is_null_where_struct_is_null() {
        // In row 2 the struct is NULL, but the field `a` holds the value 99 in storage
        let fields = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 99]));
        let nulls = NullBuffer::from(vec![true, false]);
        let column = StructArray::new(fields.clone(), vec![values], Some(nulls));
        let schema = Schema::new(vec![Field::new("s", DataType::Struct(fields), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(column)]).unwrap();

        let field = field_or_null(col("s"), "a");

        let rows = SessionContext::new()
            .read_batch(batch)
            .unwrap()
            .select(vec![field.alias("a")])
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        assert_eq!(rows[0].column(0), &expected);
    }

    /// The fields of a struct that holds `depth` levels of the struct `inner`. Each level has
    /// the field `sibling` too, and the last level has the field `x`.
    fn nested_fields(depth: usize) -> Fields {
        let first = match depth {
            0 => Field::new("x", DataType::Int32, true),
            _ => Field::new("inner", DataType::Struct(nested_fields(depth - 1)), true),
        };
        Fields::from(vec![first, Field::new("sibling", DataType::Int32, true)])
    }

    /// The number of nodes in the value of a struct, when an assignment sets the field `x`
    /// that is `depth` levels down
    fn struct_value_nodes(depth: usize) -> usize {
        use datafusion::common::tree_node::{TreeNode as _, TreeNodeRecursion};

        let mut path = vec!["inner".to_string(); depth];
        path.push("x".to_string());
        let value = lit(1);
        let assignments = [(path.as_slice(), &value)];
        let fields = nested_fields(depth);
        let current = Some((col("s"), &fields));
        let expr = struct_value(&fields, current, &assignments, false);

        let mut nodes = 0;
        expr.unwrap()
            .apply(|_| {
                nodes += 1;
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        nodes
    }

    /// The value of a struct must not double in size for each level between the struct and
    /// the field that an assignment sets
    #[test]
    fn test_struct_value_size_grows_with_depth_not_exponentially() {
        let (shallow, deep) = (struct_value_nodes(4), struct_value_nodes(8));
        assert!(
            deep <= 4 * shallow,
            "{shallow} nodes at depth 4, {deep} nodes at depth 8"
        );
    }

    /// Reads rows, one JSON object for each line, into a batch
    pub(crate) fn batch_from_json(schema: Schema, rows: &str) -> RecordBatch {
        arrow_json::ReaderBuilder::new(Arc::new(schema))
            .build(std::io::Cursor::new(rows))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
    }

    /// Rows with the struct column `s`, which holds the struct `inner`, and with the column `a`,
    /// which has the name of a field of `s`. In row 2 `s.inner` is NULL, in row 3 `s` is NULL.
    fn struct_batch() -> RecordBatch {
        let inner = Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ]);
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("inner", DataType::Struct(inner), true),
        ]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
            Field::new("s", DataType::Struct(fields), true),
        ]);
        batch_from_json(
            schema,
            r#"{"id": 1, "a": 100, "s": {"a": 1, "b": "one", "inner": {"x": 10, "y": 11}}}
               {"id": 2, "a": 200, "s": {"a": 2, "b": "two", "inner": null}}
               {"id": 3, "a": 300, "s": null}"#,
        )
    }

    /// A table with the rows of `struct_batch`
    pub(crate) async fn struct_table() -> DeltaTable {
        DeltaTable::new_in_memory()
            .write(vec![struct_batch()])
            .await
            .unwrap()
    }

    /// A table with the struct column `s`, where the field `s.a` is NOT NULL. In row 2 `s` is
    /// NULL.
    pub(crate) async fn not_null_field_table() -> DeltaTable {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("s", DataType::Struct(fields), true),
        ]);
        let batch = batch_from_json(
            schema,
            r#"{"id": 1, "s": {"a": 1, "b": "one"}}
               {"id": 2, "s": null}"#,
        );
        DeltaTable::new_in_memory()
            .write(vec![batch])
            .await
            .unwrap()
    }

    /// A table with the struct column `s`, where the field `s.inner.x` is NOT NULL
    pub(crate) async fn not_null_nested_field_table() -> DeltaTable {
        let inner = Fields::from(vec![Field::new("x", DataType::Int32, false)]);
        let fields = Fields::from(vec![
            Field::new("inner", DataType::Struct(inner), true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("s", DataType::Struct(fields), true),
        ]);
        let batch = batch_from_json(schema, r#"{"id": 1, "s": {"inner": {"x": 1}, "b": "one"}}"#);
        DeltaTable::new_in_memory()
            .write(vec![batch])
            .await
            .unwrap()
    }
}
