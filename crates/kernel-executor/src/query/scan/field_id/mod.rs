//! Matches parquet columns to the requested fields by field id, as the kernel's default engine
//! does.
//!
//! DataFusion's default adapter finds file columns by name. [`FieldIdAdapter`] renames each
//! requested field to the file column it matches, at every depth, and gives that schema to the
//! default adapter. The expressions then name the file's own columns, so DataFusion's pruning and
//! row filters, which look columns up in the file schema, read the right ones. [`RenameFields`]
//! restores the requested child names on nested results.
//!
//! Matching follows the kernel's `match_parquet_fields`: a file column matches by id
//! (`PARQUET:field_id`) if a requested field has that id, otherwise by name. An id match wins over
//! a name match. List elements and map keys and values match by position. A requested field with
//! no match is renamed away from the file's names, so DataFusion fills it with NULLs or reports it
//! missing.

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array as _, ArrayData, ArrayRef, RecordBatch, make_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult as _, TreeNode as _};
use datafusion::common::{Result, ScalarValue, internal_datafusion_err};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::ColumnarValue;
use datafusion_physical_expr_adapter::{DefaultPhysicalExprAdapter, PhysicalExprAdapter};

/// A [`PhysicalExprAdapter`] that resolves columns by field id, then runs DataFusion's default
/// adapter.
#[derive(Debug)]
pub(super) struct FieldIdAdapter {
    /// The requested schema.
    logical: SchemaRef,
    /// `logical` with each field renamed to the file column it matches. Same shape and order.
    renamed: SchemaRef,
    physical: SchemaRef,
    inner: DefaultPhysicalExprAdapter,
}

impl FieldIdAdapter {
    /// The adapter for one file, or `None` if no field changes its name.
    pub(super) fn try_new(logical: SchemaRef, physical: SchemaRef) -> Option<Self> {
        if !logical.fields().iter().any(|field| carries_field_id(field)) {
            return None;
        }
        let fields: Fields = rename_fields(logical.fields(), physical.fields())?;
        let renamed = Arc::new(Schema::new_with_metadata(
            fields,
            logical.metadata().clone(),
        ));
        Some(Self {
            inner: DefaultPhysicalExprAdapter::new(Arc::clone(&renamed), Arc::clone(&physical)),
            logical,
            renamed,
            physical,
        })
    }

    /// `expr` with every column resolved by name in `logical`, as the default adapter does.
    fn resolve_by_name(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|expr| {
            let Some(column) = expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };
            match self.logical.index_of(column.name()) {
                Ok(index) if index != column.index() => {
                    Ok(Transformed::yes(
                        Arc::new(Column::new(column.name(), index)) as _,
                    ))
                }
                _ => Ok(Transformed::no(expr)),
            }
        })
        .data()
    }

    /// `expr` with column names and `get_field` keys taken from `renamed`.
    fn rename_references(&self, expr: &Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        if let Some(column) = expr.downcast_ref::<Column>() {
            // Not a file column, e.g. a partition column.
            let Ok(index) = self.logical.index_of(column.name()) else {
                return Ok(Arc::clone(expr));
            };
            // `renamed` keeps the order of `logical`: the same index is the same field, with the
            // name of the file column it matched.
            let name = self.renamed.field(index).name();
            if name == column.name() {
                return Ok(Arc::clone(expr));
            }
            return Ok(Arc::new(Column::new(name, index)));
        }

        let children = expr.children();
        let renamed_children = children
            .iter()
            .map(|child| self.rename_references(child))
            .collect::<Result<Vec<_>>>()?;
        if let Some(get_field) =
            ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())
        {
            return self.rename_get_field(expr, get_field, renamed_children);
        }
        if children
            .iter()
            .zip(&renamed_children)
            .all(|(child, renamed)| Arc::ptr_eq(child, renamed))
        {
            return Ok(Arc::clone(expr));
        }
        match expr.downcast_ref::<ScalarFunctionExpr>() {
            // `with_new_children` keeps a function's return type, which names the requested
            // children. Build it again so a `get_field` above it sees the file's names.
            Some(function) => self.rebuild(function, renamed_children),
            None => Arc::clone(expr).with_new_children(renamed_children),
        }
    }

    /// `get_field(source, keys..)` over the renamed `args`, with each struct key renamed along the
    /// source's type. A map key is a lookup by value and stays as it is.
    fn rename_get_field(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        get_field: &ScalarFunctionExpr,
        mut args: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let (Some(source), Some(renamed_source)) = (get_field.args().first(), args.first()) else {
            return Ok(Arc::clone(expr));
        };
        // The source's type before and after renaming: the same shape, so a key's position in
        // one is its position in the other.
        let (Ok(mut logical), Ok(mut renamed)) = (
            self.resolve_by_name(Arc::clone(source))?
                .data_type(&self.logical),
            renamed_source.data_type(&self.renamed),
        ) else {
            return self.rebuild(get_field, args);
        };
        for key in args.iter_mut().skip(1) {
            let next = match (&logical, &renamed) {
                (DataType::Struct(logical_fields), DataType::Struct(renamed_fields)) => {
                    let name = key
                        .downcast_ref::<Literal>()
                        .and_then(|literal| literal.value().try_as_str().flatten())
                        .map(str::to_string);
                    name.and_then(|name| {
                        let position = logical_fields.iter().position(|f| *f.name() == name)?;
                        let renamed_field = &renamed_fields[position];
                        if *renamed_field.name() != name {
                            *key = Arc::new(Literal::new(ScalarValue::Utf8(Some(
                                renamed_field.name().clone(),
                            ))));
                        }
                        Some((
                            logical_fields[position].data_type(),
                            renamed_field.data_type(),
                        ))
                    })
                }
                (DataType::Map(logical_entries, _), DataType::Map(renamed_entries, _)) => {
                    map_value(logical_entries)
                        .zip(map_value(renamed_entries))
                        .map(|(logical, renamed)| (logical.data_type(), renamed.data_type()))
                }
                _ => None,
            };
            // Once a key does not resolve, no later key can.
            let Some((next_logical, next_renamed)) = next else {
                break;
            };
            (logical, renamed) = (next_logical.clone(), next_renamed.clone());
        }
        if get_field
            .args()
            .iter()
            .zip(&args)
            .all(|(original, renamed)| Arc::ptr_eq(original, renamed))
        {
            return Ok(Arc::clone(expr));
        }
        self.rebuild(get_field, args)
    }

    /// `function` over `args`, with its return type computed against `renamed`.
    fn rebuild(
        &self,
        function: &ScalarFunctionExpr,
        args: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(function.fun().clone()),
            args,
            &self.renamed,
            Arc::new(function.config_options().clone()),
        )?))
    }
}

impl PhysicalExprAdapter for FieldIdAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // Use the file's names, then let the default adapter fix indices, casts, and missing
        // columns by name.
        let rewritten = self.inner.rewrite(self.rename_references(&expr)?)?;

        // Only a nested result can carry the file's child names.
        let actual_datatype = rewritten.data_type(&self.physical)?;
        if !actual_datatype.is_nested() {
            return Ok(rewritten);
        }
        // The type the caller asked for, with the requested child names.
        let Ok(expected) = self.resolve_by_name(expr)?.return_field(&self.logical) else {
            return Ok(rewritten);
        };
        if &actual_datatype == expected.data_type() {
            return Ok(rewritten);
        }
        // Put the requested child names back on the rows read from the file.
        Ok(Arc::new(RenameFields::new(rewritten, expected)))
    }
}

/// Whether `field` or any field under it carries a field id.
fn carries_field_id(field: &Field) -> bool {
    field.metadata().contains_key(PARQUET_FIELD_ID_META_KEY)
        || match field.data_type() {
            DataType::Struct(fields) => fields.iter().any(|field| carries_field_id(field)),
            DataType::Map(entries, _) => carries_field_id(entries),
            DataType::Dictionary(_, value) => {
                carries_field_id(&Field::new("value", value.as_ref().clone(), true))
            }
            other => list_element(other).is_some_and(|element| carries_field_id(element)),
        }
}

fn field_id(field: &Field) -> Option<i64> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()
}

/// `requested` renamed to the file fields it matches, at every depth, or `None` if no name
/// changes.
fn rename_fields(requested: &Fields, file: &Fields) -> Option<Fields> {
    let ids: HashMap<i64, usize> = requested
        .iter()
        .enumerate()
        .filter_map(|(index, field)| Some((field_id(field)?, index)))
        .collect();

    // Id matches first, so a name match cannot take a field that another file field has the id of.
    let mut matched: Vec<Option<&FieldRef>> = vec![None; requested.len()];
    let mut by_name = Vec::new();
    for file_field in file {
        match field_id(file_field).and_then(|id| ids.get(&id)) {
            Some(&index) => {
                matched[index].get_or_insert(file_field);
            }
            None => by_name.push(file_field),
        }
    }
    for file_field in by_name {
        if let Some(index) = requested
            .iter()
            .position(|field| field.name() == file_field.name())
        {
            matched[index].get_or_insert(file_field);
        }
    }

    let mut changed = false;
    let fields: Vec<FieldRef> = requested
        .iter()
        .zip(matched)
        .map(|(field, file_field)| {
            let renamed = match file_field {
                Some(file_field) => {
                    let data_type = rename_type(field.data_type(), file_field.data_type());
                    if data_type.is_none() && field.name() == file_field.name() {
                        return Arc::clone(field);
                    }
                    let data_type = data_type.unwrap_or_else(|| field.data_type().clone());
                    field
                        .as_ref()
                        .clone()
                        .with_name(file_field.name())
                        .with_data_type(data_type)
                }
                None if file.find(field.name()).is_some() => field
                    .as_ref()
                    .clone()
                    .with_name(format!("{} (not in file)", field.name())),
                None => return Arc::clone(field),
            };
            changed = true;
            Arc::new(renamed)
        })
        .collect();
    changed.then(|| fields.into())
}

/// `requested` with its nested fields renamed to the file's, or `None` if no name changes.
fn rename_type(requested: &DataType, file: &DataType) -> Option<DataType> {
    match (requested, file) {
        (DataType::Struct(requested), DataType::Struct(file)) => {
            rename_fields(requested, file).map(DataType::Struct)
        }
        (DataType::Map(requested, sorted), DataType::Map(file, _)) => {
            let (DataType::Struct(requested_kv), DataType::Struct(file_kv)) =
                (requested.data_type(), file.data_type())
            else {
                return None;
            };
            let [key, value] = requested_kv.iter().collect::<Vec<_>>()[..] else {
                return None;
            };
            let [file_key, file_value] = file_kv.iter().collect::<Vec<_>>()[..] else {
                return None;
            };
            let (renamed_key, renamed_value) = (
                rename_element(key, file_key),
                rename_element(value, file_value),
            );
            if renamed_key.is_none() && renamed_value.is_none() {
                return None;
            }
            let entries = DataType::Struct(Fields::from(vec![
                renamed_key.unwrap_or_else(|| Arc::clone(key)),
                renamed_value.unwrap_or_else(|| Arc::clone(value)),
            ]));
            Some(DataType::Map(
                Arc::new(requested.as_ref().clone().with_data_type(entries)),
                *sorted,
            ))
        }
        (DataType::Dictionary(key, value), DataType::Dictionary(_, file_value)) => {
            rename_type(value, file_value)
                .map(|value| DataType::Dictionary(key.clone(), Box::new(value)))
        }
        // Lists match their elements by position, whatever the list type on either side.
        (requested, file) => {
            let element = rename_element(list_element(requested)?, list_element(file)?)?;
            Some(with_list_element(requested, element))
        }
    }
}

/// `requested` with its type renamed to `file`'s. The field keeps its own name.
fn rename_element(requested: &FieldRef, file: &FieldRef) -> Option<FieldRef> {
    let data_type = rename_type(requested.data_type(), file.data_type())?;
    Some(Arc::new(
        requested.as_ref().clone().with_data_type(data_type),
    ))
}

/// `list` with its element replaced by `element`.
fn with_list_element(list: &DataType, element: FieldRef) -> DataType {
    match list {
        DataType::LargeList(_) => DataType::LargeList(element),
        DataType::ListView(_) => DataType::ListView(element),
        DataType::LargeListView(_) => DataType::LargeListView(element),
        DataType::FixedSizeList(_, size) => DataType::FixedSizeList(element, *size),
        _ => DataType::List(element),
    }
}

/// The value field of a map's entries.
fn map_value(entries: &FieldRef) -> Option<&FieldRef> {
    match entries.data_type() {
        DataType::Struct(kv) if kv.len() == 2 => Some(&kv[1]),
        _ => None,
    }
}

fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::ListView(element)
        | DataType::LargeListView(element)
        | DataType::FixedSizeList(element, _) => Some(element),
        _ => None,
    }
}

/// Gives `input` the type of `field`, which differs only in struct field names. No data is copied.
#[derive(Debug, Eq)]
pub(super) struct RenameFields {
    input: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl RenameFields {
    fn new(input: Arc<dyn PhysicalExpr>, field: FieldRef) -> Self {
        Self { input, field }
    }
}

impl PartialEq for RenameFields {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input) && self.field.eq(&other.field)
    }
}

impl Hash for RenameFields {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.field.hash(state);
    }
}

impl fmt::Display for RenameFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RENAME_FIELDS({} AS {})",
            self.input,
            self.field.data_type()
        )
    }
}

impl PhysicalExpr for RenameFields {
    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.field))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(rename_array(
            array,
            self.field.data_type(),
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|children| {
            internal_datafusion_err!("RenameFields expects one child, got {}", children.len())
        })?;
        Ok(Arc::new(Self::new(input, Arc::clone(&self.field))))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.input.fmt_sql(f)
    }
}

/// `array` with the struct field names of `data_type`, reusing its buffers. Errors if the types
/// differ in more than names.
fn rename_array(array: ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() == data_type {
        return Ok(array);
    }
    let data = array.to_data();
    let children = match data_type {
        DataType::Struct(fields) => data
            .child_data()
            .iter()
            .zip(fields.iter())
            .map(|(child, field)| rename_child(child, field.data_type()))
            .collect::<Result<_>>()?,
        DataType::Map(entries, _) => {
            vec![rename_child(&data.child_data()[0], entries.data_type())?]
        }
        DataType::Dictionary(_, value) => vec![rename_child(&data.child_data()[0], value)?],
        other => match list_element(other) {
            Some(element) => vec![rename_child(&data.child_data()[0], element.data_type())?],
            None => data.child_data().to_vec(),
        },
    };
    let data = data
        .into_builder()
        .data_type(data_type.clone())
        .child_data(children)
        .build()?;
    Ok(make_array(data))
}

fn rename_child(child: &ArrayData, data_type: &DataType) -> Result<ArrayData> {
    Ok(rename_array(make_array(child.clone()), data_type)?.to_data())
}

#[cfg(test)]
mod tests;
