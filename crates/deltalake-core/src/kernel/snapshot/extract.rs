use std::sync::Arc;

use arrow_array::{
    Array, ArrowNativeTypeOp, ArrowNumericType, BooleanArray, ListArray, MapArray, PrimitiveArray,
    RecordBatch, StringArray, StructArray,
};
use arrow_schema::{ArrowError, DataType};

use crate::{DeltaResult, DeltaTableError};

pub(crate) trait ProvidesColumnByName {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>>;
}

impl ProvidesColumnByName for RecordBatch {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        self.column_by_name(name)
    }
}

impl ProvidesColumnByName for StructArray {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        self.column_by_name(name)
    }
}

pub(super) fn extract_and_cast<'a, T: Array + 'static>(
    arr: &'a dyn ProvidesColumnByName,
    name: &'a str,
) -> DeltaResult<&'a T> {
    extract_and_cast_opt::<T>(arr, name).ok_or(DeltaTableError::Generic(format!(
        "missing-column: {}",
        name
    )))
}

pub(super) fn extract_and_cast_opt<'a, T: Array + 'static>(
    array: &'a dyn ProvidesColumnByName,
    name: &'a str,
) -> Option<&'a T> {
    let mut path_steps = name.split('.');
    let first = path_steps.next()?;
    extract_column(array, first, &mut path_steps)
        .ok()?
        .as_any()
        .downcast_ref::<T>()
}

pub(super) fn extract_column<'a>(
    array: &'a dyn ProvidesColumnByName,
    path_step: &str,
    remaining_path_steps: &mut impl Iterator<Item = &'a str>,
) -> Result<&'a Arc<dyn Array>, ArrowError> {
    let child = array
        .column_by_name(path_step)
        .ok_or(ArrowError::SchemaError(format!(
            "No such field: {}",
            path_step,
        )))?;

    if let Some(next_path_step) = remaining_path_steps.next() {
        match child.data_type() {
            DataType::Map(_, _) => {
                // NOTE a map has exatly one child, but we wnat to be agnostic of its name.
                // so we case the current array as map, and use the entries accessor.
                let maparr = column_as_map(path_step, &Some(child))?;
                if let Some(next_path) = remaining_path_steps.next() {
                    extract_column(maparr.entries(), next_path, remaining_path_steps)
                } else {
                    Ok(child)
                    // if maparr.entries().num_columns() != 2 {
                    //     return Err(ArrowError::SchemaError(format!(
                    //         "Map {} has {} columns, expected 2",
                    //         path_step,
                    //         maparr.entries().num_columns()
                    //     )));
                    // }
                    // if next_path_step == *maparr.entries().column_names().first().unwrap() {
                    //     Ok(maparr.entries().column(0))
                    // } else {
                    //     Ok(maparr.entries().column(1))
                    // }
                }
            }
            DataType::List(_) => {
                let listarr = column_as_list(path_step, &Some(child))?;
                if let Some(next_path) = remaining_path_steps.next() {
                    extract_column(
                        column_as_struct(next_path_step, &Some(listarr.values()))?,
                        next_path,
                        remaining_path_steps,
                    )
                } else {
                    Ok(listarr.values())
                }
            }
            _ => extract_column(
                column_as_struct(path_step, &Some(child))?,
                next_path_step,
                remaining_path_steps,
            ),
        }
    } else {
        Ok(child)
    }
}

fn column_as_struct<'a>(
    name: &str,
    column: &Option<&'a Arc<dyn Array>>,
) -> Result<&'a StructArray, ArrowError> {
    column
        .ok_or(ArrowError::SchemaError(format!("No such column: {}", name)))?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(ArrowError::SchemaError(format!("{} is not a struct", name)))
}

fn column_as_map<'a>(
    name: &str,
    column: &Option<&'a Arc<dyn Array>>,
) -> Result<&'a MapArray, ArrowError> {
    column
        .ok_or(ArrowError::SchemaError(format!("No such column: {}", name)))?
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or(ArrowError::SchemaError(format!("{} is not a map", name)))
}

fn column_as_list<'a>(
    name: &str,
    column: &Option<&'a Arc<dyn Array>>,
) -> Result<&'a ListArray, ArrowError> {
    column
        .ok_or(ArrowError::SchemaError(format!("No such column: {}", name)))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or(ArrowError::SchemaError(format!("{} is not a map", name)))
}

#[inline]
pub(super) fn read_str(arr: &StringArray, idx: usize) -> DeltaResult<&str> {
    read_str_opt(arr, idx).ok_or(DeltaTableError::Generic("missing value".into()))
}

#[inline]
pub(super) fn read_str_opt(arr: &StringArray, idx: usize) -> Option<&str> {
    arr.is_valid(idx).then(|| arr.value(idx))
}

#[inline]
pub(super) fn read_primitive<T>(arr: &PrimitiveArray<T>, idx: usize) -> DeltaResult<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    read_primitive_opt(arr, idx).ok_or(DeltaTableError::Generic("missing value".into()))
}

#[inline]
pub(super) fn read_primitive_opt<T>(arr: &PrimitiveArray<T>, idx: usize) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    arr.is_valid(idx).then(|| arr.value(idx))
}

#[inline]
pub(super) fn read_bool(arr: &BooleanArray, idx: usize) -> DeltaResult<bool> {
    read_bool_opt(arr, idx).ok_or(DeltaTableError::Generic("missing value".into()))
}

#[inline]
pub(super) fn read_bool_opt(arr: &BooleanArray, idx: usize) -> Option<bool> {
    arr.is_valid(idx).then(|| arr.value(idx))
}
