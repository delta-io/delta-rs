use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray,
};
use chrono::{DateTime, Utc};
use delta_kernel::actions::visitors::AddVisitor;
use delta_kernel::actions::Add;
use delta_kernel::actions::ADD_NAME;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::ProvidesColumnByName;
use delta_kernel::engine_data::{GetData, RowVisitor};
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::scan::scan_row_schema;

use crate::kernel::scalars::ScalarExt;
use crate::{DeltaResult, DeltaTableError};

pub struct AddIterator<'a> {
    paths: &'a StringArray,
    getters: Arc<Vec<&'a dyn GetData<'a>>>,
    index: usize,
}

impl AddIterator<'_> {
    pub fn try_new(actions: &RecordBatch) -> DeltaResult<AddIterator<'_>> {
        validate_add(&actions)?;

        let visitor = AddVisitor::new();
        let fields = visitor.selected_column_names_and_types();

        let mut mask = HashSet::new();
        for column in fields.0 {
            for i in 0..column.len() {
                mask.insert(&column[..i + 1]);
            }
        }

        let mut getters = vec![];
        ArrowEngineData::extract_columns(&mut vec![], &mut getters, fields.1, &mask, actions)?;

        let paths = extract_column(actions, &[ADD_NAME, "path"])?.as_string::<i32>();

        Ok(AddIterator {
            paths,
            getters: Arc::new(getters),
            index: 0,
        })
    }
}

impl Iterator for AddIterator<'_> {
    type Item = DeltaResult<Add>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.paths.len() {
            let path = self.paths.value(self.index).to_string();
            let add = AddVisitor::visit_add(self.index, path, self.getters.as_slice())
                .map_err(DeltaTableError::from);
            self.index += 1;
            Some(add)
        } else {
            None
        }
    }
}

pub struct AddView {
    actions: RecordBatch,
    index: usize,
}

impl AddView {
    pub fn path(&self) -> &str {
        extract_column(&self.actions, &[ADD_NAME, "path"])
            .unwrap()
            .as_string::<i32>()
            .value(self.index)
    }

    pub fn size(&self) -> i64 {
        extract_column(&self.actions, &[ADD_NAME, "size"])
            .unwrap()
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    pub fn modification_time(&self) -> i64 {
        extract_column(&self.actions, &[ADD_NAME, "modificationTime"])
            .unwrap()
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Datetime of the last modification time of the file.
    pub fn modification_datetime(&self) -> DeltaResult<chrono::DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.modification_time()).ok_or(DeltaTableError::from(
            crate::protocol::ProtocolError::InvalidField(format!(
                "invalid modification_time: {:?}",
                self.modification_time()
            )),
        ))
    }

    pub fn data_change(&self) -> bool {
        extract_column(&self.actions, &[ADD_NAME, "dataChange"])
            .unwrap()
            .as_boolean()
            .value(self.index)
    }

    pub fn stats(&self) -> Option<&str> {
        extract_column(&self.actions, &[ADD_NAME, "stats"])
            .ok()
            .and_then(|c| c.as_string_opt::<i32>().map(|v| v.value(self.index)))
    }

    pub fn base_row_id(&self) -> Option<i64> {
        extract_column(&self.actions, &[ADD_NAME, "baseRowId"])
            .ok()
            .and_then(|c| {
                c.as_primitive_opt::<Int64Type>()
                    .map(|v| v.value(self.index))
            })
    }

    pub fn default_row_commit_version(&self) -> Option<i64> {
        extract_column(&self.actions, &[ADD_NAME, "defaultRowCommitVersion"])
            .ok()
            .and_then(|c| {
                c.as_primitive_opt::<Int64Type>()
                    .map(|v| v.value(self.index))
            })
    }

    pub fn clustering_provider(&self) -> Option<&str> {
        extract_column(&self.actions, &[ADD_NAME, "clusteringProvider"])
            .ok()
            .and_then(|c| c.as_string_opt::<i32>().map(|v| v.value(self.index)))
    }
}

#[derive(Clone)]
pub struct LogicalFileView {
    files: RecordBatch,
    index: usize,
}

impl LogicalFileView {
    /// Path of the file.
    pub fn path(&self) -> &str {
        self.files.column(0).as_string::<i32>().value(self.index)
    }

    /// Size of the file in bytes.
    pub fn size(&self) -> i64 {
        self.files
            .column(1)
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Modification time of the file in milliseconds since epoch.
    pub fn modification_time(&self) -> i64 {
        self.files
            .column(2)
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Datetime of the last modification time of the file.
    pub fn modification_datetime(&self) -> DeltaResult<chrono::DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.modification_time()).ok_or(DeltaTableError::from(
            crate::protocol::ProtocolError::InvalidField(format!(
                "invalid modification_time: {:?}",
                self.modification_time()
            )),
        ))
    }

    pub fn stats(&self) -> Option<&str> {
        let col = self.files.column(3).as_string::<i32>();
        col.is_valid(self.index).then(|| col.value(self.index))
    }

    pub fn partition_values(&self) -> Option<StructData> {
        self.files
            .column_by_name("fileConstantValues")
            .and_then(|col| col.as_struct_opt())
            .and_then(|s| s.column_by_name("partitionValues"))
            .and_then(|arr| {
                arr.is_valid(self.index)
                    .then(|| match Scalar::from_array(arr, self.index) {
                        Some(Scalar::Struct(s)) => Some(s),
                        _ => None,
                    })
                    .flatten()
            })
    }
}

impl Iterator for LogicalFileView {
    type Item = LogicalFileView;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.files.num_rows() {
            let file = LogicalFileView {
                files: self.files.clone(),
                index: self.index,
            };
            self.index += 1;
            Some(file)
        } else {
            None
        }
    }
}

pub struct LogicalFileViewIterator<I>
where
    I: IntoIterator<Item = Result<RecordBatch, DeltaTableError>>,
{
    inner: I::IntoIter,
    batch: Option<RecordBatch>,
    current: usize,
}

impl<I> LogicalFileViewIterator<I>
where
    I: IntoIterator<Item = Result<RecordBatch, DeltaTableError>>,
{
    /// Create a new [LogicalFileViewIterator].
    ///
    /// If `iter` is an infallible iterator, use `.map(Ok)`.
    pub fn new(iter: I) -> Self {
        Self {
            inner: iter.into_iter(),
            batch: None,
            current: 0,
        }
    }
}

// impl<I> Iterator for LogicalFileViewIterator<I>
// where
//     I: IntoIterator<Item = DeltaResult<RecordBatch>>,
// {
//     type Item = DeltaResult<LogicalFileView>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if let Some(batch) = &self.batch {
//             if self.current < batch.num_rows() {
//                 let item = LogicalFileView {
//                     files: batch.clone(),
//                     index: self.current,
//                 };
//                 self.current += 1;
//                 return Some(Ok(item));
//             }
//         }
//         match self.inner.next() {
//             Some(Ok(batch)) => {
//                 if validate_logical_file(&batch).is_err() {
//                     return Some(Err(DeltaTableError::generic(
//                         "Invalid logical file data encountered.",
//                     )));
//                 }
//                 self.batch = Some(batch);
//                 self.current = 0;
//                 self.next()
//             }
//             Some(Err(e)) => Some(Err(e)),
//             None => None,
//         }
//     }
//
//     fn size_hint(&self) -> (usize, Option<usize>) {
//         self.inner.size_hint()
//     }
// }

pub struct AddViewIterator<I>
where
    I: IntoIterator<Item = Result<RecordBatch, DeltaTableError>>,
{
    inner: I::IntoIter,
    batch: Option<RecordBatch>,
    current: usize,
}

impl<I> AddViewIterator<I>
where
    I: IntoIterator<Item = Result<RecordBatch, DeltaTableError>>,
{
    /// Create a new [AddViewIterator].
    ///
    /// If `iter` is an infallible iterator, use `.map(Ok)`.
    pub fn new(iter: I) -> Self {
        Self {
            inner: iter.into_iter(),
            batch: None,
            current: 0,
        }
    }
}

impl<I> Iterator for AddViewIterator<I>
where
    I: IntoIterator<Item = DeltaResult<RecordBatch>>,
{
    type Item = DeltaResult<AddView>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = &self.batch {
            if self.current < batch.num_rows() {
                let item = AddView {
                    actions: batch.clone(),
                    index: self.current,
                };
                self.current += 1;
                return Some(Ok(item));
            }
        }
        match self.inner.next() {
            Some(Ok(batch)) => {
                if validate_add(&batch).is_err() {
                    return Some(Err(DeltaTableError::generic(
                        "Invalid add action data encountered.",
                    )));
                }
                self.batch = Some(batch);
                self.current = 0;
                self.next()
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub(crate) fn validate_add(batch: &RecordBatch) -> DeltaResult<()> {
    validate_column::<StringArray>(batch, &[ADD_NAME, "path"])?;
    validate_column::<Int64Array>(batch, &[ADD_NAME, "size"])?;
    validate_column::<Int64Array>(batch, &[ADD_NAME, "modificationTime"])?;
    validate_column::<BooleanArray>(batch, &[ADD_NAME, "dataChange"])?;
    Ok(())
}

fn validate_column<'a, T: Array + 'static>(
    actions: &'a RecordBatch,
    col: &'a [impl AsRef<str>],
) -> DeltaResult<()> {
    if let Ok(arr) = extract_column(actions, col) {
        if arr.as_any().downcast_ref::<T>().is_none() {
            return Err(DeltaTableError::from(
                crate::protocol::ProtocolError::InvalidField(format!("Invalid column: {:?}", arr)),
            ));
        }
        if arr.null_count() > 0 {
            return Err(DeltaTableError::from(
                crate::protocol::ProtocolError::InvalidField(format!(
                    "Column has null values: {:?}",
                    arr
                )),
            ));
        }
    } else {
        return Err(DeltaTableError::from(
            crate::protocol::ProtocolError::InvalidField("Column not found".to_string()),
        ));
    }
    Ok(())
}

fn extract_column<'a>(
    mut parent: &'a dyn ProvidesColumnByName,
    col: &[impl AsRef<str>],
) -> DeltaResult<&'a ArrayRef> {
    let mut field_names = col.iter();
    let Some(mut field_name) = field_names.next() else {
        return Err(arrow_schema::ArrowError::SchemaError(
            "Empty column path".to_string(),
        ))?;
    };
    loop {
        let child = parent.column_by_name(field_name.as_ref()).ok_or_else(|| {
            arrow_schema::ArrowError::SchemaError(format!("No such field: {}", field_name.as_ref()))
        })?;
        field_name = match field_names.next() {
            Some(name) => name,
            None => return Ok(child),
        };
        parent = child
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                arrow_schema::ArrowError::SchemaError(format!(
                    "Not a struct: {}",
                    field_name.as_ref()
                ))
            })?;
    }
}
