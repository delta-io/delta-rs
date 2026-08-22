use std::{borrow::Cow, sync::LazyLock};

use arrow::{
    array::{AsArray, RecordBatch},
    datatypes::Int64Type,
};
use delta_kernel::{actions::Remove, schema::ToSchema};
use object_store::path::Path;
use percent_encoding::percent_decode_str;

use crate::kernel::snapshot::iterators::get_string_value;

/// A lightweight, cloneable view over a single tombstone (`Remove` action) row.
///
/// Rather than materializing a `Remove` struct, this borrows into the backing
/// [`RecordBatch`] and decodes individual fields on demand.
#[derive(Clone)]
pub struct TombstoneView {
    data: RecordBatch,
    index: usize,
}

impl TombstoneView {
    /// Creates a new view into the specified file entry.
    pub(crate) fn new(data: RecordBatch, index: usize) -> Self {
        Self { data, index }
    }

    /// Returns the file path with URL decoding applied.
    pub fn path(&self) -> Cow<'_, str> {
        static FIELD_INDEX: LazyLock<usize> =
            LazyLock::new(|| Remove::to_schema().field_with_index("path").unwrap().0);
        let raw = get_string_value(self.data.column(*FIELD_INDEX), self.index)
            .expect("valid string field");
        percent_decode_str(raw).decode_utf8_lossy()
    }

    /// Returns an object store path using the same decoded representation as logical files.
    pub(crate) fn object_store_path(&self) -> Path {
        let path = self.path();
        match Path::parse(path.as_ref()) {
            Ok(path) => path,
            Err(_) => Path::from(path.as_ref()),
        }
    }

    /// Returns the deletion timestamp (milliseconds since epoch), if recorded.
    pub fn deletion_timestamp(&self) -> Option<i64> {
        static FIELD_INDEX: LazyLock<usize> = LazyLock::new(|| {
            Remove::to_schema()
                .field_with_index("deletionTimestamp")
                .unwrap()
                .0
        });
        self.data
            .column(*FIELD_INDEX)
            .as_primitive_opt::<Int64Type>()
            .map(|a| a.value(self.index))
    }

    /// Returns whether removing this file represents a data change (vs. a compaction-style rewrite).
    pub fn data_change(&self) -> bool {
        static FIELD_INDEX: LazyLock<usize> = LazyLock::new(|| {
            Remove::to_schema()
                .field_with_index("dataChange")
                .unwrap()
                .0
        });
        self.data
            .column(*FIELD_INDEX)
            .as_boolean()
            .value(self.index)
    }

    /// Returns the size of the removed file in bytes, if recorded.
    pub fn size(&self) -> Option<i64> {
        static FIELD_INDEX: LazyLock<usize> =
            LazyLock::new(|| Remove::to_schema().field_with_index("size").unwrap().0);
        self.data
            .column(*FIELD_INDEX)
            .as_primitive_opt::<Int64Type>()
            .map(|a| a.value(self.index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, BooleanArray, StringArray, new_null_array},
        datatypes::Schema,
    };
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::scan::scan_row_schema;
    use std::sync::Arc;

    #[test]
    fn object_store_path_matches_logical_file_view() {
        let kernel_schema = Remove::to_schema();
        let schema: Schema = (&kernel_schema).try_into_arrow().unwrap();
        let mut columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), 1))
            .collect();
        let raw_path = "part=a%20b/file.parquet";
        columns[schema.index_of("path").unwrap()] =
            Arc::new(StringArray::from(vec![Some(raw_path)]));
        columns[schema.index_of("dataChange").unwrap()] =
            Arc::new(BooleanArray::from(vec![Some(true)]));

        let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
        let view = TombstoneView::new(batch, 0);

        assert_eq!(view.path(), "part=a b/file.parquet");
        let expected = Path::parse(view.path().as_ref()).unwrap();
        assert_eq!(view.object_store_path(), expected);
        assert_eq!(view.object_store_path().as_ref(), "part=a b/file.parquet");

        let logical_schema: Schema = scan_row_schema().as_ref().try_into_arrow().unwrap();
        let mut logical_columns: Vec<ArrayRef> = logical_schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), 1))
            .collect();
        logical_columns[logical_schema.index_of("path").unwrap()] =
            Arc::new(StringArray::from(vec![Some(raw_path)]));
        logical_columns[logical_schema.index_of("size").unwrap()] =
            Arc::new(arrow::array::Int64Array::from(vec![Some(1)]));
        logical_columns[logical_schema.index_of("modificationTime").unwrap()] =
            Arc::new(arrow::array::Int64Array::from(vec![Some(1)]));
        let logical_batch =
            RecordBatch::try_new(Arc::new(logical_schema), logical_columns).unwrap();
        let logical_view =
            crate::kernel::snapshot::iterators::LogicalFileView::new(logical_batch, 0);
        assert_eq!(view.object_store_path(), logical_view.object_store_path());
    }
}
