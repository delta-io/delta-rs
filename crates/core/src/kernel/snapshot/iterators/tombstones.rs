use std::{borrow::Cow, sync::LazyLock};

use arrow::{
    array::{AsArray, RecordBatch},
    datatypes::Int64Type,
};
use delta_kernel::{actions::Remove, schema::ToSchema};
use percent_encoding::percent_decode_str;

use super::{DeletionVectorView, get_string_value};
use crate::kernel::DeletionVectorDescriptor;

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
        percent_decode_str(self.path_raw()).decode_utf8_lossy()
    }

    pub(crate) fn path_raw(&self) -> &str {
        static FIELD_INDEX: LazyLock<usize> =
            LazyLock::new(|| Remove::to_schema().field_with_index("path").unwrap().0);
        get_string_value(self.data.column(*FIELD_INDEX), self.index).expect("valid string field")
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

    /// Returns the deletion vector retired by this Remove action, if present.
    pub(crate) fn deletion_vector_descriptor(&self) -> Option<DeletionVectorDescriptor> {
        let data = self
            .data
            .column_by_name("deletionVector")?
            .as_struct_opt()?;
        DeletionVectorView::new(data, self.index).map(|view| view.descriptor())
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
