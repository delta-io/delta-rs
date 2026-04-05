use std::{borrow::Cow, sync::LazyLock};

use arrow::{
    array::{AsArray, RecordBatch},
    datatypes::Int64Type,
};
use delta_kernel::{actions::Remove, schema::ToSchema};
use percent_encoding::percent_decode_str;

use crate::kernel::snapshot::iterators::get_string_value;

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

    pub fn size(&self) -> Option<i64> {
        static FIELD_INDEX: LazyLock<usize> =
            LazyLock::new(|| Remove::to_schema().field_with_index("size").unwrap().0);
        self.data
            .column(*FIELD_INDEX)
            .as_primitive_opt::<Int64Type>()
            .map(|a| a.value(self.index))
    }
}
