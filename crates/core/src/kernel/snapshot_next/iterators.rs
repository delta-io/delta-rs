use std::collections::HashMap;
use std::sync::LazyLock;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use chrono::{DateTime, Utc};
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::scan::scan_row_schema;

use crate::kernel::scalars::ScalarExt;
use crate::{DeltaResult, DeltaTableError};

const FIELD_NAME_PATH: &str = "path";
const FIELD_NAME_SIZE: &str = "size";
const FIELD_NAME_MODIFICATION_TIME: &str = "modificationTime";
const FIELD_NAME_STATS: &str = "stats";
const FIELD_NAME_FILE_CONSTANT_VALUES: &str = "fileConstantValues";
const FIELD_NAME_PARTITION_VALUES: &str = "partitionValues";

static FIELD_INDICES: LazyLock<HashMap<&'static str, usize>> = LazyLock::new(|| {
    let schema = scan_row_schema();
    let mut indices = HashMap::new();

    let path_idx = schema.index_of(FIELD_NAME_PATH).unwrap();
    indices.insert(FIELD_NAME_PATH, path_idx);

    let size_idx = schema.index_of(FIELD_NAME_SIZE).unwrap();
    indices.insert(FIELD_NAME_SIZE, size_idx);

    let modification_time_idx = schema.index_of(FIELD_NAME_MODIFICATION_TIME).unwrap();
    indices.insert(FIELD_NAME_MODIFICATION_TIME, modification_time_idx);

    let stats_idx = schema.index_of(FIELD_NAME_STATS).unwrap();
    indices.insert(FIELD_NAME_STATS, stats_idx);

    indices
});

#[derive(Clone)]
pub struct LogicalFileView {
    pub(super) files: RecordBatch,
    pub(super) index: usize,
}

impl LogicalFileView {
    /// Path of the file.
    pub fn path(&self) -> &str {
        self.files
            .column(*FIELD_INDICES.get(FIELD_NAME_PATH).unwrap())
            .as_string::<i32>()
            .value(self.index)
    }

    /// Size of the file in bytes.
    pub fn size(&self) -> i64 {
        self.files
            .column(*FIELD_INDICES.get(FIELD_NAME_SIZE).unwrap())
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Modification time of the file in milliseconds since epoch.
    pub fn modification_time(&self) -> i64 {
        self.files
            .column(*FIELD_INDICES.get(FIELD_NAME_MODIFICATION_TIME).unwrap())
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Datetime of the last modification time of the file.
    pub fn modification_datetime(&self) -> DeltaResult<chrono::DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.modification_time()).ok_or(
            DeltaTableError::MetadataError(format!(
                "invalid modification_time: {:?}",
                self.modification_time()
            )),
        )
    }

    pub fn stats(&self) -> Option<&str> {
        let col = self
            .files
            .column(*FIELD_INDICES.get(FIELD_NAME_STATS).unwrap())
            .as_string::<i32>();
        col.is_valid(self.index).then(|| col.value(self.index))
    }

    pub fn partition_values(&self) -> Option<StructData> {
        self.files
            .column_by_name(FIELD_NAME_FILE_CONSTANT_VALUES)
            .and_then(|col| col.as_struct_opt())
            .and_then(|s| s.column_by_name(FIELD_NAME_PARTITION_VALUES))
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
