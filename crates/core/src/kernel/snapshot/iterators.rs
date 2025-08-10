use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::LazyLock;

use arrow::datatypes::Int32Type;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::DataType as ArrowDataType;
use chrono::{DateTime, Utc};
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::DataType;
use object_store::path::Path;
use object_store::ObjectMeta;
use percent_encoding::percent_decode_str;

use crate::kernel::scalars::ScalarExt;
use crate::kernel::{Add, DeletionVectorDescriptor, Remove};
use crate::{DeltaResult, DeltaTableError};

const FIELD_NAME_PATH: &str = "path";
const FIELD_NAME_SIZE: &str = "size";
const FIELD_NAME_MODIFICATION_TIME: &str = "modificationTime";
const FIELD_NAME_STATS: &str = "stats";
const FIELD_NAME_STATS_PARSED: &str = "stats_parsed";
const FIELD_NAME_FILE_CONSTANT_VALUES: &str = "fileConstantValues";
const FIELD_NAME_PARTITION_VALUES: &str = "partitionValues";
const FIELD_NAME_PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
const FIELD_NAME_DELETION_VECTOR: &str = "deletionVector";

const STATS_FIELD_NUM_RECORDS: &str = "numRecords";
const STATS_FIELD_MIN_VALUES: &str = "minValues";
const STATS_FIELD_MAX_VALUES: &str = "maxValues";
const STATS_FIELD_NULL_COUNT: &str = "nullCount";

const DV_FIELD_STORAGE_TYPE: &str = "storageType";
const DV_FIELD_PATH_OR_INLINE_DV: &str = "pathOrInlineDv";
const DV_FIELD_SIZE_IN_BYTES: &str = "sizeInBytes";
const DV_FIELD_CARDINALITY: &str = "cardinality";
const DV_FIELD_OFFSET: &str = "offset";

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

static DV_FIELD_INDICES: LazyLock<HashMap<&'static str, usize>> = LazyLock::new(|| {
    let schema = scan_row_schema();
    let dv_field = schema.field(FIELD_NAME_DELETION_VECTOR).unwrap();
    let DataType::Struct(dv_type) = dv_field.data_type() else {
        panic!("Expected DataType::Struct for deletion vector field");
    };

    let mut indices = HashMap::new();

    let storage_type_idx = dv_type.index_of(DV_FIELD_STORAGE_TYPE).unwrap();
    indices.insert(DV_FIELD_STORAGE_TYPE, storage_type_idx);

    let path_or_inline_dv_idx = dv_type.index_of(DV_FIELD_PATH_OR_INLINE_DV).unwrap();
    indices.insert(DV_FIELD_PATH_OR_INLINE_DV, path_or_inline_dv_idx);

    let size_in_bytes_idx = dv_type.index_of(DV_FIELD_SIZE_IN_BYTES).unwrap();
    indices.insert(DV_FIELD_SIZE_IN_BYTES, size_in_bytes_idx);

    let cardinality_idx = dv_type.index_of(DV_FIELD_CARDINALITY).unwrap();
    indices.insert(DV_FIELD_CARDINALITY, cardinality_idx);

    indices
});

#[derive(Clone)]
pub struct LogicalFileView {
    files: RecordBatch,
    index: usize,
}

impl LogicalFileView {
    pub(crate) fn new(files: RecordBatch, index: usize) -> Self {
        Self { files, index }
    }

    /// Path of the file.
    pub fn path(&self) -> Cow<'_, str> {
        let raw = get_string_value(
            self.files
                .column(*FIELD_INDICES.get(FIELD_NAME_PATH).unwrap()),
            self.index,
        )
        .unwrap();
        percent_decode_str(raw).decode_utf8_lossy()
    }

    /// An object store [`Path`] to the file.
    ///
    /// this tries to parse the file string and if that fails, it will return the string as is.
    // TODO assert consistent handling of the paths encoding when reading log data so this logic can be removed.
    #[deprecated(
        since = "0.27.1",
        note = "Will no longer be meaningful once we have full url support"
    )]
    pub(crate) fn object_store_path(&self) -> Path {
        let path = self.path();
        // Try to preserve percent encoding if possible
        match Path::parse(path.as_ref()) {
            Ok(path) => path,
            Err(_) => Path::from(path.as_ref()),
        }
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
        get_string_value(
            self.files
                .column(*FIELD_INDICES.get(FIELD_NAME_STATS).unwrap()),
            self.index,
        )
    }

    pub fn partition_values(&self) -> Option<StructData> {
        self.files
            .column_by_name(FIELD_NAME_PARTITION_VALUES_PARSED)
            .and_then(|col| col.as_struct_opt())
            .and_then(|arr| {
                arr.is_valid(self.index)
                    .then(|| match Scalar::from_array(arr, self.index) {
                        Some(Scalar::Struct(s)) => Some(s),
                        _ => None,
                    })
                    .flatten()
            })
    }

    fn partition_values_map(&self) -> HashMap<String, Option<String>> {
        self.partition_values()
            .map(|data| {
                data.fields()
                    .iter()
                    .zip(data.values().iter())
                    .map(|(k, v)| {
                        (
                            k.name().to_string(),
                            if v.is_null() {
                                None
                            } else {
                                Some(v.serialize())
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn stats_parsed(&self) -> Option<&StructArray> {
        self.files
            .column_by_name(FIELD_NAME_STATS_PARSED)
            .and_then(|col| col.as_struct_opt())
    }

    /// The number of records stored in the data file.
    pub fn num_records(&self) -> Option<usize> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_NUM_RECORDS))
            .and_then(|col| col.as_primitive_opt::<Int64Type>())
            .map(|a| a.value(self.index) as usize)
    }

    /// Struct containing all available null counts for the columns in this file.
    pub fn null_counts(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_NULL_COUNT))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }

    /// Struct containing all available min values for the columns in this file.
    pub fn min_values(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_MIN_VALUES))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }

    /// Struct containing all available max values for the columns in this file.
    pub fn max_values(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_MAX_VALUES))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
            .map(|s| round_ms_datetimes(s, &ceil_datetime))
    }

    fn deletion_vector(&self) -> Option<DeletionVectorView<'_>> {
        let dv_col = self
            .files
            .column_by_name(FIELD_NAME_DELETION_VECTOR)
            .and_then(|col| col.as_struct_opt())?;
        if dv_col.null_count() == dv_col.len() {
            return None;
        }
        dv_col
            .is_valid(self.index)
            .then(|| {
                let storage_col =
                    dv_col.column(*DV_FIELD_INDICES.get(DV_FIELD_STORAGE_TYPE).unwrap());
                storage_col
                    .is_valid(self.index)
                    .then(|| DeletionVectorView {
                        data: dv_col,
                        index: self.index,
                    })
            })
            .flatten()
    }

    pub(crate) fn add_action(&self) -> Add {
        Add {
            path: self.path().to_string(),
            partition_values: self.partition_values_map(),
            size: self.size(),
            modification_time: self.modification_time(),
            data_change: true,
            stats: self.stats().map(|v| v.to_string()),
            tags: None,
            deletion_vector: self.deletion_vector().map(|dv| dv.descriptor()),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        }
    }

    /// Create a remove action for this logical file.
    pub fn remove_action(&self, data_change: bool) -> Remove {
        Remove {
            // TODO use the raw (still encoded) path here once we reconciled serde ...
            path: self.path().to_string(),
            data_change,
            deletion_timestamp: Some(Utc::now().timestamp_millis()),
            extended_file_metadata: Some(true),
            size: Some(self.size()),
            partition_values: Some(self.partition_values_map()),
            deletion_vector: self.deletion_vector().map(|dv| dv.descriptor()),
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
        }
    }
}

// With delta.checkpoint.writeStatsAsStruct the microsecond timestamps are truncated to ms as defined by protocol
// this basically implies that it's floored when we parse_stats on the fly they are not truncated
// to tackle this we always round upwards by 1ms
fn ceil_datetime(v: i64) -> i64 {
    let remainder = v % 1000;
    if remainder == 0 {
        // if nanoseconds precision remainder is 0, we assume it was truncated
        // else we use the exact stats
        ((v as f64 / 1000.0).floor() as i64 + 1) * 1000
    } else {
        v
    }
}

fn round_ms_datetimes<F>(value: Scalar, func: &F) -> Scalar
where
    F: Fn(i64) -> i64,
{
    match value {
        Scalar::Timestamp(v) => Scalar::Timestamp(func(v)),
        Scalar::TimestampNtz(v) => Scalar::TimestampNtz(func(v)),
        Scalar::Struct(struct_data) => {
            let mut fields = Vec::new();
            let mut scalars = Vec::new();

            for (field, value) in struct_data.fields().iter().zip(struct_data.values().iter()) {
                fields.push(field.clone());
                scalars.push(round_ms_datetimes(value.clone(), func));
            }
            let data = StructData::try_new(fields, scalars).unwrap();
            Scalar::Struct(data)
        }
        value => value,
    }
}

/// View into a deletion vector data.
#[derive(Debug)]
struct DeletionVectorView<'a> {
    data: &'a StructArray,
    /// Pointer to a specific row in the log data.
    index: usize,
}

impl DeletionVectorView<'_> {
    fn descriptor(&self) -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: self.storage_type().parse().unwrap(),
            path_or_inline_dv: self.path_or_inline_dv().to_string(),
            size_in_bytes: self.size_in_bytes(),
            cardinality: self.cardinality(),
            offset: self.offset(),
        }
    }

    fn storage_type(&self) -> &str {
        get_string_value(
            self.data
                .column(*DV_FIELD_INDICES.get(DV_FIELD_STORAGE_TYPE).unwrap()),
            self.index,
        )
        .unwrap()
    }

    fn path_or_inline_dv(&self) -> &str {
        get_string_value(
            self.data
                .column(*DV_FIELD_INDICES.get(DV_FIELD_PATH_OR_INLINE_DV).unwrap()),
            self.index,
        )
        .unwrap()
    }

    fn size_in_bytes(&self) -> i32 {
        self.data
            .column(*DV_FIELD_INDICES.get(DV_FIELD_SIZE_IN_BYTES).unwrap())
            .as_primitive::<Int32Type>()
            .value(self.index)
    }

    fn cardinality(&self) -> i64 {
        self.data
            .column(*DV_FIELD_INDICES.get(DV_FIELD_CARDINALITY).unwrap())
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    fn offset(&self) -> Option<i32> {
        let col = self
            .data
            .column_by_name(DV_FIELD_OFFSET)
            .map(|c| c.as_primitive::<Int32Type>())?;
        col.is_valid(self.index).then(|| col.value(self.index))
    }
}

fn get_string_value(data: &dyn Array, index: usize) -> Option<&str> {
    match data.data_type() {
        ArrowDataType::Utf8 => {
            let arr = data.as_string::<i32>();
            arr.is_valid(index).then(|| arr.value(index))
        }
        ArrowDataType::LargeUtf8 => {
            let arr = data.as_string::<i64>();
            arr.is_valid(index).then(|| arr.value(index))
        }
        ArrowDataType::Utf8View => {
            let arr = data.as_string_view();
            arr.is_valid(index).then(|| arr.value(index))
        }
        _ => None,
    }
}

impl TryFrom<&LogicalFileView> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(file_stats: &LogicalFileView) -> Result<Self, Self::Error> {
        Ok(ObjectMeta {
            location: file_stats.object_store_path(),
            size: file_stats.size() as u64,
            last_modified: file_stats.modification_datetime()?,
            version: None,
            e_tag: None,
        })
    }
}
