use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::LazyLock;

use arrow::datatypes::Int32Type;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, MapArray, RecordBatch, StringArray, StructArray};
use arrow_schema::DataType as ArrowDataType;
use chrono::{DateTime, Utc};
use delta_kernel::engine::arrow_expression::evaluate_expression::to_json;
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::DataType;
use object_store::ObjectMeta;
use object_store::path::Path;
use percent_encoding::percent_decode_str;

#[cfg(any(test, feature = "datafusion"))]
pub(crate) use self::scan_row::parse_stats_column_with_schema;
pub use self::tombstones::TombstoneView;
use crate::kernel::scalars::ScalarExt;
use crate::kernel::{Add, DeletionVectorDescriptor, Remove};
use crate::{DeltaResult, DeltaTableError};

pub(crate) use self::scan_row::{ScanRowOutStream, scan_row_in_eval};

mod scan_row;
mod tombstones;

const FIELD_NAME_PATH: &str = "path";
const FIELD_NAME_SIZE: &str = "size";
const FIELD_NAME_MODIFICATION_TIME: &str = "modificationTime";
const FIELD_NAME_FILE_CONSTANT_VALUES: &str = "fileConstantValues";
const FIELD_NAME_RAW_PARTITION_VALUES: &str = "partitionValues";
const FIELD_NAME_STATS: &str = "stats";
const FIELD_NAME_STATS_PARSED: &str = "stats_parsed";
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

/// Provides semantic, typed access to file metadata from Delta log replay.
///
/// This struct wraps a RecordBatch containing file data and provides zero-copy
/// access to individual file entries through an index. It serves as a view into
/// the kernel's log replay results, offering convenient methods to extract
/// file properties without unnecessary data copies.
#[derive(Clone)]
pub struct LogicalFileView {
    files: RecordBatch,
    index: usize,
}

impl LogicalFileView {
    /// Creates a new view into the specified file entry.
    pub(crate) fn new(files: RecordBatch, index: usize) -> Self {
        Self { files, index }
    }

    /// Returns the file path with URL decoding applied.
    pub fn path(&self) -> Cow<'_, str> {
        let raw = get_string_value(
            self.files
                .column(*FIELD_INDICES.get(FIELD_NAME_PATH).unwrap()),
            self.index,
        )
        .unwrap();
        percent_decode_str(raw).decode_utf8_lossy()
    }

    /// Returns the raw file path as stored in the log, without URL decoding.
    #[cfg(any(test, feature = "datafusion"))]
    pub(crate) fn path_raw(&self) -> &str {
        get_string_value(
            self.files
                .column(*FIELD_INDICES.get(FIELD_NAME_PATH).unwrap()),
            self.index,
        )
        .unwrap()
    }

    /// An object store [`Path`] to the file.
    ///
    /// this tries to parse the file string and if that fails, it will return the string as is.
    // TODO assert consistent handling of the paths encoding when reading log data so this logic can be removed.
    pub(crate) fn object_store_path(&self) -> Path {
        let path = self.path();
        // Try to preserve percent encoding if possible
        match Path::parse(path.as_ref()) {
            Ok(path) => path,
            Err(_) => Path::from(path.as_ref()),
        }
    }

    /// Returns the file size in bytes.
    pub fn size(&self) -> i64 {
        self.files
            .column(*FIELD_INDICES.get(FIELD_NAME_SIZE).unwrap())
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Returns the file modification time in milliseconds since Unix epoch.
    pub fn modification_time(&self) -> i64 {
        self.files
            .column(*FIELD_INDICES.get(FIELD_NAME_MODIFICATION_TIME).unwrap())
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Returns the file modification time as a UTC DateTime.
    pub fn modification_datetime(&self) -> DeltaResult<chrono::DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.modification_time()).ok_or(
            DeltaTableError::MetadataError(format!(
                "invalid modification_time: {:?}",
                self.modification_time()
            )),
        )
    }

    /// Returns the raw JSON statistics string for this file, if available.
    pub fn stats(&self) -> Option<String> {
        self.files
            .column_by_name(FIELD_NAME_STATS)
            .and_then(|col| get_string_value(col, self.index))
            .map(ToString::to_string)
            .or_else(|| {
                let stats = self.stats_parsed()?.slice(self.index, 1);
                let value = to_json(&stats)
                    .ok()
                    .map(|arr| arr.as_string::<i32>().value(0).to_string());
                value.and_then(|v| (!v.is_empty()).then_some(v))
            })
    }

    /// Returns the parsed partition values as structured data.
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

    fn raw_partition_values(&self) -> Option<&MapArray> {
        self.files
            .column_by_name(FIELD_NAME_FILE_CONSTANT_VALUES)
            .and_then(|col| col.as_struct_opt())
            .and_then(|file_constants| {
                file_constants.column_by_name(FIELD_NAME_RAW_PARTITION_VALUES)
            })
            .and_then(|col| col.as_map_opt())
    }

    /// Returns the raw partition value map stored in the log for this file.
    ///
    /// This preserves all partition columns even when `partitionValues_parsed` was narrowed to the
    /// predicate-referenced subset for data skipping.
    fn partition_values_map(&self) -> HashMap<String, Option<String>> {
        self.raw_partition_values()
            .filter(|partitions| partitions.is_valid(self.index))
            .and_then(|partitions| collect_string_map(&partitions.value(self.index)))
            .unwrap_or_default()
    }

    /// Returns the parsed statistics as a StructArray, if available.
    fn stats_parsed(&self) -> Option<&StructArray> {
        self.files
            .column_by_name(FIELD_NAME_STATS_PARSED)
            .and_then(|col| col.as_struct_opt())
    }

    /// Returns the raw file row count from Add-action statistics before deletion-vector filtering.
    ///
    /// Returns `None` when the stat is missing or invalid for the current platform.
    pub fn num_records(&self) -> Option<usize> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_NUM_RECORDS))
            .and_then(|col| col.as_primitive_opt::<Int64Type>())
            .and_then(|a| {
                a.is_valid(self.index)
                    .then(|| usize::try_from(a.value(self.index)).ok())
                    .flatten()
            })
    }

    /// Returns null counts for all columns in this file as structured data.
    pub fn null_counts(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_NULL_COUNT))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }

    /// Returns minimum values for all columns with statics in this file as structured data.
    pub fn min_values(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_MIN_VALUES))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }

    /// Returns maximum values for all columns in this file as structured data.
    ///
    /// For timestamp columns, values are rounded up to handle microsecond truncation
    /// in checkpoint statistics.
    pub fn max_values(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_MAX_VALUES))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
            .map(|s| round_ms_datetimes(s, &ceil_datetime))
    }

    /// Return the underlying [DeletionVectorDescriptor] if it exists.
    ///
    /// **NOTE**: THis API may be removed in the future without deprecation warnings as the
    /// utilization of deletion vectors inside of delta-rs becomes more sophisticated.
    pub fn deletion_vector_descriptor(&self) -> Option<DeletionVectorDescriptor> {
        self.deletion_vector().map(|dv| dv.descriptor())
    }

    /// Returns a view into the deletion vector for this file, if present.
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
                    .then_some(DeletionVectorView {
                        data: dv_col,
                        index: self.index,
                    })
            })
            .flatten()
    }

    /// Internal API
    pub(crate) fn to_add(&self) -> Add {
        Add {
            path: self.path().to_string(),
            partition_values: self.partition_values_map(),
            size: self.size(),
            modification_time: self.modification_time(),
            data_change: true,
            stats: self.stats(),
            tags: None,
            deletion_vector: self.deletion_vector().map(|dv| dv.descriptor()),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        }
    }

    /// Converts this file view into an Add action for log operations.
    #[deprecated(
        since = "0.31.0",
        note = "Use Arrow arrays directly instead of converting to Add actions."
    )]
    pub fn add_action(&self) -> Add {
        self.to_add()
    }

    /// Converts this file view into a Remove action for log operations.
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

/// Rounds up timestamp values to handle microsecond truncation in checkpoint statistics.
///
/// When delta.checkpoint.writeStatsAsStruct is enabled, microsecond timestamps are
/// truncated to milliseconds. This function rounds up by 1ms to ensure correct
/// range queries when stats are parsed on-the-fly.
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

/// Recursively applies a rounding function to timestamp values in scalar data.
fn round_ms_datetimes<F>(value: Scalar, func: &F) -> Scalar
where
    F: Fn(i64) -> i64,
{
    match value {
        Scalar::Timestamp(v) => Scalar::Timestamp(func(v)),
        Scalar::TimestampNtz(v) => Scalar::TimestampNtz(func(v)),
        Scalar::Struct(struct_data) => {
            let mut fields = Vec::with_capacity(struct_data.fields().len());
            let mut scalars = Vec::with_capacity(struct_data.values().len());

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

/// Provides typed access to deletion vector metadata from log data.
///
/// This struct wraps a StructArray containing deletion vector information
/// and provides zero-copy access to individual fields through an index.
#[derive(Debug)]
struct DeletionVectorView<'a> {
    data: &'a StructArray,
    /// Index into the deletion vector data array.
    index: usize,
}

impl DeletionVectorView<'_> {
    /// Converts this view into a DeletionVectorDescriptor.
    fn descriptor(&self) -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: self.storage_type().parse().unwrap(),
            path_or_inline_dv: self.path_or_inline_dv().to_string(),
            size_in_bytes: self.size_in_bytes(),
            cardinality: self.cardinality(),
            offset: self.offset(),
        }
    }

    /// Returns the storage type of the deletion vector.
    fn storage_type(&self) -> &str {
        get_string_value(
            self.data
                .column(*DV_FIELD_INDICES.get(DV_FIELD_STORAGE_TYPE).unwrap()),
            self.index,
        )
        .unwrap()
    }

    /// Returns the path or inline data for the deletion vector.
    fn path_or_inline_dv(&self) -> &str {
        get_string_value(
            self.data
                .column(*DV_FIELD_INDICES.get(DV_FIELD_PATH_OR_INLINE_DV).unwrap()),
            self.index,
        )
        .unwrap()
    }

    /// Returns the size of the deletion vector in bytes.
    fn size_in_bytes(&self) -> i32 {
        self.data
            .column(*DV_FIELD_INDICES.get(DV_FIELD_SIZE_IN_BYTES).unwrap())
            .as_primitive::<Int32Type>()
            .value(self.index)
    }

    /// Returns the number of deleted rows represented by this deletion vector.
    fn cardinality(&self) -> i64 {
        self.data
            .column(*DV_FIELD_INDICES.get(DV_FIELD_CARDINALITY).unwrap())
            .as_primitive::<Int64Type>()
            .value(self.index)
    }

    /// Returns the offset within the deletion vector file, if applicable.
    fn offset(&self) -> Option<i32> {
        let col = self
            .data
            .column_by_name(DV_FIELD_OFFSET)
            .map(|c| c.as_primitive::<Int32Type>())?;
        col.is_valid(self.index).then(|| col.value(self.index))
    }
}

/// Extracts a string value from an Arrow array at the specified index.
///
/// Handles different string array types (Utf8, LargeUtf8, Utf8View) and
/// returns None for null values or unsupported types.
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

fn collect_string_map(data: &dyn Array) -> Option<HashMap<String, Option<String>>> {
    let entries = data.as_any().downcast_ref::<StructArray>()?;
    let keys = entries.column(0).as_any().downcast_ref::<StringArray>()?;
    let values = entries.column(1).as_any().downcast_ref::<StringArray>()?;
    Some(
        keys.iter()
            .zip(values.iter())
            .filter_map(|(key, value)| key.map(|k| (k.to_string(), value.map(str::to_string))))
            .collect(),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestTables;
    use arrow::array::{ArrayRef, Int64Array, new_null_array};
    use chrono::DateTime;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::scan::scan_row_schema;
    use futures::TryStreamExt;
    use std::sync::Arc;

    fn logical_file_view_with_stats(
        raw_stats_json: Option<&str>,
        stats_parsed: StructArray,
    ) -> LogicalFileView {
        let base_schema: arrow_schema::Schema =
            scan_row_schema().as_ref().try_into_arrow().unwrap();
        let mut columns: Vec<ArrayRef> = base_schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), 1))
            .collect();
        columns[base_schema.index_of("path").unwrap()] =
            Arc::new(StringArray::from(vec![Some("part-000.parquet")]));
        columns[base_schema.index_of("size").unwrap()] = Arc::new(Int64Array::from(vec![1]));
        columns[base_schema.index_of("modificationTime").unwrap()] =
            Arc::new(Int64Array::from(vec![1]));
        columns[base_schema.index_of("stats").unwrap()] =
            Arc::new(StringArray::from(vec![raw_stats_json]));

        let mut fields = base_schema.fields().to_vec();
        fields.push(Arc::new(arrow_schema::Field::new(
            "stats_parsed",
            stats_parsed.data_type().to_owned(),
            true,
        )));
        columns.push(Arc::new(stats_parsed));

        let batch =
            RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap();
        LogicalFileView::new(batch, 0)
    }

    fn logical_file_view_with_partial_stats(full_stats_json: &str) -> LogicalFileView {
        let partial_stats = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new(
                "numRecords",
                ArrowDataType::Int64,
                true,
            )),
            Arc::new(Int64Array::from(vec![Some(11)])) as ArrayRef,
        )]);

        logical_file_view_with_stats(Some(full_stats_json), partial_stats)
    }

    fn logical_file_view_without_raw_stats() -> LogicalFileView {
        let min_values = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new(
                "value",
                ArrowDataType::Int64,
                true,
            )),
            Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
        )]);
        let max_values = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new(
                "value",
                ArrowDataType::Int64,
                true,
            )),
            Arc::new(Int64Array::from(vec![Some(9)])) as ArrayRef,
        )]);
        let null_count = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new(
                "value",
                ArrowDataType::Int64,
                true,
            )),
            Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef,
        )]);
        let full_stats = StructArray::from(vec![
            (
                Arc::new(arrow_schema::Field::new(
                    "numRecords",
                    ArrowDataType::Int64,
                    true,
                )),
                Arc::new(Int64Array::from(vec![Some(11)])) as ArrayRef,
            ),
            (
                Arc::new(arrow_schema::Field::new(
                    "minValues",
                    min_values.data_type().to_owned(),
                    true,
                )),
                Arc::new(min_values) as ArrayRef,
            ),
            (
                Arc::new(arrow_schema::Field::new(
                    "maxValues",
                    max_values.data_type().to_owned(),
                    true,
                )),
                Arc::new(max_values) as ArrayRef,
            ),
            (
                Arc::new(arrow_schema::Field::new(
                    "nullCount",
                    null_count.data_type().to_owned(),
                    true,
                )),
                Arc::new(null_count) as ArrayRef,
            ),
        ]);

        logical_file_view_with_stats(None, full_stats)
    }

    #[tokio::test]
    async fn test_logical_file_view_with_real_data() {
        // Use existing test table with real Delta log data
        let log_store = TestTables::Simple
            .table_builder()
            .expect("Failed to create table builder")
            .build_storage()
            .expect("Failed to build storage");
        let snapshot =
            crate::kernel::snapshot::Snapshot::try_new(&log_store, Default::default(), None)
                .await
                .unwrap();

        let files: Vec<_> = snapshot
            .files(&log_store, None)
            .try_collect()
            .await
            .unwrap();
        assert!(!files.is_empty(), "Should have test files");

        let first_batch = &files[0];
        let view = LogicalFileView::new(first_batch.clone(), 0);

        // Test basic properties
        assert!(!view.path().is_empty());
        assert!(view.size() > 0);
        assert!(view.modification_time() > 0);

        // Test datetime conversion
        let datetime = view.modification_datetime().unwrap();
        assert!(datetime.timestamp_millis() > 0);

        // Test action conversions
        let add_action = view.to_add();
        assert_eq!(add_action.path, view.path());
        assert_eq!(add_action.size, view.size());
        assert!(add_action.data_change);

        let remove_action = view.remove_action(true);
        assert_eq!(remove_action.path, view.path());
        assert!(remove_action.data_change);
        assert!(remove_action.deletion_timestamp.is_some());

        // Test ObjectMeta conversion
        let object_meta: ObjectMeta = (&view).try_into().unwrap();
        assert_eq!(object_meta.size as i64, view.size());
        assert_eq!(
            object_meta.last_modified.timestamp_millis(),
            view.modification_time()
        );
    }

    #[test]
    fn test_path_url_decoding() {
        // Test URL decoding using the percent_decode_str functionality
        use percent_encoding::percent_decode_str;
        use std::borrow::Cow;

        let encoded_path = "path/to/file%20with%20spaces.parquet";
        let decoded: Cow<str> = percent_decode_str(encoded_path).decode_utf8_lossy();
        assert_eq!(decoded, "path/to/file with spaces.parquet");
    }

    #[test]
    fn test_get_string_value_different_types() {
        use arrow_array::{Int32Array, StringArray};

        // Test Utf8
        let utf8_array = StringArray::from(vec![Some("test"), None]);
        assert_eq!(get_string_value(&utf8_array, 0), Some("test"));
        assert_eq!(get_string_value(&utf8_array, 1), None);

        // Test LargeUtf8
        let large_utf8_array = arrow_array::LargeStringArray::from(vec![Some("large_test"), None]);
        assert_eq!(get_string_value(&large_utf8_array, 0), Some("large_test"));
        assert_eq!(get_string_value(&large_utf8_array, 1), None);

        // Test Utf8View
        let utf8_view_array = arrow_array::StringViewArray::from(vec![Some("view_test"), None]);
        assert_eq!(get_string_value(&utf8_view_array, 0), Some("view_test"));
        assert_eq!(get_string_value(&utf8_view_array, 1), None);

        // Test unsupported type
        let int_array = Int32Array::from(vec![123]);
        assert_eq!(get_string_value(&int_array, 0), None);
    }

    #[test]
    fn test_ceil_datetime() {
        // Test exact millisecond (should be rounded up)
        assert_eq!(ceil_datetime(1609459200000), 1609459201000);

        // Test with microsecond remainder (should stay the same)
        assert_eq!(ceil_datetime(1609459200123), 1609459200123);

        // Test zero
        assert_eq!(ceil_datetime(0), 1000);
    }

    #[test]
    fn test_round_ms_datetimes() {
        use delta_kernel::expressions::{Scalar, StructData};
        use delta_kernel::schema::{DataType, PrimitiveType, StructField};

        let ceil_fn = |v: i64| v + 1000;

        // Test timestamp scalar
        let timestamp = Scalar::Timestamp(1609459200000);
        let rounded = round_ms_datetimes(timestamp, &ceil_fn);
        assert_eq!(rounded, Scalar::Timestamp(1609459201000));

        // Test timestamp ntz scalar
        let timestamp_ntz = Scalar::TimestampNtz(1609459200000);
        let rounded = round_ms_datetimes(timestamp_ntz, &ceil_fn);
        assert_eq!(rounded, Scalar::TimestampNtz(1609459201000));

        // Test non-timestamp scalar (should be unchanged)
        let string_scalar = Scalar::String("test".into());
        let rounded = round_ms_datetimes(string_scalar.clone(), &ceil_fn);
        assert_eq!(rounded, string_scalar);

        // Test struct with timestamp
        let fields = vec![StructField::new(
            "ts",
            DataType::Primitive(PrimitiveType::Timestamp),
            true,
        )];
        let values = vec![Scalar::Timestamp(1609459200000)];
        let struct_data = StructData::try_new(fields.clone(), values).unwrap();
        let struct_scalar = Scalar::Struct(struct_data);

        let rounded = round_ms_datetimes(struct_scalar, &ceil_fn);
        if let Scalar::Struct(rounded_struct) = rounded {
            assert_eq!(rounded_struct.values()[0], Scalar::Timestamp(1609459201000));
        } else {
            panic!("Expected struct scalar");
        }
    }

    #[test]
    fn test_invalid_modification_time() {
        // Test that invalid timestamps return errors
        let invalid_timestamp = i64::MAX;
        let result = DateTime::from_timestamp_millis(invalid_timestamp);
        assert!(result.is_none());

        // Test a valid timestamp for comparison
        let valid_timestamp = 1609459200000; // 2021-01-01
        let result = DateTime::from_timestamp_millis(valid_timestamp);
        assert!(result.is_some());
    }

    #[test]
    fn logical_file_view_stats_prefers_raw_stats_json() {
        let full_stats_json = r#"{"maxValues":{"value":9},"numRecords":11,"nullCount":{"value":0},"minValues":{"value":1}}"#;
        let view = logical_file_view_with_partial_stats(full_stats_json);

        assert_eq!(view.stats().as_deref(), Some(full_stats_json));
    }

    #[test]
    #[allow(deprecated)]
    fn logical_file_view_add_action_preserves_full_stats_when_stats_parsed_is_partial() {
        let full_stats_json = r#"{"maxValues":{"value":9},"numRecords":11,"nullCount":{"value":0},"minValues":{"value":1}}"#;
        let view = logical_file_view_with_partial_stats(full_stats_json);

        assert_eq!(view.add_action().stats.as_deref(), Some(full_stats_json));
        assert_eq!(view.num_records(), Some(11));
        assert!(view.min_values().is_none());
    }

    #[test]
    fn logical_file_view_stats_falls_back_to_parsed_stats_when_raw_is_missing() {
        let view = logical_file_view_without_raw_stats();
        let stats = view
            .stats()
            .expect("stats fallback should rebuild JSON from stats_parsed");
        let actual: serde_json::Value = serde_json::from_str(&stats).unwrap();

        assert_eq!(
            actual,
            serde_json::json!({
                "numRecords": 11,
                "minValues": {"value": 1},
                "maxValues": {"value": 9},
                "nullCount": {"value": 0}
            })
        );
    }
}
