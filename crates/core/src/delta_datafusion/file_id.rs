//! Internal synthetic file identifier utilities.
//!
//! `file_id` is an internal correlation mechanism used by the DataFusion integration to associate
//! rows with their source file (e.g. per-file transforms, deletion vectors, and matched-file DML
//! planning). It is intentionally centralized in this module to minimize schema/type drift.
//!
//! TODO(delta-io/delta-rs#4115): When ParquetAccessPlans can carry per-file transforms and DV
//! filtering directly into the Parquet scan (and DV semantics become order-insensitive), this
//! synthetic column should become unnecessary and can be removed.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion::scalar::ScalarValue;
use datafusion_datasource::file_scan_config::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict,
};

/// Default column name for the synthetic file identifier.
///
/// This column is used internally to correlate rows back to their source file so we can apply
/// per-file transforms (e.g. column mapping, deletion vectors) and to support DML rewrite scans.
pub(crate) const FILE_ID_COLUMN_DEFAULT: &str = "__delta_rs_file_id__";

/// Canonical Arrow type for the synthetic file-id column.
///
/// We keep this aligned with DataFusion's recommended dictionary encoding for partition values
/// (`wrap_partition_type_in_dict`) so that both partition materialization and literal construction
/// (`wrap_partition_value_in_dict`) agree on the dictionary key type (currently `UInt16`).
///
/// Note: we intentionally use `Utf8` (not `Utf8View`) because Arrow dictionary packing does not
/// support view types.
pub(crate) fn file_id_data_type() -> DataType {
    wrap_partition_type_in_dict(DataType::Utf8)
}

/// Construct the canonical `file_id` field.
pub(crate) fn file_id_field(name: Option<&str>) -> FieldRef {
    Arc::new(Field::new(
        name.unwrap_or(FILE_ID_COLUMN_DEFAULT),
        file_id_data_type(),
        false,
    ))
}

/// Wrap a file path in the canonical dictionary encoding used for `file_id` values.
pub(crate) fn wrap_file_id_value(path: impl Into<String>) -> ScalarValue {
    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(path.into())))
}
