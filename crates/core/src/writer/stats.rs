use std::ops::Not;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    ops::AddAssign,
};

use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use indexmap::IndexMap;
use itertools::Itertools;
use parquet::basic::Type;
use parquet::basic::{ConvertedType, LogicalType};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};
use parquet::{
    basic::TimeUnit,
    file::{metadata::RowGroupMetaData, statistics::Statistics},
};
use tracing::warn;

use super::*;
use crate::kernel::{Add, scalars::ScalarExt};
use crate::protocol::{ColumnValueStat, Stats};

/// Creates an [`Add`] log action struct.
pub(crate) fn create_add(
    partition_values: &IndexMap<String, Scalar>,
    path: String,
    size: i64,
    file_metadata: &ParquetMetaData,
    num_indexed_cols: DataSkippingNumIndexedCols,
    stats_columns: &Option<Vec<impl AsRef<str>>>,
) -> Result<Add, DeltaTableError> {
    let stats = stats_from_file_metadata(
        partition_values,
        file_metadata,
        num_indexed_cols,
        stats_columns,
    )?;
    let stats_string = serde_json::to_string(&stats)?;

    // Determine the modification timestamp to include in the add action - milliseconds since epoch
    // Err should be impossible in this case since `SystemTime::now()` is always greater than `UNIX_EPOCH`
    let modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let modification_time = modification_time.as_millis() as i64;

    Ok(Add {
        path,
        size,
        partition_values: partition_values
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    if v.is_null() {
                        None
                    } else {
                        Some(v.serialize())
                    },
                )
            })
            .collect(),
        modification_time,
        data_change: true,
        stats: Some(stats_string),
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    })
}

// As opposed to `stats_from_file_metadata` which operates on `parquet::format::FileMetaData`,
// this function produces the stats by reading the metadata from already written out files.
//
// Note that the file metadata used here is actually `parquet::file::metadata::FileMetaData`
// which is a thrift decoding of the `parquet::format::FileMetaData` which is typically obtained
// when flushing the write.
pub(crate) fn stats_from_parquet_metadata(
    partition_values: &IndexMap<String, Scalar>,
    parquet_metadata: &ParquetMetaData,
    num_indexed_cols: DataSkippingNumIndexedCols,
    stats_columns: &Option<Vec<String>>,
) -> Result<Stats, DeltaWriterError> {
    let num_rows = parquet_metadata.file_metadata().num_rows();
    let schema_descriptor = parquet_metadata.file_metadata().schema_descr_ptr();
    let row_group_metadata = parquet_metadata.row_groups().to_vec();

    stats_from_metadata(
        partition_values,
        schema_descriptor,
        row_group_metadata,
        num_rows,
        num_indexed_cols,
        stats_columns,
    )
}

fn stats_from_file_metadata(
    partition_values: &IndexMap<String, Scalar>,
    file_metadata: &ParquetMetaData,
    num_indexed_cols: DataSkippingNumIndexedCols,
    stats_columns: &Option<Vec<impl AsRef<str>>>,
) -> Result<Stats, DeltaWriterError> {
    let schema_descriptor = file_metadata.file_metadata().schema_descr();

    let row_group_metadata: Vec<RowGroupMetaData> = file_metadata.row_groups().to_vec();

    stats_from_metadata(
        partition_values,
        Arc::new(schema_descriptor.clone()),
        row_group_metadata,
        file_metadata.file_metadata().num_rows(),
        num_indexed_cols,
        stats_columns,
    )
}

fn stats_from_metadata(
    partition_values: &IndexMap<String, Scalar>,
    schema_descriptor: Arc<SchemaDescriptor>,
    row_group_metadata: Vec<RowGroupMetaData>,
    num_rows: i64,
    num_indexed_cols: DataSkippingNumIndexedCols,
    stats_columns: &Option<Vec<impl AsRef<str>>>,
) -> Result<Stats, DeltaWriterError> {
    let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut null_count: HashMap<String, ColumnCountStat> = HashMap::new();
    let dialect = sqlparser::dialect::GenericDialect {};

    let idx_to_iterate = if let Some(stats_cols) = stats_columns {
        let stats_cols = stats_cols
            .iter()
            .map(|v| {
                match sqlparser::parser::Parser::new(&dialect)
                    .try_with_sql(v.as_ref())
                    .map_err(|e| DeltaTableError::generic(e.to_string()))?
                    .parse_multipart_identifier()
                {
                    Ok(parts) => Ok(parts.into_iter().map(|v| v.value).join(".")),
                    Err(e) => Err(DeltaWriterError::DeltaTable(
                        DeltaTableError::GenericError {
                            source: Box::new(e),
                        },
                    )),
                }
            })
            .collect::<Result<Vec<String>, DeltaWriterError>>()?;

        schema_descriptor
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(index, col)| {
                if stats_cols.contains(&col.name().to_string()) {
                    Some(index)
                } else {
                    None
                }
            })
            .collect()
    } else if num_indexed_cols == DataSkippingNumIndexedCols::AllColumns {
        (0..schema_descriptor.num_columns()).collect::<Vec<_>>()
    } else if let DataSkippingNumIndexedCols::NumColumns(n_cols) = num_indexed_cols {
        // The `delta.dataSkippingNumIndexedCols` budget is consumed by distinct
        // top-level fields, not by parquet leaf columns. A single top-level
        // column with many nested fields therefore takes one slot, not N.
        // Partition columns do not consume a slot.
        let limit = n_cols as usize;
        let mut admitted: HashSet<String> = HashSet::new();
        let mut admitted_count: usize = 0;
        let mut idxs: Vec<usize> = Vec::new();
        for (idx, col) in schema_descriptor.columns().iter().enumerate() {
            let top = match col.path().parts().first() {
                Some(t) => t.clone(),
                None => continue,
            };
            if partition_values.contains_key(&top) {
                continue;
            }
            if !admitted.contains(&top) {
                if admitted_count >= limit {
                    break;
                }
                admitted.insert(top);
                admitted_count += 1;
            }
            idxs.push(idx);
        }
        idxs
    } else {
        return Err(DeltaWriterError::DeltaTable(DeltaTableError::Generic(
            "delta.dataSkippingNumIndexedCols valid values are >=-1".to_string(),
        )));
    };

    for idx in idx_to_iterate {
        let column_descr = schema_descriptor.column(idx);

        let column_path = column_descr.path();
        let column_path_parts = column_path.parts();

        // Do not include partition columns in statistics (still relevant for
        // the `AllColumns` and explicit `stats_columns` branches).
        if partition_values.contains_key(&column_path_parts[0]) {
            continue;
        }

        let maybe_stats: Option<AggregatedStats> = row_group_metadata
            .iter()
            .flat_map(|g| {
                g.column(idx).statistics().into_iter().filter_map(|s| {
                    let is_binary = matches!(&column_descr.physical_type(), Type::BYTE_ARRAY)
                        && matches!(column_descr.logical_type_ref(), Some(LogicalType::String))
                            .not();
                    if is_binary {
                        warn!(
                            "Skipping column {} because it's a binary field.",
                            &column_descr.name().to_string()
                        );
                        None
                    } else {
                        let logical_type = column_descr
                            .logical_type_ref()
                            .or(converted_to_logical_type(column_descr.converted_type()));
                        Some(AggregatedStats::from((s, logical_type)))
                    }
                })
            })
            .reduce(|mut left, right| {
                left += right;
                left
            });

        if let Some(stats) = maybe_stats {
            apply_min_max_for_column(
                stats,
                column_descr.clone(),
                column_descr.path().parts(),
                &mut min_values,
                &mut max_values,
                &mut null_count,
            )?;
        }
    }

    Ok(Stats {
        min_values,
        max_values,
        num_records: num_rows,
        null_count,
    })
}

/// Logical scalars extracted from statistics. These are used to aggregate
/// minimums and maximums. We can't use the physical scalars because they
/// are not ordered correctly for some types. For example, decimals are stored
/// as fixed length binary, and can't be sorted leixcographically.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum StatsScalar {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Date(chrono::NaiveDate),
    Timestamp(chrono::NaiveDateTime),
    TimestampNtz(chrono::NaiveDateTime),
    // We are serializing to f64 later and the ordering should be the same
    // Scale is stored to handle scale=0 serialization correctly
    Decimal { value: f64, scale: i32 },
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
}

impl StatsScalar {
    fn try_from_stats(
        stats: &Statistics,
        logical_type: Option<&LogicalType>,
        use_min: bool,
    ) -> Result<Self, DeltaWriterError> {
        macro_rules! get_stat {
            ($val: expr) => {
                if use_min {
                    *$val.min_opt().unwrap()
                } else {
                    *$val.max_opt().unwrap()
                }
            };
        }

        match (stats, logical_type) {
            (Statistics::Boolean(v), _) => Ok(Self::Boolean(get_stat!(v))),
            // Int32 can be date, decimal, or just int32
            (Statistics::Int32(v), Some(LogicalType::Date)) => {
                let epoch_start = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(); // creating from epoch should be infallible
                let date = epoch_start + chrono::Duration::days(get_stat!(v) as i64);
                Ok(Self::Date(date))
            }
            (Statistics::Int32(v), Some(LogicalType::Decimal { scale, .. })) => {
                let val = get_stat!(v) as f64 / 10.0_f64.powi(*scale);
                // Spark serializes these as numbers
                Ok(Self::Decimal {
                    value: val,
                    scale: *scale,
                })
            }
            (Statistics::Int32(v), _) => Ok(Self::Int32(get_stat!(v))),
            // Int64 can be timestamp, decimal, or integer
            (
                Statistics::Int64(v),
                Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c,
                    unit,
                }),
            ) => {
                let v = get_stat!(v);
                let timestamp = match unit {
                    TimeUnit::MILLIS => chrono::DateTime::from_timestamp_millis(v),
                    TimeUnit::MICROS => chrono::DateTime::from_timestamp_micros(v),
                    TimeUnit::NANOS => {
                        let secs = v / 1_000_000_000;
                        let nanosecs = (v % 1_000_000_000) as u32;
                        chrono::DateTime::from_timestamp(secs, nanosecs)
                    }
                };
                let timestamp = timestamp.ok_or(DeltaWriterError::StatsParsingFailed {
                    debug_value: v.to_string(),
                    logical_type: logical_type.cloned(),
                })?;
                if *is_adjusted_to_u_t_c {
                    Ok(Self::Timestamp(timestamp.naive_utc()))
                } else {
                    Ok(Self::TimestampNtz(timestamp.naive_utc()))
                }
            }
            (Statistics::Int64(v), Some(LogicalType::Decimal { scale, .. })) => {
                let val = get_stat!(v) as f64 / 10.0_f64.powi(*scale);
                // Spark serializes these as numbers
                Ok(Self::Decimal {
                    value: val,
                    scale: *scale,
                })
            }
            (Statistics::Int64(v), _) => Ok(Self::Int64(get_stat!(v))),
            (Statistics::Float(v), _) => Ok(Self::Float32(get_stat!(v))),
            (Statistics::Double(v), _) => Ok(Self::Float64(get_stat!(v))),
            (Statistics::ByteArray(v), logical_type) => {
                let bytes = if use_min {
                    v.min_bytes_opt()
                } else {
                    v.max_bytes_opt()
                }
                .unwrap_or_default();
                match logical_type {
                    None => Ok(Self::Bytes(bytes.to_vec())),
                    Some(LogicalType::String) => {
                        Ok(Self::String(String::from_utf8(bytes.to_vec()).map_err(
                            |_| DeltaWriterError::StatsParsingFailed {
                                debug_value: format!("{bytes:?}"),
                                logical_type: Some(LogicalType::String),
                            },
                        )?))
                    }
                    _ => Err(DeltaWriterError::StatsParsingFailed {
                        debug_value: format!("{bytes:?}"),
                        logical_type: logical_type.cloned(),
                    }),
                }
            }
            (Statistics::FixedLenByteArray(v), Some(LogicalType::Decimal { scale, precision })) => {
                let val = if use_min {
                    v.min_bytes_opt()
                } else {
                    v.max_bytes_opt()
                }
                .unwrap_or_default();

                let val = if val.len() <= 16 {
                    i128::from_be_bytes(sign_extend_be(val)) as f64
                } else {
                    return Err(DeltaWriterError::StatsParsingFailed {
                        debug_value: format!("{val:?}"),
                        logical_type: Some(LogicalType::Decimal {
                            scale: *scale,
                            precision: *precision,
                        }),
                    });
                };

                let mut val = val / 10.0_f64.powi(*scale);

                if val.is_normal()
                    && (val.trunc() as i128).to_string().len() > (precision - scale) as usize
                {
                    // For normal values with integer parts that get rounded to a number beyond
                    // the precision - scale range take the next smaller (by magnitude) value
                    val = f64::from_bits(val.to_bits() - 1);
                }

                Ok(Self::Decimal {
                    value: val,
                    scale: *scale,
                })
            }
            (Statistics::FixedLenByteArray(v), Some(LogicalType::Uuid)) => {
                let val = if use_min {
                    v.min_bytes_opt()
                } else {
                    v.max_bytes_opt()
                }
                .unwrap_or_default();

                if val.len() != 16 {
                    return Err(DeltaWriterError::StatsParsingFailed {
                        debug_value: format!("{val:?}"),
                        logical_type: Some(LogicalType::Uuid),
                    });
                }

                let mut bytes = [0; 16];
                bytes.copy_from_slice(val);

                let val = uuid::Uuid::from_bytes(bytes);
                Ok(Self::Uuid(val))
            }
            (stats, _) => Err(DeltaWriterError::StatsParsingFailed {
                debug_value: format!("{stats:?}"),
                logical_type: logical_type.cloned(),
            }),
        }
    }
}

/// Performs big endian sign extension
/// Copied from arrow-rs repo/parquet crate:
/// https://github.com/apache/arrow-rs/blob/b25c441745602c9967b1e3cc4a28bc469cfb1311/parquet/src/arrow/buffer/bit_util.rs#L54
pub fn sign_extend_be<const N: usize>(b: &[u8]) -> [u8; N] {
    assert!(b.len() <= N, "Array too large, expected less than {N}");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - b.len()).zip(b) {
        *d = *s;
    }
    result
}

impl From<StatsScalar> for serde_json::Value {
    fn from(scalar: StatsScalar) -> Self {
        match scalar {
            StatsScalar::Boolean(v) => serde_json::Value::Bool(v),
            StatsScalar::Int32(v) => serde_json::Value::from(v),
            StatsScalar::Int64(v) => serde_json::Value::from(v),
            StatsScalar::Float32(v) => serde_json::Value::from(v),
            StatsScalar::Float64(v) => serde_json::Value::from(v),
            StatsScalar::Date(v) => serde_json::Value::from(v.format("%Y-%m-%d").to_string()),
            StatsScalar::Timestamp(v) => {
                serde_json::Value::from(v.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string())
            }
            StatsScalar::TimestampNtz(v) => {
                serde_json::Value::from(v.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            }
            StatsScalar::Decimal { value, scale } => {
                // For scale=0, serialize as integer since serde_json would otherwise
                // serialize f64 as "1234.0" instead of "1234"
                if scale == 0 {
                    serde_json::Value::from(value.round() as i64)
                } else {
                    serde_json::Value::from(value)
                }
            }
            StatsScalar::String(v) => serde_json::Value::from(v),
            StatsScalar::Bytes(v) => {
                let escaped_bytes = v
                    .into_iter()
                    .flat_map(std::ascii::escape_default)
                    .collect::<Vec<u8>>();
                let escaped_string = String::from_utf8(escaped_bytes).unwrap();
                serde_json::Value::from(escaped_string)
            }
            StatsScalar::Uuid(v) => serde_json::Value::from(v.hyphenated().to_string()),
        }
    }
}

/// Aggregated stats
struct AggregatedStats {
    pub min: Option<StatsScalar>,
    pub max: Option<StatsScalar>,
    pub null_count: u64,
}

impl From<(&Statistics, Option<&LogicalType>)> for AggregatedStats {
    fn from(value: (&Statistics, Option<&LogicalType>)) -> Self {
        let (stats, logical_type) = value;
        let null_count = stats.null_count_opt().unwrap_or_default();
        if stats.min_bytes_opt().is_some() && stats.max_bytes_opt().is_some() {
            let min = StatsScalar::try_from_stats(stats, logical_type, true).ok();
            let max = StatsScalar::try_from_stats(stats, logical_type, false).ok();
            Self {
                min,
                max,
                null_count,
            }
        } else {
            Self {
                min: None,
                max: None,
                null_count,
            }
        }
    }
}

impl AddAssign for AggregatedStats {
    fn add_assign(&mut self, rhs: Self) {
        self.min = match (self.min.take(), rhs.min) {
            (Some(lhs), Some(rhs)) => {
                if lhs < rhs {
                    Some(lhs)
                } else {
                    Some(rhs)
                }
            }
            (lhs, rhs) => lhs.or(rhs),
        };
        self.max = match (self.max.take(), rhs.max) {
            (Some(lhs), Some(rhs)) => {
                if lhs > rhs {
                    Some(lhs)
                } else {
                    Some(rhs)
                }
            }
            (lhs, rhs) => lhs.or(rhs),
        };

        self.null_count += rhs.null_count;
    }
}

fn apply_min_max_for_column(
    statistics: AggregatedStats,
    column_descr: Arc<ColumnDescriptor>,
    column_path_parts: &[String],
    min_values: &mut HashMap<String, ColumnValueStat>,
    max_values: &mut HashMap<String, ColumnValueStat>,
    null_counts: &mut HashMap<String, ColumnCountStat>,
) -> Result<(), DeltaWriterError> {
    // Repeated leaf null counts describe nested values only.
    if column_descr.max_rep_level() > 0 {
        return Ok(());
    }

    match (column_path_parts.len(), column_path_parts.first()) {
        // Base case - we are at the leaf struct level in the path
        (1, _) => {
            let key = column_descr.name().to_string();

            if let Some(min) = statistics.min {
                let min = ColumnValueStat::Value(min.into());
                min_values.insert(key.clone(), min);
            }

            if let Some(max) = statistics.max {
                let max = ColumnValueStat::Value(max.into());
                max_values.insert(key.clone(), max);
            }

            null_counts.insert(key, ColumnCountStat::Value(statistics.null_count as i64));

            Ok(())
        }
        // Recurse to load value at the appropriate level of HashMap
        (_, Some(key)) => {
            let child_min_values = min_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
            let child_max_values = max_values
                .entry(key.to_owned())
                .or_insert_with(|| ColumnValueStat::Column(HashMap::new()));
            let child_null_counts = null_counts
                .entry(key.to_owned())
                .or_insert_with(|| ColumnCountStat::Column(HashMap::new()));

            match (child_min_values, child_max_values, child_null_counts) {
                (
                    ColumnValueStat::Column(mins),
                    ColumnValueStat::Column(maxes),
                    ColumnCountStat::Column(null_counts),
                ) => {
                    let remaining_parts: Vec<String> = column_path_parts
                        .iter()
                        .skip(1)
                        .map(|s| s.to_string())
                        .collect();

                    apply_min_max_for_column(
                        statistics,
                        column_descr,
                        remaining_parts.as_slice(),
                        mins,
                        maxes,
                        null_counts,
                    )?;

                    Ok(())
                }
                _ => {
                    unreachable!();
                }
            }
        }
        // column path parts will always have at least one element.
        (_, None) => {
            unreachable!();
        }
    }
}

/// Map the old (old!) [parquet::basic::ConvertedType] into the more modern
/// [parquet::basic::LogicalType]
///
/// Because this is a legacy format helper function, ro types which might not be easy to convert
/// from one struct to the other, it will just return `None`
fn converted_to_logical_type(converted: ConvertedType) -> Option<&'static LogicalType> {
    match converted {
        ConvertedType::UTF8 => Some(&LogicalType::String),
        ConvertedType::DATE => Some(&LogicalType::Date),
        ConvertedType::JSON => Some(&LogicalType::Json),
        ConvertedType::BSON => Some(&LogicalType::Bson),
        _others => None,
    }
}

#[cfg(test)]
mod tests {
    use super::utils::record_batch_from_message;
    use super::*;
    use crate::{
        DeltaTable,
        errors::DeltaTableError,
        protocol::{ColumnCountStat, ColumnValueStat},
        table::builder::DeltaTableBuilder,
    };
    use parquet::data_type::{ByteArray, FixedLenByteArray};
    use parquet::file::statistics::ValueStatistics;
    use parquet::{basic::Compression, file::properties::WriterProperties};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::LazyLock;
    use url::Url;

    macro_rules! simple_parquet_stat {
        ($variant:expr, $value:expr) => {
            $variant(ValueStatistics::new(
                Some($value),
                Some($value),
                None,
                Some(0),
                false,
            ))
        };
    }

    #[test]
    fn test_stats_scalar_serialization() {
        let cases = &[
            (
                simple_parquet_stat!(Statistics::Boolean, true),
                Some(LogicalType::Integer {
                    bit_width: 1,
                    is_signed: true,
                }),
                Value::Bool(true),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 1),
                Some(LogicalType::Integer {
                    bit_width: 32,
                    is_signed: true,
                }),
                Value::from(1),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 1234),
                Some(LogicalType::Decimal {
                    scale: 3,
                    precision: 4,
                }),
                Value::from(1.234),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 1234),
                Some(LogicalType::Decimal {
                    scale: -1,
                    precision: 4,
                }),
                Value::from(12340.0),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 1234),
                Some(LogicalType::Decimal {
                    scale: 0,
                    precision: 4,
                }),
                Value::from(1234),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 10561),
                Some(LogicalType::Date),
                Value::from("1998-12-01"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789123456),
                Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::basic::TimeUnit::NANOS,
                }),
                Value::from("2022-01-01T12:34:56.789123456Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789123),
                Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::basic::TimeUnit::MICROS,
                }),
                Value::from("2022-01-01T12:34:56.789123Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789),
                Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::basic::TimeUnit::MILLIS,
                }),
                Value::from("2022-01-01T12:34:56.789Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1234),
                Some(LogicalType::Decimal {
                    scale: 3,
                    precision: 4,
                }),
                Value::from(1.234),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1234),
                Some(LogicalType::Decimal {
                    scale: -1,
                    precision: 4,
                }),
                Value::from(12340.0),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1234),
                Some(LogicalType::Decimal {
                    scale: 0,
                    precision: 4,
                }),
                Value::from(1234),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1234),
                None,
                Value::from(1234),
            ),
            (
                simple_parquet_stat!(Statistics::ByteArray, ByteArray::from(b"hello".to_vec())),
                Some(LogicalType::String),
                Value::from("hello"),
            ),
            (
                simple_parquet_stat!(Statistics::ByteArray, ByteArray::from(b"\x00\\".to_vec())),
                None,
                Value::from("\\x00\\\\"),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(1243124142314423i128.to_be_bytes().to_vec())
                ),
                Some(LogicalType::Decimal {
                    scale: 3,
                    precision: 16,
                }),
                Value::from(1243124142314.423),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(vec![0, 39, 16])
                ),
                Some(LogicalType::Decimal {
                    scale: 3,
                    precision: 5,
                }),
                Value::from(10.0),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(1234i128.to_be_bytes().to_vec())
                ),
                Some(LogicalType::Decimal {
                    scale: 0,
                    precision: 4,
                }),
                Value::from(1234),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(vec![
                        75, 59, 76, 168, 90, 134, 196, 122, 9, 138, 34, 63, 255, 255, 255, 255
                    ])
                ),
                Some(LogicalType::Decimal {
                    scale: 6,
                    precision: 38,
                }),
                Value::from(9.999999999999999e31),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(vec![
                        180, 196, 179, 87, 165, 121, 59, 133, 246, 117, 221, 192, 0, 0, 0, 1
                    ])
                ),
                Some(LogicalType::Decimal {
                    scale: 6,
                    precision: 38,
                }),
                Value::from(-9.999999999999999e31),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(
                        [
                            0xc2, 0xe8, 0xc7, 0xf7, 0xd1, 0xf9, 0x4b, 0x49, 0xa5, 0xd9, 0x4b, 0xfe,
                            0x75, 0xc3, 0x17, 0xe2
                        ]
                        .to_vec()
                    )
                ),
                Some(LogicalType::Uuid),
                Value::from("c2e8c7f7-d1f9-4b49-a5d9-4bfe75c317e2"),
            ),
        ];

        for (stats, logical_type, expected) in cases {
            let scalar = StatsScalar::try_from_stats(stats, logical_type.as_ref(), true).unwrap();
            let actual = serde_json::Value::from(scalar);
            assert_eq!(&actual, expected);
        }
    }

    #[tokio::test]
    async fn test_delta_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path();
        create_temp_table(table_path);

        let table_uri = Url::from_directory_path(table_path).unwrap();
        let table = load_table(&table_uri, HashMap::new()).await.unwrap();

        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        writer = writer.with_writer_properties(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_max_row_group_row_count(Some(128))
                .build(),
        );

        let arrow_schema = writer.arrow_schema();
        let batch = record_batch_from_message(arrow_schema, JSON_ROWS.clone().as_ref()).unwrap();

        writer.write(batch).await.unwrap();
        let add = writer.flush().await.unwrap();
        assert_eq!(add.len(), 1);
        let stats = add[0].get_stats().unwrap().unwrap();

        let min_max_keys = vec!["meta", "some_int", "some_string", "some_bool", "uuid"];
        let null_count_keys = min_max_keys.clone();

        assert_eq!(
            min_max_keys.len(),
            stats.min_values.len(),
            "min values don't match"
        );
        assert_eq!(
            min_max_keys.len(),
            stats.max_values.len(),
            "max values don't match"
        );
        assert_eq!(
            null_count_keys.len(),
            stats.null_count.len(),
            "null counts don't match"
        );

        // assert on min values
        for (k, v) in stats.min_values.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnValueStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(0, partition.as_i64().unwrap());

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!("2021-06-22", timestamp.as_str().unwrap());
                }
                ("some_int", ColumnValueStat::Value(v)) => assert_eq!(302, v.as_i64().unwrap()),
                ("some_bool", ColumnValueStat::Value(v)) => assert!(!v.as_bool().unwrap()),
                ("some_string", ColumnValueStat::Value(v)) => {
                    assert_eq!("GET", v.as_str().unwrap())
                }
                ("date", ColumnValueStat::Value(v)) => {
                    assert_eq!("2021-06-22", v.as_str().unwrap())
                }
                ("uuid", ColumnValueStat::Value(v)) => {
                    assert_eq!("176c770d-92af-4a21-bf76-5d8c5261d659", v.as_str().unwrap())
                }
                k => panic!("Key {k:?} should not be present in min_values"),
            }
        }

        // assert on max values
        for (k, v) in stats.max_values.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnValueStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(1, partition.as_i64().unwrap());

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!("2021-06-22", timestamp.as_str().unwrap());
                }
                ("some_int", ColumnValueStat::Value(v)) => assert_eq!(400, v.as_i64().unwrap()),
                ("some_bool", ColumnValueStat::Value(v)) => assert!(v.as_bool().unwrap()),
                ("some_string", ColumnValueStat::Value(v)) => {
                    assert_eq!("PUT", v.as_str().unwrap())
                }
                ("date", ColumnValueStat::Value(v)) => {
                    assert_eq!("2021-06-22", v.as_str().unwrap())
                }
                ("uuid", ColumnValueStat::Value(v)) => {
                    assert_eq!("a98bea04-d119-4f21-8edc-eb218b5849af", v.as_str().unwrap())
                }
                k => panic!("Key {k:?} should not be present in max_values"),
            }
        }

        // assert on null count
        for (k, v) in stats.null_count.iter() {
            match (k.as_str(), v) {
                ("meta", ColumnCountStat::Column(map)) => {
                    assert_eq!(2, map.len());

                    let kafka = map.get("kafka").unwrap().as_column().unwrap();
                    assert_eq!(3, kafka.len());
                    let partition = kafka.get("partition").unwrap().as_value().unwrap();
                    assert_eq!(0, partition);

                    let producer = map.get("producer").unwrap().as_column().unwrap();
                    assert_eq!(1, producer.len());
                    let timestamp = producer.get("timestamp").unwrap().as_value().unwrap();
                    assert_eq!(0, timestamp);
                }
                ("some_int", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_bool", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_string", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("date", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                ("uuid", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                k => panic!("Key {k:?} should not be present in null_count"),
            }
        }
    }

    #[tokio::test]
    async fn test_repeated_leaf_stats_skip_container_null_counts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path();
        let schema = json!({
            "type": "struct",
            "fields": [
                { "name": "id", "type": "string", "nullable": true, "metadata": {} },
                {
                    "name": "b",
                    "type": {
                        "type": "array",
                        "elementType": "integer",
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
                },
                {
                    "name": "m",
                    "type": {
                        "type": "map",
                        "keyType": "string",
                        "valueType": "integer",
                        "valueContainsNull": true
                    },
                    "nullable": true, "metadata": {}
                }
            ]
        });
        create_temp_table_with_schema(table_path, &schema);

        let table_uri = Url::from_directory_path(table_path).unwrap();
        let table = load_table(&table_uri, HashMap::new()).await.unwrap();

        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        writer = writer.with_writer_properties(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );

        let arrow_schema = writer.arrow_schema();
        let rows = [
            json!({
                "id": "with_null_element",
                "b": [1, null, 2],
                "m": {"a": 1, "b": null},
            }),
            json!({
                "id": "empty",
                "b": [],
                "m": {},
            }),
        ];
        let batch = record_batch_from_message(arrow_schema, rows.as_slice()).unwrap();

        writer.write(batch).await.unwrap();
        let add = writer.flush().await.unwrap();
        assert_eq!(add.len(), 1);
        let stats = add[0].get_stats().unwrap().unwrap();

        assert!(
            !stats.null_count.contains_key("b"),
            "writer copied list element null counts into list nullCount: {:?}",
            stats.null_count
        );
        assert!(
            !stats.null_count.contains_key("m"),
            "writer copied map value null counts into map nullCount: {:?}",
            stats.null_count
        );
        assert_eq!(
            Some(&ColumnCountStat::Value(0)),
            stats.null_count.get("id"),
            "writer kept scalar null counts"
        );
    }

    // Regression test for delta-io/delta-rs#3172: leaves under a nested
    // top-level field used to consume the `delta.dataSkippingNumIndexedCols`
    // budget one-by-one, starving later top-level columns of stats. After the
    // fix the budget is counted per distinct top-level field, so every
    // top-level column up to the limit gets stats.
    #[tokio::test]
    async fn test_nested_fields_do_not_consume_stats_budget() {
        use crate::kernel::{DataType as DeltaDataType, PrimitiveType, StructField, StructType};

        // 5 top-level columns, 8 parquet leaves total ("1", nested.{2,3,4,5},
        // year, month, day). With `dataSkippingNumIndexedCols=5` the
        // leaf-counted implementation would burn the budget on "1" plus the
        // four `nested.*` leaves, dropping year/month/day. With the
        // top-level-counted budget all five top-level columns are admitted.
        let nested = StructType::try_new([
            StructField::nullable("2", DeltaDataType::Primitive(PrimitiveType::Long)),
            StructField::nullable("3", DeltaDataType::Primitive(PrimitiveType::Long)),
            StructField::nullable("4", DeltaDataType::Primitive(PrimitiveType::Long)),
            StructField::nullable("5", DeltaDataType::Primitive(PrimitiveType::Long)),
        ])
        .unwrap();
        let configuration: HashMap<String, Option<String>> = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            Some("5".to_string()),
        )]
        .into_iter()
        .collect();

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                StructField::nullable("1", DeltaDataType::Primitive(PrimitiveType::String)),
                StructField::nullable("nested", DeltaDataType::Struct(Box::new(nested))),
                StructField::nullable("year", DeltaDataType::Primitive(PrimitiveType::Long)),
                StructField::nullable("month", DeltaDataType::Primitive(PrimitiveType::Long)),
                StructField::nullable("day", DeltaDataType::Primitive(PrimitiveType::Long)),
            ])
            .with_configuration(configuration)
            .await
            .unwrap();

        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        let arrow_schema = writer.arrow_schema();
        let rows = vec![json!({
            "1": "foo",
            "nested": {"2": 100, "3": 200, "4": 300, "5": 400},
            "year": 2024,
            "month": 12,
            "day": 1
        })];
        let batch = record_batch_from_message(arrow_schema, rows.as_slice()).unwrap();

        writer.write(batch).await.unwrap();
        let add = writer.flush().await.unwrap();
        assert_eq!(add.len(), 1);
        let stats = add[0].get_stats().unwrap().unwrap();

        // Every top-level non-partition column should have min/max/nullCount.
        for key in ["1", "year", "month", "day"] {
            assert!(
                stats.min_values.contains_key(key),
                "min_values missing top-level column {key:?}: {:?}",
                stats.min_values.keys().collect::<Vec<_>>()
            );
            assert!(
                stats.max_values.contains_key(key),
                "max_values missing top-level column {key:?}: {:?}",
                stats.max_values.keys().collect::<Vec<_>>()
            );
            assert!(
                stats.null_count.contains_key(key),
                "null_count missing top-level column {key:?}: {:?}",
                stats.null_count.keys().collect::<Vec<_>>()
            );
        }

        // The nested struct's leaves should still produce per-field stats
        // under the "nested" key (one top-level slot, all leaves admitted).
        let nested_min = stats
            .min_values
            .get("nested")
            .and_then(ColumnValueStat::as_column)
            .expect("nested entry should be a column map");
        for key in ["2", "3", "4", "5"] {
            assert!(
                nested_min.contains_key(key),
                "nested.{key} missing from min_values"
            );
        }
    }

    async fn load_table(
        table_url: &Url,
        options: HashMap<String, String>,
    ) -> Result<DeltaTable, DeltaTableError> {
        DeltaTableBuilder::from_url(table_url.clone())?
            .with_storage_options(options)
            .load()
            .await
    }

    fn create_temp_table(table_path: &Path) {
        let log_path = table_path.join("_delta_log");

        std::fs::create_dir(log_path.as_path()).unwrap();
        std::fs::write(
            log_path.join("00000000000000000000.json"),
            V0_COMMIT.as_str(),
        )
        .unwrap();
    }

    fn create_temp_table_with_schema(table_path: &Path, schema: &Value) {
        let log_path = table_path.join("_delta_log");
        let schema_string = serde_json::to_string(schema).unwrap();
        let jsons = [
            json!({
                "protocol":{"minReaderVersion":1,"minWriterVersion":2}
            }),
            json!({
                "metaData": {
                    "id": "22ef18ba-191c-4c36-a606-3dad5cdf3830",
                    "format": {
                        "provider": "parquet", "options": {}
                    },
                    "schemaString": schema_string,
                    "partitionColumns": [], "configuration": {}, "createdTime": 1564524294376i64
                }
            }),
        ];

        std::fs::create_dir(log_path.as_path()).unwrap();
        std::fs::write(
            log_path.join("00000000000000000000.json"),
            jsons
                .iter()
                .map(|j| serde_json::to_string(j).unwrap())
                .collect::<Vec<String>>()
                .join("\n"),
        )
        .unwrap();
    }

    static SCHEMA: LazyLock<Value> = LazyLock::new(|| {
        json!({
            "type": "struct",
            "fields": [
                {
                    "name": "meta",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "kafka",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "topic",
                                            "type": "string",
                                            "nullable": true, "metadata": {}
                                        },
                                        {
                                            "name": "partition",
                                            "type": "integer",
                                            "nullable": true, "metadata": {}
                                        },
                                        {
                                            "name": "offset",
                                            "type": "long",
                                            "nullable": true, "metadata": {}
                                        }
                                    ],
                                },
                                "nullable": true, "metadata": {}
                            },
                            {
                                "name": "producer",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "timestamp",
                                            "type": "string",
                                            "nullable": true, "metadata": {}
                                        }
                                    ],
                                },
                                "nullable": true, "metadata": {}
                            }
                        ]
                    },
                    "nullable": true, "metadata": {}
                },
                { "name": "some_string", "type": "string", "nullable": true, "metadata": {} },
                { "name": "some_int", "type": "integer", "nullable": true, "metadata": {} },
                { "name": "some_bool", "type": "boolean", "nullable": true, "metadata": {} },
                {
                    "name": "some_list",
                    "type": {
                        "type": "array",
                        "elementType": "string",
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
                },
                {
                    "name": "some_nested_list",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "array",
                            "elementType": "integer",
                            "containsNull": true
                        },
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
               },
               { "name": "date", "type": "string", "nullable": true, "metadata": {} },
               { "name": "uuid", "type": "string", "nullable": true, "metadata": {} },
            ]
        })
    });
    static V0_COMMIT: LazyLock<String> = LazyLock::new(|| {
        let schema_string = serde_json::to_string(&SCHEMA.clone()).unwrap();
        let jsons = [
            json!({
                "protocol":{"minReaderVersion":1,"minWriterVersion":2}
            }),
            json!({
                "metaData": {
                    "id": "22ef18ba-191c-4c36-a606-3dad5cdf3830",
                    "format": {
                        "provider": "parquet", "options": {}
                    },
                    "schemaString": schema_string,
                    "partitionColumns": ["date"], "configuration": {}, "createdTime": 1564524294376i64
                }
            }),
        ];

        jsons
            .iter()
            .map(|j| serde_json::to_string(j).unwrap())
            .collect::<Vec<String>>()
            .join("\n")
    });
    static JSON_ROWS: LazyLock<Vec<Value>> = LazyLock::new(|| {
        std::iter::repeat_n(
            json!({
                "meta": {
                    "kafka": {
                        "offset": 0,
                        "partition": 0,
                        "topic": "some_topic"
                    },
                    "producer": {
                        "timestamp": "2021-06-22"
                    },
                },
                "some_string": "GET",
                "some_int": 302,
                "some_bool": true,
                "some_list": ["a", "b", "c"],
                "some_nested_list": [[42], [84]],
                "date": "2021-06-22",
                "uuid": "176c770d-92af-4a21-bf76-5d8c5261d659",
            }),
            100,
        )
        .chain(std::iter::repeat_n(
            json!({
                "meta": {
                    "kafka": {
                        "offset": 100,
                        "partition": 1,
                        "topic": "another_topic"
                    },
                    "producer": {
                        "timestamp": "2021-06-22"
                    },
                },
                "some_string": "PUT",
                "some_int": 400,
                "some_bool": false,
                "some_list": ["x", "y", "z"],
                "some_nested_list": [[42], [84]],
                "date": "2021-06-22",
                "uuid": "54f3e867-3f7b-4122-a452-9d74fb4fe1ba",
            }),
            100,
        ))
        .chain(std::iter::repeat_n(
            json!({
                "meta": {
                    "kafka": {
                        "offset": 0,
                        "partition": 0,
                        "topic": "some_topic"
                    },
                    "producer": {
                        "timestamp": "2021-06-22"
                    },
                },
                "some_nested_list": [[42], null],
                "date": "2021-06-22",
                "uuid": "a98bea04-d119-4f21-8edc-eb218b5849af",
            }),
            100,
        ))
        .collect()
    });
}
