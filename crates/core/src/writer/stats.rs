use std::cmp::Ordering;
use std::ops::Not;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    ops::AddAssign,
};

use chrono::Timelike;
use delta_kernel::expressions::Scalar;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use indexmap::IndexMap;
use itertools::Itertools;
use parquet::basic::Type;
use parquet::basic::{ConvertedType, DecimalType, LogicalType};
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
    // All values of a column share one precision and scale, so this orders by the unscaled value
    Decimal {
        unscaled: i128,
        precision: i32,
        scale: i32,
    },
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
            (Statistics::Int32(v), Some(LogicalType::Decimal(decimal_type))) => Ok(Self::Decimal {
                unscaled: get_stat!(v).into(),
                precision: decimal_type.precision,
                scale: decimal_type.scale,
            }),
            (Statistics::Int32(v), _) => Ok(Self::Int32(get_stat!(v))),
            // Int64 can be timestamp, decimal, or integer
            (Statistics::Int64(v), Some(LogicalType::Timestamp(timestamp_type))) => {
                let v = get_stat!(v);
                let timestamp = match timestamp_type.unit {
                    TimeUnit::MILLIS => chrono::DateTime::from_timestamp_millis(v),
                    TimeUnit::MICROS => chrono::DateTime::from_timestamp_micros(v),
                    TimeUnit::NANOS => {
                        // Euclidean division keeps the seconds part floored and
                        // the sub-second part non-negative, so pre-epoch
                        // (negative) values convert like positive ones.
                        let secs = v.div_euclid(1_000_000_000);
                        let nanosecs = v.rem_euclid(1_000_000_000) as u32;
                        chrono::DateTime::from_timestamp(secs, nanosecs)
                    }
                };
                let timestamp = timestamp.ok_or(DeltaWriterError::StatsParsingFailed {
                    debug_value: v.to_string(),
                    logical_type: logical_type.cloned(),
                })?;
                if timestamp_type.is_adjusted_to_u_t_c {
                    Ok(Self::Timestamp(timestamp.naive_utc()))
                } else {
                    Ok(Self::TimestampNtz(timestamp.naive_utc()))
                }
            }
            (Statistics::Int64(v), Some(LogicalType::Decimal(decimal_type))) => Ok(Self::Decimal {
                unscaled: get_stat!(v).into(),
                precision: decimal_type.precision,
                scale: decimal_type.scale,
            }),
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
            (Statistics::FixedLenByteArray(v), Some(LogicalType::Decimal(decimal_type))) => {
                let val = if use_min {
                    v.min_bytes_opt()
                } else {
                    v.max_bytes_opt()
                }
                .unwrap_or_default();

                let val = if val.len() <= 16 {
                    i128::from_be_bytes(sign_extend_be(val))
                } else {
                    return Err(DeltaWriterError::StatsParsingFailed {
                        debug_value: format!("{val:?}"),
                        logical_type: Some(LogicalType::Decimal(DecimalType {
                            scale: decimal_type.scale,
                            precision: decimal_type.precision,
                        })),
                    });
                };

                Ok(Self::Decimal {
                    unscaled: val,
                    precision: decimal_type.precision,
                    scale: decimal_type.scale,
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

impl StatsScalar {
    /// Returns `None` when a decimal bound can't be written, see [`decimal_bound`].
    fn into_json(self, use_min: bool) -> Option<serde_json::Value> {
        let value = match self {
            StatsScalar::Boolean(v) => serde_json::Value::Bool(v),
            StatsScalar::Int32(v) => serde_json::Value::from(v),
            StatsScalar::Int64(v) => serde_json::Value::from(v),
            StatsScalar::Float32(v) => serde_json::Value::from(v),
            StatsScalar::Float64(v) => serde_json::Value::from(v),
            StatsScalar::Date(v) => serde_json::Value::from(v.format("%Y-%m-%d").to_string()),
            StatsScalar::Timestamp(v) => {
                // Timestamps must be truncated to millisecond precision when writing
                // to JSON for compatibility with the Delta Protocol.
                let format = if v.nanosecond() == 0 {
                    // Omit the sub-second component if it is zero
                    "%Y-%m-%dT%H:%M:%SZ"
                } else {
                    "%Y-%m-%dT%H:%M:%S%.3fZ"
                };
                serde_json::Value::from(v.format(format).to_string())
            }
            StatsScalar::TimestampNtz(v) => {
                let format = if v.nanosecond() == 0 {
                    "%Y-%m-%d %H:%M:%S"
                } else {
                    "%Y-%m-%d %H:%M:%S%.3f"
                };
                serde_json::Value::from(v.format(format).to_string())
            }
            StatsScalar::Decimal {
                unscaled,
                precision,
                scale,
            } => return decimal_bound(unscaled, precision, scale, use_min),
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
        };
        Some(value)
    }
}

/// Writes a decimal min (`use_min`) or max as a JSON number that is <= or >= the decimal.
///
/// Decimal stats are JSON numbers, which `serde_json` can only hold as `f64`. The nearest `f64`
/// can be written on the wrong side of the decimal and make readers skip a file that has
/// matching rows, so then the next `f64` outward is used instead. Returns `None` when the bound
/// would fall outside the decimal type, since readers can't parse it there and a missing bound
/// is always safe.
fn decimal_bound(
    unscaled: i128,
    precision: i32,
    scale: i32,
    use_min: bool,
) -> Option<serde_json::Value> {
    if scale == 0
        && let Some(exact) = serde_json::Number::from_i128(unscaled)
    {
        return Some(exact.into());
    }
    let nearest: f64 = format!("{unscaled}e{}", -scale).parse().ok()?;
    if !nearest.is_finite() {
        return None;
    }
    let (outward, wrong_side) = if use_min {
        (nearest.next_down(), Ordering::Greater)
    } else {
        (nearest.next_up(), Ordering::Less)
    };
    let largest = 10_i128
        .checked_pow(u32::try_from(precision).ok()?)
        .map(|p| p - 1);
    let type_limit = largest.map(|l| if use_min { -l } else { l });
    [nearest, outward]
        .into_iter()
        .map(serde_json::Value::from)
        .find(|json| {
            split_json_number(&json.to_string()).is_some_and(|(digits, exponent)| {
                compare_scaled(digits, exponent, unscaled, -scale) != wrong_side
                    && type_limit.is_none_or(|limit| {
                        compare_scaled(digits, exponent, limit, -scale) != wrong_side.reverse()
                    })
            })
        })
}

/// Splits JSON number text into digits and a power of ten, e.g. `-1.25e-7` into `(-125, -9)`.
fn split_json_number(text: &str) -> Option<(i128, i32)> {
    let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{integer}{fraction}").parse().ok()?;
    let exponent = exponent.parse::<i32>().ok()? - i32::try_from(fraction.len()).ok()?;
    Some((digits, exponent))
}

/// Compares `a * 10^a_exponent` with `b * 10^b_exponent` exactly.
fn compare_scaled(a: i128, a_exponent: i32, b: i128, b_exponent: i32) -> Ordering {
    let shift = |value: i128, by: i32| {
        10_i128
            .checked_pow(by.unsigned_abs())
            .and_then(|p| value.checked_mul(p))
    };
    // A shift that overflows makes that side larger in magnitude than any i128, so its sign wins
    if a_exponent >= b_exponent {
        shift(a, a_exponent - b_exponent).map_or(a.cmp(&0), |a| a.cmp(&b))
    } else {
        shift(b, b_exponent - a_exponent).map_or(0.cmp(&b), |b| a.cmp(&b))
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

            if let Some(min) = statistics.min.and_then(|min| min.into_json(true)) {
                min_values.insert(key.clone(), ColumnValueStat::Value(min));
            }

            if let Some(max) = statistics.max.and_then(|max| max.into_json(false)) {
                max_values.insert(key.clone(), ColumnValueStat::Value(max));
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
    use futures::TryStreamExt;
    use parquet::basic::{Compression, IntType, TimestampType};
    use parquet::data_type::{ByteArray, FixedLenByteArray};
    use parquet::file::properties::WriterProperties;
    use parquet::file::statistics::ValueStatistics;
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
                Some(LogicalType::Integer(IntType {
                    bit_width: 1,
                    is_signed: true,
                })),
                Value::Bool(true),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 1),
                Some(LogicalType::Integer(IntType {
                    bit_width: 32,
                    is_signed: true,
                })),
                Value::from(1),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 1234),
                Some(LogicalType::Decimal(DecimalType {
                    scale: 3,
                    precision: 4,
                })),
                Value::from(1.234),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 1234),
                Some(LogicalType::Decimal(DecimalType {
                    scale: -1,
                    precision: 4,
                })),
                Value::from(12340.0),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 1234),
                Some(LogicalType::Decimal(DecimalType {
                    scale: 0,
                    precision: 4,
                })),
                Value::from(1234),
            ),
            (
                simple_parquet_stat!(Statistics::Int32, 10561),
                Some(LogicalType::Date),
                Value::from("1998-12-01"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789723456),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::basic::TimeUnit::NANOS,
                })),
                // Should be truncated to millisecond precision
                Value::from("2022-01-01T12:34:56.789Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789723456),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: false, // Maps to TimestampNanosNtz
                    unit: parquet::basic::TimeUnit::NANOS,
                })),
                // Should be truncated to millisecond precision
                Value::from("2022-01-01 12:34:56.789"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789723),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::basic::TimeUnit::MICROS,
                })),
                // Should be truncated to millisecond precision
                Value::from("2022-01-01T12:34:56.789Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789723),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: false, // Maps to TimestampNtz
                    unit: parquet::basic::TimeUnit::MICROS,
                })),
                // Should be truncated to millisecond precision
                Value::from("2022-01-01 12:34:56.789"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::basic::TimeUnit::MILLIS,
                })),
                Value::from("2022-01-01T12:34:56.789Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: false, // Maps to TimestampNtz
                    unit: parquet::basic::TimeUnit::MILLIS,
                })),
                Value::from("2022-01-01 12:34:56.789"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496000000000),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::basic::TimeUnit::NANOS,
                })),
                // No sub-second component
                Value::from("2022-01-01T12:34:56Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496000000000),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: false, // Maps to TimestampNanosNtz
                    unit: parquet::basic::TimeUnit::NANOS,
                })),
                // No sub-second component
                Value::from("2022-01-01 12:34:56"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496000000500),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::basic::TimeUnit::NANOS,
                })),
                // Sub-second component truncated to zero milliseconds
                Value::from("2022-01-01T12:34:56.000Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496000000500),
                Some(LogicalType::Timestamp(TimestampType {
                    is_adjusted_to_u_t_c: false, // Maps to TimestampNanosNtz
                    unit: parquet::basic::TimeUnit::NANOS,
                })),
                // Sub-second component truncated to zero milliseconds
                Value::from("2022-01-01 12:34:56.000"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1234),
                Some(LogicalType::Decimal(DecimalType {
                    scale: 3,
                    precision: 4,
                })),
                Value::from(1.234),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1234),
                Some(LogicalType::Decimal(DecimalType {
                    scale: -1,
                    precision: 4,
                })),
                Value::from(12340.0),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1234),
                Some(LogicalType::Decimal(DecimalType {
                    scale: 0,
                    precision: 4,
                })),
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
                Some(LogicalType::Decimal(DecimalType {
                    scale: 3,
                    precision: 16,
                })),
                Value::from(1243124142314.423),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(vec![0, 39, 16])
                ),
                Some(LogicalType::Decimal(DecimalType {
                    scale: 3,
                    precision: 5,
                })),
                Value::from(10.0),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(1234i128.to_be_bytes().to_vec())
                ),
                Some(LogicalType::Decimal(DecimalType {
                    scale: 0,
                    precision: 4,
                })),
                Value::from(1234),
            ),
            (
                simple_parquet_stat!(
                    Statistics::FixedLenByteArray,
                    FixedLenByteArray::from(vec![
                        75, 59, 76, 168, 90, 134, 196, 122, 9, 138, 34, 63, 255, 255, 255, 255
                    ])
                ),
                Some(LogicalType::Decimal(DecimalType {
                    scale: 6,
                    precision: 38,
                })),
                Value::from(9.999999999999999e31),
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
            let actual = scalar.into_json(true).unwrap();
            assert_eq!(&actual, expected);
        }
    }

    #[test]
    fn test_stats_scalar_pre_epoch_nanos_timestamp() {
        let logical_type = Some(LogicalType::Timestamp(TimestampType {
            is_adjusted_to_u_t_c: true,
            unit: parquet::basic::TimeUnit::NANOS,
        }));
        let cases = &[
            // Exact whole seconds before the epoch already converted fine
            (-1_000_000_000, "1969-12-31T23:59:59Z"),
            (-2_000_000_000, "1969-12-31T23:59:58Z"),
            // A sub-second component used to fail with StatsParsingFailed,
            // which silently dropped the bound from the file statistics
            (-1_500_000_000, "1969-12-31T23:59:58.500Z"),
            (-500_000_000, "1969-12-31T23:59:59.500Z"),
            // Serialized truncated to millisecond precision, like positive values
            (-1, "1969-12-31T23:59:59.999Z"),
        ];
        for (value, expected) in cases {
            // the same value must convert as a minimum and as a maximum
            for use_min in [true, false] {
                let stats = simple_parquet_stat!(Statistics::Int64, *value);
                let scalar =
                    StatsScalar::try_from_stats(&stats, logical_type.as_ref(), use_min).unwrap();
                let actual = serde_json::Value::from(scalar);
                assert_eq!(
                    actual,
                    Value::from(*expected),
                    "value: {value}, use_min: {use_min}"
                );
            }
        }
    }

    #[test]
    fn test_int64_decimal_min_stat_respects_precision() {
        // A decimal(18, 0) column whose true minimum is the largest value the
        // precision allows. Converting to f64 rounds 999999999999999999 up to
        // 1e18, which serializes to a 19 digit minValue that is greater than the
        // true minimum and would let a reader skip a file that does match.
        let stats = simple_parquet_stat!(Statistics::Int64, 999999999999999999i64);
        let logical_type = LogicalType::Decimal(DecimalType {
            scale: 0,
            precision: 18,
        });

        let scalar = StatsScalar::try_from_stats(&stats, Some(&logical_type), true).unwrap();
        let min_value = scalar.into_json(true).unwrap();
        let min = min_value.as_i64().unwrap();

        assert!(
            min <= 999999999999999999,
            "minValue {min} is greater than the true minimum 999999999999999999"
        );
    }

    #[test]
    fn test_int64_decimal_zero_scale_bounds_are_exact() {
        let logical_type = LogicalType::Decimal(DecimalType {
            scale: 0,
            precision: 18,
        });
        for value in [
            -999_999_999_999_999_999_i64,
            -9_007_199_254_740_993,
            -9_007_199_254_740_991,
            -1234,
            0,
            1234,
            9_007_199_254_740_991,
            9_007_199_254_740_993,
            999_999_999_999_999_999,
        ] {
            let stats = simple_parquet_stat!(Statistics::Int64, value);
            for use_min in [true, false] {
                let scalar =
                    StatsScalar::try_from_stats(&stats, Some(&logical_type), use_min).unwrap();
                assert_eq!(
                    scalar.into_json(use_min).unwrap(),
                    serde_json::Value::from(value),
                    "value={value}, use_min={use_min}"
                );
            }
        }
    }

    #[test]
    fn test_int64_decimal_nonzero_scale_stats() {
        for (scale, expected) in [(3, json!(1.234)), (-1, json!(12340.0))] {
            let logical_type = LogicalType::Decimal(DecimalType {
                scale,
                precision: 4,
            });
            let stats = simple_parquet_stat!(Statistics::Int64, 1234);
            for use_min in [true, false] {
                let scalar =
                    StatsScalar::try_from_stats(&stats, Some(&logical_type), use_min).unwrap();
                assert_eq!(scalar.into_json(use_min).unwrap(), expected);
            }
        }
    }

    fn parse_decimal_stat(stat: &str, precision: u8, scale: i8) -> i128 {
        arrow_cast::parse::parse_decimal::<arrow::datatypes::Decimal128Type>(stat, precision, scale)
            .unwrap()
    }

    fn decimal_stat(unscaled: i128, precision: u8, scale: i8, use_min: bool) -> Option<Value> {
        let stats = match i64::try_from(unscaled) {
            Ok(unscaled) if precision <= 18 => simple_parquet_stat!(Statistics::Int64, unscaled),
            _ => simple_parquet_stat!(
                Statistics::FixedLenByteArray,
                FixedLenByteArray::from(unscaled.to_be_bytes().to_vec())
            ),
        };
        let logical_type = LogicalType::Decimal(DecimalType {
            scale: scale.into(),
            precision: precision.into(),
        });
        StatsScalar::try_from_stats(&stats, Some(&logical_type), use_min)
            .unwrap()
            .into_json(use_min)
    }

    #[test]
    fn test_decimal_stats_contain_value() {
        let cases: [(i128, u8, i8); 16] = [
            (1_123_456_789_012_345_678, 38, 18),
            (-1_123_456_789_012_345_678, 38, 18),
            (1_123_456_789_012_345_560, 38, 18),
            (-1_123_456_789_012_345_560, 38, 18),
            (2_500_000_000_000_000_001, 38, 18),
            (123_456_789_012_345_678, 18, 6),
            (-999_999_999_999_999_999, 18, 6),
            (99_999_999_999_999_999, 38, 0),
            (10_i128.pow(19), 38, 0),
            (-(10_i128.pow(19)) - 1, 38, 0),
            (1, 38, 18),
            (10_i128.pow(38) - 1, 38, 6),
            (1 - 10_i128.pow(38), 38, 6),
            (999_999_999_999_999_999, 18, 2),
            (-999_999_999_999_999_999, 18, 2),
            (999_999_999_999_999_873, 18, 2),
        ];
        for (unscaled, precision, scale) in cases {
            let min = decimal_stat(unscaled, precision, scale, true);
            let max = decimal_stat(unscaled, precision, scale, false);
            assert!(
                min.is_some() || max.is_some(),
                "{unscaled} as decimal({precision},{scale}) has no stats"
            );
            if let Some(min) = min {
                assert!(
                    parse_decimal_stat(&min.to_string(), precision, scale) <= unscaled,
                    "min {min} is above {unscaled} as decimal({precision},{scale})"
                );
            }
            if let Some(max) = max {
                assert!(
                    parse_decimal_stat(&max.to_string(), precision, scale) >= unscaled,
                    "max {max} is below {unscaled} as decimal({precision},{scale})"
                );
            }
        }
    }

    #[test]
    fn test_decimal_stats_keep_values_that_serialize_exactly() {
        for (unscaled, precision, scale, expected) in [
            (1, 1, 1, json!(0.1)),
            (-1, 1, 1, json!(-0.1)),
            (1050, 4, 2, json!(10.5)),
            (29, 2, 2, json!(0.29)),
            (1_000_000_000_000_000_000, 38, 18, json!(1.0)),
            (123_456_789_012_345, 38, 3, json!(123456789012.345)),
        ] {
            for use_min in [true, false] {
                assert_eq!(
                    decimal_stat(unscaled, precision, scale, use_min),
                    Some(expected.clone()),
                    "{unscaled} as decimal({precision},{scale}), use_min={use_min}"
                );
            }
        }
    }

    #[test]
    fn test_decimal_stats_omit_bounds_outside_the_type() {
        let largest = 10_i128.pow(38) - 1;
        for (unscaled, precision, scale, use_min) in [
            (largest, 38, 18, false),
            (-largest, 38, 18, true),
            (largest, 38, 6, false),
            (-largest, 38, 6, true),
            (largest, 38, 0, false),
            (-largest, 38, 0, true),
            (999_999_999_999_999_999, 18, 2, false),
            (-999_999_999_999_999_999, 18, 2, true),
            (999_999_999_999_999_873, 18, 2, false),
        ] {
            assert_eq!(
                decimal_stat(unscaled, precision, scale, use_min),
                None,
                "{unscaled} as decimal({precision},{scale}), use_min={use_min}"
            );
        }
    }

    #[tokio::test]
    async fn test_int64_decimal_zero_scale_add_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let schema = json!({
            "type": "struct",
            "fields": [
                { "name": "amount", "type": "decimal(18,0)", "nullable": true, "metadata": {} }
            ]
        });
        create_temp_table_with_schema(temp_dir.path(), &schema);
        let table_uri = Url::from_directory_path(temp_dir.path()).unwrap();
        let table = load_table(&table_uri, HashMap::new()).await.unwrap();
        let mut writer = RecordBatchWriter::for_table(&table)
            .unwrap()
            .with_writer_properties(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1))
                    .build(),
            );
        let values = arrow_array::Decimal128Array::from(vec![
            9_007_199_254_740_993,
            -999_999_999_999_999_999,
            999_999_999_999_999_999,
            -9_007_199_254_740_993,
        ])
        .with_precision_and_scale(18, 0)
        .unwrap();
        let batch =
            arrow_array::RecordBatch::try_new(writer.arrow_schema(), vec![Arc::new(values)])
                .unwrap();
        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 1);
        let stats = adds[0].get_stats().unwrap().unwrap();
        assert_eq!(
            stats.min_values["amount"].as_value().unwrap().as_i64(),
            Some(-999_999_999_999_999_999)
        );
        assert_eq!(
            stats.max_values["amount"].as_value().unwrap().as_i64(),
            Some(999_999_999_999_999_999)
        );
    }

    #[tokio::test]
    async fn test_int64_decimal_add_stats_at_the_top_of_the_type() {
        for values in [
            vec![999_999_999_999_999_999],
            vec![999_999_999_999_999_873, 1],
        ] {
            let temp_dir = tempfile::tempdir().unwrap();
            let schema = json!({
                "type": "struct",
                "fields": [
                    { "name": "d", "type": "decimal(18,2)", "nullable": true, "metadata": {} }
                ]
            });
            create_temp_table_with_schema(temp_dir.path(), &schema);
            let table_uri = Url::from_directory_path(temp_dir.path()).unwrap();
            let mut table = load_table(&table_uri, HashMap::new()).await.unwrap();
            let mut writer = RecordBatchWriter::for_table(&table).unwrap();
            let min = *values.iter().min().unwrap();
            let max = *values.iter().max().unwrap();
            let array = arrow_array::Decimal128Array::from(values)
                .with_precision_and_scale(18, 2)
                .unwrap();
            let batch =
                arrow_array::RecordBatch::try_new(writer.arrow_schema(), vec![Arc::new(array)])
                    .unwrap();
            writer.write(batch).await.unwrap();
            writer.flush_and_commit(&mut table).await.unwrap();

            let table = load_table(&table_uri, HashMap::new()).await.unwrap();
            let adds: Vec<_> = table
                .get_active_add_actions_by_partitions(&[])
                .try_collect()
                .await
                .unwrap();
            assert_eq!(adds.len(), 1);
            let read_stat = |stat: Option<Scalar>| match stat {
                Some(Scalar::Struct(data)) => match &data.values()[0] {
                    Scalar::Decimal(decimal) => Some(decimal.bits()),
                    Scalar::Null(_) => None,
                    other => panic!("unexpected stat {other:?}"),
                },
                other => panic!("unexpected stats {other:?}"),
            };
            let min_stat = read_stat(adds[0].min_values());
            assert!(
                min_stat.is_some_and(|stat| stat <= min),
                "minValues {min_stat:?} doesn't bound {min}"
            );
            let max_stat = read_stat(adds[0].max_values());
            assert!(
                max_stat.is_none_or(|stat| stat >= max),
                "maxValues {max_stat:?} is below {max}"
            );
        }
    }

    #[tokio::test]
    async fn test_decimal128_add_stats_contain_values() {
        let temp_dir = tempfile::tempdir().unwrap();
        let schema = json!({
            "type": "struct",
            "fields": [
                { "name": "amount", "type": "decimal(38,18)", "nullable": true, "metadata": {} }
            ]
        });
        create_temp_table_with_schema(temp_dir.path(), &schema);
        let table_uri = Url::from_directory_path(temp_dir.path()).unwrap();
        let table = load_table(&table_uri, HashMap::new()).await.unwrap();
        let mut writer = RecordBatchWriter::for_table(&table)
            .unwrap()
            .with_writer_properties(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1))
                    .build(),
            );
        let min = 1_123_456_789_012_345_678;
        let max = 2_500_000_000_000_000_001;
        let values = arrow_array::Decimal128Array::from(vec![
            2_000_000_000_000_000_000,
            min,
            max,
            1_500_000_000_000_000_000,
        ])
        .with_precision_and_scale(38, 18)
        .unwrap();
        let batch =
            arrow_array::RecordBatch::try_new(writer.arrow_schema(), vec![Arc::new(values)])
                .unwrap();
        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();
        assert_eq!(adds.len(), 1);
        // serde_json parses floats lossily by default, so read the stats as written
        let stats = adds[0].stats.as_deref().unwrap();
        let stat_text = |key: &str| {
            let start = stats.find(&format!(r#""{key}":{{"amount":"#)).unwrap() + key.len() + 13;
            &stats[start..start + stats[start..].find('}').unwrap()]
        };
        let min_stat = parse_decimal_stat(stat_text("minValues"), 38, 18);
        let max_stat = parse_decimal_stat(stat_text("maxValues"), 38, 18);
        assert!(min_stat <= min, "minValues {min_stat} is above {min}");
        assert!(max_stat >= max, "maxValues {max_stat} is below {max}");
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
