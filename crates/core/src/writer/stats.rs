use std::cmp::min;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, ops::AddAssign};

use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use itertools::Itertools;
use parquet::file::metadata::ParquetMetaData;
use parquet::format::FileMetaData;
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};
use parquet::{basic::LogicalType, errors::ParquetError};
use parquet::{
    file::{metadata::RowGroupMetaData, statistics::Statistics},
    format::TimeUnit,
};

use super::*;
use crate::kernel::{scalars::ScalarExt, Add};
use crate::protocol::{ColumnValueStat, Stats};

/// Creates an [`Add`] log action struct.
pub fn create_add(
    partition_values: &IndexMap<String, Scalar>,
    path: String,
    size: i64,
    file_metadata: &FileMetaData,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<String>>,
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
        stats_parsed: None,
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
    num_indexed_cols: i32,
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
    file_metadata: &FileMetaData,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<String>>,
) -> Result<Stats, DeltaWriterError> {
    let type_ptr = parquet::schema::types::from_thrift(file_metadata.schema.as_slice());
    let schema_descriptor = type_ptr.map(|type_| Arc::new(SchemaDescriptor::new(type_)))?;

    let row_group_metadata: Vec<RowGroupMetaData> = file_metadata
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_descriptor.clone(), rg.clone()))
        .collect::<Result<Vec<RowGroupMetaData>, ParquetError>>()?;

    stats_from_metadata(
        partition_values,
        schema_descriptor,
        row_group_metadata,
        file_metadata.num_rows,
        num_indexed_cols,
        stats_columns,
    )
}

fn stats_from_metadata(
    partition_values: &IndexMap<String, Scalar>,
    schema_descriptor: Arc<SchemaDescriptor>,
    row_group_metadata: Vec<RowGroupMetaData>,
    num_rows: i64,
    num_indexed_cols: i32,
    stats_columns: &Option<Vec<String>>,
) -> Result<Stats, DeltaWriterError> {
    let mut min_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut max_values: HashMap<String, ColumnValueStat> = HashMap::new();
    let mut null_count: HashMap<String, ColumnCountStat> = HashMap::new();
    let dialect = sqlparser::dialect::GenericDialect {};

    let idx_to_iterate = if let Some(stats_cols) = stats_columns {
        let stats_cols = stats_cols
            .into_iter()
            .map(|v| {
                match sqlparser::parser::Parser::new(&dialect)
                    .try_with_sql(v)
                    .map_err(|e| DeltaTableError::generic(e.to_string()))?
                    .parse_multipart_identifier()
                {
                    Ok(parts) => Ok(parts.into_iter().map(|v| v.value).join(".")),
                    Err(e) => {
                        return Err(DeltaWriterError::DeltaTable(
                            DeltaTableError::GenericError {
                                source: Box::new(e),
                            },
                        ))
                    }
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
    } else if num_indexed_cols == -1 {
        (0..schema_descriptor.num_columns()).collect::<Vec<_>>()
    } else if num_indexed_cols >= 0 {
        (0..min(num_indexed_cols as usize, schema_descriptor.num_columns())).collect::<Vec<_>>()
    } else {
        return Err(DeltaWriterError::DeltaTable(DeltaTableError::Generic(
            "delta.dataSkippingNumIndexedCols valid values are >=-1".to_string(),
        )));
    };

    for idx in idx_to_iterate {
        let column_descr = schema_descriptor.column(idx);

        let column_path = column_descr.path();
        let column_path_parts = column_path.parts();

        // Do not include partition columns in statistics
        if partition_values.contains_key(&column_path_parts[0]) {
            continue;
        }

        let maybe_stats: Option<AggregatedStats> = row_group_metadata
            .iter()
            .map(|g| {
                g.column(idx)
                    .statistics()
                    .map(|s| AggregatedStats::from((s, &column_descr.logical_type())))
            })
            .reduce(|left, right| match (left, right) {
                (Some(mut left), Some(right)) => {
                    left += right;
                    Some(left)
                }
                _ => None,
            })
            .flatten();

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
    // We are serializing to f64 later and the ordering should be the same
    Decimal(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
}

impl StatsScalar {
    fn try_from_stats(
        stats: &Statistics,
        logical_type: &Option<LogicalType>,
        use_min: bool,
    ) -> Result<Self, DeltaWriterError> {
        macro_rules! get_stat {
            ($val: expr) => {
                if use_min {
                    *$val.min()
                } else {
                    *$val.max()
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
                Ok(Self::Decimal(val))
            }
            (Statistics::Int32(v), _) => Ok(Self::Int32(get_stat!(v))),
            // Int64 can be timestamp, decimal, or integer
            (Statistics::Int64(v), Some(LogicalType::Timestamp { unit, .. })) => {
                // For now, we assume timestamps are adjusted to UTC. Non-UTC timestamps
                // are behind a feature gate in Delta:
                // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#timestamp-without-timezone-timestampntz
                let v = get_stat!(v);
                let timestamp = match unit {
                    TimeUnit::MILLIS(_) => chrono::DateTime::from_timestamp_millis(v),
                    TimeUnit::MICROS(_) => chrono::DateTime::from_timestamp_micros(v),
                    TimeUnit::NANOS(_) => {
                        let secs = v / 1_000_000_000;
                        let nanosecs = (v % 1_000_000_000) as u32;
                        chrono::DateTime::from_timestamp(secs, nanosecs)
                    }
                };
                let timestamp = timestamp.ok_or(DeltaWriterError::StatsParsingFailed {
                    debug_value: v.to_string(),
                    logical_type: logical_type.clone(),
                })?;
                Ok(Self::Timestamp(timestamp.naive_utc()))
            }
            (Statistics::Int64(v), Some(LogicalType::Decimal { scale, .. })) => {
                let val = get_stat!(v) as f64 / 10.0_f64.powi(*scale);
                // Spark serializes these as numbers
                Ok(Self::Decimal(val))
            }
            (Statistics::Int64(v), _) => Ok(Self::Int64(get_stat!(v))),
            (Statistics::Float(v), _) => Ok(Self::Float32(get_stat!(v))),
            (Statistics::Double(v), _) => Ok(Self::Float64(get_stat!(v))),
            (Statistics::ByteArray(v), logical_type) => {
                let bytes = if use_min {
                    v.min_bytes()
                } else {
                    v.max_bytes()
                };
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
                        logical_type: logical_type.clone(),
                    }),
                }
            }
            (Statistics::FixedLenByteArray(v), Some(LogicalType::Decimal { scale, precision })) => {
                let val = if use_min {
                    v.min_bytes()
                } else {
                    v.max_bytes()
                };

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

                let val = val / 10.0_f64.powi(*scale);
                Ok(Self::Decimal(val))
            }
            (Statistics::FixedLenByteArray(v), Some(LogicalType::Uuid)) => {
                let val = if use_min {
                    v.min_bytes()
                } else {
                    v.max_bytes()
                };

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
                logical_type: logical_type.clone(),
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
            StatsScalar::Decimal(v) => serde_json::Value::from(v),
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

impl From<(&Statistics, &Option<LogicalType>)> for AggregatedStats {
    fn from(value: (&Statistics, &Option<LogicalType>)) -> Self {
        let (stats, logical_type) = value;
        let null_count = stats.null_count();
        if stats.has_min_max_set() {
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

/// For a list field, we don't want the inner field names. We need to chuck out
/// the list and items fields from the path, but also need to handle the
/// peculiar case where the user named the list field "list" or "item".
///
/// For example:
///
/// * ["some_nested_list", "list", "item", "list", "item"] -> "some_nested_list"
/// * ["some_list", "list", "item"] -> "some_list"
/// * ["list", "list", "item"] -> "list"
/// * ["item", "list", "item"] -> "item"
fn get_list_field_name(column_descr: &Arc<ColumnDescriptor>) -> Option<String> {
    let max_rep_levels = column_descr.max_rep_level();
    let column_path_parts = column_descr.path().parts();

    // If there are more nested names, we can't handle them yet.
    if column_path_parts.len() > (2 * max_rep_levels + 1) as usize {
        return None;
    }

    let mut column_path_parts = column_path_parts.to_vec();
    let mut items_seen = 0;
    let mut lists_seen = 0;
    while let Some(part) = column_path_parts.pop() {
        match (part.as_str(), lists_seen, items_seen) {
            ("list", seen, _) if seen == max_rep_levels => return Some("list".to_string()),
            ("item", _, seen) if seen == max_rep_levels => return Some("item".to_string()),
            ("list", _, _) => lists_seen += 1,
            ("item", _, _) => items_seen += 1,
            (other, _, _) => return Some(other.to_string()),
        }
    }
    None
}

fn apply_min_max_for_column(
    statistics: AggregatedStats,
    column_descr: Arc<ColumnDescriptor>,
    column_path_parts: &[String],
    min_values: &mut HashMap<String, ColumnValueStat>,
    max_values: &mut HashMap<String, ColumnValueStat>,
    null_counts: &mut HashMap<String, ColumnCountStat>,
) -> Result<(), DeltaWriterError> {
    // Special handling for list column
    if column_descr.max_rep_level() > 0 {
        let key = get_list_field_name(&column_descr);

        if let Some(key) = key {
            null_counts.insert(key, ColumnCountStat::Value(statistics.null_count as i64));
        }

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

#[cfg(test)]
mod tests {
    use super::utils::record_batch_from_message;
    use super::*;
    use crate::{
        errors::DeltaTableError,
        protocol::{ColumnCountStat, ColumnValueStat},
        table::builder::DeltaTableBuilder,
        DeltaTable,
    };
    use lazy_static::lazy_static;
    use parquet::data_type::{ByteArray, FixedLenByteArray};
    use parquet::file::statistics::ValueStatistics;
    use parquet::{basic::Compression, file::properties::WriterProperties};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::path::Path;

    macro_rules! simple_parquet_stat {
        ($variant:expr, $value:expr) => {
            $variant(ValueStatistics::new(
                Some($value),
                Some($value),
                None,
                0,
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
                simple_parquet_stat!(Statistics::Int32, 10561),
                Some(LogicalType::Date),
                Value::from("1998-12-01"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789123456),
                Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::format::TimeUnit::NANOS(parquet::format::NanoSeconds {}),
                }),
                Value::from("2022-01-01T12:34:56.789123456Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789123),
                Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
                }),
                Value::from("2022-01-01T12:34:56.789123Z"),
            ),
            (
                simple_parquet_stat!(Statistics::Int64, 1641040496789),
                Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::format::TimeUnit::MILLIS(parquet::format::MilliSeconds {}),
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
            let scalar = StatsScalar::try_from_stats(stats, logical_type, true).unwrap();
            let actual = serde_json::Value::from(scalar);
            assert_eq!(&actual, expected);
        }
    }

    #[tokio::test]
    async fn test_delta_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = temp_dir.path();
        create_temp_table(table_path);

        let table = load_table(table_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap();

        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        writer = writer.with_writer_properties(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_max_row_group_size(128)
                .build(),
        );

        let arrow_schema = writer.arrow_schema();
        let batch = record_batch_from_message(arrow_schema, JSON_ROWS.clone().as_ref()).unwrap();

        writer.write(batch).await.unwrap();
        let add = writer.flush().await.unwrap();
        assert_eq!(add.len(), 1);
        let stats = add[0].get_stats().unwrap().unwrap();

        let min_max_keys = vec!["meta", "some_int", "some_string", "some_bool", "uuid"];
        let mut null_count_keys = vec!["some_list", "some_nested_list"];
        null_count_keys.extend_from_slice(min_max_keys.as_slice());

        assert_eq!(min_max_keys.len(), stats.min_values.len());
        assert_eq!(min_max_keys.len(), stats.max_values.len());
        assert_eq!(null_count_keys.len(), stats.null_count.len());

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
                _ => panic!("Key should not be present"),
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
                _ => panic!("Key should not be present"),
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
                ("some_list", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("some_nested_list", ColumnCountStat::Value(v)) => assert_eq!(100, *v),
                ("date", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                ("uuid", ColumnCountStat::Value(v)) => assert_eq!(0, *v),
                _ => panic!("Key should not be present"),
            }
        }
    }

    async fn load_table(
        table_uri: &str,
        options: HashMap<String, String>,
    ) -> Result<DeltaTable, DeltaTableError> {
        DeltaTableBuilder::from_uri(table_uri)
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

    lazy_static! {
        static ref SCHEMA: Value = json!({
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
        });
        static ref V0_COMMIT: String = {
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
        };
        static ref JSON_ROWS: Vec<Value> = {
            std::iter::repeat(json!({
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
            }))
            .take(100)
            .chain(
                std::iter::repeat(json!({
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
                }))
                .take(100),
            )
            .chain(
                std::iter::repeat(json!({
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
                }))
                .take(100),
            )
            .collect()
        };
    }
}
