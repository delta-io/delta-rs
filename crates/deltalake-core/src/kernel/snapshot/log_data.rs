use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, MapArray, RecordBatch, StringArray, StructArray};
use chrono::{NaiveDateTime, TimeZone, Utc};
use object_store::path::Path;
use object_store::ObjectMeta;
use percent_encoding::percent_decode_str;

use super::extract::extract_and_cast;
use crate::kernel::{DataType, Metadata, Scalar, StructField, StructType};
use crate::{DeltaResult, DeltaTableError};

const COL_NUM_RECORDS: &str = "numRecords";
const COL_MIN_VALUES: &str = "minValues";
const COL_MAX_VALUES: &str = "maxValues";
const COL_NULL_COUNT: &str = "nullCount";

pub(crate) type PartitionFields<'a> = Arc<BTreeMap<&'a str, &'a StructField>>;
pub(crate) type PartitionValues<'a> = BTreeMap<&'a str, Scalar>;

pub(crate) trait PartitionsExt {
    fn hive_partition_path(&self) -> String;
}

impl PartitionsExt for BTreeMap<&str, Scalar> {
    fn hive_partition_path(&self) -> String {
        let mut fields = self
            .iter()
            .map(|(k, v)| {
                let encoded = v.serialize_encoded();
                format!("{k}={encoded}")
            })
            .collect::<Vec<_>>();
        fields.reverse();
        fields.join("/")
    }
}

impl PartitionsExt for BTreeMap<String, Scalar> {
    fn hive_partition_path(&self) -> String {
        let mut fields = self
            .iter()
            .map(|(k, v)| {
                let encoded = v.serialize_encoded();
                format!("{k}={encoded}")
            })
            .collect::<Vec<_>>();
        fields.reverse();
        fields.join("/")
    }
}

impl<T: PartitionsExt> PartitionsExt for Arc<T> {
    fn hive_partition_path(&self) -> String {
        self.as_ref().hive_partition_path()
    }
}

/// A view into the log data for a single logical file.
#[derive(Debug, PartialEq)]
pub struct FileStats<'a> {
    path: &'a StringArray,
    size: &'a Int64Array,
    modification_time: &'a Int64Array,
    partition_values: &'a MapArray,
    partition_fields: PartitionFields<'a>,
    stats: &'a StructArray,
    index: usize,
}

impl FileStats<'_> {
    /// Path to the files storage location.
    pub fn path(&self) -> Cow<'_, str> {
        percent_decode_str(self.path.value(self.index))
            .decode_utf8()
            .unwrap()
    }

    /// an object store [`Path`] to the file.
    ///
    /// this tries to parse the file string and if that fails, it will return the string as is.
    // TODO assert consisent handling of the paths encoding when reading log data so this logic can be removed.
    pub fn object_store_path(&self) -> Path {
        let path = self.path();
        // Try to preserve percent encoding if possible
        match Path::parse(path.as_ref()) {
            Ok(path) => path,
            Err(_) => Path::from(path.as_ref()),
        }
    }

    /// File size stored on disk.
    pub fn size(&self) -> i64 {
        self.size.value(self.index)
    }

    /// Last modification time of the file.
    pub fn modification_time(&self) -> i64 {
        self.modification_time.value(self.index)
    }

    /// Datetime of the last modification time of the file.
    pub fn modification_datetime(&self) -> DeltaResult<chrono::DateTime<Utc>> {
        Ok(Utc.from_utc_datetime(
            &NaiveDateTime::from_timestamp_millis(self.modification_time()).ok_or(
                DeltaTableError::from(crate::protocol::ProtocolError::InvalidField(format!(
                    "invalid modification_time: {:?}",
                    self.modification_time()
                ))),
            )?,
        ))
    }

    /// The partition values for this logical file.
    // TODO make this fallible
    pub fn partition_values(&self) -> DeltaResult<PartitionValues<'_>> {
        if self.partition_fields.is_empty() {
            return Ok(BTreeMap::new());
        }
        let map_value = self.partition_values.value(self.index);
        let keys = map_value
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::Generic("()".into()))?;
        let values = map_value
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::Generic("()".into()))?;

        let values = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| {
                let (key, field) = self.partition_fields.get_key_value(k.unwrap()).unwrap();
                let field_type = match field.data_type() {
                    DataType::Primitive(p) => Ok(p),
                    _ => Err(DeltaTableError::Generic(
                        "nested partitioning values are not supported".to_string(),
                    )),
                }?;
                Ok((
                    *key,
                    v.map(|vv| field_type.parse_scalar(vv))
                        .transpose()?
                        .unwrap_or(Scalar::Null(field.data_type().clone())),
                ))
            })
            .collect::<DeltaResult<HashMap<_, _>>>()?;

        // NOTE: we recreate the map as a BTreeMap to ensure the order of the keys is consistently
        // the same as the order of partition fields.
        self.partition_fields
            .iter()
            .map(|(k, f)| {
                let val = values
                    .get(*k)
                    .cloned()
                    .unwrap_or(Scalar::Null(f.data_type.clone()));
                Ok((*k, val))
            })
            .collect::<DeltaResult<BTreeMap<_, _>>>()
    }

    /// The number of records stored in the data file.
    pub fn num_records(&self) -> Option<usize> {
        self.stats
            .column_by_name(COL_NUM_RECORDS)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(self.index) as usize)
    }

    /// Struct containing all available null counts for the columns in this file.
    pub fn null_counts(&self) -> Option<Scalar> {
        self.stats
            .column_by_name(COL_NULL_COUNT)
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }

    /// Struct containing all available min values for the columns in this file.
    pub fn min_values(&self) -> Option<Scalar> {
        self.stats
            .column_by_name(COL_MIN_VALUES)
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }

    /// Struct containing all available max values for the columns in this file.
    pub fn max_values(&self) -> Option<Scalar> {
        self.stats
            .column_by_name(COL_MAX_VALUES)
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }
}

impl<'a> TryFrom<&FileStats<'a>> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(file_stats: &FileStats<'a>) -> Result<Self, Self::Error> {
        let location = file_stats.object_store_path();
        let size = file_stats.size() as usize;
        let last_modified = file_stats.modification_datetime()?;
        Ok(ObjectMeta {
            location,
            size,
            last_modified,
            version: None,
            e_tag: None,
        })
    }
}

/// Helper for processing data from the materialized Delta log.
pub struct FileStatsAccessor<'a> {
    partition_fields: PartitionFields<'a>,
    paths: &'a StringArray,
    sizes: &'a Int64Array,
    modification_times: &'a Int64Array,
    stats: &'a StructArray,
    partition_values: &'a MapArray,
    length: usize,
    pointer: usize,
}

impl<'a> FileStatsAccessor<'a> {
    pub(crate) fn try_new(
        data: &'a RecordBatch,
        metadata: &'a Metadata,
        schema: &'a StructType,
    ) -> DeltaResult<Self> {
        let paths = extract_and_cast::<StringArray>(data, "add.path")?;
        let sizes = extract_and_cast::<Int64Array>(data, "add.size")?;
        let modification_times = extract_and_cast::<Int64Array>(data, "add.modificationTime")?;
        let stats = extract_and_cast::<StructArray>(data, "add.stats_parsed")?;
        let partition_values = extract_and_cast::<MapArray>(data, "add.partitionValues")?;
        let partition_fields = Arc::new(
            metadata
                .partition_columns
                .iter()
                .map(|c| Ok((c.as_str(), schema.field_with_name(c.as_str())?)))
                .collect::<DeltaResult<BTreeMap<_, _>>>()?,
        );
        Ok(Self {
            partition_fields,
            paths,
            sizes,
            modification_times,
            stats,
            partition_values,
            length: data.num_rows(),
            pointer: 0,
        })
    }

    pub(crate) fn get(&self, index: usize) -> DeltaResult<FileStats<'a>> {
        if index >= self.length {
            return Err(DeltaTableError::Generic(format!(
                "index out of bounds: {} >= {}",
                index, self.length
            )));
        }
        Ok(FileStats {
            path: self.paths,
            size: self.sizes,
            modification_time: self.modification_times,
            partition_values: self.partition_values,
            partition_fields: self.partition_fields.clone(),
            stats: self.stats,
            index,
        })
    }
}

impl<'a> Iterator for FileStatsAccessor<'a> {
    type Item = FileStats<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pointer >= self.length {
            return None;
        }
        // Safety: we know that the pointer is within bounds
        let file_stats = self.get(self.pointer).unwrap();
        self.pointer += 1;
        Some(file_stats)
    }
}

/// Provides semanitc access to the log data.
///
/// This is a helper struct that provides access to the log data in a more semantic way
/// to avid the necessiity of knowing the exact layout of the underlying log data.
pub struct LogDataHandler<'a> {
    data: &'a Vec<RecordBatch>,
    metadata: &'a Metadata,
    schema: &'a StructType,
}

impl<'a> LogDataHandler<'a> {
    pub(crate) fn new(
        data: &'a Vec<RecordBatch>,
        metadata: &'a Metadata,
        schema: &'a StructType,
    ) -> Self {
        Self {
            data,
            metadata,
            schema,
        }
    }
}

impl<'a> IntoIterator for LogDataHandler<'a> {
    type Item = FileStats<'a>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.data
                .iter()
                .flat_map(|data| {
                    FileStatsAccessor::try_new(data, self.metadata, self.schema).into_iter()
                })
                .flatten(),
        )
    }
}

#[cfg(feature = "datafusion")]
mod datafusion {
    use std::sync::Arc;

    use arrow_arith::aggregate::sum;
    use arrow_array::Int64Array;
    use arrow_schema::DataType as ArrowDataType;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
    use datafusion_expr::AggregateFunction;
    use datafusion_physical_expr::aggregate::AggregateExpr;
    use datafusion_physical_expr::expressions::{Column, Max, Min};

    use super::*;
    use crate::kernel::extract::{extract_and_cast_opt, extract_column};

    // TODO validate this works with "wide and narrow" boulds / stats

    impl FileStatsAccessor<'_> {
        fn collect_count(&self, name: &str) -> Precision<usize> {
            let num_records = extract_and_cast_opt::<Int64Array>(self.stats, name);
            if let Some(num_records) = num_records {
                if let Some(null_count_mulls) = num_records.nulls() {
                    if null_count_mulls.null_count() > 0 {
                        Precision::Absent
                    } else {
                        sum(num_records)
                            .map(|s| Precision::Exact(s as usize))
                            .unwrap_or(Precision::Absent)
                    }
                } else {
                    sum(num_records)
                        .map(|s| Precision::Exact(s as usize))
                        .unwrap_or(Precision::Absent)
                }
            } else {
                Precision::Absent
            }
        }

        fn column_bounds(
            &self,
            path_step: &str,
            name: &str,
            fun: &AggregateFunction,
        ) -> Precision<ScalarValue> {
            let mut path = name.split('.');
            let array = if let Ok(array) = extract_column(self.stats, path_step, &mut path) {
                array
            } else {
                return Precision::Absent;
            };

            if array.data_type().is_primitive() {
                let agg: Box<dyn AggregateExpr> = match fun {
                    AggregateFunction::Min => Box::new(Min::new(
                        // NOTE: this is just a placeholder, we never evalutae this expression
                        Arc::new(Column::new(name, 0)),
                        name,
                        array.data_type().clone(),
                    )),
                    AggregateFunction::Max => Box::new(Max::new(
                        // NOTE: this is just a placeholder, we never evalutae this expression
                        Arc::new(Column::new(name, 0)),
                        name,
                        array.data_type().clone(),
                    )),
                    _ => return Precision::Absent,
                };
                let mut accum = agg.create_accumulator().ok().unwrap();
                return accum
                    .update_batch(&[array.clone()])
                    .ok()
                    .and_then(|_| accum.evaluate().ok())
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent);
            }

            match array.data_type() {
                ArrowDataType::Struct(fields) => {
                    return fields
                        .iter()
                        .map(|f| {
                            self.column_bounds(path_step, &format!("{name}.{}", f.name()), fun)
                        })
                        .map(|s| match s {
                            Precision::Exact(s) => Some(s),
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>()
                        .map(|o| Precision::Exact(ScalarValue::Struct(Some(o), fields.clone())))
                        .unwrap_or(Precision::Absent);
                }
                _ => Precision::Absent,
            }
        }

        fn num_records(&self) -> Precision<usize> {
            self.collect_count(COL_NUM_RECORDS)
        }

        fn total_size_files(&self) -> Precision<usize> {
            let size = self
                .sizes
                .iter()
                .flat_map(|s| s.map(|s| s as usize))
                .sum::<usize>();
            Precision::Inexact(size)
        }

        fn column_stats(&self, name: impl AsRef<str>) -> DeltaResult<ColumnStatistics> {
            let null_count_col = format!("{COL_NULL_COUNT}.{}", name.as_ref());
            let null_count = self.collect_count(&null_count_col);

            let min_value =
                self.column_bounds(COL_MIN_VALUES, name.as_ref(), &AggregateFunction::Min);
            let min_value = match &min_value {
                Precision::Exact(value) if value.is_null() => Precision::Absent,
                // TODO this is a hack, we should not be casting here but rather when we read the checkpoint data.
                // it seems sometimes the min/max values are stored as nanoseconds and sometimes as microseconds?
                Precision::Exact(ScalarValue::TimestampNanosecond(a, b)) => Precision::Exact(
                    ScalarValue::TimestampMicrosecond(a.map(|v| v / 1000), b.clone()),
                ),
                _ => min_value,
            };

            let max_value =
                self.column_bounds(COL_MAX_VALUES, name.as_ref(), &AggregateFunction::Max);
            let max_value = match &max_value {
                Precision::Exact(value) if value.is_null() => Precision::Absent,
                Precision::Exact(ScalarValue::TimestampNanosecond(a, b)) => Precision::Exact(
                    ScalarValue::TimestampMicrosecond(a.map(|v| v / 1000), b.clone()),
                ),
                _ => max_value,
            };

            Ok(ColumnStatistics {
                null_count,
                max_value,
                min_value,
                distinct_count: Precision::Absent,
            })
        }
    }

    trait StatsExt {
        fn add(&self, other: &Self) -> Self;
    }

    impl StatsExt for ColumnStatistics {
        fn add(&self, other: &Self) -> Self {
            Self {
                null_count: self.null_count.add(&other.null_count),
                max_value: self.max_value.max(&other.max_value),
                min_value: self.min_value.min(&other.min_value),
                distinct_count: self.distinct_count.add(&other.distinct_count),
            }
        }
    }

    impl LogDataHandler<'_> {
        fn num_records(&self) -> Precision<usize> {
            self.data
                .iter()
                .flat_map(|b| {
                    FileStatsAccessor::try_new(b, self.metadata, self.schema)
                        .map(|a| a.num_records())
                })
                .reduce(|acc, num_records| acc.add(&num_records))
                .unwrap_or(Precision::Absent)
        }

        fn total_size_files(&self) -> Precision<usize> {
            self.data
                .iter()
                .flat_map(|b| {
                    FileStatsAccessor::try_new(b, self.metadata, self.schema)
                        .map(|a| a.total_size_files())
                })
                .reduce(|acc, size| acc.add(&size))
                .unwrap_or(Precision::Absent)
        }

        pub(crate) fn column_stats(&self, name: impl AsRef<str>) -> Option<ColumnStatistics> {
            self.data
                .iter()
                .flat_map(|b| {
                    FileStatsAccessor::try_new(b, self.metadata, self.schema)
                        .map(|a| a.column_stats(name.as_ref()))
                })
                .collect::<Result<Vec<_>, _>>()
                .ok()?
                .iter()
                .fold(None::<ColumnStatistics>, |acc, stats| match (acc, stats) {
                    (None, stats) => Some(stats.clone()),
                    (Some(acc), stats) => Some(acc.add(stats)),
                })
        }

        pub(crate) fn statistics(&self) -> Option<Statistics> {
            let num_rows = self.num_records();
            let total_byte_size = self.total_size_files();
            let column_statistics = self
                .schema
                .fields()
                .iter()
                .map(|f| self.column_stats(f.name()))
                .collect::<Option<Vec<_>>>()?;
            Some(Statistics {
                num_rows,
                total_byte_size,
                column_statistics,
            })
        }
    }
}

#[cfg(all(test, feature = "datafusion"))]
mod tests {

    #[tokio::test]
    async fn read_delta_1_2_1_struct_stats_table() {
        let table_uri = "../deltalake-test/tests/data/delta-1.2.1-only-struct-stats";
        let table_from_struct_stats = crate::open_table(table_uri).await.unwrap();
        let table_from_json_stats = crate::open_table_with_version(table_uri, 1).await.unwrap();

        let json_action = table_from_json_stats
            .snapshot()
            .unwrap()
            .snapshot
            .file_stats()
            .find(|f| {
                f.path().ends_with(
                    "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
                )
            })
            .unwrap();

        let struct_action = table_from_struct_stats
            .snapshot()
            .unwrap()
            .snapshot
            .file_stats()
            .find(|f| {
                f.path().ends_with(
                    "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
                )
            })
            .unwrap();

        assert_eq!(json_action.path(), struct_action.path());
        assert_eq!(
            json_action.partition_values().unwrap(),
            struct_action.partition_values().unwrap()
        );
        // assert_eq!(
        //     json_action.max_values().unwrap(),
        //     struct_action.max_values().unwrap()
        // );
        // assert_eq!(
        //     json_action.min_values().unwrap(),
        //     struct_action.min_values().unwrap()
        // );
    }

    #[tokio::test]
    async fn df_stats_delta_1_2_1_struct_stats_table() {
        let table_uri = "../deltalake-test/tests/data/delta-1.2.1-only-struct-stats";
        let table_from_struct_stats = crate::open_table(table_uri).await.unwrap();

        let file_stats = table_from_struct_stats
            .snapshot()
            .unwrap()
            .snapshot
            .log_data();

        let col_stats = file_stats.statistics();
        println!("{:?}", col_stats);
    }
}
