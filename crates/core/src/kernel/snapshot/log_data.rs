use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, Int32Array, Int64Array, MapArray, RecordBatch, StringArray, StructArray, UInt64Array,
};
use chrono::{DateTime, Utc};
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::ObjectMeta;
use percent_encoding::percent_decode_str;

use super::super::scalars::ScalarExt;
use crate::kernel::arrow::extract::{extract_and_cast, extract_and_cast_opt};
use crate::kernel::{
    DataType, DeletionVectorDescriptor, Metadata, Remove, StructField, StructType,
};
use crate::{DeltaResult, DeltaTableError};

const COL_NUM_RECORDS: &str = "numRecords";
const COL_MIN_VALUES: &str = "minValues";
const COL_MAX_VALUES: &str = "maxValues";
const COL_NULL_COUNT: &str = "nullCount";

pub(crate) type PartitionFields<'a> = Arc<IndexMap<&'a str, &'a StructField>>;
pub(crate) type PartitionValues<'a> = IndexMap<&'a str, Scalar>;

pub(crate) trait PartitionsExt {
    fn hive_partition_path(&self) -> String;
}

impl PartitionsExt for IndexMap<&str, Scalar> {
    fn hive_partition_path(&self) -> String {
        let fields = self
            .iter()
            .map(|(k, v)| {
                let encoded = v.serialize_encoded();
                format!("{k}={encoded}")
            })
            .collect::<Vec<_>>();
        fields.join("/")
    }
}

impl PartitionsExt for IndexMap<String, Scalar> {
    fn hive_partition_path(&self) -> String {
        let fields = self
            .iter()
            .map(|(k, v)| {
                let encoded = v.serialize_encoded();
                format!("{k}={encoded}")
            })
            .collect::<Vec<_>>();
        fields.join("/")
    }
}

impl<T: PartitionsExt> PartitionsExt for Arc<T> {
    fn hive_partition_path(&self) -> String {
        self.as_ref().hive_partition_path()
    }
}

/// Defines a deletion vector
#[derive(Debug, PartialEq, Clone)]
pub struct DeletionVector<'a> {
    storage_type: &'a StringArray,
    path_or_inline_dv: &'a StringArray,
    size_in_bytes: &'a Int32Array,
    cardinality: &'a Int64Array,
    offset: Option<&'a Int32Array>,
}

/// View into a deletion vector data.
#[derive(Debug)]
pub struct DeletionVectorView<'a> {
    data: &'a DeletionVector<'a>,
    /// Pointer to a specific row in the log data.
    index: usize,
}

impl<'a> DeletionVectorView<'a> {
    /// get a unique idenitfier for the deletion vector
    pub fn unique_id(&self) -> String {
        if let Some(offset) = self.offset() {
            format!(
                "{}{}@{offset}",
                self.storage_type(),
                self.path_or_inline_dv()
            )
        } else {
            format!("{}{}", self.storage_type(), self.path_or_inline_dv())
        }
    }

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
        self.data.storage_type.value(self.index)
    }
    fn path_or_inline_dv(&self) -> &str {
        self.data.path_or_inline_dv.value(self.index)
    }
    fn size_in_bytes(&self) -> i32 {
        self.data.size_in_bytes.value(self.index)
    }
    fn cardinality(&self) -> i64 {
        self.data.cardinality.value(self.index)
    }
    fn offset(&self) -> Option<i32> {
        self.data
            .offset
            .and_then(|a| a.is_null(self.index).then(|| a.value(self.index)))
    }
}

/// A view into the log data representing a single logical file.
///
/// This struct holds a pointer to a specific row in the log data and provides access to the
/// information stored in that row by tracking references to the underlying arrays.
///
/// Additionally, references to some table metadata is tracked to provide higher level
/// functionality, e.g. parsing partition values.
#[derive(Debug, PartialEq)]
pub struct LogicalFile<'a> {
    path: &'a StringArray,
    /// The on-disk size of this data file in bytes
    size: &'a Int64Array,
    /// Last modification time of the file in milliseconds since the epoch.
    modification_time: &'a Int64Array,
    /// The partition values for this logical file.
    partition_values: &'a MapArray,
    /// Struct containing all available statistics for the columns in this file.
    stats: &'a StructArray,
    /// Array containing the deletion vector data.
    deletion_vector: Option<DeletionVector<'a>>,

    /// Pointer to a specific row in the log data.
    index: usize,
    /// Schema fields the table is partitioned by.
    partition_fields: PartitionFields<'a>,
}

impl LogicalFile<'_> {
    /// Path to the files storage location.
    pub fn path(&self) -> Cow<'_, str> {
        percent_decode_str(self.path.value(self.index)).decode_utf8_lossy()
    }

    /// An object store [`Path`] to the file.
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
        DateTime::from_timestamp_millis(self.modification_time()).ok_or(DeltaTableError::from(
            crate::protocol::ProtocolError::InvalidField(format!(
                "invalid modification_time: {:?}",
                self.modification_time()
            )),
        ))
    }

    /// The partition values for this logical file.
    pub fn partition_values(&self) -> DeltaResult<PartitionValues<'_>> {
        if self.partition_fields.is_empty() {
            return Ok(IndexMap::new());
        }
        let map_value = self.partition_values.value(self.index);
        let keys = map_value
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::generic(
                "expected partition values key field to be of type string",
            ))?;
        let values = map_value
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::generic(
                "expected partition values value field to be of type string",
            ))?;

        let values = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| {
                let (key, field) = self.partition_fields.get_key_value(k.unwrap()).unwrap();
                let field_type = match field.data_type() {
                    DataType::Primitive(p) => Ok(p),
                    _ => Err(DeltaTableError::generic(
                        "nested partitioning values are not supported",
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

        // NOTE: we recreate the map as a IndexMap to ensure the order of the keys is consistently
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
            .collect::<DeltaResult<IndexMap<_, _>>>()
    }

    /// Defines a deletion vector
    pub fn deletion_vector(&self) -> Option<DeletionVectorView<'_>> {
        self.deletion_vector.as_ref().and_then(|arr| {
            arr.storage_type
                .is_valid(self.index)
                .then_some(DeletionVectorView {
                    data: arr,
                    index: self.index,
                })
        })
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

    /// Create a remove action for this logical file.
    pub fn remove_action(&self, data_change: bool) -> Remove {
        Remove {
            // TODO use the raw (still encoded) path here once we reconciled serde ...
            path: self.path().to_string(),
            data_change,
            deletion_timestamp: Some(Utc::now().timestamp_millis()),
            extended_file_metadata: Some(true),
            size: Some(self.size()),
            partition_values: self.partition_values().ok().map(|pv| {
                pv.iter()
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            if v.is_null() {
                                None
                            } else {
                                Some(v.serialize())
                            },
                        )
                    })
                    .collect()
            }),
            deletion_vector: self.deletion_vector().map(|dv| dv.descriptor()),
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
        }
    }
}

impl<'a> TryFrom<&LogicalFile<'a>> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(file_stats: &LogicalFile<'a>) -> Result<Self, Self::Error> {
        Ok(ObjectMeta {
            location: file_stats.object_store_path(),
            size: file_stats.size() as usize,
            last_modified: file_stats.modification_datetime()?,
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
    deletion_vector: Option<DeletionVector<'a>>,
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
                .map(|c| {
                    Ok((
                        c.as_str(),
                        schema
                            .field(c.as_str())
                            .ok_or(DeltaTableError::PartitionError {
                                partition: c.clone(),
                            })?,
                    ))
                })
                .collect::<DeltaResult<IndexMap<_, _>>>()?,
        );
        let deletion_vector = extract_and_cast_opt::<StructArray>(data, "add.deletionVector");
        let deletion_vector = deletion_vector.and_then(|dv| {
            let storage_type = extract_and_cast::<StringArray>(dv, "storageType").ok()?;
            let path_or_inline_dv = extract_and_cast::<StringArray>(dv, "pathOrInlineDv").ok()?;
            let size_in_bytes = extract_and_cast::<Int32Array>(dv, "sizeInBytes").ok()?;
            let cardinality = extract_and_cast::<Int64Array>(dv, "cardinality").ok()?;
            let offset = extract_and_cast_opt::<Int32Array>(dv, "offset");
            Some(DeletionVector {
                storage_type,
                path_or_inline_dv,
                size_in_bytes,
                cardinality,
                offset,
            })
        });

        Ok(Self {
            partition_fields,
            paths,
            sizes,
            modification_times,
            stats,
            deletion_vector,
            partition_values,
            length: data.num_rows(),
            pointer: 0,
        })
    }

    pub(crate) fn get(&self, index: usize) -> DeltaResult<LogicalFile<'a>> {
        if index >= self.length {
            return Err(DeltaTableError::Generic(format!(
                "index out of bounds: {} >= {}",
                index, self.length
            )));
        }
        Ok(LogicalFile {
            path: self.paths,
            size: self.sizes,
            modification_time: self.modification_times,
            partition_values: self.partition_values,
            partition_fields: self.partition_fields.clone(),
            stats: self.stats,
            deletion_vector: self.deletion_vector.clone(),
            index,
        })
    }
}

impl<'a> Iterator for FileStatsAccessor<'a> {
    type Item = LogicalFile<'a>;

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
    type Item = LogicalFile<'a>;
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
    use std::collections::HashSet;
    use std::sync::Arc;

    use ::datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
    use ::datafusion::physical_optimizer::pruning::PruningStatistics;
    use ::datafusion::physical_plan::Accumulator;
    use arrow::compute::concat_batches;
    use arrow_arith::aggregate::sum;
    use arrow_array::{ArrayRef, BooleanArray, Int64Array};
    use arrow_schema::DataType as ArrowDataType;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
    use datafusion_common::Column;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::Expression;
    use delta_kernel::schema::{DataType, PrimitiveType};
    use delta_kernel::{ExpressionEvaluator, ExpressionHandler};

    use super::*;
    use crate::kernel::arrow::extract::{extract_and_cast_opt, extract_column};
    use crate::kernel::ARROW_HANDLER;

    #[derive(Debug, Default, Clone)]
    enum AccumulatorType {
        Min,
        Max,
        #[default]
        Unused,
    }
    // TODO validate this works with "wide and narrow" builds / stats

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
            fun_type: AccumulatorType,
        ) -> Precision<ScalarValue> {
            let mut path = name.split('.');
            let array = if let Ok(array) = extract_column(self.stats, path_step, &mut path) {
                array
            } else {
                return Precision::Absent;
            };

            if array.data_type().is_primitive() {
                let accumulator: Option<Box<dyn Accumulator>> = match fun_type {
                    AccumulatorType::Min => MinAccumulator::try_new(array.data_type())
                        .map_or(None, |a| Some(Box::new(a))),
                    AccumulatorType::Max => MaxAccumulator::try_new(array.data_type())
                        .map_or(None, |a| Some(Box::new(a))),
                    _ => None,
                };

                if let Some(mut accumulator) = accumulator {
                    return accumulator
                        .update_batch(&[array.clone()])
                        .ok()
                        .and_then(|_| accumulator.evaluate().ok())
                        .map(Precision::Exact)
                        .unwrap_or(Precision::Absent);
                }

                return Precision::Absent;
            }

            match array.data_type() {
                ArrowDataType::Struct(fields) => {
                    return fields
                        .iter()
                        .map(|f| {
                            self.column_bounds(
                                path_step,
                                &format!("{name}.{}", f.name()),
                                fun_type.clone(),
                            )
                        })
                        .map(|s| match s {
                            Precision::Exact(s) => Some(s),
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>()
                        .map(|o| {
                            let arrays = o
                                .into_iter()
                                .map(|sv| sv.to_array())
                                .collect::<Result<Vec<_>, datafusion_common::DataFusionError>>()
                                .unwrap();
                            let sa = StructArray::new(fields.clone(), arrays, None);
                            Precision::Exact(ScalarValue::Struct(Arc::new(sa)))
                        })
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

            let min_value = self.column_bounds(COL_MIN_VALUES, name.as_ref(), AccumulatorType::Min);
            let min_value = match &min_value {
                Precision::Exact(value) if value.is_null() => Precision::Absent,
                // TODO this is a hack, we should not be casting here but rather when we read the checkpoint data.
                // it seems sometimes the min/max values are stored as nanoseconds and sometimes as microseconds?
                Precision::Exact(ScalarValue::TimestampNanosecond(a, b)) => Precision::Exact(
                    ScalarValue::TimestampMicrosecond(a.map(|v| v / 1000), b.clone()),
                ),
                _ => min_value,
            };

            let max_value = self.column_bounds(COL_MAX_VALUES, name.as_ref(), AccumulatorType::Max);
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
                .map(|f| self.column_stats(f.name()))
                .collect::<Option<Vec<_>>>()?;
            Some(Statistics {
                num_rows,
                total_byte_size,
                column_statistics,
            })
        }

        fn pick_stats(&self, column: &Column, stats_field: &'static str) -> Option<ArrayRef> {
            let field = self.schema.field(&column.name)?;
            // See issue #1214. Binary type does not support natural order which is required for Datafusion to prune
            if field.data_type() == &DataType::Primitive(PrimitiveType::Binary) {
                return None;
            }
            let expression = if self.metadata.partition_columns.contains(&column.name) {
                Expression::Column(format!("add.partitionValues_parsed.{}", column.name))
            } else {
                Expression::Column(format!("add.stats_parsed.{}.{}", stats_field, column.name))
            };
            let evaluator = ARROW_HANDLER.get_evaluator(
                crate::kernel::models::fields::log_schema_ref().clone(),
                expression,
                field.data_type().clone(),
            );
            let mut results = Vec::with_capacity(self.data.len());
            for batch in self.data.iter() {
                let engine = ArrowEngineData::new(batch.clone());
                let result = evaluator.evaluate(&engine).ok()?;
                let result = result
                    .as_any()
                    .downcast_ref::<ArrowEngineData>()
                    .ok_or(DeltaTableError::generic(
                        "failed to downcast evaluator result to ArrowEngineData.",
                    ))
                    .ok()?;
                results.push(result.record_batch().clone());
            }
            let batch = concat_batches(results[0].schema_ref(), &results).ok()?;
            batch.column_by_name("output").map(|c| c.clone())
        }
    }

    impl<'a> PruningStatistics for LogDataHandler<'a> {
        /// return the minimum values for the named column, if known.
        /// Note: the returned array must contain `num_containers()` rows
        fn min_values(&self, column: &Column) -> Option<ArrayRef> {
            self.pick_stats(column, "minValues")
        }

        /// return the maximum values for the named column, if known.
        /// Note: the returned array must contain `num_containers()` rows.
        fn max_values(&self, column: &Column) -> Option<ArrayRef> {
            self.pick_stats(column, "maxValues")
        }

        /// return the number of containers (e.g. row groups) being
        /// pruned with these statistics
        fn num_containers(&self) -> usize {
            self.data.iter().map(|f| f.num_rows()).sum()
        }

        /// return the number of null values for the named column as an
        /// `Option<UInt64Array>`.
        ///
        /// Note: the returned array must contain `num_containers()` rows.
        fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
            if !self.metadata.partition_columns.contains(&column.name) {
                let counts = self.pick_stats(column, "nullCount")?;
                return arrow_cast::cast(counts.as_ref(), &ArrowDataType::UInt64).ok();
            }
            let partition_values = self.pick_stats(column, "__dummy__")?;
            let row_counts = self.row_counts(column)?;
            let row_counts = row_counts.as_any().downcast_ref::<UInt64Array>()?;
            let mut null_counts = Vec::with_capacity(partition_values.len());
            for i in 0..partition_values.len() {
                let null_count = if partition_values.is_null(i) {
                    row_counts.value(i)
                } else {
                    0
                };
                null_counts.push(null_count);
            }
            Some(Arc::new(UInt64Array::from(null_counts)))
        }

        /// return the number of rows for the named column in each container
        /// as an `Option<UInt64Array>`.
        ///
        /// Note: the returned array must contain `num_containers()` rows
        fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
            lazy_static::lazy_static! {
                static ref ROW_COUNTS_EVAL: Arc<dyn ExpressionEvaluator> =  ARROW_HANDLER.get_evaluator(
                    crate::kernel::models::fields::log_schema_ref().clone(),
                    Expression::column("add.stats_parsed.numRecords"),
                    DataType::Primitive(PrimitiveType::Long),
                );
            }
            let mut results = Vec::with_capacity(self.data.len());
            for batch in self.data.iter() {
                let engine = ArrowEngineData::new(batch.clone());
                let result = ROW_COUNTS_EVAL.evaluate(&engine).ok()?;
                let result = result
                    .as_any()
                    .downcast_ref::<ArrowEngineData>()
                    .ok_or(DeltaTableError::generic(
                        "failed to downcast evaluator result to ArrowEngineData.",
                    ))
                    .ok()?;
                results.push(result.record_batch().clone());
            }
            let batch = concat_batches(results[0].schema_ref(), &results).ok()?;
            arrow_cast::cast(batch.column_by_name("output")?, &ArrowDataType::UInt64).ok()
        }

        // This function is required since DataFusion 35.0, but is implemented as a no-op
        // https://github.com/apache/arrow-datafusion/blob/ec6abece2dcfa68007b87c69eefa6b0d7333f628/datafusion/core/src/datasource/physical_plan/parquet/page_filter.rs#L550
        fn contained(
            &self,
            _column: &Column,
            _value: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            None
        }
    }
}

#[cfg(all(test, feature = "datafusion"))]
mod tests {

    #[tokio::test]
    async fn read_delta_1_2_1_struct_stats_table() {
        let table_uri = "../test/tests/data/delta-1.2.1-only-struct-stats";
        let table_from_struct_stats = crate::open_table(table_uri).await.unwrap();
        let table_from_json_stats = crate::open_table_with_version(table_uri, 1).await.unwrap();

        let json_action = table_from_json_stats
            .snapshot()
            .unwrap()
            .snapshot
            .files()
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
            .files()
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
        let table_uri = "../test/tests/data/delta-1.2.1-only-struct-stats";
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
