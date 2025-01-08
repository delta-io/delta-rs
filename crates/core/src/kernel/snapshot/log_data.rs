use std::borrow::Cow;
use std::sync::Arc;

use arrow_arith::aggregate::sum;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, Int64Array, MapArray, RecordBatch, StructArray};
use arrow_cast::pretty::print_columns;
use arrow_select::filter::filter_record_batch;
use chrono::{DateTime, Utc};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::schema::SchemaRef;
use delta_kernel::{Expression, ExpressionEvaluator, ExpressionHandler};
use indexmap::IndexMap;
use object_store::path::Path;
use object_store::ObjectMeta;
use percent_encoding::percent_decode_str;

use super::super::scalars::ScalarExt;
use super::handler::{eval_expr, get_evaluator, AddOrdinals, DVOrdinals};
use crate::kernel::arrow::extract::{extract_and_cast, extract_and_cast_opt};
use crate::kernel::{
    Add, DataType, DeletionVectorDescriptor, Metadata, Remove, StorageType, StructField, StructType,
};
use crate::{DeltaResult, DeltaTableError};

const COL_NUM_RECORDS: &str = "numRecords";
const COL_MIN_VALUES: &str = "minValues";
const COL_MAX_VALUES: &str = "maxValues";
const COL_NULL_COUNT: &str = "nullCount";

pub(crate) trait PartitionsExt {
    fn hive_partition_path(&self) -> String;
}

impl PartitionsExt for IndexMap<String, Scalar> {
    fn hive_partition_path(&self) -> String {
        self.iter()
            .map(|(k, v)| format!("{k}={}", v.serialize_encoded()))
            .collect::<Vec<_>>()
            .join("/")
    }
}

impl PartitionsExt for StructData {
    fn hive_partition_path(&self) -> String {
        self.fields()
            .iter()
            .zip(self.values().iter())
            .map(|(k, v)| format!("{}={}", k.name(), v.serialize_encoded()))
            .collect::<Vec<_>>()
            .join("/")
    }
}

pub trait StructDataExt {
    fn get(&self, key: &str) -> Option<&Scalar>;
}

impl StructDataExt for StructData {
    fn get(&self, key: &str) -> Option<&Scalar> {
        self.fields()
            .iter()
            .zip(self.values().iter())
            .find(|(k, _)| k.name() == key)
            .map(|(_, v)| v)
    }
}

impl<T: PartitionsExt> PartitionsExt for Arc<T> {
    fn hive_partition_path(&self) -> String {
        self.as_ref().hive_partition_path()
    }
}

/// View into a deletion vector data.
///
// The data is assumed to be valid at the given index.
// Validity checks should be performed before creating a view.
#[derive(Debug)]
pub struct DeletionVectorView<'a> {
    data: &'a StructArray,
    /// Pointer to a specific row in the log data.
    index: usize,
}

impl DeletionVectorView<'_> {
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
        if self.storage_type().parse::<StorageType>().is_err() {
            print_columns("dv", &[Arc::new(self.data.clone())]).unwrap();
        }
        DeletionVectorDescriptor {
            storage_type: self.storage_type().parse().unwrap(),
            path_or_inline_dv: self.path_or_inline_dv().to_string(),
            size_in_bytes: self.size_in_bytes(),
            cardinality: self.cardinality(),
            offset: self.offset(),
        }
    }

    fn storage_type(&self) -> &str {
        self.data
            .column(DVOrdinals::STORAGE_TYPE)
            .as_string::<i32>()
            .value(self.index)
    }
    fn path_or_inline_dv(&self) -> &str {
        self.data
            .column(DVOrdinals::PATH_OR_INLINE_DV)
            .as_string::<i32>()
            .value(self.index)
    }
    fn size_in_bytes(&self) -> i32 {
        self.data
            .column(DVOrdinals::SIZE_IN_BYTES)
            .as_primitive::<Int32Type>()
            .value(self.index)
    }
    fn cardinality(&self) -> i64 {
        self.data
            .column(DVOrdinals::CARDINALITY)
            .as_primitive::<Int64Type>()
            .value(self.index)
    }
    fn offset(&self) -> Option<i32> {
        self.data.column_by_name("offset").and_then(|a| {
            (!a.is_null(self.index)).then(|| a.as_primitive::<Int32Type>().value(self.index))
        })
    }
}

#[derive(Debug, Clone)]
pub struct LogFileView {
    data: LogDataView,
    current: usize,
}

impl LogFileView {
    /// Path to the files storage location.
    pub fn path(&self) -> Cow<'_, str> {
        percent_decode_str(
            self.data
                .data
                .column(AddOrdinals::PATH)
                .as_string::<i32>()
                .value(self.current),
        )
        .decode_utf8_lossy()
    }

    /// An object store [`Path`] to the file.
    ///
    /// this tries to parse the file string and if that fails, it will return the string as is.
    // TODO assert consistent handling of the paths encoding when reading log data so this logic can be removed.
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
        self.data
            .data
            .column(AddOrdinals::SIZE)
            .as_primitive::<Int64Type>()
            .value(self.current)
    }

    /// Last modified time of the file.
    pub fn modification_time(&self) -> i64 {
        self.data
            .data
            .column(AddOrdinals::MODIFICATION_TIME)
            .as_primitive::<Int64Type>()
            .value(self.current)
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

    /// Last modified time of the file.
    pub fn data_change(&self) -> bool {
        self.data
            .data
            .column(AddOrdinals::DATA_CHANGE)
            .as_boolean()
            .value(self.current)
    }

    pub fn partition_values(&self) -> Option<StructData> {
        self.data
            .partition_data()
            .and_then(|arr| match Scalar::from_array(arr, self.current) {
                Some(Scalar::Struct(s)) => Some(s),
                _ => None,
            })
    }

    /// Defines a deletion vector
    pub fn deletion_vector(&self) -> Option<DeletionVectorView<'_>> {
        self.data
            .data
            .column_by_name("deletion_vector")
            .and_then(|c| c.as_struct_opt())
            .and_then(|c| {
                c.column_by_name("storage_type").and_then(|arr| {
                    let cond = arr.is_valid(self.current)
                        && !arr
                            .as_string_opt::<i32>()
                            .map(|v| v.value(self.current).is_empty())
                            .unwrap_or(true);
                    cond.then_some(DeletionVectorView {
                        data: c,
                        index: self.current,
                    })
                })
            })
    }

    fn stats_raw(&self) -> Option<&str> {
        self.data
            .data
            .column_by_name("stats")
            .and_then(|c| c.as_string_opt::<i32>())
            .and_then(|s| s.is_valid(self.current).then(|| s.value(self.current)))
    }

    /// The number of records stored in the data file.
    pub fn num_records(&self) -> Option<usize> {
        self.data.stats_data().and_then(|c| {
            c.column_by_name(COL_NUM_RECORDS)
                .and_then(|c| c.as_primitive_opt::<Int64Type>())
                .map(|a| a.value(self.current) as usize)
        })
    }

    /// Struct containing all available null counts for the columns in this file.
    pub fn null_counts(&self) -> Option<Scalar> {
        self.data.stats_data().and_then(|c| {
            c.column_by_name(COL_NULL_COUNT)
                .and_then(|c| Scalar::from_array(c.as_ref(), self.current))
        })
    }

    /// Struct containing all available min values for the columns in this file.
    pub fn min_values(&self) -> Option<Scalar> {
        self.data.stats_data().and_then(|c| {
            c.column_by_name(COL_MIN_VALUES)
                .and_then(|c| Scalar::from_array(c.as_ref(), self.current))
        })
    }

    /// Struct containing all available max values for the columns in this file.
    pub fn max_values(&self) -> Option<Scalar> {
        self.data.stats_data().and_then(|c| {
            c.column_by_name(COL_MAX_VALUES)
                .and_then(|c| Scalar::from_array(c.as_ref(), self.current))
        })
    }

    pub(crate) fn add_action(&self) -> Add {
        Add {
            // TODO use the raw (still encoded) path here once we reconciled serde ...
            path: self.path().to_string(),
            size: self.size(),
            modification_time: self.modification_time(),
            data_change: self.data_change(),
            stats: self.stats_raw().map(|s| s.to_string()),
            partition_values: self
                .partition_values()
                .map(|pv| {
                    pv.fields()
                        .iter()
                        .zip(pv.values().iter())
                        .map(|(k, v)| {
                            (
                                k.name().to_owned(),
                                if v.is_null() {
                                    None
                                } else {
                                    Some(v.serialize())
                                },
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            deletion_vector: self.deletion_vector().map(|dv| dv.descriptor()),
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            stats_parsed: None,
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
            partition_values: self.partition_values().map(|pv| {
                pv.fields()
                    .iter()
                    .zip(pv.values().iter())
                    .map(|(k, v)| {
                        (
                            k.name().to_owned(),
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

impl Iterator for LogFileView {
    type Item = LogFileView;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.data.data.num_rows() {
            None
        } else {
            let old = self.current;
            self.current += 1;
            Some(Self {
                data: self.data.clone(),
                current: old,
            })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.data.data.num_rows() - self.current,
            Some(self.data.data.num_rows() - self.current),
        )
    }
}

// impl From<&LogFileView> for Add {
//     fn from(value: &LogFileView) -> Self {
//         Add {}
//     }
// }

impl TryFrom<&LogFileView> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: &LogFileView) -> Result<Self, Self::Error> {
        Ok(ObjectMeta {
            location: value.object_store_path(),
            size: value.size() as usize,
            last_modified: value.modification_datetime()?,
            version: None,
            e_tag: None,
        })
    }
}

impl TryFrom<LogFileView> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: LogFileView) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

#[derive(Debug, Clone)]
pub struct LogDataView {
    data: RecordBatch,
    metadata: Arc<Metadata>,
    schema: SchemaRef,
}

impl LogDataView {
    pub(crate) fn new(data: RecordBatch, metadata: Arc<Metadata>, schema: SchemaRef) -> Self {
        Self {
            data,
            metadata,
            schema,
        }
    }

    fn evaluate(&self, expression: Expression, data_type: DataType) -> DeltaResult<RecordBatch> {
        let evaluator = get_evaluator(
            Arc::new(self.data.schema().try_into()?),
            expression,
            data_type,
        );
        eval_expr(&evaluator, &self.data)
    }

    fn partition_data(&self) -> Option<&StructArray> {
        self.data
            .column_by_name("partition_values")
            .and_then(|c| c.as_struct_opt())
    }

    fn stats_data(&self) -> Option<&StructArray> {
        self.data
            .column_by_name("stats_parsed")
            .and_then(|c| c.as_struct_opt())
    }

    pub fn with_partition_filter(self, predicate: Option<&Expression>) -> DeltaResult<Self> {
        if let (Some(pred), Some(data)) = (predicate, self.partition_data()) {
            let data = ArrowEngineData::new(data.into());
            let evaluator = get_evaluator(
                Arc::new(data.record_batch().schema_ref().as_ref().try_into()?),
                pred.clone(),
                DataType::BOOLEAN,
            );
            let result = ArrowEngineData::try_from_engine_data(evaluator.evaluate(&data)?)?;
            let filter = result.record_batch().column(0).as_boolean();
            return Ok(Self {
                data: filter_record_batch(&self.data, filter)?,
                metadata: self.metadata,
                schema: self.schema,
            });
        }
        Ok(self)
    }

    pub fn iter(&self) -> impl Iterator<Item = LogFileView> {
        LogFileView {
            data: self.clone(),
            current: 0,
        }
    }
}

impl IntoIterator for LogDataView {
    type Item = LogFileView;
    type IntoIter = LogFileView;

    fn into_iter(self) -> Self::IntoIter {
        LogFileView {
            data: self,
            current: 0,
        }
    }
}

#[cfg(feature = "datafusion")]
mod datafusion {
    use std::collections::HashSet;
    use std::sync::Arc;

    use ::datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
    use ::datafusion::physical_optimizer::pruning::PruningStatistics;
    use ::datafusion::physical_plan::Accumulator;
    use arrow::datatypes::UInt64Type;
    use arrow_arith::aggregate::sum;
    use arrow_array::{ArrayRef, BooleanArray, UInt64Array};
    use arrow_schema::DataType as ArrowDataType;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
    use datafusion_common::Column;
    use delta_kernel::expressions::Expression;
    use delta_kernel::schema::{DataType, PrimitiveType};

    use super::super::handler::get_evaluator;
    use super::*;
    use crate::kernel::arrow::extract::extract_column;
    use crate::kernel::snapshot::handler::eval_expr;

    #[derive(Debug, Default, Clone)]
    enum AccumulatorType {
        Min,
        Max,
        #[default]
        Unused,
    }
    // TODO validate this works with "wide and narrow" builds / stats

    impl LogDataView {
        fn collect_count(&self, name: &str) -> Precision<usize> {
            let stat = self
                .evaluate(Expression::column(["stats_parsed", name]), DataType::LONG)
                .ok()
                .and_then(|b| b.column(0).as_primitive_opt::<Int64Type>().cloned());
            if let Some(stat) = &stat {
                if stat.is_empty() {
                    Precision::Exact(0)
                } else if let Some(nulls) = stat.nulls() {
                    if nulls.null_count() > 0 {
                        Precision::Absent
                    } else {
                        sum(stat)
                            .map(|s| Precision::Exact(s as usize))
                            .unwrap_or(Precision::Absent)
                    }
                } else {
                    sum(stat)
                        .map(|s| Precision::Exact(s as usize))
                        .unwrap_or(Precision::Absent)
                }
            } else {
                Precision::Absent
            }
        }

        fn num_records(&self) -> Precision<usize> {
            self.collect_count(COL_NUM_RECORDS)
        }

        fn total_size_files(&self) -> Precision<usize> {
            self.evaluate(Expression::column(["size"]), DataType::LONG)
                .ok()
                .and_then(|b| {
                    b.column(0)
                        .as_primitive_opt::<Int64Type>()
                        .and_then(|s| sum(s))
                })
                .map(|p| Precision::Inexact(p as usize))
                .unwrap_or(Precision::Absent)
        }

        fn column_bounds(
            &self,
            path_step: &str,
            name: &str,
            fun_type: AccumulatorType,
        ) -> Precision<ScalarValue> {
            let mut path = name.split('.');
            let stats = self
                .data
                .column_by_name("stats_parsed")
                .and_then(|c| c.as_struct_opt());
            if stats.is_none() {
                return Precision::Absent;
            }
            let stats = stats.unwrap();
            let Ok(array) = extract_column(stats, path_step, &mut path) else {
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
                ArrowDataType::Struct(fields) => fields
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
                    .unwrap_or(Precision::Absent),
                _ => Precision::Absent,
            }
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

        pub(crate) fn statistics(&self) -> Option<Statistics> {
            let num_rows = self.num_records();
            let total_byte_size = self.total_size_files();
            let column_statistics = self
                .schema
                .fields()
                .map(|f| self.column_stats(f.name()).ok())
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
                Expression::column(["partition_values", &column.name])
            } else {
                Expression::column(["stats_parsed", stats_field, &column.name])
            };
            self.evaluate(expression, field.data_type().clone())
                .map(|b| b.column(0).clone())
                .ok()
        }
    }

    impl PruningStatistics for LogDataView {
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
            self.data.num_rows()
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
            let row_counts = row_counts.as_primitive_opt::<UInt64Type>()?;
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
            let evaluator = get_evaluator(
                Arc::new(self.data.schema().try_into().ok()?),
                Expression::column(["stats_parsed", "numRecords"]),
                DataType::LONG,
            );
            let batch = eval_expr(&evaluator, &self.data).ok()?;
            arrow_cast::cast(batch.column(0), &ArrowDataType::UInt64).ok()
        }

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
    #[ignore]
    async fn read_delta_1_2_1_struct_stats_table() {
        let table_uri = "../test/tests/data/delta-1.2.1-only-struct-stats";
        let table_from_struct_stats = crate::open_table(table_uri).await.unwrap();
        let table_from_json_stats = crate::open_table_with_version(table_uri, 1).await.unwrap();

        let json_action = table_from_json_stats
            .snapshot()
            .unwrap()
            .snapshot
            .log_data()
            .unwrap()
            .iter()
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
            .log_data()
            .unwrap()
            .iter()
            .find(|f| {
                f.path().ends_with(
                    "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
                )
            })
            .unwrap();

        assert_eq!(json_action.path(), struct_action.path());
        assert_eq!(
            json_action.partition_values(),
            struct_action.partition_values()
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
            .log_data()
            .unwrap();

        let col_stats = file_stats.statistics();
        println!("{:?}", col_stats);
    }
}
