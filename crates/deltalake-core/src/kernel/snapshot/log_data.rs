use std::collections::HashMap;

use arrow_array::{Array, Int64Array, MapArray, RecordBatch, StringArray, StructArray};

use super::extract::extract_and_cast;
use crate::kernel::scalars::Scalar;
use crate::kernel::{DataType, StructField, StructType};
use crate::DeltaTableError;
use crate::{kernel::Metadata, DeltaResult};

#[derive(Debug, PartialEq)]
pub struct FileStats<'a> {
    path: &'a str,
    size: i64,
    partition_values: HashMap<&'a str, Option<Scalar>>,
    stats: StructArray,
}

pub struct FileStatsAccessor<'a> {
    partition_fields: HashMap<&'a str, &'a StructField>,
    paths: &'a StringArray,
    sizes: &'a Int64Array,
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
        let stats = extract_and_cast::<StructArray>(data, "add.stats_parsed")?;
        let partition_values = extract_and_cast::<MapArray>(data, "add.partitionValues")?;
        let partition_fields = metadata
            .partition_columns
            .iter()
            .map(|c| Ok::<_, DeltaTableError>((c.as_str(), schema.field_with_name(c.as_str())?)))
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(Self {
            partition_fields,
            paths,
            sizes,
            stats,
            partition_values,
            length: data.num_rows(),
            pointer: 0,
        })
    }

    fn get_partition_values(&self, index: usize) -> DeltaResult<HashMap<&'a str, Option<Scalar>>> {
        let map_value = self.partition_values.value(index);
        let keys = map_value
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::Generic("unexpected key type".into()))?;
        let values = map_value
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::Generic("unexpected value type".into()))?;
        keys.iter()
            .zip(values.iter())
            .map(|(k, v)| {
                let (key, field) = self.partition_fields.get_key_value(k.unwrap()).unwrap();
                let field_type = match field.data_type() {
                    DataType::Primitive(p) => p,
                    _ => todo!(),
                };
                Ok((*key, v.and_then(|vv| field_type.parse_scalar(vv).ok())))
            })
            .collect::<Result<HashMap<_, _>, _>>()
    }

    pub(crate) fn get(&self, index: usize) -> DeltaResult<FileStats<'a>> {
        let path = self.paths.value(index);
        let size = self.sizes.value(index);
        let stats = self.stats.slice(index, 1);
        let partition_values = self.get_partition_values(index)?;
        Ok(FileStats {
            path,
            size,
            partition_values,
            stats,
        })
    }
}

impl<'a> Iterator for FileStatsAccessor<'a> {
    type Item = DeltaResult<FileStats<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pointer >= self.length {
            return None;
        }

        let file_stats = self.get(self.pointer);
        if file_stats.is_err() {
            return Some(Err(file_stats.unwrap_err()));
        }

        self.pointer += 1;
        Some(file_stats)
    }
}

pub struct FileStatsHandler<'a> {
    data: &'a Vec<RecordBatch>,
    metadata: &'a Metadata,
    schema: &'a StructType,
}

impl<'a> FileStatsHandler<'a> {
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

impl<'a> IntoIterator for FileStatsHandler<'a> {
    type Item = DeltaResult<FileStats<'a>>;
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
            self.collect_count("numRecords")
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
            let null_count_col = format!("nullCount.{}", name.as_ref());
            let null_count = self.collect_count(&null_count_col);

            let min_value = self.column_bounds("minValues", name.as_ref(), &AggregateFunction::Min);
            let min_value = match &min_value {
                Precision::Exact(value) if value.is_null() => Precision::Absent,
                // TODO this is a hack, we should not be casting here but rather when we read the checkpoint data.
                // it seems sometimes the min/max values are stored as nanoseconds and sometimes as microseconds?
                Precision::Exact(ScalarValue::TimestampNanosecond(a, b)) => Precision::Exact(
                    ScalarValue::TimestampMicrosecond(a.map(|v| v / 1000), b.clone()),
                ),
                _ => min_value,
            };

            let max_value = self.column_bounds("maxValues", name.as_ref(), &AggregateFunction::Max);
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

    impl FileStatsHandler<'_> {
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
    use arrow_array::Array;

    #[tokio::test]
    async fn read_delta_1_2_1_struct_stats_table() {
        let table_uri = "../deltalake-test/tests/data/delta-1.2.1-only-struct-stats";
        let table_from_struct_stats = crate::open_table(table_uri).await.unwrap();
        let table_from_json_stats = crate::open_table_with_version(table_uri, 1).await.unwrap();

        let json_action = table_from_json_stats
             .snapshot()
             .unwrap()
             .snapshot
             .file_stats_iter()
             .find(|f| matches!(f, Ok(f) if f.path.ends_with("part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet"))).unwrap().unwrap();

        let struct_action = table_from_struct_stats
             .snapshot()
             .unwrap()
             .snapshot
             .file_stats_iter()
             .find(|f| matches!(f, Ok(f) if f.path.ends_with("part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet"))).unwrap().unwrap();

        assert_eq!(json_action.path, struct_action.path);
        assert_eq!(json_action.partition_values, struct_action.partition_values);
        assert_eq!(json_action.stats.len(), 1);
        assert!(json_action
            .stats
            .column(0)
            .eq(struct_action.stats.column(0)));
        assert_eq!(json_action.stats.len(), struct_action.stats.len());
    }

    #[tokio::test]
    async fn df_stats_delta_1_2_1_struct_stats_table() {
        let table_uri = "../deltalake-test/tests/data/delta-1.2.1-only-struct-stats";
        let table_from_struct_stats = crate::open_table(table_uri).await.unwrap();

        let file_stats = table_from_struct_stats
            .snapshot()
            .unwrap()
            .snapshot
            .file_stats();

        let col_stats = file_stats.statistics();
        println!("{:?}", col_stats);
    }
}
