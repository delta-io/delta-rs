use std::sync::Arc;

use arrow_array::RecordBatch;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_properties::TableProperties;
use indexmap::IndexMap;

use super::super::scalars::ScalarExt;
use super::iterators::LogicalFileView;

pub(crate) trait PartitionsExt {
    fn hive_partition_path(&self) -> String;
}

impl PartitionsExt for IndexMap<String, Scalar> {
    fn hive_partition_path(&self) -> String {
        let sep = String::from('/');
        itertools::Itertools::intersperse(
            self.iter().map(|(k, v)| {
                let encoded = v.serialize_encoded();
                format!("{k}={encoded}")
            }),
            sep,
        )
        .collect()
    }
}

impl PartitionsExt for StructData {
    fn hive_partition_path(&self) -> String {
        let sep = String::from('/');
        itertools::Itertools::intersperse(
            self.fields()
                .iter()
                .zip(self.values().iter())
                .map(|(k, v)| {
                    let encoded = v.serialize_encoded();
                    format!("{}={encoded}", k.name())
                }),
            sep,
        )
        .collect()
    }
}

impl<T: PartitionsExt> PartitionsExt for Arc<T> {
    fn hive_partition_path(&self) -> String {
        self.as_ref().hive_partition_path()
    }
}

/// Provides semanitc access to the log data.
///
/// This is a helper struct that provides access to the log data in a more semantic way
/// to avid the necessiity of knowing the exact layout of the underlying log data.
#[derive(Clone)]
pub struct LogDataHandler<'a> {
    data: &'a [RecordBatch],
    config: &'a TableConfiguration,
}

impl<'a> LogDataHandler<'a> {
    pub(crate) fn new(data: &'a [RecordBatch], config: &'a TableConfiguration) -> Self {
        Self { data, config }
    }

    pub(crate) fn table_configuration(&self) -> &TableConfiguration {
        self.config
    }

    pub(crate) fn table_properties(&self) -> &TableProperties {
        self.config.table_properties()
    }

    pub(crate) fn protocol(&self) -> &Protocol {
        self.config.protocol()
    }

    pub(crate) fn metadata(&self) -> &Metadata {
        self.config.metadata()
    }

    /// The number of files in the log data.
    pub fn num_files(&self) -> usize {
        self.data.iter().map(|batch| batch.num_rows()).sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = LogicalFileView> + '_ {
        self.data.iter().flat_map(|batch| {
            (0..batch.num_rows()).map(move |idx| LogicalFileView::new(batch.clone(), idx))
        })
    }
}

impl IntoIterator for LogDataHandler<'_> {
    type Item = LogicalFileView;
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.data.to_vec().into_iter().flat_map(|batch| {
            (0..batch.num_rows()).map(move |idx| LogicalFileView::new(batch.clone(), idx))
        }))
    }
}

#[cfg(feature = "datafusion")]
mod datafusion {
    use std::collections::HashSet;
    use std::sync::{Arc, LazyLock};

    use ::datafusion::common::Column;
    use ::datafusion::common::scalar::ScalarValue;
    use ::datafusion::physical_optimizer::pruning::PruningStatistics;
    use arrow::compute::concat;
    use arrow_array::{Array, StringArray};
    use arrow_array::{ArrayRef, BooleanArray, UInt64Array};
    use arrow_schema::DataType as ArrowDataType;
    use delta_kernel::expressions::Expression;
    use delta_kernel::schema::{DataType, PrimitiveType};
    use delta_kernel::{EvaluationHandler, ExpressionEvaluator};
    use itertools::Itertools;

    use super::*;
    use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt as _;
    use crate::{DeltaResult, DeltaTableError};

    use crate::kernel::ARROW_HANDLER;

    impl LogDataHandler<'_> {
        fn pick_stats(&self, column: &Column, stats_field: &'static str) -> Option<ArrayRef> {
            let schema = self.config.schema();
            let field = schema.field(&column.name)?;
            // See issue #1214. Binary type does not support natural order which is required for Datafusion to prune
            if field.data_type() == &DataType::Primitive(PrimitiveType::Binary) {
                return None;
            }
            let expression = if self
                .config
                .metadata()
                .partition_columns()
                .contains(&column.name)
            {
                Expression::column(["partitionValues_parsed", &column.name])
            } else {
                Expression::column(["stats_parsed", stats_field, &column.name])
            };
            let evaluator = ARROW_HANDLER
                .new_expression_evaluator(
                    crate::kernel::models::fields::log_schema_ref().clone(),
                    expression.into(),
                    field.data_type().clone(),
                )
                .ok()?;

            let results: Vec<_> = self
                .data
                .iter()
                .map(|data| -> DeltaResult<_> {
                    Ok(evaluator.evaluate_arrow(data.clone())?.column(0).clone())
                })
                .try_collect()
                .ok()?;
            concat(
                &results
                    .iter()
                    .map(|result| result.as_ref())
                    .collect::<Vec<_>>(),
            )
            .ok()
        }
    }

    impl PruningStatistics for LogDataHandler<'_> {
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
            self.data.iter().map(|b| b.num_rows()).sum()
        }

        /// return the number of null values for the named column as an
        /// `Option<UInt64Array>`.
        ///
        /// Note: the returned array must contain `num_containers()` rows.
        fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
            if !self
                .config
                .metadata()
                .partition_columns()
                .contains(&column.name)
            {
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
            static ROW_COUNTS_EVAL: LazyLock<Arc<dyn ExpressionEvaluator>> = LazyLock::new(|| {
                ARROW_HANDLER
                    .new_expression_evaluator(
                        crate::kernel::models::fields::log_schema_ref().clone(),
                        Expression::column(["add", "stats_parsed", "numRecords"]).into(),
                        DataType::Primitive(PrimitiveType::Long),
                    )
                    .expect("Failed to create row counts evaluator")
            });

            let results: Vec<_> = self
                .data
                .iter()
                .map(|data| -> DeltaResult<_> {
                    let batch = ROW_COUNTS_EVAL.evaluate_arrow(data.clone())?;
                    Ok(arrow_cast::cast(batch.column(0), &ArrowDataType::UInt64)?)
                })
                .try_collect()
                .ok()?;

            concat(
                &results
                    .iter()
                    .map(|result| result.as_ref())
                    .collect::<Vec<_>>(),
            )
            .ok()
        }

        // This function is optional but will optimize partition column pruning
        fn contained(&self, column: &Column, value: &HashSet<ScalarValue>) -> Option<BooleanArray> {
            if value.is_empty()
                || !self
                    .config
                    .metadata()
                    .partition_columns()
                    .contains(&column.name)
            {
                return None;
            }

            // Retrieve the partition values for the column
            let partition_values = self.pick_stats(column, "__dummy__")?;

            let partition_values = partition_values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(DeltaTableError::generic(
                    "failed to downcast string result to StringArray.",
                ))
                .ok()?;

            let mut contains = Vec::with_capacity(partition_values.len());

            // TODO: this was inspired by parquet's BloomFilter pruning, decide if we should
            //  just convert to Vec<String> for a subset of column types and use .contains
            fn check_scalar(pv: &str, value: &ScalarValue) -> bool {
                match value {
                    ScalarValue::Utf8(Some(v))
                    | ScalarValue::Utf8View(Some(v))
                    | ScalarValue::LargeUtf8(Some(v)) => pv == v,

                    ScalarValue::Dictionary(_, inner) => check_scalar(pv, inner),
                    // FIXME: is this a good enough default or should we sync this with
                    //  expr_applicable_for_cols and bail out with None
                    _ => value.to_string() == pv,
                }
            }

            for i in 0..partition_values.len() {
                if partition_values.is_null(i) {
                    contains.push(false);
                } else {
                    contains.push(
                        value
                            .iter()
                            .any(|scalar| check_scalar(partition_values.value(i), scalar)),
                    );
                }
            }

            Some(BooleanArray::from(contains))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::schema::DataType;
    use delta_kernel::schema::PrimitiveType;
    use delta_kernel::schema::StructField;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_partitionsext_structdata() {
        let partitions = StructData::try_new(
            vec![
                StructField::new("year", DataType::LONG, false),
                StructField::new("month", DataType::LONG, false),
                StructField::new("day", DataType::LONG, false),
            ],
            vec![Scalar::Long(2025), Scalar::Long(1), Scalar::Long(1)],
        )
        .expect("Failed to make StructData");
        assert_eq!("year=2025/month=1/day=1", partitions.hive_partition_path());

        let partitions = StructData::try_new(
            vec![StructField::new("year", DataType::LONG, true)],
            vec![Scalar::Null(DataType::Primitive(PrimitiveType::Long))],
        )
        .expect("Failed to make StructData");
        assert_eq!(
            "year=__HIVE_DEFAULT_PARTITION__",
            partitions.hive_partition_path()
        );
    }

    #[test]
    fn test_partitionsext_indexmap() {
        let partitions: IndexMap<String, Scalar> = IndexMap::from([
            ("year".to_string(), Scalar::Long(2025)),
            ("month".to_string(), Scalar::Long(1)),
            ("day".to_string(), Scalar::Long(1)),
        ]);
        assert_eq!("year=2025/month=1/day=1", partitions.hive_partition_path());

        let partitions: IndexMap<String, Scalar> = IndexMap::from([(
            "year".to_string(),
            Scalar::Null(DataType::Primitive(PrimitiveType::String)),
        )]);
        assert_eq!(
            "year=__HIVE_DEFAULT_PARTITION__",
            partitions.hive_partition_path()
        );
    }
}

#[cfg(all(test, feature = "datafusion"))]
mod df_tests {
    use futures::TryStreamExt;

    #[tokio::test]
    async fn read_delta_1_2_1_struct_stats_table() {
        let table_path = std::path::Path::new("../test/tests/data/delta-1.2.1-only-struct-stats");
        let table_uri =
            url::Url::from_directory_path(std::fs::canonicalize(table_path).unwrap()).unwrap();
        let table_from_struct_stats = crate::open_table(table_uri.clone()).await.unwrap();
        let table_from_json_stats = crate::open_table_with_version(table_uri, 1).await.unwrap();
        let log_store = table_from_struct_stats.log_store();

        let json_adds: Vec<_> = table_from_json_stats
            .snapshot()
            .unwrap()
            .snapshot()
            .file_views(&log_store, None)
            .try_collect()
            .await
            .unwrap();
        let json_action = json_adds
            .iter()
            .find(|f| {
                f.path().ends_with(
                    "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
                )
            })
            .unwrap();

        let struct_adds: Vec<_> = table_from_struct_stats
            .snapshot()
            .unwrap()
            .snapshot()
            .file_views(&log_store, None)
            .try_collect()
            .await
            .unwrap();
        let struct_action = struct_adds
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
}
