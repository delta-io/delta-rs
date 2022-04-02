//! Datafusion integration for Delta Table
//!
//! Example:
//!
//! ```rust
//! use std::sync::Arc;
//! use datafusion::execution::context::ExecutionContext;
//!
//! async {
//!   let mut ctx = ExecutionContext::new();
//!   let table = deltalake::open_table("./tests/data/simple_table")
//!       .await
//!       .unwrap();
//!   ctx.register_table("demo", Arc::new(table)).unwrap();
//!
//!   let batches = ctx
//!       .sql("SELECT * FROM demo").await.unwrap()
//!       .collect()
//!       .await.unwrap();
//! };
//! ```

use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use async_trait::async_trait;
use datafusion::datafusion_data_access::object_store::local::LocalFileSystem;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{ColumnStatistics, Statistics};
use datafusion::scalar::ScalarValue;

use crate::action;
use crate::delta;
use crate::schema;

impl delta::DeltaTable {
    /// Return statistics for Datafusion Table
    pub fn datafusion_table_statistics(&self) -> Statistics {
        let stats = self
            .get_active_add_actions()
            .iter()
            .fold(
                Some(Statistics {
                    num_rows: Some(0),
                    total_byte_size: Some(0),
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            max_value: None,
                            min_value: None,
                            distinct_count: None
                        };
                        self.schema().unwrap().get_fields().len()
                    ]),
                    is_exact: true,
                }),
                |acc, action| {
                    let acc = acc?;
                    let new_stats = action
                        .get_stats()
                        .unwrap_or_else(|_| Some(action::Stats::default()))?;
                    Some(Statistics {
                        num_rows: acc
                            .num_rows
                            .map(|rows| rows + new_stats.num_records as usize),
                        total_byte_size: acc
                            .total_byte_size
                            .map(|total_size| total_size + action.size as usize),
                        column_statistics: acc.column_statistics.map(|col_stats| {
                            self.schema()
                                .unwrap()
                                .get_fields()
                                .iter()
                                .zip(col_stats)
                                .map(|(field, stats)| ColumnStatistics {
                                    null_count: new_stats
                                        .null_count
                                        .get(field.get_name())
                                        .and_then(|x| {
                                            let null_count_acc = stats.null_count?;
                                            let null_count = x.as_value()? as usize;
                                            Some(null_count_acc + null_count)
                                        })
                                        .or(stats.null_count),
                                    max_value: new_stats
                                        .max_values
                                        .get(field.get_name())
                                        .and_then(|x| {
                                            let old_stats = stats.clone();
                                            let max_value = to_scalar_value(x.as_value()?);

                                            match (max_value, old_stats.max_value) {
                                                (Some(max_value), Some(old_max_value)) => {
                                                    if left_larger_than_right(
                                                        old_max_value.clone(),
                                                        max_value.clone(),
                                                    ) {
                                                        Some(old_max_value)
                                                    } else {
                                                        Some(max_value)
                                                    }
                                                }
                                                (Some(max_value), None) => Some(max_value),
                                                (None, old) => old,
                                            }
                                        })
                                        .or_else(|| stats.max_value.clone()),
                                    min_value: new_stats
                                        .min_values
                                        .get(field.get_name())
                                        .and_then(|x| {
                                            let old_stats = stats.clone();
                                            let min_value = to_scalar_value(x.as_value()?);

                                            match (min_value, old_stats.min_value) {
                                                (Some(min_value), Some(old_min_value)) => {
                                                    if left_larger_than_right(
                                                        min_value.clone(),
                                                        old_min_value.clone(),
                                                    ) {
                                                        Some(old_min_value)
                                                    } else {
                                                        Some(min_value)
                                                    }
                                                }
                                                (Some(min_value), None) => Some(min_value),
                                                (None, old) => old,
                                            }
                                        })
                                        .or_else(|| stats.min_value.clone()),
                                    distinct_count: None, // TODO: distinct
                                })
                                .collect()
                        }),
                        is_exact: true,
                    })
                },
            )
            .unwrap_or_default();
        // Convert column max/min scalar values to correct types based on arrow types.
        Statistics {
            is_exact: true,
            num_rows: stats.num_rows,
            total_byte_size: stats.total_byte_size,
            column_statistics: stats.column_statistics.map(|col_stats| {
                let fields = self.schema().unwrap().get_fields();
                col_stats
                    .iter()
                    .zip(fields)
                    .map(|(col_states, field)| {
                        let dt = (self as &dyn TableProvider)
                            .schema()
                            .field_with_name(field.get_name())
                            .unwrap()
                            .data_type()
                            .clone();
                        ColumnStatistics {
                            null_count: col_states.null_count,
                            max_value: col_states
                                .max_value
                                .as_ref()
                                .and_then(|scalar| correct_scalar_value_type(scalar.clone(), &dt)),
                            min_value: col_states
                                .min_value
                                .as_ref()
                                .and_then(|scalar| correct_scalar_value_type(scalar.clone(), &dt)),
                            distinct_count: col_states.distinct_count,
                        }
                    })
                    .collect()
            }),
        }
    }
}

// TODO: uncomment this when datafusion supports per partitioned file stats
// fn add_action_df_stats(add: &action::Add, schema: &schema::Schema) -> Statistics {
//     if let Ok(Some(statistics)) = add.get_stats() {
//         Statistics {
//             num_rows: Some(statistics.num_records as usize),
//             total_byte_size: Some(add.size as usize),
//             column_statistics: Some(
//                 schema
//                     .get_fields()
//                     .iter()
//                     .map(|field| ColumnStatistics {
//                         null_count: statistics
//                             .null_count
//                             .get(field.get_name())
//                             .and_then(|f| f.as_value().map(|v| v as usize)),
//                         max_value: statistics
//                             .max_values
//                             .get(field.get_name())
//                             .and_then(|f| to_scalar_value(f.as_value()?)),
//                         min_value: statistics
//                             .min_values
//                             .get(field.get_name())
//                             .and_then(|f| to_scalar_value(f.as_value()?)),
//                         distinct_count: None, // TODO: distinct
//                     })
//                     .collect(),
//             ),
//             is_exact: true,
//         }
//     } else {
//         Statistics::default()
//     }
// }

#[async_trait]
impl TableProvider for delta::DeltaTable {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::new(
            <ArrowSchema as TryFrom<&schema::Schema>>::try_from(
                delta::DeltaTable::schema(self).unwrap(),
            )
            .unwrap(),
        )
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(<ArrowSchema as TryFrom<&schema::Schema>>::try_from(
            delta::DeltaTable::schema(self).unwrap(),
        )?);
        let filenames = self.get_file_uris();

        let partitions = filenames
            .into_iter()
            .zip(self.get_active_add_actions())
            .enumerate()
            .map(|(_idx, (fname, action))| {
                // TODO: no way to associate stats per file in datafusion at the moment, see:
                // https://github.com/apache/arrow-datafusion/issues/1301
                Ok(vec![PartitionedFile::new(fname, action.size as u64)])
            })
            .collect::<datafusion::error::Result<_>>()?;

        let df_object_store = Arc::new(LocalFileSystem {});
        ParquetFormat::default()
            .create_physical_plan(
                FileScanConfig {
                    object_store: df_object_store,
                    file_schema: schema,
                    file_groups: partitions,
                    statistics: self.datafusion_table_statistics(),
                    projection: projection.clone(),
                    limit,
                    table_partition_cols: self.get_metadata().unwrap().partition_columns.clone(),
                },
                filters,
            )
            .await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn to_scalar_value(stat_val: &serde_json::Value) -> Option<datafusion::scalar::ScalarValue> {
    if stat_val.is_number() {
        if let Some(val) = stat_val.as_i64() {
            Some(ScalarValue::from(val))
        } else if let Some(val) = stat_val.as_u64() {
            Some(ScalarValue::from(val))
        } else {
            stat_val.as_f64().map(ScalarValue::from)
        }
    } else {
        None
    }
}

fn correct_scalar_value_type(
    value: datafusion::scalar::ScalarValue,
    field_dt: &ArrowDataType,
) -> Option<datafusion::scalar::ScalarValue> {
    match field_dt {
        ArrowDataType::Int64 => {
            let raw_value = i64::try_from(value).unwrap();
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int32 => {
            let raw_value = i64::try_from(value).unwrap() as i32;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int16 => {
            let raw_value = i64::try_from(value).unwrap() as i16;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int8 => {
            let raw_value = i64::try_from(value).unwrap() as i8;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Float32 => {
            let raw_value = f64::try_from(value).unwrap() as f32;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Float64 => {
            let raw_value = f64::try_from(value).unwrap();
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Decimal(_, _) => {
            let raw_value = f64::try_from(value).unwrap();
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Date32 => {
            let raw_value = i64::try_from(value).unwrap() as i32;
            Some(ScalarValue::Date32(Some(raw_value)))
        }
        ArrowDataType::Date64 => {
            let raw_value = i64::try_from(value).unwrap();
            Some(ScalarValue::Date64(Some(raw_value)))
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let raw_value = i64::try_from(value).unwrap();
            Some(ScalarValue::TimestampNanosecond(Some(raw_value), None))
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
            let raw_value = i64::try_from(value).unwrap();
            Some(ScalarValue::TimestampMicrosecond(Some(raw_value), None))
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => {
            let raw_value = i64::try_from(value).unwrap();
            Some(ScalarValue::TimestampMillisecond(Some(raw_value), None))
        }
        _ => {
            log::error!(
                "Scalar value of arrow type unimplemented for {:?} and {:?}",
                value,
                field_dt
            );
            None
        }
    }
}

fn left_larger_than_right(
    left: datafusion::scalar::ScalarValue,
    right: datafusion::scalar::ScalarValue,
) -> bool {
    match left {
        ScalarValue::Float64(Some(v)) => {
            let f_right = f64::try_from(right).unwrap();
            v > f_right
        }
        ScalarValue::Float32(Some(v)) => {
            let f_right = f32::try_from(right).unwrap();
            v > f_right
        }
        ScalarValue::Int8(Some(v)) => {
            let i_right = i8::try_from(right).unwrap();
            v > i_right
        }
        ScalarValue::Int16(Some(v)) => {
            let i_right = i16::try_from(right).unwrap();
            v > i_right
        }
        ScalarValue::Int32(Some(v)) => {
            let i_right = i32::try_from(right).unwrap();
            v > i_right
        }
        ScalarValue::Int64(Some(v)) => {
            let i_right = i64::try_from(right).unwrap();
            v > i_right
        }
        _ => unimplemented!(
            "Scalar value comparison unimplemented for {:?} and {:?}",
            left,
            right
        ),
    }
}
