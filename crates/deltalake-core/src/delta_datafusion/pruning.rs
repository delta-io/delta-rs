//! Pruning statistics for Datafusion
use std::convert::TryFrom;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::datasource::physical_plan::wrap_partition_type_in_dict;
use datafusion::execution::context::SessionState;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::{ColumnStatistics, Statistics};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{Column, DFSchema};
use datafusion_expr::Expr;

use super::expr::parse_predicate_expression;
use super::{
    get_null_of_arrow_type, left_larger_than_right, logical_expr_to_physical_expr,
    to_correct_scalar_value, to_scalar_value,
};
use crate::errors::DeltaResult;
use crate::kernel::snapshot::Snapshot;
use crate::kernel::Add;
use crate::protocol::Stats;
use crate::table::state::DeltaTableState;

/// Extension trait to implement datafusion helpers for DeltaTable
pub trait DatafusionExt {
    /// Get the table schema as an [`ArrowSchemaRef`]
    fn arrow_schema(&self, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef>;

    /// Return statistics for Datafusion Table
    fn datafusion_table_statistics(&self) -> Statistics;

    /// Parse an expression string into a datafusion [`Expr`]
    fn parse_predicate_expression(&self, expr: &str, df_state: &SessionState) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema(true)?.as_ref().to_owned())?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl<S: Snapshot> DatafusionExt for S {
    fn arrow_schema(&self, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
        let meta = self.metadata()?;
        let fields = meta
            .schema()?
            .fields()
            .iter()
            .filter(|f| !meta.partition_columns.contains(&f.name().to_string()))
            .map(|f| f.try_into())
            .chain(
                meta.schema()?
                    .fields()
                    .iter()
                    .filter(|f| meta.partition_columns.contains(&f.name().to_string()))
                    .map(|f| {
                        let field = ArrowField::try_from(f)?;
                        let corrected = if wrap_partitions {
                            match field.data_type() {
                                // Only dictionary-encode types that may be large
                                // // https://github.com/apache/arrow-datafusion/pull/5545
                                ArrowDataType::Utf8
                                | ArrowDataType::LargeUtf8
                                | ArrowDataType::Binary
                                | ArrowDataType::LargeBinary => {
                                    wrap_partition_type_in_dict(field.data_type().clone())
                                }
                                _ => field.data_type().clone(),
                            }
                        } else {
                            field.data_type().clone()
                        };
                        Ok(field.with_data_type(corrected))
                    }),
            )
            .collect::<Result<Vec<ArrowField>, _>>()?;

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    /// Return statistics for Datafusion Table
    fn datafusion_table_statistics(&self) -> Statistics {
        let stats = self
            .files()
            .unwrap()
            .try_fold(
                Statistics {
                    num_rows: Some(0),
                    total_byte_size: Some(0),
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            max_value: None,
                            min_value: None,
                            distinct_count: None
                        };
                        self.schema().unwrap().fields().len()
                    ]),
                    is_exact: true,
                },
                |acc, action| {
                    let new_stats = action
                        .get_stats()
                        .unwrap_or_else(|_| Some(Stats::default()))?;
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
                                .fields()
                                .iter()
                                .zip(col_stats)
                                .map(|(field, stats)| {
                                    let null_count = new_stats
                                        .null_count
                                        .get(field.name())
                                        .and_then(|x| {
                                            let null_count_acc = stats.null_count?;
                                            let null_count = x.as_value()? as usize;
                                            Some(null_count_acc + null_count)
                                        })
                                        .or(stats.null_count);

                                    let max_value = new_stats
                                        .max_values
                                        .get(field.name())
                                        .and_then(|x| {
                                            let old_stats = stats.clone();
                                            let max_value = to_scalar_value(x.as_value()?);

                                            match (max_value, old_stats.max_value) {
                                                (Some(max_value), Some(old_max_value)) => {
                                                    if left_larger_than_right(
                                                        old_max_value.clone(),
                                                        max_value.clone(),
                                                    )? {
                                                        Some(old_max_value)
                                                    } else {
                                                        Some(max_value)
                                                    }
                                                }
                                                (Some(max_value), None) => Some(max_value),
                                                (None, old) => old,
                                            }
                                        })
                                        .or_else(|| stats.max_value.clone());

                                    let min_value = new_stats
                                        .min_values
                                        .get(field.name())
                                        .and_then(|x| {
                                            let old_stats = stats.clone();
                                            let min_value = to_scalar_value(x.as_value()?);

                                            match (min_value, old_stats.min_value) {
                                                (Some(min_value), Some(old_min_value)) => {
                                                    if left_larger_than_right(
                                                        min_value.clone(),
                                                        old_min_value.clone(),
                                                    )? {
                                                        Some(old_min_value)
                                                    } else {
                                                        Some(min_value)
                                                    }
                                                }
                                                (Some(min_value), None) => Some(min_value),
                                                (None, old) => old,
                                            }
                                        })
                                        .or_else(|| stats.min_value.clone());

                                    ColumnStatistics {
                                        null_count,
                                        max_value,
                                        min_value,
                                        distinct_count: None, // TODO: distinct
                                    }
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
                let fields = self.schema().unwrap().fields();
                col_stats
                    .iter()
                    .zip(fields)
                    .map(|(col_states, field)| {
                        let dt = self
                            .arrow_schema(true)
                            .unwrap()
                            .field_with_name(field.name())
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

impl DeltaTableState {
    /// Return statistics for Datafusion Table
    pub fn datafusion_table_statistics(&self) -> Statistics {
        let stats = self
            .files()
            .iter()
            .try_fold(
                Statistics {
                    num_rows: Some(0),
                    total_byte_size: Some(0),
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            max_value: None,
                            min_value: None,
                            distinct_count: None
                        };
                        self.schema().unwrap().fields().len()
                    ]),
                    is_exact: true,
                },
                |acc, action| {
                    let new_stats = action
                        .get_stats()
                        .unwrap_or_else(|_| Some(Stats::default()))?;
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
                                .fields()
                                .iter()
                                .zip(col_stats)
                                .map(|(field, stats)| {
                                    let null_count = new_stats
                                        .null_count
                                        .get(field.name())
                                        .and_then(|x| {
                                            let null_count_acc = stats.null_count?;
                                            let null_count = x.as_value()? as usize;
                                            Some(null_count_acc + null_count)
                                        })
                                        .or(stats.null_count);

                                    let max_value = new_stats
                                        .max_values
                                        .get(field.name())
                                        .and_then(|x| {
                                            let old_stats = stats.clone();
                                            let max_value = to_scalar_value(x.as_value()?);

                                            match (max_value, old_stats.max_value) {
                                                (Some(max_value), Some(old_max_value)) => {
                                                    if left_larger_than_right(
                                                        old_max_value.clone(),
                                                        max_value.clone(),
                                                    )? {
                                                        Some(old_max_value)
                                                    } else {
                                                        Some(max_value)
                                                    }
                                                }
                                                (Some(max_value), None) => Some(max_value),
                                                (None, old) => old,
                                            }
                                        })
                                        .or_else(|| stats.max_value.clone());

                                    let min_value = new_stats
                                        .min_values
                                        .get(field.name())
                                        .and_then(|x| {
                                            let old_stats = stats.clone();
                                            let min_value = to_scalar_value(x.as_value()?);

                                            match (min_value, old_stats.min_value) {
                                                (Some(min_value), Some(old_min_value)) => {
                                                    if left_larger_than_right(
                                                        min_value.clone(),
                                                        old_min_value.clone(),
                                                    )? {
                                                        Some(old_min_value)
                                                    } else {
                                                        Some(min_value)
                                                    }
                                                }
                                                (Some(min_value), None) => Some(min_value),
                                                (None, old) => old,
                                            }
                                        })
                                        .or_else(|| stats.min_value.clone());

                                    ColumnStatistics {
                                        null_count,
                                        max_value,
                                        min_value,
                                        distinct_count: None, // TODO: distinct
                                    }
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
                let fields = self.schema().unwrap().fields();
                col_stats
                    .iter()
                    .zip(fields)
                    .map(|(col_states, field)| {
                        let dt = self
                            .arrow_schema(true)
                            .unwrap()
                            .field_with_name(field.name())
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

fn correct_scalar_value_type(value: ScalarValue, field_dt: &ArrowDataType) -> Option<ScalarValue> {
    match field_dt {
        ArrowDataType::Int64 => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int32 => {
            let raw_value = i64::try_from(value).ok()? as i32;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int16 => {
            let raw_value = i64::try_from(value).ok()? as i16;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int8 => {
            let raw_value = i64::try_from(value).ok()? as i8;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Float32 => {
            let raw_value = f64::try_from(value).ok()? as f32;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Float64 => {
            let raw_value = f64::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Utf8 => match value {
            ScalarValue::Utf8(val) => Some(ScalarValue::Utf8(val)),
            _ => None,
        },
        ArrowDataType::LargeUtf8 => match value {
            ScalarValue::Utf8(val) => Some(ScalarValue::LargeUtf8(val)),
            _ => None,
        },
        ArrowDataType::Boolean => {
            let raw_value = bool::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Decimal128(_, _) => {
            let raw_value = f64::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Decimal256(_, _) => {
            let raw_value = f64::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Date32 => {
            let raw_value = i64::try_from(value).ok()? as i32;
            Some(ScalarValue::Date32(Some(raw_value)))
        }
        ArrowDataType::Date64 => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::Date64(Some(raw_value)))
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::TimestampNanosecond(Some(raw_value), None))
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::TimestampMicrosecond(Some(raw_value), None))
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => {
            let raw_value = i64::try_from(value).ok()?;
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

/// Container for [`Add`] actions to implement helper functions for pruning
pub struct AddContainer<'a> {
    inner: &'a Vec<Add>,
    partition_columns: &'a Vec<String>,
    schema: ArrowSchemaRef,
}

impl<'a> AddContainer<'a> {
    /// Create a new instance of [`AddContainer`]
    pub fn new(
        adds: &'a Vec<Add>,
        partition_columns: &'a Vec<String>,
        schema: ArrowSchemaRef,
    ) -> Self {
        Self {
            inner: adds,
            partition_columns,
            schema,
        }
    }

    /// get column statistics
    pub fn get_prune_stats(&self, column: &Column, get_max: bool) -> Option<ArrayRef> {
        let (_, field) = self.schema.column_with_name(&column.name)?;

        // See issue 1214. Binary type does not support natural order which is required for Datafusion to prune
        if field.data_type() == &ArrowDataType::Binary {
            return None;
        }

        let data_type = field.data_type();

        let values = self.inner.iter().map(|add| {
            if self.partition_columns.contains(&column.name) {
                let value = add.partition_values.get(&column.name).unwrap();
                let value = match value {
                    Some(v) => serde_json::Value::String(v.to_string()),
                    None => serde_json::Value::Null,
                };
                to_correct_scalar_value(&value, data_type).unwrap_or(
                    get_null_of_arrow_type(data_type).expect("Could not determine null type"),
                )
            } else if let Ok(Some(statistics)) = add.get_stats() {
                let values = if get_max {
                    statistics.max_values
                } else {
                    statistics.min_values
                };

                values
                    .get(&column.name)
                    .and_then(|f| to_correct_scalar_value(f.as_value()?, data_type))
                    .unwrap_or(
                        get_null_of_arrow_type(data_type).expect("Could not determine null type"),
                    )
            } else {
                get_null_of_arrow_type(data_type).expect("Could not determine null type")
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    /// Get an iterator of add actions / files, that MAY contain data matching the predicate.
    ///
    /// Expressions are evaluated for file statistics, essentially column-wise min max bounds,
    /// so evaluating expressions is inexact. However, excluded files are guaranteed (for a correct log)
    /// to not contain matches by the predicate expression.
    pub fn predicate_matches(&self, predicate: Expr) -> DeltaResult<impl Iterator<Item = &Add>> {
        let expr = logical_expr_to_physical_expr(&predicate, &self.schema);
        let pruning_predicate = PruningPredicate::try_new(expr, self.schema.clone())?;
        Ok(self
            .inner
            .iter()
            .zip(pruning_predicate.prune(self)?)
            .filter_map(
                |(action, keep_file)| {
                    if keep_file {
                        Some(action)
                    } else {
                        None
                    }
                },
            ))
    }
}

impl<'a> PruningStatistics for AddContainer<'a> {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.get_prune_stats(column, false)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.get_prune_stats(column, true)
    }

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize {
        self.inner.len()
    }

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let values = self.inner.iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                if self.partition_columns.contains(&column.name) {
                    let value = add.partition_values.get(&column.name).unwrap();
                    match value {
                        Some(_) => ScalarValue::UInt64(Some(0)),
                        None => ScalarValue::UInt64(Some(statistics.num_records as u64)),
                    }
                } else {
                    statistics
                        .null_count
                        .get(&column.name)
                        .map(|f| ScalarValue::UInt64(f.as_value().map(|val| val as u64)))
                        .unwrap_or(ScalarValue::UInt64(None))
                }
            } else if self.partition_columns.contains(&column.name) {
                let value = add.partition_values.get(&column.name).unwrap();
                match value {
                    Some(_) => ScalarValue::UInt64(Some(0)),
                    None => ScalarValue::UInt64(None),
                }
            } else {
                ScalarValue::UInt64(None)
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }
}

impl PruningStatistics for DeltaTableState {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let partition_columns = &self.current_metadata()?.partition_columns;
        let container = AddContainer::new(
            self.files(),
            partition_columns,
            self.arrow_schema(true).ok()?,
        );
        container.min_values(column)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let partition_columns = &self.current_metadata()?.partition_columns;
        let container = AddContainer::new(
            self.files(),
            partition_columns,
            self.arrow_schema(true).ok()?,
        );
        container.max_values(column)
    }

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize {
        self.files().len()
    }

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let partition_columns = &self.current_metadata()?.partition_columns;
        let container = AddContainer::new(
            self.files(),
            partition_columns,
            self.arrow_schema(true).ok()?,
        );
        container.null_counts(column)
    }
}
