use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{
    DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::datasource::physical_plan::wrap_partition_type_in_dict;
use datafusion::execution::context::SessionState;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{Column, DFSchema};
use datafusion_expr::{utils::conjunction, Expr};
use itertools::Either;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::{
    get_null_of_arrow_type, logical_expr_to_physical_expr, to_correct_scalar_value,
};
use crate::errors::DeltaResult;
use crate::kernel::Add;
use crate::table::state::DeltaTableState;

impl DeltaTableState {
    /// Get the table schema as an [`ArrowSchemaRef`]
    pub fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self._arrow_schema(true)
    }

    fn _arrow_schema(&self, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
        let meta = self.metadata();
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
                                DataType::Utf8
                                | DataType::LargeUtf8
                                | DataType::Binary
                                | DataType::LargeBinary => {
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

    pub(crate) fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self._arrow_schema(false)
    }

    /// Iterate over all files in the log matching a predicate
    pub fn files_matching_predicate(
        &self,
        filters: &[Expr],
    ) -> DeltaResult<impl Iterator<Item = Add>> {
        if let Some(Some(predicate)) =
            (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
        {
            let expr = logical_expr_to_physical_expr(&predicate, self.arrow_schema()?.as_ref());
            let pruning_predicate = PruningPredicate::try_new(expr, self.arrow_schema()?)?;
            Ok(Either::Left(
                self.file_actions()?
                    .into_iter()
                    .zip(pruning_predicate.prune(self)?)
                    .filter_map(
                        |(action, keep_file)| {
                            if keep_file {
                                Some(action)
                            } else {
                                None
                            }
                        },
                    ),
            ))
        } else {
            Ok(Either::Right(self.file_actions()?.into_iter()))
        }
    }

    /// Parse an expression string into a datafusion [`Expr`]
    pub fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())?;
        parse_predicate_expression(&schema, expr, df_state)
    }

    /// Get the physical table schema.
    ///
    /// This will construct a schema derived from the parquet schema of the latest data file,
    /// and fields for partition columns from the schema defined in table meta data.
    pub async fn physical_arrow_schema(
        &self,
        object_store: Arc<dyn ObjectStore>,
    ) -> DeltaResult<ArrowSchemaRef> {
        if let Some(add) = self
            .file_actions()?
            .iter()
            .max_by_key(|obj| obj.modification_time)
        {
            let file_meta = add.try_into()?;
            let file_reader = ParquetObjectReader::new(object_store, file_meta);
            let file_schema = ParquetRecordBatchStreamBuilder::new(file_reader)
                .await?
                .build()?
                .schema()
                .clone();

            let table_schema = Arc::new(ArrowSchema::new(
                self.arrow_schema()?
                    .fields
                    .clone()
                    .into_iter()
                    .map(|field| {
                        // field is an &Arc<Field>
                        let owned_field: ArrowField = field.as_ref().clone();
                        file_schema
                            .field_with_name(field.name())
                            // yielded with &Field
                            .cloned()
                            .unwrap_or(owned_field)
                    })
                    .collect::<Vec<ArrowField>>(),
            ));

            Ok(table_schema)
        } else {
            self.arrow_schema()
        }
    }
}

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

    pub fn get_prune_stats(&self, column: &Column, get_max: bool) -> Option<ArrayRef> {
        let (_, field) = self.schema.column_with_name(&column.name)?;

        // See issue 1214. Binary type does not support natural order which is required for Datafusion to prune
        if field.data_type() == &DataType::Binary {
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
                to_correct_scalar_value(&value, data_type)
                    .ok()
                    .flatten()
                    .unwrap_or(
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
                    .and_then(|f| {
                        to_correct_scalar_value(f.as_value()?, data_type)
                            .ok()
                            .flatten()
                    })
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

    // This function is required since DataFusion 35.0, but is implemented as a no-op
    // https://github.com/apache/arrow-datafusion/blob/ec6abece2dcfa68007b87c69eefa6b0d7333f628/datafusion/core/src/datasource/physical_plan/parquet/page_filter.rs#L550
    fn contained(&self, _column: &Column, _value: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

impl PruningStatistics for DeltaTableState {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let files = self.file_actions().ok()?;
        let partition_columns = &self.metadata().partition_columns;
        let container = AddContainer::new(&files, partition_columns, self.arrow_schema().ok()?);
        container.min_values(column)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let files = self.file_actions().ok()?;
        let partition_columns = &self.metadata().partition_columns;
        let container = AddContainer::new(&files, partition_columns, self.arrow_schema().ok()?);
        container.max_values(column)
    }

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize {
        self.files_count()
    }

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let files = self.file_actions().ok()?;
        let partition_columns = &self.metadata().partition_columns;
        let container = AddContainer::new(&files, partition_columns, self.arrow_schema().ok()?);
        container.null_counts(column)
    }

    // This function is required since DataFusion 35.0, but is implemented as a no-op
    // https://github.com/apache/arrow-datafusion/blob/ec6abece2dcfa68007b87c69eefa6b0d7333f628/datafusion/core/src/datasource/physical_plan/parquet/page_filter.rs#L550
    fn contained(&self, _column: &Column, _value: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::transaction::test_utils::{create_add_action, init_table_actions};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{col, lit};

    #[test]
    fn test_parse_predicate_expression() {
        let snapshot = DeltaTableState::from_actions(init_table_actions(None)).unwrap();
        let session = SessionContext::new();
        let state = session.state();

        // parses simple expression
        let parsed = snapshot
            .parse_predicate_expression("value > 10", &state)
            .unwrap();
        let expected = col("value").gt(lit::<i64>(10));
        assert_eq!(parsed, expected);

        // fails for unknown column
        let parsed = snapshot.parse_predicate_expression("non_existent > 10", &state);
        assert!(parsed.is_err());

        // parses complex expression
        let parsed = snapshot
            .parse_predicate_expression("value > 10 OR value <= 0", &state)
            .unwrap();
        let expected = col("value")
            .gt(lit::<i64>(10))
            .or(col("value").lt_eq(lit::<i64>(0)));
        assert_eq!(parsed, expected)
    }

    #[test]
    fn test_files_matching_predicate() {
        let mut actions = init_table_actions(None);
        actions.push(create_add_action("excluded", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":10},\"nullCount\":{\"value\":0}}".into())));
        actions.push(create_add_action("included-1", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":100},\"nullCount\":{\"value\":0}}".into())));
        actions.push(create_add_action("included-2", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":-10},\"maxValues\":{\"value\":3},\"nullCount\":{\"value\":0}}".into())));

        let state = DeltaTableState::from_actions(actions).unwrap();
        let files = state
            .files_matching_predicate(&[])
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 3);

        let predictate = col("value")
            .gt(lit::<i32>(10))
            .or(col("value").lt_eq(lit::<i32>(0)));

        let files = state
            .files_matching_predicate(&[predictate])
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|add| add.path.contains("included")));
    }
}
