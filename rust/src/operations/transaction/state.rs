use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{
    DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::file_format::wrap_partition_type_in_dict;
use datafusion_common::config::ConfigOptions;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{Column, DFSchema, Result as DFResult, TableReference};
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use itertools::Either;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

use crate::action::Add;
use crate::delta_datafusion::{
    get_null_of_arrow_type, logical_expr_to_physical_expr, to_correct_scalar_value,
};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::table_state::DeltaTableState;

impl DeltaTableState {
    /// Get the table schema as an [`ArrowSchemaRef`]
    pub fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self._arrow_schema(true)
    }

    fn _arrow_schema(&self, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
        let meta = self.current_metadata().ok_or(DeltaTableError::NoMetadata)?;
        let fields = meta
            .schema
            .get_fields()
            .iter()
            .filter(|f| !meta.partition_columns.contains(&f.get_name().to_string()))
            .map(|f| f.try_into())
            .chain(
                meta.schema
                    .get_fields()
                    .iter()
                    .filter(|f| meta.partition_columns.contains(&f.get_name().to_string()))
                    .map(|f| {
                        let field = ArrowField::try_from(f)?;
                        let corrected = if wrap_partitions {
                            match field.data_type() {
                                // Dictionary encoding boolean types does not yield benefits
                                // https://github.com/apache/arrow-datafusion/pull/5545#issuecomment-1526917997
                                DataType::Boolean => field.data_type().clone(),
                                _ => wrap_partition_type_in_dict(field.data_type().clone()),
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
    ) -> DeltaResult<impl Iterator<Item = &Add>> {
        if let Some(Some(predicate)) =
            (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
        {
            let expr = logical_expr_to_physical_expr(&predicate, self.arrow_schema()?.as_ref());
            let pruning_predicate = PruningPredicate::try_new(expr, self.arrow_schema()?)?;
            Ok(Either::Left(
                self.files()
                    .iter()
                    .zip(pruning_predicate.prune(self)?.into_iter())
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
            Ok(Either::Right(self.files().iter()))
        }
    }

    /// Parse an expression string into a datafusion [`Expr`]
    pub fn parse_predicate_expression(&self, expr: impl AsRef<str>) -> DeltaResult<Expr> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, expr.as_ref());
        let tokens = tokenizer
            .tokenize()
            .map_err(|err| DeltaTableError::GenericError {
                source: Box::new(err),
            })?;
        let sql = Parser::new(dialect)
            .with_tokens(tokens)
            .parse_expr()
            .map_err(|err| DeltaTableError::GenericError {
                source: Box::new(err),
            })?;

        // TODO should we add the table name as qualifier when available?
        let df_schema = DFSchema::try_from_qualified_schema("", self.arrow_schema()?.as_ref())?;
        let context_provider = DummyContextProvider::default();
        let sql_to_rel = SqlToRel::new(&context_provider);

        Ok(sql_to_rel.sql_to_expr(sql, &df_schema, &mut Default::default())?)
    }

    /// Get the physical table schema.
    ///
    /// This will construct a schema derived from the parquet schema of the latest data file,
    /// and fields for partition columns from the schema defined in table meta data.
    pub async fn physical_arrow_schema(
        &self,
        object_store: Arc<dyn ObjectStore>,
    ) -> DeltaResult<ArrowSchemaRef> {
        if let Some(add) = self.files().iter().max_by_key(|obj| obj.modification_time) {
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
            .zip(pruning_predicate.prune(self)?.into_iter())
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
        let container =
            AddContainer::new(self.files(), partition_columns, self.arrow_schema().ok()?);
        container.min_values(column)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let partition_columns = &self.current_metadata()?.partition_columns;
        let container =
            AddContainer::new(self.files(), partition_columns, self.arrow_schema().ok()?);
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
        let container =
            AddContainer::new(self.files(), partition_columns, self.arrow_schema().ok()?);
        container.null_counts(column)
    }
}

#[derive(Default)]
struct DummyContextProvider {
    options: ConfigOptions,
}

impl ContextProvider for DummyContextProvider {
    fn get_table_provider(&self, _name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        unimplemented!()
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        unimplemented!()
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        unimplemented!()
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        unimplemented!()
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::transaction::test_utils::{create_add_action, init_table_actions};
    use datafusion_expr::{col, lit};

    #[test]
    fn test_parse_predicate_expression() {
        let state = DeltaTableState::from_actions(init_table_actions(), 0).unwrap();

        // parses simple expression
        let parsed = state.parse_predicate_expression("value > 10").unwrap();
        let expected = col("value").gt(lit::<i64>(10));
        assert_eq!(parsed, expected);

        // fails for unknown column
        let parsed = state.parse_predicate_expression("non_existent > 10");
        assert!(parsed.is_err());

        // parses complex expression
        let parsed = state
            .parse_predicate_expression("value > 10 OR value <= 0")
            .unwrap();
        let expected = col("value")
            .gt(lit::<i64>(10))
            .or(col("value").lt_eq(lit::<i64>(0)));
        assert_eq!(parsed, expected)
    }

    #[test]
    fn test_files_matching_predicate() {
        let mut actions = init_table_actions();
        actions.push(create_add_action("excluded", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":10},\"nullCount\":{\"value\":0}}".into())));
        actions.push(create_add_action("included-1", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":100},\"nullCount\":{\"value\":0}}".into())));
        actions.push(create_add_action("included-2", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":-10},\"maxValues\":{\"value\":3},\"nullCount\":{\"value\":0}}".into())));

        let state = DeltaTableState::from_actions(actions, 0).unwrap();
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
