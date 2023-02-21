use crate::action::Add;
use crate::delta_datafusion::to_correct_scalar_value;
use crate::table_state::DeltaTableState;
use crate::DeltaResult;
use crate::{schema, DeltaTableError};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion_common::config::ConfigOptions;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{Column, DFSchema, Result as DFResult, TableReference};
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use itertools::Either;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use std::convert::TryFrom;
use std::sync::Arc;

impl DeltaTableState {
    /// Get the table schema as an [`ArrowSchemaRef`]
    pub fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        Ok(Arc::new(
            <ArrowSchema as TryFrom<&schema::Schema>>::try_from(
                self.schema().ok_or(DeltaTableError::NoMetadata)?,
            )?,
        ))
    }

    pub fn files_matching_predicate(
        &self,
        filters: &[Expr],
    ) -> DeltaResult<impl Iterator<Item = &Add>> {
        if let Some(Some(predicate)) =
            (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
        {
            let pruning_predicate = PruningPredicate::try_new(predicate, self.arrow_schema()?)?;
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
}

pub struct AddContainer<'a> {
    inner: &'a Vec<Add>,
    schema: ArrowSchemaRef,
}

impl<'a> AddContainer<'a> {
    pub fn new(adds: &'a Vec<Add>, schema: ArrowSchemaRef) -> Self {
        Self {
            inner: adds,
            schema,
        }
    }
}

impl<'a> PruningStatistics for AddContainer<'a> {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let data_type = self.schema.field_with_name(&column.name).ok()?.data_type();
        let values = self.inner.iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                statistics
                    .min_values
                    .get(&column.name)
                    .and_then(|f| to_correct_scalar_value(f.as_value()?, &data_type))
                    .unwrap_or(ScalarValue::Null)
            } else {
                ScalarValue::Null
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let data_type = self.schema.field_with_name(&column.name).ok()?.data_type();
        let values = self.inner.iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                statistics
                    .max_values
                    .get(&column.name)
                    .and_then(|f| to_correct_scalar_value(f.as_value()?, &data_type))
                    .unwrap_or(ScalarValue::Null)
            } else {
                ScalarValue::Null
            }
        });
        ScalarValue::iter_to_array(values).ok()
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
                statistics
                    .null_count
                    .get(&column.name)
                    .map(|f| ScalarValue::UInt64(f.as_value().map(|val| val as u64)))
                    .unwrap_or(ScalarValue::UInt64(None))
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
        let container = AddContainer::new(self.files(), self.arrow_schema().ok()?);
        container.min_values(column)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let container = AddContainer::new(self.files(), self.arrow_schema().ok()?);
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
        let container = AddContainer::new(self.files(), self.arrow_schema().ok()?);
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
    use crate::action::Action;
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
        assert_eq!(parsed, expected);

        println!("{:?}", parsed)
    }

    #[test]
    fn test_files_matching_predicate() {
        let mut actions = init_table_actions();
        actions.push(Action::add(create_add_action("excluded", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":10},\"nullCount\":{\"value\":0}}"))));
        actions.push(Action::add(create_add_action("included-1", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":100},\"nullCount\":{\"value\":0}}"))));
        actions.push(Action::add(create_add_action("included-2", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":-10},\"maxValues\":{\"value\":3},\"nullCount\":{\"value\":0}}"))));

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
