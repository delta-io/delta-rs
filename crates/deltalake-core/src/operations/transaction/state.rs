use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::physical_plan::wrap_partition_type_in_dict;
use datafusion::execution::context::SessionState;
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use itertools::Either;

use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::logical_expr_to_physical_expr;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;
use crate::table::state::DeltaTableState;

impl DeltaTableState {
    /// Get the table schema as an [`SchemaRef`]
    pub fn arrow_schema(&self, wrap_partitions: bool) -> DeltaResult<SchemaRef> {
        let meta = self.current_metadata().ok_or(DeltaTableError::NoMetadata)?;
        let fields = meta
            .schema
            .fields()
            .iter()
            .filter(|f| !meta.partition_columns.contains(&f.name().to_string()))
            .map(|f| f.try_into())
            .chain(
                meta.schema
                    .fields()
                    .iter()
                    .filter(|f| meta.partition_columns.contains(&f.name().to_string()))
                    .map(|f| {
                        let field = Field::try_from(f)?;
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
            .collect::<Result<Vec<Field>, _>>()?;

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Iterate over all files in the log matching a predicate
    pub fn files_matching_predicate(
        &self,
        filters: &[Expr],
    ) -> DeltaResult<impl Iterator<Item = &Add>> {
        if let Some(Some(predicate)) =
            (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
        {
            let expr = logical_expr_to_physical_expr(&predicate, self.arrow_schema(true)?.as_ref());
            let pruning_predicate = PruningPredicate::try_new(expr, self.arrow_schema(true)?)?;
            Ok(Either::Left(
                self.files()
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
                    ),
            ))
        } else {
            Ok(Either::Right(self.files().iter()))
        }
    }

    /// Parse an expression string into a datafusion [`Expr`]
    pub fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema(true)?.as_ref().to_owned())?;
        parse_predicate_expression(&schema, expr, df_state)
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
        let snapshot = DeltaTableState::from_actions(init_table_actions(None), 0).unwrap();
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
