use std::sync::Arc;

use arrow_schema::{
    DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::datasource::physical_plan::wrap_partition_type_in_dict;
use datafusion::execution::context::SessionState;
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use itertools::Either;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::logical_expr_to_physical_expr;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;
use crate::table::state::DeltaTableState;

impl DeltaTableState {
    /// Get the table schema as an [`ArrowSchemaRef`]
    // pub fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
    //     self._arrow_schema(true)
    // }

    pub fn arrow_schema(&self, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
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
                self.arrow_schema(true)?
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
            self.arrow_schema(true)
        }
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
