use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::AsArray;
use arrow::compute::{filter_record_batch, not};
use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, Statistics, exec_err, plan_datafusion_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::optimizer::simplify_expressions::simplify_predicates;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use datafusion::prelude::{Expr, binary_expr, col};
use datafusion::scalar::ScalarValue;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_features::TableFeature;
use futures::Stream;
use itertools::Itertools as _;
use pin_project_lite::pin_project;

use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::table_provider::simplify_expr;
use crate::table::config::TablePropertiesExt as _;
use crate::table::{Constraint, GeneratedColumn};
use crate::{DeltaTableError, StructTypeExt as _};

/// Generate validation predicates based on the table configuration
///
/// This function generates a list of DataFusion expressions
/// that represent the data validation rules specified in the
/// Delta table configuration. This includes:
/// - Non-nullable column checks
/// - [column invariants]
/// - [check constraints]
/// - [generated columns]
///
/// # Arguments
/// - `session`: The DataFusion session
/// - `input`: The input execution plan
/// - `table_configuration`: Configuration object of Delta table
///
/// [column invariants]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-invariants
/// [check constraints]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#check-constraints
/// [generated columns]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#generated-columns
pub(crate) fn validation_predicates(
    session: &dyn Session,
    input: Arc<dyn ExecutionPlan>,
    table_configuration: &TableConfiguration,
) -> Result<Vec<Expr>> {
    let df_schema = DFSchema::try_from(input.schema())?;

    let mut validations = table_configuration
        .schema()
        .fields()
        .filter(|f| !f.is_nullable())
        .map(|f| col(f.name()).is_not_null())
        .collect_vec();

    if table_configuration.is_feature_enabled(&TableFeature::Invariants) {
        let invariants = table_configuration
            .schema()
            .get_invariants()
            .map_err(|e| plan_datafusion_err!("Failed to read invariants from schema: {}", e))?;
        for invariant in invariants {
            let expr = parse_predicate_expression(&df_schema, &invariant.invariant_sql, session)?;
            validations.push(expr);
        }
    }

    if table_configuration.is_feature_enabled(&TableFeature::CheckConstraints) {
        let constraints = table_configuration.table_properties().get_constraints();
        validations.extend(constraints_to_exprs(session, &df_schema, &constraints)?);
    }

    if table_configuration.is_feature_enabled(&TableFeature::GeneratedColumns) {
        let generated = table_configuration
            .schema()
            .get_generated_columns()
            .map_err(|e| {
                plan_datafusion_err!("Failed to read generated columns from schema: {}", e)
            })?;
        validations.extend(generated_columns_to_exprs(session, &df_schema, &generated)?);
    }

    Ok(validations)
}

pub(crate) fn constraints_to_exprs<'a>(
    session: &dyn Session,
    df_schema: &DFSchema,
    constraints: impl IntoIterator<Item = &'a Constraint>,
) -> Result<Vec<Expr>> {
    Ok(constraints
        .into_iter()
        .map(|constraint| parse_predicate_expression(df_schema, &constraint.expr, session))
        .try_collect()?)
}

pub(crate) fn generated_columns_to_exprs<'a>(
    session: &dyn Session,
    df_schema: &DFSchema,
    generated_columns: impl IntoIterator<Item = &'a GeneratedColumn>,
) -> Result<Vec<Expr>> {
    generated_columns
        .into_iter()
        .map(|gen_col| {
            let expr = parse_predicate_expression(df_schema, &gen_col.generation_expr, session)?;
            let col_expr = col(&gen_col.name);
            let validation_expr = binary_expr(col_expr, Operator::IsNotDistinctFrom, expr);
            Ok::<_, DataFusionError>(validation_expr)
        })
        .collect()
}

/// Execution plan for validating data
///
/// The Delta protocol specifies data validation steps that must be
/// performed when writing data to a Delta table mainly via the
/// [column invariants], [check constraints], and [generated columns]
/// features. Additionally, the table schema contains nullability information
/// that must also be enforced.
///
/// [column invariants]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-invariants
/// [check constraints]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#check-constraints
/// [generated columns]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#generated-columns
#[derive(Clone, Debug)]
pub struct DataValidationExec {
    /// The input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The expression to use for checking data validity
    check_expression: Arc<dyn PhysicalExpr>,
}

impl DataValidationExec {
    pub fn try_new_with_config(
        session: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        config: &TableConfiguration,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let predicates = validation_predicates(session, Arc::clone(&input), config)?;
        Self::try_new_with_predicates(session, input, predicates)
    }

    /// Create a new [`DataValidationExec`] if there are any predicates to apply
    /// otherwise return the input execution plan as-is.
    pub fn try_new_with_predicates(
        session: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        predicates: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let df_schema = DFSchema::try_from(input.schema())?;
        if let Some(validation_expr) = conjunction(simplify_predicates(predicates)?) {
            let check_expression = simplify_expr(session, df_schema.into(), validation_expr)?;
            return Ok(Arc::new(Self::try_new(input, check_expression)?));
        }
        Ok(input)
    }

    /// Create a new [`DataValidationExec`]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        check_expression: Arc<dyn PhysicalExpr>,
    ) -> Result<Self> {
        let result_type = check_expression.data_type(input.schema().as_ref())?;
        if !matches!(result_type, DataType::Boolean) {
            return plan_err!(
                "Data validation expression must return boolean values, got {:?}",
                result_type
            );
        }
        Ok(Self {
            input,
            check_expression,
        })
    }
}

impl DisplayAs for DataValidationExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::TreeRender
            | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DataValidationExec: check_expression={:?}",
                    self.check_expression
                )
            }
        }
    }
}

impl ExecutionPlan for DataValidationExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "DataValidationExec"
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "DataValidationExec wrong number of children: expected 1, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
            check_expression: Arc::clone(&self.check_expression),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(DataValidationStream::new(
            self.input.execute(partition, context)?,
            self.input.schema(),
            Arc::clone(&self.check_expression),
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(repartitioned) = self.input.repartitioned(target_partitions, config)? {
            Ok(Some(Arc::new(Self {
                input: repartitioned,
                check_expression: Arc::clone(&self.check_expression),
            })))
        } else {
            Ok(None)
        }
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let input_with_fetch = self.input.with_fetch(limit)?;
        Some(Arc::new(Self {
            input: input_with_fetch,
            check_expression: Arc::clone(&self.check_expression),
        }))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.input.cardinality_effect()
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }
}

pin_project! {
    /// Stream that validates data according to a check expression
    /// before yielding it.
    pub(crate) struct DataValidationStream<S> {
        // The expression to use for checking data validity
        check_expression: Arc<dyn PhysicalExpr>,

        // The schema of the output stream
        schema: SchemaRef,

        #[pin]
        // The input stream
        stream: S,
    }
}

impl<S> DataValidationStream<S> {
    /// Create a new DataValidationStream
    pub(crate) fn new(
        stream: S,
        schema: SchemaRef,
        check_expression: Arc<dyn PhysicalExpr>,
    ) -> DataValidationStream<S> {
        DataValidationStream {
            check_expression,
            schema,
            stream,
        }
    }
}

impl<S> Stream for DataValidationStream<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                match this.check_expression.evaluate(&batch)? {
                    ColumnarValue::Array(array) => {
                        let validity_mask = array.as_boolean();
                        let invalid_count = validity_mask
                            .iter()
                            .filter(|v| matches!(v, Some(false) | None))
                            .count();
                        if invalid_count > 0 {
                            let invalid_data = filter_record_batch(&batch, &not(&validity_mask)?)?;
                            let invalid_slice =
                                invalid_data.slice(0, invalid_data.num_rows().min(5));
                            let preview = pretty_format_batches(&[invalid_slice])?;
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                                DeltaTableError::InvalidData {
                                    message: format!(
                                        "Invalid data found: {invalid_count} rows failed validation check. \nPreview of invalid data:\n\n{preview}"
                                    ),
                                },
                            )))));
                        }
                    }
                    ColumnarValue::Scalar(value) => {
                        if !matches!(value, ScalarValue::Boolean(Some(true))) {
                            return Poll::Ready(Some(exec_err!(
                                "Invalid data found: scalar validation check failed with value {value:?}."
                            )));
                        }
                    }
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> RecordBatchStream for DataValidationStream<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use datafusion::catalog::{MemTable, TableProvider};
    // use datafusion::physical_plan::MemoryExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionContext, binary_expr, col};
    use futures::StreamExt;

    fn create_test_schema(nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, nullable),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(
        schema: SchemaRef,
        ids: Vec<Option<i32>>,
        names: Vec<Option<&str>>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    async fn get_memory_exec(
        session: &dyn Session,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Arc<dyn ExecutionPlan> {
        let table = MemTable::try_new(schema, vec![batches]).unwrap();
        table.scan(session, None, &[], None).await.unwrap()
    }

    #[tokio::test]
    async fn test_validation_nullability_pass() -> Result<()> {
        let schema = create_test_schema(false);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(1), Some(2), Some(3)],
            vec![Some("a"), Some("b"), Some("c")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // Create validation for non-nullable id column
        let predicates = vec![col("id").is_not_null()];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_check_constraint_pass() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(20), Some(30)],
            vec![Some("a"), Some("b"), Some("c")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // Create validation: id > 5
        let predicates = vec![col("id").gt(datafusion::prelude::lit(5i32))];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_check_constraint_fail() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(2), Some(30)], // One value fails constraint
            vec![Some("a"), Some("b"), Some("c")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // Create validation: id > 5
        let predicates = vec![col("id").gt(datafusion::prelude::lit(5i32))];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid data found")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_multiple_constraints() -> Result<()> {
        let schema = create_test_schema(false);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(20), Some(30)],
            vec![Some("a"), Some("b"), Some("c")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // Multiple validations: id NOT NULL AND id > 5 AND id < 100
        let predicates = vec![
            col("id").is_not_null(),
            col("id").gt(datafusion::prelude::lit(5i32)),
            col("id").lt(datafusion::prelude::lit(100i32)),
        ];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_multiple_constraints_fail() -> Result<()> {
        let schema = create_test_schema(false);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(200), Some(30)], // 200 fails id < 100 constraint
            vec![Some("a"), Some("b"), Some("c")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        let predicates = vec![
            col("id").is_not_null(),
            col("id").gt(datafusion::prelude::lit(5i32)),
            col("id").lt(datafusion::prelude::lit(100i32)),
        ];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_generated_column_match() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("id_times_2", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![2, 4, 6])), // Correctly doubled
            ],
        )?;

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // Validate: id_times_2 IS NOT DISTINCT FROM (id * 2)
        let predicates = vec![binary_expr(
            col("id_times_2"),
            Operator::IsNotDistinctFrom,
            col("id") * datafusion::prelude::lit(2i32),
        )];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_generated_column_mismatch() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("id_times_2", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![2, 5, 6])), // 5 is incorrect (should be 4)
            ],
        )?;

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        let predicates = vec![binary_expr(
            col("id_times_2"),
            Operator::IsNotDistinctFrom,
            col("id") * datafusion::prelude::lit(2i32),
        )];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_empty_batch() -> Result<()> {
        let schema = create_test_schema(false);
        let batch = create_test_batch(schema.clone(), vec![], vec![]);

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        let predicates = vec![col("id").is_not_null()];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_no_predicates() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(1), Some(2)],
            vec![Some("a"), Some("b")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // No predicates - should return input plan unchanged
        let predicates = vec![];
        let result_exec = DataValidationExec::try_new_with_predicates(
            &ctx.state(),
            memory_exec.clone(),
            predicates,
        )?;

        // When no predicates, should return the input plan
        assert!(Arc::ptr_eq(
            &result_exec,
            &(memory_exec as Arc<dyn ExecutionPlan>)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_partial_batch_failure() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(2), Some(30), Some(1)], // Two values fail
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        let predicates = vec![col("id").gt(datafusion::prelude::lit(5i32))];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("2 rows failed validation"));

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_maintains_order() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(20), Some(30)],
            vec![Some("a"), Some("b"), Some("c")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        let predicates = vec![col("id").is_not_null()];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        // Check that maintains_input_order returns true
        let downcast = validated_exec
            .as_any()
            .downcast_ref::<DataValidationExec>()
            .unwrap();
        assert_eq!(downcast.maintains_input_order(), vec![true]);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_display() -> Result<()> {
        let schema = create_test_schema(true);
        let empty_exec = Arc::new(EmptyExec::new(schema));
        let ctx = SessionContext::new();

        let predicates = vec![col("id").is_not_null()];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), empty_exec, predicates)?;

        let display_str = displayable(validated_exec.as_ref())
            .indent(false)
            .to_string();
        assert!(display_str.contains("DataValidationExec"));
        assert!(display_str.contains("check_expression"));

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_with_new_children() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(20)],
            vec![Some("a"), Some("b")],
        );

        let ctx = SessionContext::new();
        let memory_exec1 = get_memory_exec(&ctx.state(), schema.clone(), vec![batch.clone()]).await;
        let memory_exec2 = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        let predicates = vec![col("id").is_not_null()];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec1, predicates)?;

        // Create new plan with different child
        let new_exec = validated_exec.with_new_children(vec![memory_exec2])?;
        assert!(
            new_exec
                .as_any()
                .downcast_ref::<DataValidationExec>()
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_wrong_number_of_children() -> Result<()> {
        let schema = create_test_schema(true);
        let memory_exec = Arc::new(EmptyExec::new(schema));
        let ctx = SessionContext::new();

        let predicates = vec![col("id").is_not_null()];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        // Try to create with wrong number of children
        let result = validated_exec.with_new_children(vec![]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong number of children")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_stream_directly() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(20)],
            vec![Some("a"), Some("b")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        let predicates = vec![col("id").gt(datafusion::prelude::lit(5i32))];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let mut stream = validated_exec.execute(0, ctx.task_ctx())?;

        // Manually poll the stream
        let mut count = 0;
        while let Some(result) = stream.next().await {
            result?;
            count += 1;
        }
        assert_eq!(count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_scalar_true() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(20)],
            vec![Some("a"), Some("b")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // Use a literal true predicate (scalar)
        let predicates = vec![datafusion::prelude::lit(true)];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_scalar_false() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(20)],
            vec![Some("a"), Some("b")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // Use a literal false predicate (scalar)
        let predicates = vec![datafusion::prelude::lit(false)];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("scalar validation check failed"));

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_detailed_error_message() -> Result<()> {
        let schema = create_test_schema(true);
        // Create batch with multiple invalid rows to test the detailed error message
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(1), Some(2), Some(30)], // First 2 fail constraint id > 5
            vec![Some("a"), Some("b"), Some("c")],
        );

        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), schema, vec![batch]).await;

        // Create validation: id > 5 (2 rows should fail)
        let predicates = vec![col("id").gt(datafusion::prelude::lit(5i32))];
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, predicates)?;

        let result = collect(validated_exec, ctx.task_ctx()).await;
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();

        let data = vec![
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 1  | a    |",
            "| 2  | b    |",
            "+----+------+",
        ];
        for line in data {
            assert!(err_msg.contains(line));
        }

        Ok(())
    }
}
