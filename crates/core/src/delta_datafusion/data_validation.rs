use std::cmp::Ordering;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::AsArray;
use arrow::compute::{filter_record_batch, not};
use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    DFSchema, DFSchemaRef, Statistics, ToDFSchema, exec_err, plan_datafusion_err, plan_err,
};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{
    RecordBatchStream, SendableRecordBatchStream, SessionState, TaskContext,
};
use datafusion::functions::core::expr_ext::FieldAccessor as _;
use datafusion::functions_nested::expr_fn::{array_compact, array_length, map_values};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    ColumnarValue, ExprSchemable as _, LogicalPlan, Operator, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::simplify_expressions::simplify_predicates;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{Expr, binary_expr, col, ident};
use datafusion::scalar::ScalarValue;
use delta_kernel::Expression;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::ColumnName;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_features::TableFeature;
use futures::Stream;
use itertools::Itertools as _;
use pin_project_lite::pin_project;

use crate::delta_datafusion::engine::to_delta_expression;
use crate::delta_datafusion::expr::{
    parse_generated_column_expression, parse_predicate_expression,
};
use crate::delta_datafusion::table_provider::simplify_expr;
use crate::table::config::TablePropertiesExt as _;
use crate::table::{Constraint, GeneratedColumn};
use crate::{DeltaTableError, StructTypeExt as _};

/// Logical plan node for data validation
///
/// This node represents a data validation step in the logical plan,
/// where a set of validation expressions are applied to the input data.
/// It also updates the schema to reflect any non-nullable columns
/// inferred from the validation expressions.
#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) struct DataValidation {
    input: LogicalPlan,
    validations: Vec<Expr>,
    validated_schema: DFSchemaRef,
}

impl PartialOrd for DataValidation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => self.validations.partial_cmp(&other.validations),
            cmp => cmp,
        }
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

impl DataValidation {
    #[allow(dead_code)]
    pub(crate) fn try_new(
        input: LogicalPlan,
        validations: impl IntoIterator<Item = Expr>,
    ) -> Result<Arc<Self>> {
        let validations = validations
            .into_iter()
            .map(|e| {
                let dt = e.get_type(input.schema())?;
                if dt != DataType::Boolean {
                    return plan_err!("Validation expression must be boolean, got {dt:?}");
                }
                Ok::<_, DataFusionError>(e)
            })
            .try_collect()?;
        let mut extractor = NotNullExtractor {
            non_nullable_columns: Vec::new(),
        };
        for expr in &validations {
            TreeNode::visit(expr, &mut extractor)?;
        }
        let validated_schema =
            make_fields_non_nullable(input.schema().as_arrow(), &extractor.non_nullable_columns)
                .to_dfschema_ref()?;
        Ok(Self {
            input,
            validations,
            validated_schema,
        }
        .into())
    }
}

/// Visitor to extract non-null assertions in expressions
///
/// This visitor traverses DataFusion expressions to find instances of
/// `IS NOT NULL` and `NOT IS NULL` patterns, which indicate that certain
/// columns are being asserted as non-nullable. It collects the column names
/// involved in these assertions.
///
/// This is used to update the schema of the data after validation to
/// reflect the non-nullability of these columns.
#[allow(dead_code)]
struct NotNullExtractor {
    non_nullable_columns: Vec<ColumnName>,
}

impl TreeNodeVisitor<'_> for NotNullExtractor {
    type Node = Expr;

    fn f_down(&mut self, node: &'_ Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            Expr::IsNotNull(expr) => {
                // we dont't actually need a kernel expression here, but DF field access
                // if somewhat tedious to extract and the kernel ColumnName is convenient
                // for this purpose.
                if let Ok(Expression::Column(col_name)) = to_delta_expression(expr) {
                    self.non_nullable_columns.push(col_name);
                }
                Ok(TreeNodeRecursion::Continue)
            }
            Expr::Not(expr) => match expr.as_ref() {
                Expr::IsNull(inner_expr) => {
                    if let Ok(Expression::Column(col_name)) = to_delta_expression(inner_expr) {
                        self.non_nullable_columns.push(col_name);
                    }
                    Ok(TreeNodeRecursion::Continue)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            },
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }
}

impl UserDefinedLogicalNodeCore for DataValidation {
    fn name(&self) -> &str {
        "DataValidation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.validated_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.validations.clone()
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataValidation validations={:?}", self.validations)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return plan_err!(
                "DataValidation node expects exactly one input, got: {}.",
                inputs.len()
            );
        }
        Ok(Self {
            input: inputs.remove(0),
            validations: exprs,
            validated_schema: self.validated_schema.clone(),
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DataValidationExtensionPlanner;

impl DataValidationExtensionPlanner {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl ExtensionPlanner for DataValidationExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<DataValidation>() {
            if physical_inputs.len() != 1 {
                return plan_err!(
                    "DataValidation node expects exactly one input, got: {}.",
                    physical_inputs.len()
                );
            }
            return Ok(Some(
                DataValidationExec::try_new_with_predicates_and_schema(
                    session_state,
                    physical_inputs[0].clone(),
                    node.validations.clone(),
                    node.validated_schema.inner().clone(),
                )?,
            ));
        }
        Ok(None)
    }
}

/// Generate validation predicates based on the table configuration
///
/// This function generates a list of DataFusion expressions that represent the data
/// validation rules specified in the Delta table configuration. This includes:
/// - Non-nullable field checks
/// - [column invariants]
/// - [check constraints]
/// - [generated columns]
///
/// # Arguments
/// - `session`: The DataFusion session
/// - `source_schema`: The schema of the source data.
/// - `table_configuration`: Configuration object of Delta table
///
/// [column invariants]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-invariants
/// [check constraints]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#check-constraints
/// [generated columns]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#generated-columns
pub(crate) fn validation_predicates(
    session: &dyn Session,
    source_schema: &DFSchema,
    table_configuration: &TableConfiguration,
) -> Result<Vec<Expr>> {
    // find all columns that are non-nullable in the table schema but nullable
    // in the source schema and add IS NOT NULL checks for them.
    let table_schema: Schema = table_configuration
        .logical_schema()
        .as_ref()
        .try_into_arrow()?;
    let non_nullable_table: HashSet<_> = collect_non_nullable_fields(&table_schema)
        .into_iter()
        .collect();
    let non_nullable_source: HashSet<_> = collect_non_nullable_fields(source_schema.as_arrow())
        .into_iter()
        .collect();
    // A scalar field (top-level or nested inside structs) holds one value per row, so it can be
    // validated with `col IS NOT NULL`. We trust the source schema and only check fields that are
    // nullable in the source, since a non-nullable source already guarantees the values.
    //
    // Collection element non-nullability (map values and list elements) cannot be expressed as a
    // scalar `IS NOT NULL` and the source's declared element-nullability is unreliable for the very
    // case #3690 reports, so those checks are driven from the table schema regardless of the source
    // and emitted separately below.
    let mut validations: Vec<_> = non_nullable_table
        .difference(&non_nullable_source)
        .filter_map(|col| non_nullable_scalar_check_expr(col, &table_schema))
        .collect();
    validations.extend(
        non_nullable_table
            .iter()
            .filter_map(|col| non_nullable_collection_check_expr(col, &table_schema)),
    );

    if table_configuration.is_feature_enabled(&TableFeature::Invariants) {
        let invariants = table_configuration
            .logical_schema()
            .get_invariants()
            .map_err(|e| plan_datafusion_err!("Failed to read invariants from schema: {}", e))?;
        for invariant in invariants {
            let expr =
                parse_predicate_expression(source_schema, &invariant.invariant_sql, session)?;
            validations.push(expr);
        }
    }

    if table_configuration.is_feature_enabled(&TableFeature::CheckConstraints) {
        let constraints = table_configuration.table_properties().get_constraints();
        validations.extend(constraints_to_exprs(session, source_schema, &constraints)?);
    }

    if table_configuration.is_feature_enabled(&TableFeature::GeneratedColumns) {
        let generated = table_configuration
            .logical_schema()
            .get_generated_columns()
            .map_err(|e| {
                plan_datafusion_err!("Failed to read generated columns from schema: {}", e)
            })?;
        validations.extend(generated_columns_to_exprs(
            session,
            source_schema,
            &generated,
        )?);
    }

    Ok(validations)
}

/// Build a column reference expression from a kernel column path.
///
/// Uses `ident`/`field` to match the segment handling in `to_datafusion_expr` (no SQL
/// normalization, `.` treated literally), so camelCase and other names resolve correctly.
fn col_expr(path: &[String]) -> Option<Expr> {
    let mut iter = path.iter();
    let base = iter.next()?;
    Some(iter.fold(ident(base), |acc, n| acc.field(n)))
}

/// Build a `col IS NOT NULL` check for a non-nullable scalar field.
///
/// Returns `None` for collection elements (list/map), which are handled by
/// [`non_nullable_collection_check_expr`] instead.
fn non_nullable_scalar_check_expr(path: &ColumnName, table_schema: &Schema) -> Option<Expr> {
    let segments = path.path();
    let mut data_type = table_schema
        .field_with_name(segments.first()?)
        .ok()?
        .data_type();
    // Walk the remaining segments through structs only; anything reached past a list or map
    // element is not a scalar field.
    for segment in &segments[1..] {
        match data_type {
            DataType::Struct(fields) => {
                data_type = fields.iter().find(|f| f.name() == segment)?.data_type();
            }
            _ => return None,
        }
    }
    col_expr(segments).map(|e| e.is_not_null())
}

/// Build a "no null elements" check for a non-nullable map value or list element.
///
/// Collection element nullability cannot be expressed as a scalar `IS NOT NULL`: the values form
/// an array per row, so we compare the element count before and after dropping nulls. Returns
/// `None` for scalar fields and for paths that reach past a list/map element (those cannot be
/// addressed per-element today and are skipped rather than erroring).
fn non_nullable_collection_check_expr(path: &ColumnName, table_schema: &Schema) -> Option<Expr> {
    let segments = path.path();
    // A collection element path has at least a parent and the element/value segment; a
    // single-segment path is a top-level scalar field, handled by the scalar check.
    if segments.len() < 2 {
        return None;
    }
    // Resolve the parent collection and the element/value field of the final path segment.
    let last = segments.last()?;
    let mut data_type = table_schema
        .field_with_name(segments.first()?)
        .ok()?
        .data_type();
    for segment in &segments[1..segments.len() - 1] {
        match data_type {
            DataType::Struct(fields) => {
                data_type = fields.iter().find(|f| f.name() == segment)?.data_type();
            }
            // Path crosses a list/map element; per-element access isn't expressible, skip it.
            _ => return None,
        }
    }

    let parent = &segments[..segments.len() - 1];
    match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _)
            if !element.is_nullable() =>
        {
            col_expr(parent).map(no_null_elements_check)
        }
        DataType::Map(entries, _) if last == "value" => {
            if let DataType::Struct(fields) = entries.data_type() {
                let value = fields.iter().find(|f| f.name() == "value")?;
                if !value.is_nullable() {
                    return col_expr(parent).map(|m| no_null_elements_check(map_values(m)));
                }
            }
            None
        }
        _ => None,
    }
}

/// Assert an array expression contains no null elements.
///
/// `array_compact` drops nulls; if the length is unchanged there were none. `IS NOT DISTINCT FROM`
/// is null-safe, so a NULL collection (e.g. a nullable list column whose slot is null) compares
/// NULL to NULL and passes — only the element is required to be non-null, not the collection.
fn no_null_elements_check(array: Expr) -> Expr {
    binary_expr(
        array_length(array.clone()),
        Operator::IsNotDistinctFrom,
        array_length(array_compact(array)),
    )
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
            let expr = parse_generated_column_expression(df_schema, gen_col, session)?;
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
    /// Plan properties including the schema after validation
    /// (may have updated nullability)
    properties: Arc<PlanProperties>,
}

impl DataValidationExec {
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
            return Ok(Arc::new(Self::try_new(input, check_expression, None)?));
        }
        Ok(input)
    }

    pub(crate) fn try_new_with_predicates_and_schema(
        session: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        predicates: Vec<Expr>,
        validated_schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let df_schema = DFSchema::try_from(input.schema())?;
        if let Some(validation_expr) = conjunction(simplify_predicates(predicates)?) {
            let check_expression = simplify_expr(session, df_schema.into(), validation_expr)?;
            return Ok(Arc::new(Self::try_new(
                input,
                check_expression,
                Some(validated_schema),
            )?));
        }
        Ok(input)
    }

    /// Create a new [`DataValidationExec`]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        check_expression: Arc<dyn PhysicalExpr>,
        validated_schema: Option<SchemaRef>,
    ) -> Result<Self> {
        let result_type = check_expression.data_type(input.schema().as_ref())?;
        if !matches!(result_type, DataType::Boolean) {
            return plan_err!(
                "Data validation expression must return boolean values, got {:?}",
                result_type
            );
        }
        let schema = validated_schema.unwrap_or_else(|| input.schema());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.properties().partitioning.clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            check_expression,
            properties,
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
    fn name(&self) -> &str {
        "DataValidationExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(DataValidationStream::new(
            self.input.execute(partition, context)?,
            self.schema(),
            Arc::clone(&self.check_expression),
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
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
                properties: self.properties.clone(),
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
            properties: self.properties.clone(),
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
                            let invalid_data = filter_record_batch(&batch, &not(validity_mask)?)?;
                            let invalid_slice =
                                invalid_data.slice(0, invalid_data.num_rows().min(5));
                            let preview = pretty_format_batches(&[invalid_slice])?;
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                                DeltaTableError::InvalidData {
                                    message: format!(
                                        "Invalid data found: {invalid_count} rows failed \
                                        validation check.\nPreview of invalid data:\n\n{preview}"
                                    ),
                                },
                            )))));
                        }
                    }
                    ColumnarValue::Scalar(value) => {
                        if !matches!(value, ScalarValue::Boolean(Some(true))) {
                            return Poll::Ready(Some(exec_err!(
                                "Invalid data found: validation check failed with value {value:?}."
                            )));
                        }
                    }
                }
                let (_, arrays, _) = batch.into_parts();
                Poll::Ready(Some(Ok(RecordBatch::try_new(
                    Arc::clone(this.schema),
                    arrays,
                )?)))
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

/// Collect all non-nullable field paths from an Arrow schema.
///
/// This function traverses the schema recursively and returns the paths of all
/// fields that are marked as non-nullable (nullable=false). This is useful for
/// identifying required fields in a schema.
///
/// ## Arguments
/// - `schema`: The Arrow schema to traverse
///
/// ## Returns
/// A vector of ColumnName objects representing paths to all non-nullable fields
///
/// ## Example
/// ```ignore
/// use arrow_schema::Schema;
///
/// let non_nullable_paths = collect_non_nullable_fields(schema.as_ref());
/// // Returns paths like: [ColumnName(["id"]), ColumnName(["person", "name"])]
/// ```
pub(crate) fn collect_non_nullable_fields(schema: &Schema) -> Vec<ColumnName> {
    let mut non_nullable_paths = Vec::new();

    for field in schema.fields() {
        let path = vec![field.name()];
        collect_non_nullable_fields_recursive(field, path, &mut non_nullable_paths);
    }

    non_nullable_paths
}

/// Recursively collect non-nullable field paths.
fn collect_non_nullable_fields_recursive(
    field: &Field,
    current_path: Vec<&String>,
    non_nullable_paths: &mut Vec<ColumnName>,
) {
    // If this field is non-nullable, add it to the collection
    if !field.is_nullable() {
        non_nullable_paths.push(ColumnName::from_iter(current_path.iter().cloned()));
    }

    // Recursively traverse nested types
    match field.data_type() {
        DataType::Struct(fields) => {
            for child in fields.iter() {
                let mut child_path = current_path.clone();
                child_path.push(child.name());
                collect_non_nullable_fields_recursive(child, child_path, non_nullable_paths);
            }
        }
        DataType::Map(child, _) => {
            // Map's child is a struct with "key" and "value" fields
            if let DataType::Struct(fields) = child.data_type() {
                for map_field in fields.iter() {
                    let mut map_path = current_path.clone();
                    map_path.push(map_field.name());
                    collect_non_nullable_fields_recursive(map_field, map_path, non_nullable_paths);
                }
            }
        }
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => {
            // Record a non-nullable element so the collection check can enforce it; we don't
            // recurse further as paths past a list element aren't addressable today.
            if !element.is_nullable() {
                let mut element_path = current_path.clone();
                element_path.push(element.name());
                non_nullable_paths.push(ColumnName::from_iter(element_path.iter().cloned()));
            }
        }
        // For primitive types, we've already recorded if non-nullable above
        _ => {}
    }
}

/// Make specified fields nullable in an Arrow schema.
///
/// This function traverses the schema recursively and sets the specified fields
/// to nullable. This is a one-way operation: non-nullable → nullable only.
/// Fields that are already nullable remain unchanged.
///
/// ## Arguments
/// - `schema`: The Arrow schema to modify
/// - `paths`: Vector of column paths (as ColumnName) to make nullable
///
/// ## Returns
/// A new Arrow schema with updated field nullability
///
/// ## Example
/// ```ignore
/// use delta_kernel::expressions::ColumnName;
/// use arrow_schema::Schema;
///
/// let paths = vec![
///     ColumnName::new(["person", "name"]),
///     ColumnName::new(["id"]),
/// ];
/// let new_schema = make_fields_non_nullable(schema.as_ref(), &paths);
/// ```
#[allow(dead_code)]
pub(crate) fn make_fields_non_nullable(schema: &Schema, paths: &[ColumnName]) -> Schema {
    // Convert ColumnName paths to Vec<String> for easier comparison
    let target_paths: HashSet<Vec<String>> = paths
        .iter()
        .map(|col_name| col_name.path().to_vec())
        .collect();

    // Recursively update fields
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            let path = vec![field.name().to_string()];
            make_fields_non_nullable_recursive(field, path, &target_paths)
        })
        .collect();

    Schema::new(new_fields)
}

/// Recursively make fields non-nullable based on target paths.
#[allow(dead_code)]
fn make_fields_non_nullable_recursive(
    field: &arrow_schema::Field,
    current_path: Vec<String>,
    target_paths: &HashSet<Vec<String>>,
) -> arrow_schema::Field {
    use arrow_schema::{DataType, Field, Fields};

    // Check if this field's path matches any target path
    let should_be_non_nullable = target_paths.contains(&current_path);

    // Process nested types recursively
    let new_data_type = match field.data_type() {
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|child| {
                    let mut child_path = current_path.clone();
                    child_path.push(child.name().to_string());
                    make_fields_non_nullable_recursive(child, child_path, target_paths)
                })
                .collect();
            DataType::Struct(Fields::from(new_fields))
        }
        DataType::List(child) => {
            let mut child_path = current_path.clone();
            child_path.push("element".to_string());
            let new_child = make_fields_non_nullable_recursive(child, child_path, target_paths);
            DataType::List(Arc::new(new_child))
        }
        DataType::LargeList(child) => {
            let mut child_path = current_path.clone();
            child_path.push("element".to_string());
            let new_child = make_fields_non_nullable_recursive(child, child_path, target_paths);
            DataType::LargeList(Arc::new(new_child))
        }
        DataType::Map(child, sorted) => {
            // Map's child is a struct with "key" and "value" fields
            if let DataType::Struct(fields) = child.data_type() {
                let new_fields: Vec<Field> = fields
                    .iter()
                    .map(|map_field| {
                        let mut map_path = current_path.clone();
                        map_path.push(map_field.name().to_string());
                        make_fields_non_nullable_recursive(map_field, map_path, target_paths)
                    })
                    .collect();
                let new_map_struct =
                    Field::new("entries", DataType::Struct(Fields::from(new_fields)), false);
                DataType::Map(Arc::new(new_map_struct), *sorted)
            } else {
                // Shouldn't happen, but keep original if malformed
                field.data_type().clone()
            }
        }
        // For all other types (primitives, etc), keep the data type as-is
        other => other.clone(),
    };

    let new_field = field.clone().with_data_type(new_data_type);
    if should_be_non_nullable {
        new_field.with_nullable(false)
    } else {
        new_field
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use datafusion::catalog::{MemTable, TableProvider};
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::{Extension, LogicalPlanBuilder};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionContext, binary_expr, col};
    use futures::StreamExt;

    use crate::delta_datafusion::create_session;

    use super::*;

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
    async fn test_validation_check_constraint_fail_logical() -> Result<()> {
        let schema = create_test_schema(true);
        let batch = create_test_batch(
            schema.clone(),
            vec![Some(10), Some(2), Some(30)], // One value fails constraint
            vec![Some("a"), Some("b"), Some("c")],
        );

        let ctx = create_session().into_inner();
        let memory_table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();

        // Create validation: id > 5
        let validations = vec![col("id").gt(datafusion::prelude::lit(5i32))];

        let input =
            LogicalPlanBuilder::scan("scan", provider_as_source(Arc::new(memory_table)), None)?
                .build()?;
        let validated_plan = LogicalPlan::Extension(Extension {
            node: DataValidation::try_new(input, validations)?,
        });

        let result = ctx
            .execute_logical_plan(validated_plan)
            .await
            .unwrap()
            .collect()
            .await;

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
        let downcast = validated_exec.downcast_ref::<DataValidationExec>().unwrap();
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
        assert!(new_exec.downcast_ref::<DataValidationExec>().is_some());

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
        assert!(err_msg.contains("validation check failed"));

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

    // Tests for collect_non_nullable_fields

    #[test]
    fn test_collect_non_nullable_simple() {
        // Test collecting non-nullable fields from a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),   // non-nullable
            Field::new("name", DataType::Utf8, true),   // nullable
            Field::new("email", DataType::Utf8, false), // non-nullable
            Field::new("optional", DataType::Int32, true), // nullable
        ]));

        let non_nullable = collect_non_nullable_fields(schema.as_ref());

        assert_eq!(non_nullable.len(), 2);
        assert_eq!(non_nullable[0], ColumnName::new(["id"]));
        assert_eq!(non_nullable[1], ColumnName::new(["email"]));
    }

    #[test]
    fn test_collect_non_nullable_all_nullable() {
        // Test schema with all nullable fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let non_nullable = collect_non_nullable_fields(schema.as_ref());

        assert_eq!(non_nullable.len(), 0);
    }

    #[test]
    fn test_collect_non_nullable_all_non_nullable() {
        // Test schema with all non-nullable fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]));

        let non_nullable = collect_non_nullable_fields(schema.as_ref());

        assert_eq!(non_nullable.len(), 3);
        assert_eq!(non_nullable[0], ColumnName::new(["id"]));
        assert_eq!(non_nullable[1], ColumnName::new(["name"]));
        assert_eq!(non_nullable[2], ColumnName::new(["email"]));
    }

    #[test]
    fn test_collect_non_nullable_nested_struct() {
        // Test collecting from nested struct fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "person",
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, false),  // non-nullable
                        Field::new("age", DataType::Int32, true),   // nullable
                        Field::new("email", DataType::Utf8, false), // non-nullable
                    ]
                    .into(),
                ),
                true, // parent struct is nullable
            ),
        ]));

        let non_nullable = collect_non_nullable_fields(schema.as_ref());

        assert_eq!(non_nullable.len(), 3);
        assert_eq!(non_nullable[0], ColumnName::new(["id"]));
        assert_eq!(non_nullable[1], ColumnName::new(["person", "name"]));
        assert_eq!(non_nullable[2], ColumnName::new(["person", "email"]));
    }

    #[test]
    fn test_collect_non_nullable_deeply_nested() {
        // Test multi-level nesting
        let schema = Arc::new(Schema::new(vec![Field::new(
            "person",
            DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new(
                        "address",
                        DataType::Struct(
                            vec![
                                Field::new("street", DataType::Utf8, false),
                                Field::new("city", DataType::Utf8, true),
                                Field::new("zipcode", DataType::Utf8, false),
                            ]
                            .into(),
                        ),
                        false, // address struct itself is non-nullable
                    ),
                ]
                .into(),
            ),
            true,
        )]));

        let non_nullable = collect_non_nullable_fields(schema.as_ref());

        assert_eq!(non_nullable.len(), 4);
        assert_eq!(non_nullable[0], ColumnName::new(["person", "name"]));
        assert_eq!(non_nullable[1], ColumnName::new(["person", "address"]));
        assert_eq!(
            non_nullable[2],
            ColumnName::new(["person", "address", "street"])
        );
        assert_eq!(
            non_nullable[3],
            ColumnName::new(["person", "address", "zipcode"])
        );
    }

    #[test]
    fn test_collect_non_nullable_list_field() {
        // Test list with non-nullable element
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new(
                    "element",
                    DataType::Utf8,
                    false, // non-nullable element
                ))),
                true, // list itself is nullable
            ),
        ]));

        let non_nullable = collect_non_nullable_fields(schema.as_ref());

        assert_eq!(non_nullable.len(), 2);
        assert_eq!(non_nullable[0], ColumnName::new(["id"]));
        assert_eq!(non_nullable[1], ColumnName::new(["tags", "element"]));
    }

    #[test]
    fn test_collect_non_nullable_multiple_structs() {
        // Test multiple independent structs
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "person",
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("age", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false, // non-nullable
            ),
            Field::new(
                "contact",
                DataType::Struct(
                    vec![
                        Field::new("email", DataType::Utf8, true),
                        Field::new("phone", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                true, // nullable
            ),
        ]));

        let non_nullable = collect_non_nullable_fields(schema.as_ref());

        assert_eq!(non_nullable.len(), 4);
        assert_eq!(non_nullable[0], ColumnName::new(["id"]));
        assert_eq!(non_nullable[1], ColumnName::new(["person"]));
        assert_eq!(non_nullable[2], ColumnName::new(["person", "name"]));
        assert_eq!(non_nullable[3], ColumnName::new(["contact", "phone"]));
    }

    #[test]
    fn test_collect_non_nullable_empty_schema() {
        // Test empty schema
        let schema = Arc::new(Schema::empty());

        let non_nullable = collect_non_nullable_fields(schema.as_ref());

        assert_eq!(non_nullable.len(), 0);
    }

    // Tests for make_fields_nullable

    #[test]
    fn test_make_nullable_simple() {
        // Test making a simple non-nullable field nullable
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let paths = vec![ColumnName::new(["id"])];

        let new_schema = make_fields_non_nullable(schema.as_ref(), &paths);

        // id should now be nullable, name stays nullable
        assert!(!new_schema.field(0).is_nullable());
        assert!(new_schema.field(1).is_nullable());
    }

    #[test]
    fn test_make_non_nullable_already_non_nullable() {
        // Test that already nullable fields remain unchanged
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let paths = vec![ColumnName::new(["id"]), ColumnName::new(["name"])];

        let new_schema = make_fields_non_nullable(schema.as_ref(), &paths);

        // Both should remain nullable
        assert!(!new_schema.field(0).is_nullable());
        assert!(!new_schema.field(1).is_nullable());
    }

    #[test]
    fn test_make_non_nullable_nested_struct() {
        // Test making nested struct field nullable
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new(
                "person",
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, true),
                        Field::new("age", DataType::Int32, false),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let paths = vec![ColumnName::new(["person", "name"])];

        let new_schema = make_fields_non_nullable(schema.as_ref(), &paths);

        // person.name should now be nullable
        if let DataType::Struct(fields) = new_schema.field(1).data_type() {
            assert!(!fields[0].is_nullable()); // name
            assert!(!fields[1].is_nullable()); // age
        } else {
            panic!("Expected Struct type");
        }
    }

    #[test]
    fn test_make_nullable_deeply_nested() {
        // Test multi-level nesting
        let schema = Arc::new(Schema::new(vec![Field::new(
            "person",
            DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new(
                        "address",
                        DataType::Struct(
                            vec![
                                Field::new("street", DataType::Utf8, true),
                                Field::new("city", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        )]));

        let paths = vec![
            ColumnName::new(["person", "address"]),
            ColumnName::new(["person", "address", "street"]),
        ];

        let new_schema = make_fields_non_nullable(schema.as_ref(), &paths);

        // person.address and person.address.street should be nullable
        if let DataType::Struct(person_fields) = new_schema.field(0).data_type() {
            assert!(!person_fields[1].is_nullable()); // address
            if let DataType::Struct(address_fields) = person_fields[1].data_type() {
                assert!(!address_fields[0].is_nullable()); // street
                assert!(address_fields[1].is_nullable()); // city unchanged
            } else {
                panic!("Expected Struct type for address");
            }
        } else {
            panic!("Expected Struct type for person");
        }
    }

    #[test]
    fn test_make_nullable_nonexistent_path() {
        // Test that non-existent paths are silently skipped
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let paths = vec![
            ColumnName::new(["nonexistent"]),
            ColumnName::new(["also", "missing"]),
        ];

        let new_schema = make_fields_non_nullable(schema.as_ref(), &paths);

        // All fields should remain unchanged (non-nullable)
        assert!(!new_schema.field(0).is_nullable());
        assert!(!new_schema.field(1).is_nullable());
    }

    #[test]
    fn test_make_non_nullable_multiple_paths() {
        // Test making multiple fields non-nullable at once
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "person",
                DataType::Struct(
                    vec![
                        Field::new("email", DataType::Utf8, true),
                        Field::new("phone", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let paths = vec![
            ColumnName::new(["id"]),
            ColumnName::new(["person"]),
            ColumnName::new(["person", "email"]),
        ];

        let new_schema = make_fields_non_nullable(schema.as_ref(), &paths);

        // id, person, and person.email should be nullable
        assert!(!new_schema.field(0).is_nullable()); // id
        assert!(new_schema.field(1).is_nullable()); // name unchanged
        assert!(!new_schema.field(2).is_nullable()); // person

        if let DataType::Struct(fields) = new_schema.field(2).data_type() {
            assert!(!fields[0].is_nullable()); // email
            assert!(fields[1].is_nullable()); // phone unchanged
        } else {
            panic!("Expected Struct type");
        }
    }

    // Tests for non-nullable collection element / map value enforcement (#3690)

    use arrow_array::builder::{
        Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    };

    /// Table schema for a Map<Utf8, Int32> column whose value field is non-nullable.
    fn map_table_schema() -> Schema {
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, false),
                ]
                .into(),
            ),
            false,
        );
        Schema::new(vec![Field::new(
            "m",
            DataType::Map(Arc::new(entries), false),
            true,
        )])
    }

    /// Build a map batch from `(key, value)` rows. The physical array declares the value field
    /// nullable (so nulls can be appended); the non-nullable contract lives in the table schema.
    fn map_batch(rows: Vec<Vec<(&str, Option<i32>)>>) -> RecordBatch {
        let names = MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), Int32Builder::new());
        for row in rows {
            for (k, v) in row {
                builder.keys().append_value(k);
                builder.values().append_option(v);
            }
            builder.append(true).unwrap();
        }
        let array = builder.finish();
        RecordBatch::try_from_iter(vec![("m", Arc::new(array) as _)]).unwrap()
    }

    /// Table schema for a List<Int32> column with a non-nullable element.
    fn list_table_schema() -> Schema {
        Schema::new(vec![Field::new(
            "l",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
            true,
        )])
    }

    fn list_batch(rows: Vec<Vec<Option<i32>>>) -> RecordBatch {
        let mut builder = ListBuilder::new(Int32Builder::new());
        for row in rows {
            for v in row {
                builder.values().append_option(v);
            }
            builder.append(true);
        }
        let array = builder.finish();
        RecordBatch::try_from_iter(vec![("l", Arc::new(array) as _)]).unwrap()
    }

    /// Derive the collection check from `table_schema` and run it against `batch`.
    async fn run_collection_check(table_schema: &Schema, batch: RecordBatch) -> Result<()> {
        let check = collect_non_nullable_fields(table_schema)
            .into_iter()
            .find_map(|p| non_nullable_collection_check_expr(&p, table_schema))
            .expect("expected a collection check");
        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), batch.schema(), vec![batch]).await;
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, vec![check])?;
        collect(validated_exec, ctx.task_ctx()).await.map(|_| ())
    }

    #[tokio::test]
    async fn test_map_null_value_rejected() {
        // The literal #3690 repro: real keys, a null value in one row.
        let batch = map_batch(vec![
            vec![("a", Some(1)), ("b", Some(2))],
            vec![("a", None)],
        ]);
        let result = run_collection_check(&map_table_schema(), batch).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid data found")
        );
    }

    #[tokio::test]
    async fn test_map_all_values_present_accepted() {
        let batch = map_batch(vec![
            vec![("a", Some(1)), ("b", Some(2))],
            vec![("c", Some(3))],
        ]);
        assert!(
            run_collection_check(&map_table_schema(), batch)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_list_null_element_rejected() {
        let batch = list_batch(vec![vec![Some(1), None, Some(3)]]);
        assert!(
            run_collection_check(&list_table_schema(), batch)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_list_valid_accepted() {
        let batch = list_batch(vec![vec![Some(1), Some(2)]]);
        assert!(
            run_collection_check(&list_table_schema(), batch)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_list_empty_accepted() {
        let batch = list_batch(vec![vec![]]);
        assert!(
            run_collection_check(&list_table_schema(), batch)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_list_all_null_rejected() {
        let batch = list_batch(vec![vec![None, None]]);
        assert!(
            run_collection_check(&list_table_schema(), batch)
                .await
                .is_err()
        );
    }

    /// Derive the scalar check from `table_schema` and run it against `batch`.
    async fn run_scalar_check(table_schema: &Schema, batch: RecordBatch) -> Result<()> {
        let check = collect_non_nullable_fields(table_schema)
            .into_iter()
            .find_map(|p| non_nullable_scalar_check_expr(&p, table_schema))
            .expect("expected a scalar check");
        let ctx = SessionContext::new();
        let memory_exec = get_memory_exec(&ctx.state(), batch.schema(), vec![batch]).await;
        let validated_exec =
            DataValidationExec::try_new_with_predicates(&ctx.state(), memory_exec, vec![check])?;
        collect(validated_exec, ctx.task_ctx()).await.map(|_| ())
    }

    /// Table schema with a non-nullable struct field, and a batch with a nullable physical field.
    fn struct_table_schema() -> Schema {
        Schema::new(vec![Field::new(
            "person",
            DataType::Struct(vec![Field::new("name", DataType::Utf8, false)].into()),
            true,
        )])
    }

    fn struct_batch(names: Vec<Option<&str>>) -> RecordBatch {
        let array = arrow_array::StructArray::from(vec![(
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(StringArray::from(names)) as Arc<dyn arrow_array::Array>,
        )]);
        RecordBatch::try_from_iter(vec![("person", Arc::new(array) as _)]).unwrap()
    }

    #[tokio::test]
    async fn test_struct_field_null_rejected() {
        let batch = struct_batch(vec![Some("a"), None]);
        assert!(
            run_scalar_check(&struct_table_schema(), batch)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_struct_field_present_accepted() {
        let batch = struct_batch(vec![Some("a"), Some("b")]);
        assert!(
            run_scalar_check(&struct_table_schema(), batch)
                .await
                .is_ok()
        );
    }

    #[test]
    fn test_make_nullable_preserves_metadata() {
        // Test that field metadata is preserved
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true).with_metadata(metadata.clone()),
        ]));

        let paths = vec![ColumnName::new(["id"])];

        let new_schema = make_fields_non_nullable(schema.as_ref(), &paths);

        // Nullability should be updated and metadata preserved
        assert!(!new_schema.field(0).is_nullable());
        assert_eq!(new_schema.field(0).metadata(), &metadata);
    }
}
