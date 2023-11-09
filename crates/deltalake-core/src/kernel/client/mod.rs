//! Delta kernel client implementation.
use std::sync::Arc;

use arrow_array::RecordBatch;

use self::expressions::evaluate_expression;
use crate::kernel::error::DeltaResult;
use crate::kernel::expressions::Expression;
use crate::kernel::schema::SchemaRef;

pub mod expressions;

/// Interface for implementing an Expression evaluator.
///
/// It contains one Expression which can be evaluated on multiple ColumnarBatches.
/// Connectors can implement this interface to optimize the evaluation using the
/// connector specific capabilities.
pub trait ExpressionEvaluator {
    /// Evaluate the expression on given ColumnarBatch data.
    ///
    /// Contains one value for each row of the input.
    /// The data type of the output is same as the type output of the expression this evaluator is using.
    fn evaluate(&self, batch: &RecordBatch, output_schema: SchemaRef) -> DeltaResult<RecordBatch>;
}

#[derive(Debug)]
/// Expression evaluator based on arrow compute kernels.
pub struct ArrowExpressionEvaluator {
    _input_schema: SchemaRef,
    expression: Box<Expression>,
}

impl ExpressionEvaluator for ArrowExpressionEvaluator {
    fn evaluate(&self, batch: &RecordBatch, output_schema: SchemaRef) -> DeltaResult<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(output_schema.as_ref().try_into()?),
            vec![evaluate_expression(&self.expression, batch)?],
        )?)
    }
}
