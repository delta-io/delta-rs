//! Delta kernel client implementation.
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema as ArrowSchema;

use self::expressions::evaluate_expression;
use crate::kernel::error::DeltaResult;
use crate::kernel::expressions::Expression;
use crate::kernel::schema::SchemaRef;

use super::DataType;

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
    fn evaluate(&self, batch: &RecordBatch) -> DeltaResult<ArrayRef>;
}

/// Provides expression evaluation capability to Delta Kernel.
///
/// Delta Kernel can use this client to evaluate predicate on partition filters,
/// fill up partition column values and any computation on data using Expressions.
pub trait ExpressionHandler {
    /// Create an [`ExpressionEvaluator`] that can evaluate the given [`Expression`]
    /// on columnar batches with the given [`Schema`] to produce data of [`DataType`].
    ///
    /// # Parameters
    ///
    /// - `schema`: Schema of the input data.
    /// - `expression`: Expression to evaluate.
    /// - `output_type`: Expected result data type.
    ///
    /// [`Schema`]: crate::schema::StructType
    /// [`DataType`]: crate::schema::DataType
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator>;
}

/// Default implementation of [`ExpressionHandler`] that uses [`evaluate_expression`]
#[derive(Debug)]
pub struct ArrowExpressionHandler {}

impl ExpressionHandler for ArrowExpressionHandler {
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator> {
        Arc::new(DefaultExpressionEvaluator {
            input_schema: schema,
            expression: Box::new(expression),
            output_type,
        })
    }
}

/// Default implementation of [`ExpressionEvaluator`] that uses [`evaluate_expression`]
#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    input_schema: SchemaRef,
    expression: Box<Expression>,
    output_type: DataType,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &RecordBatch) -> DeltaResult<ArrayRef> {
        let _input_schema: ArrowSchema = self.input_schema.as_ref().try_into()?;
        // TODO: make sure we have matching schemas for validation
        // if batch.schema().as_ref() != &input_schema {
        //     return Err(Error::Generic(format!(
        //         "input schema does not match batch schema: {:?} != {:?}",
        //         input_schema,
        //         batch.schema()
        //     )));
        // };
        evaluate_expression(&self.expression, batch, Some(&self.output_type))
    }
}
