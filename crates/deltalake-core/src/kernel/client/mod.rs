//! Delta kernel client implementation.

use arrow_array::RecordBatch;

use super::error::DeltaResult;

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
    fn evaluate(&self, batch: &RecordBatch) -> DeltaResult<RecordBatch>;
}
