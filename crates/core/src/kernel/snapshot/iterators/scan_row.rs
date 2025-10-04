use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::expressions::UnaryExpressionOp;
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::DataType;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::{EvaluationHandler, Expression, ExpressionEvaluator};
use futures::Stream;
use pin_project_lite::pin_project;

use crate::kernel::arrow::engine_ext::SnapshotExt;
use crate::kernel::ARROW_HANDLER;
use crate::DeltaResult;

pin_project! {
    pub(crate) struct ScanRowOutStream<S> {
        snapshot: Arc<KernelSnapshot>,

        #[pin]
        stream: S,
    }
}

impl<S> ScanRowOutStream<S> {
    pub fn new(snapshot: Arc<KernelSnapshot>, stream: S) -> Self {
        Self { snapshot, stream }
    }
}

impl<S> Stream for ScanRowOutStream<S>
where
    S: Stream<Item = DeltaResult<RecordBatch>>,
{
    type Item = DeltaResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => match this.snapshot.parse_stats_column(&batch) {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(err) => Poll::Ready(Some(Err(err))),
            },
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

pub(crate) fn scan_row_in_eval(
    snapshot: &KernelSnapshot,
) -> DeltaResult<Arc<dyn ExpressionEvaluator>> {
    static EXPRESSION: LazyLock<Arc<Expression>> = LazyLock::new(|| {
        Expression::struct_from(scan_row_schema().fields().map(|field| {
            if field.name == "stats" {
                Expression::unary(
                    UnaryExpressionOp::ToJson,
                    Expression::column(["stats_parsed"]),
                )
            } else {
                Expression::column([field.name.clone()])
            }
        }))
        .into()
    });
    static OUT_TYPE: LazyLock<DataType> =
        LazyLock::new(|| DataType::Struct(Box::new(scan_row_schema().as_ref().clone())));

    let input_schema = snapshot.scan_row_parsed_schema_arrow()?;
    let input_schema = Arc::new(input_schema.as_ref().try_into_kernel()?);

    Ok(ARROW_HANDLER.new_expression_evaluator(input_schema, EXPRESSION.clone(), OUT_TYPE.clone()))
}
