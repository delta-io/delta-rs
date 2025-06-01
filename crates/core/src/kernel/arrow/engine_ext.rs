//! Utilities for interacting with Kernel APIs using Arrow data structures.
//!
use delta_kernel::arrow::array::BooleanArray;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::scan::{Scan, ScanMetadata};
use delta_kernel::{
    DeltaResult, Engine, EngineData, ExpressionEvaluator, ExpressionRef, PredicateRef, Version,
};
use itertools::Itertools;

/// [`ScanMetadata`] contains (1) a [`RecordBatch`] specifying data files to be scanned
/// and (2) a vector of transforms (one transform per scan file) that must be applied to the data read
/// from those files.
pub(crate) struct ScanMetadataArrow {
    /// Record batch with one row per file to scan
    pub scan_files: RecordBatch,

    /// Row-level transformations to apply to data read from files.
    ///
    /// Each entry in this vector corresponds to a row in the `scan_files` data. The entry is an
    /// expression that must be applied to convert the file's data into the logical schema
    /// expected by the scan:
    ///
    /// - `Some(expr)`: Apply this expression to transform the data to match [`Scan::schema()`].
    /// - `None`: No transformation is needed; the data is already in the correct logical form.
    ///
    /// Note: This vector can be indexed by row number.
    pub scan_file_transforms: Vec<Option<ExpressionRef>>,
}

pub(crate) trait ScanExt {
    /// Get the metadata for a table scan.
    ///
    /// This method handles translation between `EngineData` and `RecordBatch`
    /// and will already apply any selection vectors to the data.
    /// See [`Scan::scan_metadata`] for details.
    fn scan_metadata_arrow(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;

    fn scan_metadata_from_arrow(
        &self,
        engine: &dyn Engine,
        existing_version: Version,
        existing_data: Box<dyn Iterator<Item = RecordBatch>>,
        existing_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;
}

impl ScanExt for Scan {
    fn scan_metadata_arrow(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
        Ok(self
            .scan_metadata(engine)?
            .map_ok(kernel_to_arrow)
            .flatten())
    }

    fn scan_metadata_from_arrow(
        &self,
        engine: &dyn Engine,
        existing_version: Version,
        existing_data: Box<dyn Iterator<Item = RecordBatch>>,
        existing_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
        let engine_iter =
            existing_data.map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>);
        Ok(self
            .scan_metadata_from(engine, existing_version, engine_iter, existing_predicate)?
            .map_ok(kernel_to_arrow)
            .flatten())
    }
}

fn kernel_to_arrow(metadata: ScanMetadata) -> DeltaResult<ScanMetadataArrow> {
    let scan_file_transforms = metadata
        .scan_file_transforms
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| metadata.scan_files.selection_vector[i].then_some(v))
        .collect();
    let batch = ArrowEngineData::try_from_engine_data(metadata.scan_files.data)?.into();
    let scan_files = filter_record_batch(
        &batch,
        &BooleanArray::from(metadata.scan_files.selection_vector),
    )?;
    Ok(ScanMetadataArrow {
        scan_files,
        scan_file_transforms,
    })
}

pub(crate) trait ExpressionEvaluatorExt {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch>;
}

impl<T: ExpressionEvaluator + ?Sized> ExpressionEvaluatorExt for T {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let engine_data = ArrowEngineData::new(batch);
        Ok(ArrowEngineData::try_from_engine_data(T::evaluate(self, &engine_data)?)?.into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::ExpressionEvaluatorExt as _;

    use delta_kernel::arrow::array::Int32Array;
    use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::engine::arrow_conversion::TryIntoKernel;
    use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
    use delta_kernel::expressions::*;
    use delta_kernel::EvaluationHandler;

    #[test]
    fn test_evaluate_arrow() {
        let handler = ArrowEvaluationHandler;

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();

        let expression = column_expr!("a");
        let expr = handler.new_expression_evaluator(
            Arc::new((&schema).try_into_kernel().unwrap()),
            expression,
            delta_kernel::schema::DataType::INTEGER,
        );

        let result = expr.evaluate_arrow(batch);
        assert!(result.is_ok());
    }
}
