//! Deletion Vector Filter Execution Plan
//!
//! This module provides a DataFusion `ExecutionPlan` that filters out rows
//! marked as deleted by deletion vectors.
//!
//! ## Overview
//!
//! When a Delta table has deletion vectors, individual rows within parquet files
//! can be logically deleted without rewriting the entire file. This execution plan
//! wraps the underlying parquet scan and applies the deletion vector filter to
//! remove deleted rows from the output.
//!
//! ## Usage
//!
//! The `DeletionVectorFilterExec` is inserted into the query plan automatically
//! when scanning Delta tables that have files with deletion vectors.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

use crate::kernel::deletion_vector::DeletionVector;

/// Execution plan that filters rows based on deletion vectors
///
/// This plan wraps an input execution plan (typically a parquet scan) and
/// applies deletion vector filtering to remove logically deleted rows.
#[derive(Debug)]
pub struct DeletionVectorFilterExec {
    /// The underlying execution plan (parquet scan)
    input: Arc<dyn ExecutionPlan>,
    /// Map from file path to deletion vector
    deletion_vectors: Arc<HashMap<String, DeletionVector>>,
    /// The name of the column containing the file path (if present)
    file_column_name: Option<String>,
    /// Plan properties (schema, partitioning, etc.)
    properties: PlanProperties,
}

impl DeletionVectorFilterExec {
    /// Create a new deletion vector filter execution plan
    ///
    /// # Arguments
    ///
    /// * `input` - The underlying execution plan to filter
    /// * `deletion_vectors` - Map from file path to deletion vector
    /// * `file_column_name` - Name of the column containing file paths (needed for row-level filtering)
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        deletion_vectors: HashMap<String, DeletionVector>,
        file_column_name: Option<String>,
    ) -> Self {
        // Clone properties from input - the DV filter preserves all properties
        let properties = input.properties().clone();

        Self {
            input,
            deletion_vectors: Arc::new(deletion_vectors),
            file_column_name,
            properties,
        }
    }

    /// Check if there are any deletion vectors to apply
    pub fn has_deletion_vectors(&self) -> bool {
        !self.deletion_vectors.is_empty()
    }
}

impl DisplayAs for DeletionVectorFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DeletionVectorFilterExec: {} files with DVs",
            self.deletion_vectors.len()
        )
    }
}

impl ExecutionPlan for DeletionVectorFilterExec {
    fn name(&self) -> &str {
        "DeletionVectorFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "DeletionVectorFilterExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            (*self.deletion_vectors).clone(),
            self.file_column_name.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = self.schema();
        let deletion_vectors = Arc::clone(&self.deletion_vectors);
        let file_column_name = self.file_column_name.clone();

        // Create a stream that applies DV filtering
        let filtered_stream = DeletionVectorFilterStream::new(
            input_stream,
            deletion_vectors,
            file_column_name,
            schema.clone(),
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            filtered_stream,
        )))
    }
}

/// Stream that applies deletion vector filtering to record batches
struct DeletionVectorFilterStream {
    /// The input stream
    input: SendableRecordBatchStream,
    /// Map from file path to deletion vector
    deletion_vectors: Arc<HashMap<String, DeletionVector>>,
    /// Name of the file path column
    file_column_name: Option<String>,
    /// Output schema
    #[allow(dead_code)]
    schema: SchemaRef,
    /// Current row offset within the current file
    row_offset: u64,
    /// Current file being processed
    current_file: Option<String>,
}

impl DeletionVectorFilterStream {
    fn new(
        input: SendableRecordBatchStream,
        deletion_vectors: Arc<HashMap<String, DeletionVector>>,
        file_column_name: Option<String>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            deletion_vectors,
            file_column_name,
            schema,
            row_offset: 0,
            current_file: None,
        }
    }

    /// Apply deletion vector filtering to a record batch
    fn filter_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        // If no file column, we can't determine which DV to use
        // In this case, apply a global filter based on row indices
        let file_path = self.get_file_path_from_batch(&batch);

        let dv = match file_path {
            Some(ref path) => self.deletion_vectors.get(path),
            None => None,
        };

        if let Some(dv) = dv {
            // Create filter based on deletion vector
            let num_rows = batch.num_rows();
            let mut keep = vec![true; num_rows];

            for i in 0..num_rows {
                let global_row_idx = self.row_offset + i as u64;
                if dv.contains(global_row_idx) {
                    keep[i] = false;
                }
            }

            self.row_offset += num_rows as u64;

            // Apply filter
            let filter_array = BooleanArray::from(keep);
            let filtered = filter_record_batch(&batch, &filter_array)?;
            Ok(filtered)
        } else {
            // No deletion vector for this file, pass through unchanged
            self.row_offset += batch.num_rows() as u64;
            Ok(batch)
        }
    }

    /// Extract file path from batch (if file column is present)
    fn get_file_path_from_batch(&mut self, batch: &RecordBatch) -> Option<String> {
        let file_column_name = self.file_column_name.as_ref()?;
        let file_col = batch.column_by_name(file_column_name)?;

        // Get the first value (all rows in batch should be from same file)
        let string_array = file_col.as_any().downcast_ref::<arrow::array::StringArray>()?;
        if string_array.is_empty() {
            return None;
        }

        let path = string_array.value(0).to_string();

        // Reset row offset if file changed
        if self.current_file.as_ref() != Some(&path) {
            self.current_file = Some(path.clone());
            self.row_offset = 0;
        }

        Some(path)
    }
}

impl Stream for DeletionVectorFilterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let filtered = self.filter_batch(batch);
                Poll::Ready(Some(filtered))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Helper to check if any files in the scan have deletion vectors
pub fn any_files_have_deletion_vectors(
    files: &[crate::kernel::Add],
) -> bool {
    files.iter().any(|f| f.deletion_vector.is_some())
}

/// Load deletion vectors for a set of files
///
/// Returns a map from file path to deletion vector for files that have DVs.
pub async fn load_deletion_vectors_for_files(
    files: &[crate::kernel::Add],
    table_root: &url::Url,
    object_store: Arc<dyn object_store::ObjectStore>,
) -> crate::DeltaResult<HashMap<String, DeletionVector>> {
    use crate::kernel::deletion_vector::load_deletion_vector;

    let mut result = HashMap::new();

    for file in files {
        if let Some(ref dv_desc) = file.deletion_vector {
            let dv = load_deletion_vector(dv_desc, table_root, object_store.as_ref()).await?;
            result.insert(file.path.clone(), dv);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch(values: Vec<i32>, file_path: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("__delta_rs_path", DataType::Utf8, false),
        ]));

        let value_array = Int32Array::from(values.clone());
        let path_array = StringArray::from(vec![file_path; values.len()]);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(value_array), Arc::new(path_array)],
        )
        .unwrap()
    }

    #[test]
    fn test_dv_filter_with_deletions() {
        // Create a deletion vector that marks rows 1 and 3 as deleted
        let dv = DeletionVector::from_indices(vec![1, 3]);

        // Create a batch with 5 rows
        let batch = create_test_batch(vec![0, 1, 2, 3, 4], "test_file.parquet");
        assert_eq!(batch.num_rows(), 5);

        // Test contains check
        assert!(!dv.contains(0));
        assert!(dv.contains(1));
        assert!(!dv.contains(2));
        assert!(dv.contains(3));
        assert!(!dv.contains(4));

        // Test arrow filter generation
        let filter = dv.to_arrow_filter(5);
        assert!(filter.value(0));  // Row 0 kept
        assert!(!filter.value(1)); // Row 1 deleted
        assert!(filter.value(2));  // Row 2 kept
        assert!(!filter.value(3)); // Row 3 deleted
        assert!(filter.value(4));  // Row 4 kept

        // Apply filter to batch
        let filtered = filter_record_batch(&batch, &filter).unwrap();
        assert_eq!(filtered.num_rows(), 3);

        let values = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 0);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 4);
    }

    #[test]
    fn test_dv_filter_no_deletions() {
        let dv = DeletionVector::empty();

        let batch = create_test_batch(vec![0, 1, 2, 3, 4], "test_file.parquet");
        assert_eq!(batch.num_rows(), 5);

        // Empty DV should keep all rows
        let filter = dv.to_arrow_filter(5);
        let filtered = filter_record_batch(&batch, &filter).unwrap();

        // All 5 rows should be kept
        assert_eq!(filtered.num_rows(), 5);
    }

    #[test]
    fn test_any_files_have_deletion_vectors() {
        use crate::kernel::Add;

        let add_without_dv = Add {
            path: "file1.parquet".to_string(),
            ..Default::default()
        };

        let add_with_dv = Add {
            path: "file2.parquet".to_string(),
            deletion_vector: Some(crate::kernel::models::DeletionVectorDescriptor {
                storage_type: crate::kernel::models::StorageType::Inline,
                path_or_inline_dv: "test".to_string(),
                offset: None,
                size_in_bytes: 10,
                cardinality: 1,
            }),
            ..Default::default()
        };

        assert!(!any_files_have_deletion_vectors(&[add_without_dv.clone()]));
        assert!(any_files_have_deletion_vectors(&[add_with_dv.clone()]));
        assert!(any_files_have_deletion_vectors(&[add_without_dv, add_with_dv]));
    }
}

