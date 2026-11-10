use std::sync::Arc;

use deltalake::arrow::array::{BooleanBuilder, LargeStringBuilder, ListBuilder, RecordBatchReader};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::delta_datafusion::{DeletionVectorSelection, SendableDeletionVectorStream};
use futures::StreamExt;

use crate::utils::rt;

fn deletion_vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("filepath", DataType::LargeUtf8, false),
        Field::new(
            "selection_vector",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, false))),
            false,
        ),
    ]))
}

struct DeletionVectorReader {
    stream: Option<SendableDeletionVectorStream>,
}

impl Iterator for DeletionVectorReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream = self.stream.as_mut()?;
        let next = tokio::task::block_in_place(|| rt().block_on(stream.next()));
        let result = match next {
            Some(Ok(vector)) => deletion_vector_batch(vector),
            Some(Err(err)) => Err(ArrowError::ExternalError(Box::new(err))),
            None => {
                self.stream = None;
                return None;
            }
        };
        if result.is_err() {
            self.stream = None;
        }
        Some(result)
    }
}

impl RecordBatchReader for DeletionVectorReader {
    fn schema(&self) -> SchemaRef {
        deletion_vector_schema()
    }
}

pub(crate) fn deletion_vectors_reader(
    stream: SendableDeletionVectorStream,
) -> Box<dyn RecordBatchReader + Send> {
    Box::new(DeletionVectorReader {
        stream: Some(stream),
    })
}

fn validate_mask_length(length: usize) -> Result<(), ArrowError> {
    if length > i32::MAX as usize {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Deletion vector keep mask length {length} exceeds List<Boolean> offset limit {}",
            i32::MAX
        )));
    }
    Ok(())
}

fn deletion_vector_batch(vector: DeletionVectorSelection) -> Result<RecordBatch, ArrowError> {
    validate_mask_length(vector.keep_mask.len())?;
    let mut paths = LargeStringBuilder::new();
    paths.append_value(vector.filepath);
    let mut masks = ListBuilder::new(BooleanBuilder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Boolean,
        false,
    )));
    masks.values().append_slice(&vector.keep_mask);
    masks.append(true);
    RecordBatch::try_new(
        deletion_vector_schema(),
        vec![Arc::new(paths.finish()), Arc::new(masks.finish())],
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use deltalake::DeltaTableError;
    use deltalake::arrow::array::{BooleanArray, LargeStringArray, ListArray};
    use futures::stream;

    use super::*;

    #[test]
    fn reader_loads_only_the_requested_file() {
        let loaded = Arc::new(AtomicUsize::new(0));
        let count = loaded.clone();
        let stream = stream::iter(0..1000).map(move |i| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(DeletionVectorSelection {
                filepath: format!("f{i}"),
                keep_mask: vec![true, false, true],
            })
        });
        let mut reader = deletion_vectors_reader(Box::pin(stream));
        let schema = reader.schema();
        assert_eq!(loaded.load(Ordering::SeqCst), 0);
        assert_eq!(schema.field(0).data_type(), &DataType::LargeUtf8);
        let DataType::List(item) = schema.field(1).data_type() else {
            panic!("expected List<Boolean>");
        };
        assert_eq!(item.data_type(), &DataType::Boolean);
        assert!(!item.is_nullable());

        for i in 0..2 {
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(batch.schema(), schema);
            assert_eq!(batch.num_rows(), 1);
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            assert_eq!(paths.value(0), format!("f{i}"));
            let masks = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let mask = masks.value(0);
            let mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(
                mask.values().iter().collect::<Vec<_>>(),
                vec![true, false, true]
            );
            assert_eq!(loaded.load(Ordering::SeqCst), i + 1);
        }
        drop(reader);
        assert_eq!(loaded.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn reader_propagates_errors_without_polling_subsequent_files() {
        let polled = Arc::new(AtomicUsize::new(0));
        let count = polled.clone();
        let stream = stream::iter(0..3).map(move |i| {
            count.fetch_add(1, Ordering::SeqCst);
            if i == 1 {
                Err(DeltaTableError::generic("broken DV"))
            } else {
                Ok(DeletionVectorSelection {
                    filepath: format!("f{i}"),
                    keep_mask: vec![],
                })
            }
        });
        let mut reader = deletion_vectors_reader(Box::pin(stream));
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        assert!(
            reader
                .next()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("broken DV")
        );
        assert!(reader.next().is_none());
        assert_eq!(polled.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn empty_reader_has_a_schema_and_stays_exhausted() {
        let mut reader = deletion_vectors_reader(Box::pin(stream::empty()));
        assert_eq!(reader.schema().fields().len(), 2);
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
    }

    #[test]
    fn list_offset_limit_is_checked_without_allocating_masks() {
        let limit = i32::MAX as usize;
        assert!(validate_mask_length(limit).is_ok());
        assert!(matches!(
            validate_mask_length(limit + 1),
            Err(ArrowError::InvalidArgumentError(_))
        ));
        assert!(validate_mask_length(usize::MAX).is_err());
    }
}
