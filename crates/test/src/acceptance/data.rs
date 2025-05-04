use std::{path::Path, sync::Arc};

use arrow_array::{Array, RecordBatch};
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use arrow_schema::{DataType, Schema};
use arrow_select::{concat::concat_batches, take::take};
use delta_kernel::DeltaResult;
use futures::{stream::TryStreamExt, StreamExt};
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use super::TestCaseInfo;
use crate::TestResult;

pub async fn read_golden(path: &Path, _version: Option<&str>) -> DeltaResult<RecordBatch> {
    let expected_root = path.join("expected").join("latest").join("table_content");
    let store = Arc::new(LocalFileSystem::new_with_prefix(&expected_root)?);
    let files: Vec<_> = store.list(None).try_collect().await?;
    let mut batches = vec![];
    let mut schema = None;
    for meta in files.into_iter() {
        if let Some(ext) = meta.location.extension() {
            if ext == "parquet" {
                let reader = ParquetObjectReader::new(store.clone(), meta.location);
                let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
                if schema.is_none() {
                    schema = Some(builder.schema().clone());
                }
                let mut stream = builder.build()?;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
            }
        }
    }
    let all_data = concat_batches(&schema.unwrap(), &batches)?;
    Ok(all_data)
}

pub fn sort_record_batch(batch: RecordBatch) -> DeltaResult<RecordBatch> {
    // Sort by all columns
    let mut sort_columns = vec![];
    for col in batch.columns() {
        match col.data_type() {
            DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
                // can't sort structs, lists, or maps
            }
            _ => sort_columns.push(SortColumn {
                values: col.clone(),
                options: None,
            }),
        }
    }
    let indices = lexsort_to_indices(&sort_columns, None)?;
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

// Ensure that two schema have the same field names, and dict_id/ordering.
// We ignore:
//  - data type: This is checked already in `assert_columns_match`
//  - nullability: parquet marks many things as nullable that we don't in our schema
//  - metadata: because that diverges from the real data to the golden tabled data
fn assert_schema_fields_match(schema: &Schema, golden: &Schema) {
    for (schema_field, golden_field) in schema.fields.iter().zip(golden.fields.iter()) {
        assert!(
            schema_field.name() == golden_field.name(),
            "Field names don't match"
        );
        assert!(
            schema_field.dict_id() == golden_field.dict_id(),
            "Field dict_id doesn't match"
        );
        assert!(
            schema_field.dict_is_ordered() == golden_field.dict_is_ordered(),
            "Field dict_is_ordered doesn't match"
        );
    }
}

// some things are equivalent, but don't show up as equivalent for `==`, so we normalize here
fn normalize_col(col: Arc<dyn Array>) -> Arc<dyn Array> {
    if let DataType::Timestamp(unit, Some(zone)) = col.data_type() {
        if **zone == *"+00:00" {
            arrow_cast::cast::cast(&col, &DataType::Timestamp(*unit, Some("UTC".into())))
                .expect("Could not cast to UTC")
        } else {
            col
        }
    } else {
        col
    }
}

fn assert_columns_match(actual: &[Arc<dyn Array>], expected: &[Arc<dyn Array>]) {
    for (actual, expected) in actual.iter().zip(expected) {
        let actual = normalize_col(actual.clone());
        let expected = normalize_col(expected.clone());
        // note that array equality includes data_type equality
        // See: https://arrow.apache.org/rust/arrow_data/equal/fn.equal.html
        assert_eq!(
            &actual, &expected,
            "Column data didn't match. Got {actual:?}, expected {expected:?}"
        );
    }
}

pub async fn assert_scan_data(
    all_data: Vec<RecordBatch>,
    test_case: &TestCaseInfo,
) -> TestResult<()> {
    let all_data = concat_batches(&all_data[0].schema(), all_data.iter()).unwrap();
    let all_data = sort_record_batch(all_data)?;

    let golden = read_golden(test_case.root_dir(), None).await?;
    let golden = sort_record_batch(golden)?;

    assert_columns_match(all_data.columns(), golden.columns());
    assert_schema_fields_match(all_data.schema().as_ref(), golden.schema().as_ref());
    assert!(
        all_data.num_rows() == golden.num_rows(),
        "Didn't have same number of rows"
    );

    Ok(())
}
