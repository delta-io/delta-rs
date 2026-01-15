//!
//! The CDC module contains private tools for managing CDC files
//!

use crate::DeltaResult;

use datafusion::common::ScalarValue;
use datafusion::prelude::*;

pub const CDC_COLUMN_NAME: &str = "_change_type";

/// The CDCTracker is useful for hooking reads/writes in a manner nececessary to create CDC files
/// associated with commits
pub(crate) struct CDCTracker {
    pre_dataframe: DataFrame,
    post_dataframe: DataFrame,
}

impl CDCTracker {
    ///  construct
    pub(crate) fn new(pre_dataframe: DataFrame, post_dataframe: DataFrame) -> Self {
        Self {
            pre_dataframe,
            post_dataframe,
        }
    }

    pub(crate) fn collect(self) -> DeltaResult<DataFrame> {
        // Collect _all_ the batches for consideration
        let pre_df = self.pre_dataframe;
        let post_df = self.post_dataframe;

        // There is certainly a better way to do this other than stupidly cloning data for diffing
        // purposes, but this is the quickest and easiest way to "diff" the two sets of batches
        let preimage = pre_df.clone().except(post_df.clone())?;
        let postimage = post_df.except(pre_df)?;

        let preimage = preimage.with_column(
            "_change_type",
            lit(ScalarValue::Utf8(Some("update_preimage".to_string()))),
        )?;

        let postimage = postimage.with_column(
            "_change_type",
            lit(ScalarValue::Utf8(Some("update_postimage".to_string()))),
        )?;

        let final_df = preimage.union(postimage)?;
        Ok(final_df)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StructArray};
    use arrow::datatypes::{DataType, Field};
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::datasource::{MemTable, TableProvider};

    use super::*;

    #[tokio::test]
    async fn test_sanity_check() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema.clone()),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        )
        .unwrap();
        let table_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let source_df = ctx.read_table(table_provider).unwrap();

        let updated_batch = RecordBatch::try_new(
            Arc::clone(&schema.clone()),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(12), Some(3)]))],
        )
        .unwrap();
        let table_provider_updated: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![updated_batch]]).unwrap());
        let updated_df = ctx.read_table(table_provider_updated).unwrap();

        let tracker = CDCTracker::new(source_df, updated_df);

        match tracker.collect() {
            Ok(df) => {
                let batches = &df.collect().await.unwrap();
                let _ = arrow::util::pretty::print_batches(batches);
                assert_eq!(batches.len(), 2);
                assert_batches_sorted_eq! {[
                "+-------+------------------+",
                "| value | _change_type     |",
                "+-------+------------------+",
                "| 2     | update_preimage  |",
                "| 12    | update_postimage |",
                "+-------+------------------+",
                    ], &batches }
            }
            Err(err) => {
                println!("err: {err:#?}");
                panic!("Should have never reached this assertion");
            }
        }
    }

    #[tokio::test]
    async fn test_sanity_check_with_pure_df() {
        let nested_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("lat", DataType::Int32, true),
            Field::new("long", DataType::Int32, true),
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new(
                "nested",
                DataType::Struct(nested_schema.fields.clone()),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("id", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("lat", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("long", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();

        let updated_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(12), Some(3)])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("id", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("lat", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("long", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();
        let _ = arrow::util::pretty::print_batches(&[batch.clone()]);
        let _ = arrow::util::pretty::print_batches(&[updated_batch.clone()]);

        let ctx = SessionContext::new();
        let before = ctx.read_batch(batch).expect("Failed to make DataFrame");
        let after = ctx
            .read_batch(updated_batch)
            .expect("Failed to make DataFrame");

        let diff = before
            .except(after)
            .expect("Failed to except")
            .collect()
            .await
            .expect("Failed to diff");
        assert_eq!(diff.len(), 1);
    }

    #[tokio::test]
    async fn test_sanity_check_with_struct() {
        let ctx = SessionContext::new();
        let nested_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("lat", DataType::Int32, true),
            Field::new("long", DataType::Int32, true),
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new(
                "nested",
                DataType::Struct(nested_schema.fields.clone()),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("id", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("lat", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("long", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();
        let table_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let source_df = ctx.read_table(table_provider).unwrap();

        let updated_batch = RecordBatch::try_new(
            Arc::clone(&schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(12), Some(3)])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("id", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("lat", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("long", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();
        let table_provider_updated: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![updated_batch]]).unwrap());
        let updated_df = ctx.read_table(table_provider_updated).unwrap();

        let tracker = CDCTracker::new(source_df, updated_df);

        match tracker.collect() {
            Ok(df) => {
                let batches = &df.collect().await.unwrap();
                let _ = arrow::util::pretty::print_batches(batches);
                assert_eq!(batches.len(), 2);
                assert_batches_sorted_eq! {[
                "+-------+--------------------------+------------------+",
                "| value | nested                   | _change_type     |",
                "+-------+--------------------------+------------------+",
                "| 12    | {id: 2, lat: 2, long: 2} | update_postimage |",
                "| 2     | {id: 2, lat: 2, long: 2} | update_preimage  |",
                "+-------+--------------------------+------------------+",
                ], &batches }
            }
            Err(err) => {
                println!("err: {err:#?}");
                panic!("Should have never reached this assertion");
            }
        }
    }
}
