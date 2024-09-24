//!
//! The CDC module contains private tools for managing CDC files
//!

use crate::table::state::DeltaTableState;
use crate::DeltaResult;

use datafusion::prelude::*;
use datafusion_common::ScalarValue;

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

///
/// Return true if the specified table is capable of writing Change Data files
///
/// From the Protocol:
///
/// > For Writer Versions 4 up to 6, all writers must respect the delta.enableChangeDataFeed
/// > configuration flag in the metadata of the table. When delta.enableChangeDataFeed is true,
/// > writers must produce the relevant AddCDCFile's for any operation that changes data, as
/// > specified in Change Data Files.
/// >
/// > For Writer Version 7, all writers must respect the delta.enableChangeDataFeed configuration flag in
/// > the metadata of the table only if the feature changeDataFeed exists in the table protocol's
/// > writerFeatures.
pub(crate) fn should_write_cdc(snapshot: &DeltaTableState) -> DeltaResult<bool> {
    if let Some(features) = &snapshot.protocol().writer_features {
        // Features should only exist at writer version 7 but to avoid cases where
        // the Option<HashSet<T>> can get filled with an empty set, checking for the value
        // explicitly
        if snapshot.protocol().min_writer_version == 7
            && !features.contains(&crate::kernel::WriterFeatures::ChangeDataFeed)
        {
            // If the writer feature has not been set, then the table should not have CDC written
            // to it. Otherwise fallback to the configured table configuration
            return Ok(false);
        }
    }
    Ok(snapshot.table_config().enable_change_data_feed())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kernel::DataType as DeltaDataType;
    use crate::kernel::{Action, PrimitiveType, Protocol};
    use crate::operations::DeltaOps;
    use crate::{DeltaTable, TableProperty};
    use arrow::array::{ArrayRef, Int32Array, StructArray};
    use arrow::datatypes::{DataType, Field};
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::datasource::{MemTable, TableProvider};

    /// A simple test which validates primitive writer version 1 tables should
    /// not write Change Data Files
    #[tokio::test]
    async fn test_should_write_cdc_basic_table() {
        let mut table = DeltaOps::new_in_memory()
            .create()
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .await
            .expect("Failed to make a table");
        table.load().await.expect("Failed to reload table");
        let result = should_write_cdc(table.snapshot().unwrap()).expect("Failed to use table");
        assert!(!result, "A default table should not create CDC files");
    }

    ///
    /// This test manually creates a table with writer version 4 that has the configuration sets
    ///
    #[tokio::test]
    async fn test_should_write_cdc_table_with_configuration() {
        let actions = vec![Action::Protocol(Protocol::new(1, 4))];
        let mut table: DeltaTable = DeltaOps::new_in_memory()
            .create()
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_actions(actions)
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .expect("failed to make a version 4 table with EnableChangeDataFeed");
        table.load().await.expect("Failed to reload table");

        let result = should_write_cdc(table.snapshot().unwrap()).expect("Failed to use table");
        assert!(
            result,
            "A table with the EnableChangeDataFeed should create CDC files"
        );
    }

    ///
    /// This test creates a writer version 7 table which has a slightly different way of
    /// determining whether CDC files should be written or not.
    #[tokio::test]
    async fn test_should_write_cdc_v7_table_no_writer_feature() {
        let actions = vec![Action::Protocol(Protocol::new(1, 7))];
        let mut table: DeltaTable = DeltaOps::new_in_memory()
            .create()
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_actions(actions)
            .await
            .expect("failed to make a version 4 table with EnableChangeDataFeed");
        table.load().await.expect("Failed to reload table");

        let result = should_write_cdc(table.snapshot().unwrap()).expect("Failed to use table");
        assert!(
            !result,
            "A v7 table must not write CDC files unless the writer feature is set"
        );
    }

    ///
    /// This test creates a writer version 7 table with a writer table feature enabled for CDC and
    /// therefore should write CDC files
    #[tokio::test]
    async fn test_should_write_cdc_v7_table_with_writer_feature() {
        let protocol = Protocol::new(1, 7)
            .with_writer_features(vec![crate::kernel::WriterFeatures::ChangeDataFeed]);
        let actions = vec![Action::Protocol(protocol)];
        let mut table: DeltaTable = DeltaOps::new_in_memory()
            .create()
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_actions(actions)
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .expect("failed to make a version 4 table with EnableChangeDataFeed");
        table.load().await.expect("Failed to reload table");

        let result = should_write_cdc(table.snapshot().unwrap()).expect("Failed to use table");
        assert!(
            result,
            "A v7 table must not write CDC files unless the writer feature is set"
        );
    }

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
