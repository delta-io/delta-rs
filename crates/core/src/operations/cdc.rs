//!
//! The CDC module contains private tools for managing CDC files
//!

use crate::table::state::DeltaTableState;
use crate::DeltaResult;

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::{
    metrics::MetricsSet, DisplayAs, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::prelude::*;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc::*;
use tracing::log::*;

/// Maximum in-memory channel size for the tracker to use
const MAX_CHANNEL_SIZE: usize = 1024;

/// The CDCTracker is useful for hooking reads/writes in a manner nececessary to create CDC files
/// associated with commits
pub(crate) struct CDCTracker {
    schema: SchemaRef,
    pre_sender: Sender<RecordBatch>,
    pre_receiver: Receiver<RecordBatch>,
    post_sender: Sender<RecordBatch>,
    post_receiver: Receiver<RecordBatch>,
}

impl CDCTracker {
    ///  construct
    pub(crate) fn new(schema: SchemaRef) -> Self {
        let (pre_sender, pre_receiver) = channel(MAX_CHANNEL_SIZE);
        let (post_sender, post_receiver) = channel(MAX_CHANNEL_SIZE);
        Self {
            schema,
            pre_sender,
            pre_receiver,
            post_sender,
            post_receiver,
        }
    }

    /// Return an owned [Sender] for the caller to use when sending read but not altered batches
    pub(crate) fn pre_sender(&self) -> Sender<RecordBatch> {
        self.pre_sender.clone()
    }

    /// Return an owned [Sender][ for the caller to use when sending altered batches
    pub(crate) fn post_sender(&self) -> Sender<RecordBatch> {
        self.post_sender.clone()
    }

    pub(crate) async fn collect(mut self) -> DeltaResult<Vec<RecordBatch>> {
        debug!("Collecting all the batches for diffing");
        let ctx = SessionContext::new();
        let mut pre = vec![];
        let mut post = vec![];

        while !self.pre_receiver.is_empty() {
            if let Ok(batch) = self.pre_receiver.try_recv() {
                pre.push(batch);
            } else {
                warn!("Error when receiving on the pre-receiver");
            }
        }

        while !self.post_receiver.is_empty() {
            if let Ok(batch) = self.post_receiver.try_recv() {
                post.push(batch);
            } else {
                warn!("Error when receiving on the post-receiver");
            }
        }

        // Collect _all_ the batches for consideration
        let pre = ctx.read_batches(pre)?;
        let post = ctx.read_batches(post)?;

        // There is certainly a better way to do this other than stupidly cloning data for diffing
        // purposes, but this is the quickest and easiest way to "diff" the two sets of batches
        let preimage = pre.clone().except(post.clone())?;
        let postimage = post.except(pre)?;

        // Create a new schema which represents the input batch along with the CDC
        // columns
        let mut fields: Vec<Arc<Field>> = self.schema.fields().to_vec().clone();

        let mut has_struct = false;
        for field in fields.iter() {
            match field.data_type() {
                DataType::Struct(_) => {
                    has_struct = true;
                }
                DataType::List(_) => {
                    has_struct = true;
                }
                _ => {}
            }
        }

        if has_struct {
            warn!("The schema contains a Struct or List type, which unfortunately means a change data file cannot be captured in this release of delta-rs: <https://github.com/delta-io/delta-rs/issues/2568>. The write operation will complete properly, but no CDC data will be generated for schema: {fields:?}");
        }

        fields.push(Arc::new(Field::new("_change_type", DataType::Utf8, true)));
        let schema = Arc::new(Schema::new(fields));

        let mut batches = vec![];

        let mut pre_stream = preimage.execute_stream().await?;
        let mut post_stream = postimage.execute_stream().await?;

        // Fill up on pre image batches
        while let Some(Ok(batch)) = pre_stream.next().await {
            let batch = crate::operations::cast::cast_record_batch(
                &batch,
                self.schema.clone(),
                true,
                false,
            )?;
            debug!("prestream: {batch:?}");
            let new_column = Arc::new(StringArray::from(vec![
                Some("update_preimage");
                batch.num_rows()
            ]));
            let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
            columns.push(new_column);

            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            batches.push(batch);
        }

        // Fill up on the post-image batches
        while let Some(Ok(batch)) = post_stream.next().await {
            let batch = crate::operations::cast::cast_record_batch(
                &batch,
                self.schema.clone(),
                true,
                false,
            )?;
            debug!("poststream: {batch:?}");
            let new_column = Arc::new(StringArray::from(vec![
                Some("update_postimage");
                batch.num_rows()
            ]));
            let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
            columns.push(new_column);

            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            batches.push(batch);
        }

        debug!("Found {} batches to consider `CDC` data", batches.len());

        // At this point the batches should just contain the changes
        Ok(batches)
    }
}

/// A DataFusion observer to help pick up on pre-image changes
pub(crate) struct CDCObserver {
    parent: Arc<dyn ExecutionPlan>,
    id: String,
    sender: Sender<RecordBatch>,
}

impl CDCObserver {
    pub(crate) fn new(
        id: String,
        sender: Sender<RecordBatch>,
        parent: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self { id, sender, parent }
    }
}

impl std::fmt::Debug for CDCObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CDCObserver").field("id", &self.id).finish()
    }
}

impl DisplayAs for CDCObserver {
    fn fmt_as(
        &self,
        _: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CDCObserver id={}", self.id)
    }
}

impl ExecutionPlan for CDCObserver {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parent.schema()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.parent.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.parent]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let res = self.parent.execute(partition, context)?;
        Ok(Box::pin(CDCObserverStream {
            schema: self.schema(),
            input: res,
            sender: self.sender.clone(),
        }))
    }

    fn statistics(&self) -> DataFusionResult<datafusion_common::Statistics> {
        self.parent.statistics()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if let Some(parent) = children.first() {
            Ok(Arc::new(CDCObserver {
                id: self.id.clone(),
                sender: self.sender.clone(),
                parent: parent.clone(),
            }))
        } else {
            Err(datafusion_common::DataFusionError::Internal(
                "Failed to handle CDCObserver".into(),
            ))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.parent.metrics()
    }
}

/// The CDCObserverStream simply acts to help observe the stream of data being
/// read by DataFusion to capture the pre-image versions of data
pub(crate) struct CDCObserverStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    sender: Sender<RecordBatch>,
}

impl Stream for CDCObserverStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                let _ = self.sender.try_send(batch.clone());
                Some(Ok(batch))
            }
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for CDCObserverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    use super::*;
    use crate::kernel::DataType as DeltaDataType;
    use crate::kernel::{Action, PrimitiveType, Protocol};
    use crate::operations::DeltaOps;
    use crate::{DeltaConfigKey, DeltaTable};
    use arrow::array::{ArrayRef, Int32Array, StructArray};
    use datafusion::assert_batches_sorted_eq;

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
        assert!(
            result == false,
            "A default table should not create CDC files"
        );
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
            .with_configuration_property(DeltaConfigKey::EnableChangeDataFeed, Some("true"))
            .await
            .expect("failed to make a version 4 table with EnableChangeDataFeed");
        table.load().await.expect("Failed to reload table");

        let result = should_write_cdc(table.snapshot().unwrap()).expect("Failed to use table");
        assert!(
            result == true,
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
            result == false,
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
            .with_configuration_property(DeltaConfigKey::EnableChangeDataFeed, Some("true"))
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let tracker = CDCTracker::new(schema.clone());

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        )
        .unwrap();
        let updated_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(12), Some(3)]))],
        )
        .unwrap();

        let _ = tracker.pre_sender().send(batch).await;
        let _ = tracker.post_sender().send(updated_batch).await;

        match tracker.collect().await {
            Ok(batches) => {
                let _ = arrow::util::pretty::print_batches(&batches);
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

    // This cannot be re-enabled until DataFrame.except() works: <https://github.com/apache/datafusion/issues/10749>
    #[ignore]
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
        let _ = arrow::util::pretty::print_batches(&vec![batch.clone()]);
        let _ = arrow::util::pretty::print_batches(&vec![updated_batch.clone()]);

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

    // This cannot be re-enabled until DataFrame.except() works: <https://github.com/apache/datafusion/issues/10749>
    #[ignore]
    #[tokio::test]
    async fn test_sanity_check_with_struct() {
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

        let tracker = CDCTracker::new(schema.clone());

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

        let _ = tracker.pre_sender().send(batch).await;
        let _ = tracker.post_sender().send(updated_batch).await;

        match tracker.collect().await {
            Ok(batches) => {
                let _ = arrow::util::pretty::print_batches(&batches);
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
}
