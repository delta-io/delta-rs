//! Builds the early filter of a MERGE while the join reads a streaming source.
//!
//! A streaming source can be read only once, so the MERGE cannot aggregate the source before it
//! plans the target scan. Instead, [`SourceStatsExec`] passes each source batch on to the join,
//! and also sends it to an aggregation. The aggregation computes the values for the placeholders
//! of the early filter: the distinct values of the source columns that the join condition matches
//! with partition columns, and the minimum and maximum of the source columns that it matches with
//! other columns. When the source ends, [`SourceStatsExec`] builds the early filter and sets its
//! file skipping predicates in the [`RuntimeFileFilter`] of the target scan. Only then does it
//! report the end of the source.
//!
//! The target scan reads the filter once, when it is first polled, it does not wait for it to
//! be set. A hash join polls its probe side only after it has read the whole build side. When the
//! source is the build side, the predicates are set before the target scan starts, and the scan
//! skips every file that cannot match. When the target is the build side, the target is read
//! before the predicates are set, and no file is skipped.
//!
//! The plan of a streamed upsert, for a target with the files f0 to f3 of which only f0 can
//! match the source:
//!
//! ```text
//!                         ┌─────────────────────┐
//!                         │   write + commit    │
//!                         └──────────▲──────────┘
//!                                    │
//!                         ┌──────────┴──────────┐
//!                         │  MergeBarrierExec   │
//!                         └──────────▲──────────┘
//!                                    │
//!                         ┌──────────┴──────────┐
//!                         │    HashJoinExec     │
//!                         │     (FULL JOIN)     │
//!                         └────▲───────────▲────┘
//!           ┌──────────────────┘           └────────────────────┐
//!           │ build side                             probe side │
//! ┌─────────┴─────────┐                             ┌───────────┴──────────┐
//! │  SourceStatsExec  │                             │    DeltaScanExec     │
//! │at the source end: │      RuntimeFileFilter      │ first poll: read the │
//! │  set predicates,  │─ ─ ─ ─ ─ predicates ─ ─ ─ ─►│ filter, do not wait  │
//! │     then EOF      │                             │ if predicates: set   │
//! └─────────▲─────────┘                             │ the kept files       ╞═══╗
//!           │                                       └───────────▲──────────┘   ║
//!           │                                                   │              ║ file filter:
//! ┌─────────┴─────────┐                             ┌───────────┴──────────┐   ║ file id in the
//! │      source       │                             │CoalescePartitionsExec│   ║ kept files
//! │     read once     │                             └───────────▲──────────┘   ║ [T F F F]
//! └───────────────────┘                                         │              ║
//!                                                   ┌───────────┴──────────┐   ║
//!                                                   │    DataSourceExec    │◄══╝
//!                                                   │p0: f0 f1   p1: f2 f3 │
//!                                                   │skips f1 f2 f3 before │
//!                                                   │  reading the footer  │
//!                                                   └──────────────────────┘
//!
//! early filter with placeholders:  id >= $min AND id <= $max AND modified = $m
//! target files f0..f3 have ids 0..3; the scan counts 4 files until the kept files lower it
//! ```
//!
//! Single lines show how data flows up. `─ ─►` is the [`RuntimeFileFilter`]. Double lines show
//! the file filter: the target scan sets the kept files in a dynamic filter on the file id column
//! of its Parquet input. The Parquet scan then skips the other files before it reads their
//! footers.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, Result as DataFusionResult, ScalarValue, Statistics};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::ptr_eq::PtrEq;
use datafusion::logical_expr::{Between, ColumnarValue, Expr, LogicalPlan, UserDefinedLogicalNodeCore, lit};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PhysicalExpr, PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, collect,
};
use futures::channel::mpsc;
use futures::{SinkExt as _, StreamExt as _};
use parking_lot::Mutex;
use tracing::warn;

use super::filter::{StreamingEarlyFilter, filter_from_placeholder_values};
use super::{build_file_skipping_predicates, normalize_target_subset_filter};
use crate::DeltaResult;
use crate::delta_datafusion::RuntimeFileFilter;
use crate::operations::optimize::util::flatten_join_error;

type Aggregation = SpawnedTask<DataFusionResult<Vec<RecordBatch>>>;

/// Default maximum number of distinct values collected for exact key IN list pruning.
/// Matches DataFusion's default for `hash_join_inlist_pushdown_max_distinct_values`.
pub(crate) const DEFAULT_HASH_JOIN_INLIST_PUSHDOWN_MAX_DISTINCT_VALUES: usize = 150;

/// Default maximum memory size in bytes for exact key IN list pruning.
/// Matches DataFusion's default for `hash_join_inlist_pushdown_max_size` (128 KiB).
pub(crate) const DEFAULT_HASH_JOIN_INLIST_PUSHDOWN_MAX_SIZE: usize = 131072;

#[derive(Debug)]
pub(crate) struct ActiveKeyTracker {
    pub target_expr: Expr,
    pub physical_expr: Arc<dyn PhysicalExpr>,
    pub values: BTreeSet<ScalarValue>,
    pub total_bytes: usize,
    pub exceeded: bool,
}

/// Shared by [`SourceStats`], [`SourceStatsExec`] and the MERGE operation.
pub(crate) struct SourceStatsCollector {
    /// Early filter with placeholders for the source values
    filter: Expr,
    /// Rows that the `WHEN NOT MATCHED BY SOURCE` clauses can touch, ORed onto the early filter
    not_match_source_filter: Option<Expr>,
    /// Aggregates the placeholder values from the source batches
    aggregate: Arc<dyn ExecutionPlan>,
    state: Mutex<CollectState>,
    source_rows: AtomicUsize,
    target_schema: DFSchemaRef,
    target_alias: Option<String>,
    /// The file skipping predicates of the early filter, for the target scan
    runtime_file_filter: RuntimeFileFilter,
    early_filter: OnceLock<Expr>,
    key_trackers: Mutex<Vec<ActiveKeyTracker>>,
    max_distinct_values: usize,
    max_size_bytes: usize,
}

struct CollectState {
    /// Sends the source batches to the aggregation. Dropped after the last source partition.
    batch_sender: Option<mpsc::Sender<RecordBatch>>,
    aggregation: Option<Aggregation>,
    /// Source partitions that have not ended. Set when the first partition starts.
    open_partitions: Option<usize>,
}

impl SourceStatsCollector {
    pub(crate) fn new(
        filter: StreamingEarlyFilter,
        not_match_source_filter: Option<Expr>,
        target_schema: DFSchemaRef,
        target_alias: Option<String>,
    ) -> Arc<Self> {
        let key_trackers = filter
            .key_trackers
            .into_iter()
            .map(|t| ActiveKeyTracker {
                target_expr: t.target_expr,
                physical_expr: t.physical_expr,
                values: BTreeSet::new(),
                total_bytes: 0,
                exceeded: false,
            })
            .collect();
        Arc::new(Self {
            filter: filter.filter,
            not_match_source_filter,
            aggregate: filter.aggregate,
            state: Mutex::new(CollectState {
                batch_sender: Some(filter.batch_sender),
                aggregation: None,
                open_partitions: None,
            }),
            source_rows: AtomicUsize::new(0),
            target_schema,
            target_alias,
            runtime_file_filter: RuntimeFileFilter::default(),
            early_filter: OnceLock::new(),
            key_trackers: Mutex::new(key_trackers),
            max_distinct_values: filter.max_distinct_values,
            max_size_bytes: filter.max_size_bytes,
        })
    }

    /// The filter that skips target files, for the target scan.
    pub(crate) fn runtime_file_filter(&self) -> RuntimeFileFilter {
        Arc::clone(&self.runtime_file_filter)
    }

    /// The early filter, if the whole source was read and the filter was built.
    pub(crate) fn early_filter(&self) -> Option<&Expr> {
        self.early_filter.get()
    }

    /// Record a batch of source rows for exact key IN list pruning.
    pub(crate) fn record_batch(&self, batch: &RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }
        let mut trackers = self.key_trackers.lock();
        for tracker in trackers.iter_mut() {
            if tracker.exceeded {
                continue;
            }
            let columnar_value = match tracker.physical_expr.evaluate(batch) {
                Ok(val) => val,
                Err(err) => {
                    warn!("Failed to evaluate source expr for merge exact key pruning: {err}");
                    tracker.exceeded = true;
                    tracker.values.clear();
                    continue;
                }
            };
            match columnar_value {
                ColumnarValue::Array(array) => {
                    for i in 0..array.len() {
                        if array.is_null(i) {
                            continue;
                        }
                        let scalar = match ScalarValue::try_from_array(&array, i) {
                            Ok(s) => s,
                            Err(_) => {
                                tracker.exceeded = true;
                                tracker.values.clear();
                                break;
                            }
                        };
                        if !tracker.values.contains(&scalar) {
                            let scalar_size = scalar.size();
                            if tracker.values.len() + 1 > self.max_distinct_values
                                || tracker.total_bytes + scalar_size > self.max_size_bytes
                            {
                                tracker.exceeded = true;
                                tracker.values.clear();
                                break;
                            }
                            tracker.total_bytes += scalar_size;
                            tracker.values.insert(scalar);
                        }
                    }
                }
                ColumnarValue::Scalar(scalar) => {
                    if !scalar.is_null() && !tracker.values.contains(&scalar) {
                        let scalar_size = scalar.size();
                        if tracker.values.len() + 1 > self.max_distinct_values
                            || tracker.total_bytes + scalar_size > self.max_size_bytes
                        {
                            tracker.exceeded = true;
                            tracker.values.clear();
                        } else {
                            tracker.total_bytes += scalar_size;
                            tracker.values.insert(scalar);
                        }
                    }
                }
            }
        }
    }

    /// Replaces `Expr::Between` with `Expr::InList` for equality keys whose source values fit
    /// within `max_distinct_values` and `max_size_bytes`.
    pub(crate) fn apply_exact_in_list_filters(&self, filter: Expr) -> Expr {
        let trackers = self.key_trackers.lock();
        let valid_trackers: Vec<_> = trackers
            .iter()
            .filter(|t| !t.exceeded && !t.values.is_empty())
            .collect();
        if valid_trackers.is_empty() {
            return filter;
        }

        filter
            .transform(&|expr| match expr {
                Expr::Between(Between {
                    expr,
                    negated: false,
                    low,
                    high,
                }) => {
                    if let Some(tracker) = valid_trackers.iter().find(|t| t.target_expr == *expr) {
                        let list = tracker.values.iter().cloned().map(lit).collect();
                        Ok(Transformed::yes(Expr::InList(InList {
                            expr,
                            list,
                            negated: false,
                        })))
                    } else {
                        Ok(Transformed::no(Expr::Between(Between {
                            expr,
                            negated: false,
                            low,
                            high,
                        })))
                    }
                }
                other => Ok(Transformed::no(other)),
            })
            .map(|t| t.data)
            .unwrap_or(filter)
    }

    /// Start reading one source partition. Returns the channel to the aggregation.
    fn open_partition(
        &self,
        partitions: usize,
        context: Arc<TaskContext>,
    ) -> Option<mpsc::Sender<RecordBatch>> {
        let mut state = self.state.lock();
        if state.open_partitions.is_none() {
            state.open_partitions = Some(partitions);
            let aggregate = Arc::clone(&self.aggregate);
            state.aggregation = Some(SpawnedTask::spawn(collect(aggregate, context)));
        }
        state.batch_sender.clone()
    }

    /// End one source partition. Returns the aggregation after the last partition.
    fn close_partition(&self) -> Option<Aggregation> {
        let mut state = self.state.lock();
        let open = state.open_partitions.as_mut()?;
        *open = open.saturating_sub(1);
        if *open > 0 {
            return None;
        }
        // The aggregation ends when the last sender is dropped.
        state.batch_sender = None;
        state.aggregation.take()
    }

    /// Build the early filter from the whole source and set its predicates for the target scan.
    async fn complete(&self, aggregation: Aggregation) {
        if let Err(err) = self.try_complete(aggregation).await {
            warn!("Could not build the early filter of a streaming MERGE source: {err}");
        }
    }

    async fn try_complete(&self, aggregation: Aggregation) -> DeltaResult<()> {
        let batches = flatten_join_error(aggregation.join_unwind()).await?;
        let filter = if self.source_rows.load(AtomicOrdering::Relaxed) == 0 {
            // No source row can match a target row, so only the not matched by source rows remain.
            Some(self.not_match_source_filter.clone().unwrap_or(lit(false)))
        } else {
            let items = concat_batches(&self.aggregate.schema(), &batches)?;
            filter_from_placeholder_values(&self.filter, &items)?
                .map(|filter| self.apply_exact_in_list_filters(filter))
                .map(|filter| match &self.not_match_source_filter {
                    Some(not_match_source_filter) => filter.or(not_match_source_filter.clone()),
                    None => filter,
                })
        };
        let Some(filter) = filter else {
            return Ok(());
        };
        let filter = normalize_target_subset_filter(Arc::clone(&self.target_schema), filter)?;
        let predicates =
            build_file_skipping_predicates(Some(filter.clone()), self.target_alias.as_deref());
        let _ = self.runtime_file_filter.set(predicates);
        let _ = self.early_filter.set(filter);
        Ok(())
    }
}

impl fmt::Debug for SourceStatsCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceStatsCollector")
            .field("filter", &self.filter)
            .finish_non_exhaustive()
    }
}

/// Logical node that collects the statistics of a streaming MERGE source.
///
/// Like all extension nodes by default, it stops filters from being pushed below it. Such
/// filters would change the statistics.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SourceStats {
    pub input: LogicalPlan,
    pub collector: PtrEq<Arc<SourceStatsCollector>>,
}

impl PartialOrd for SourceStats {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.collector != other.collector {
            return None;
        }
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for SourceStats {
    fn name(&self) -> &str {
        "SourceStats"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SourceStats")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        Ok(Self {
            input: inputs.swap_remove(0),
            collector: self.collector.clone(),
        })
    }
}

/// Passes the source through, and sends each batch to the aggregation of the early filter.
#[derive(Debug)]
pub(crate) struct SourceStatsExec {
    input: Arc<dyn ExecutionPlan>,
    collector: Arc<SourceStatsCollector>,
}

impl SourceStatsExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, collector: Arc<SourceStatsCollector>) -> Self {
        Self { input, collector }
    }
}

impl DisplayAs for SourceStatsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SourceStatsExec")
    }
}

impl ExecutionPlan for SourceStatsExec {
    fn name(&self) -> &str {
        "SourceStatsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            Arc::clone(&self.collector),
        )))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let partitions = self.input.output_partitioning().partition_count();

        let batch_sender = self.collector.open_partition(partitions, context);
        let source = SourcePartition {
            input,
            batch_sender,
            collector: Arc::clone(&self.collector),
        };
        // `unfold` calls `next_batch` for each batch, until it returns `None`.
        let stream = futures::stream::unfold(source, SourcePartition::next_batch).fuse();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> DataFusionResult<Arc<Statistics>> {
        input_stats.first().map(Arc::clone).ok_or_else(|| {
            datafusion::common::internal_datafusion_err!(
                "SourceStatsExec expects statistics for exactly one child"
            )
        })
    }

    fn apply_expressions(
        &self,
        _visit: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

/// One source partition while it is read. Passes each batch on, and sends it to the aggregation.
struct SourcePartition {
    input: SendableRecordBatchStream,
    /// `None` when the batches cannot be sent to the aggregation
    batch_sender: Option<mpsc::Sender<RecordBatch>>,
    collector: Arc<SourceStatsCollector>,
}

impl SourcePartition {
    /// Read the next batch and send it to the aggregation. Returns the batch, and `self` for the
    /// next call. Returns `None` at the end of the input.
    async fn next_batch(mut self) -> Option<(DataFusionResult<RecordBatch>, Self)> {
        let batch = match self.input.next().await {
            Some(Ok(batch)) => batch,
            Some(Err(err)) => return Some((Err(err), self)),
            None => {
                // Drop the sender before `complete` waits for the aggregation, which ends only
                // after every sender is dropped.
                self.batch_sender = None;
                if let Some(aggregation) = self.collector.close_partition() {
                    self.collector.complete(aggregation).await;
                }
                return None;
            }
        };
        self.collector
            .source_rows
            .fetch_add(batch.num_rows(), AtomicOrdering::Relaxed);
        self.collector.record_batch(&batch);
        // When the aggregation stopped, its error is reported when it is joined.
        if batch.num_rows() > 0
            && let Some(sender) = self.batch_sender.as_mut()
            && sender.feed(batch.clone()).await.is_err()
        {
            self.batch_sender = None;
        }
        Some((Ok(batch), self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::logical_expr::{col, lit, Between, InList};
    use datafusion::physical_expr::expressions::Column as PhysColumn;
    use datafusion::physical_plan::empty::EmptyExec;

    fn make_test_collector(
        trackers: Vec<ActiveKeyTracker>,
        max_distinct_values: usize,
        max_size_bytes: usize,
    ) -> SourceStatsCollector {
        let arrow_schema = Arc::new(ArrowSchema::empty());
        SourceStatsCollector {
            filter: lit(true),
            not_match_source_filter: None,
            aggregate: Arc::new(EmptyExec::new(arrow_schema)),
            state: Mutex::new(CollectState {
                batch_sender: None,
                aggregation: None,
                open_partitions: None,
            }),
            source_rows: AtomicUsize::new(0),
            target_schema: Arc::new(datafusion::common::DFSchema::empty()),
            target_alias: None,
            runtime_file_filter: RuntimeFileFilter::default(),
            early_filter: OnceLock::new(),
            key_trackers: Mutex::new(trackers),
            max_distinct_values,
            max_size_bytes,
        }
    }

    #[test]
    fn test_apply_exact_in_list_filters_converts_between_to_in_list() {
        let target_expr = col("target.id");
        let phys_expr = Arc::new(PhysColumn::new("id", 0));
        let values = BTreeSet::from([
            ScalarValue::Utf8(Some("00001".into())),
            ScalarValue::Utf8(Some("09998".into())),
        ]);
        let tracker = ActiveKeyTracker {
            target_expr: target_expr.clone(),
            physical_expr: phys_expr,
            values,
            total_bytes: 100,
            exceeded: false,
        };
        let collector = make_test_collector(vec![tracker], 150, 131072);

        let input_filter = Expr::Between(Between {
            expr: Box::new(target_expr.clone()),
            negated: false,
            low: Box::new(lit("00001")),
            high: Box::new(lit("09998")),
        });

        let transformed = collector.apply_exact_in_list_filters(input_filter);
        assert_eq!(
            transformed,
            Expr::InList(InList {
                expr: Box::new(target_expr),
                list: vec![lit("00001"), lit("09998")],
                negated: false,
            })
        );
    }

    #[test]
    fn test_apply_exact_in_list_filters_fallback_when_exceeded() {
        let target_expr = col("target.id");
        let phys_expr = Arc::new(PhysColumn::new("id", 0));
        let tracker = ActiveKeyTracker {
            target_expr: target_expr.clone(),
            physical_expr: phys_expr,
            values: BTreeSet::new(),
            total_bytes: 0,
            exceeded: true,
        };
        let collector = make_test_collector(vec![tracker], 150, 131072);

        let input_filter = Expr::Between(Between {
            expr: Box::new(target_expr.clone()),
            negated: false,
            low: Box::new(lit("00001")),
            high: Box::new(lit("09998")),
        });

        let transformed = collector.apply_exact_in_list_filters(input_filter.clone());
        assert_eq!(transformed, input_filter);
    }

    #[test]
    fn test_record_batch_deduplicates_and_ignores_nulls() {
        let target_expr = col("target.id");
        let phys_expr = Arc::new(PhysColumn::new("id", 0));
        let tracker = ActiveKeyTracker {
            target_expr,
            physical_expr: phys_expr,
            values: BTreeSet::new(),
            total_bytes: 0,
            exceeded: false,
        };
        let collector = make_test_collector(vec![tracker], 150, 131072);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("00001"),
                None,
                Some("00001"),
                Some("09998"),
            ]))],
        )
        .unwrap();

        collector.record_batch(&batch);
        let trackers = collector.key_trackers.lock();
        assert_eq!(trackers[0].values.len(), 2);
        assert!(!trackers[0].exceeded);
        assert!(trackers[0]
            .values
            .contains(&ScalarValue::Utf8(Some("00001".into()))));
        assert!(trackers[0]
            .values
            .contains(&ScalarValue::Utf8(Some("09998".into()))));
    }

    #[test]
    fn test_record_batch_exceeds_distinct_count_limit() {
        let target_expr = col("target.id");
        let phys_expr = Arc::new(PhysColumn::new("id", 0));
        let tracker = ActiveKeyTracker {
            target_expr,
            physical_expr: phys_expr,
            values: BTreeSet::new(),
            total_bytes: 0,
            exceeded: false,
        };
        // Set max_distinct_values to 2
        let collector = make_test_collector(vec![tracker], 2, 131072);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("01"),
                Some("02"),
                Some("03"),
            ]))],
        )
        .unwrap();

        collector.record_batch(&batch);
        let trackers = collector.key_trackers.lock();
        assert!(trackers[0].exceeded);
        assert!(trackers[0].values.is_empty());
    }

    #[test]
    fn test_record_batch_exceeds_byte_size_limit() {
        let target_expr = col("target.id");
        let phys_expr = Arc::new(PhysColumn::new("id", 0));
        let tracker = ActiveKeyTracker {
            target_expr,
            physical_expr: phys_expr,
            values: BTreeSet::new(),
            total_bytes: 0,
            exceeded: false,
        };
        // Set max_size_bytes to 10 bytes (smaller than the memory of a string scalar)
        let collector = make_test_collector(vec![tracker], 150, 10);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some(
                "a_very_long_string_that_exceeds_ten_bytes",
            )]))],
        )
        .unwrap();

        collector.record_batch(&batch);
        let trackers = collector.key_trackers.lock();
        assert!(trackers[0].exceeded);
        assert!(trackers[0].values.is_empty());
    }
}
