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
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchemaRef, Result as DataFusionResult, Statistics};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::ptr_eq::PtrEq;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore, lit};
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

/// Shared by [`SourceStats`], [`SourceStatsExec`] and the MERGE operation.
pub(crate) struct SourceStatsCollector {
    /// Early filter with placeholders for the source values
    filter: Expr,
    /// Aggregates the placeholder values from the source batches
    aggregate: Arc<dyn ExecutionPlan>,
    state: Mutex<CollectState>,
    source_rows: AtomicUsize,
    target_schema: DFSchemaRef,
    target_alias: Option<String>,
    /// The file skipping predicates of the early filter, for the target scan
    runtime_file_filter: RuntimeFileFilter,
    early_filter: OnceLock<Expr>,
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
        target_schema: DFSchemaRef,
        target_alias: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            filter: filter.filter,
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
            // No source row can match a target row.
            Some(lit(false))
        } else {
            let items = concat_batches(&self.aggregate.schema(), &batches)?;
            filter_from_placeholder_values(&self.filter, &items)?
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
