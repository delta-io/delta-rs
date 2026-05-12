//! Optimize a Delta Table
//!
//! Perform bin-packing on a Delta Table which merges small files into a large
//! file. Bin-packing reduces the number of API calls required for read
//! operations.
//!
//! Optimize will fail if a concurrent write operation removes files from the
//! table (such as in an overwrite). It will always succeed if concurrent writers
//! are only appending.
//!
//! Optimize increments the table's version and creates remove actions for
//! optimized files. Optimize does not delete files from storage. To delete
//! files that were removed, call `vacuum` on [`DeltaTable`].
//!
//! See [`OptimizeBuilder`] for configuration.
//!
//! # Example
//! ```rust ignore
//! let table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
//! let (table, metrics) = OptimizeBuilder::new(table.object_store(), table.state).await?;
//! ````

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::execution::context::{SessionContext, SessionState};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::Scalar;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use itertools::Itertools;
use num_cpus;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};
use tracing::*;
use uuid::Uuid;

use super::write::writer::{PartitionWriter, PartitionWriterConfig};
use super::{CustomExecuteHandler, Operation};
use crate::delta_datafusion::{
    DataFusionMixins, DeltaScanConfig, DeltaScanNext, SessionFallbackPolicy, SessionResolveContext,
    create_session_state_with_spill_config, resolve_session_state, update_datafusion_session,
};
use crate::errors::{DeltaResult, DeltaTableError, unsupported_column_mapping_write};
use crate::kernel::transaction::{CommitBuilder, CommitProperties, DEFAULT_RETRIES, PROTOCOL};
use crate::kernel::{Action, Add, DataType, PartitionsExt, Remove, StructType, Version};
use crate::kernel::{EagerSnapshot, resolve_snapshot};
use crate::logstore::{LogStore, LogStoreRef, ObjectStoreRef};
use crate::parquet_utils::default_writer_properties;
use crate::protocol::DeltaOperation;
use crate::table::config::TablePropertiesExt as _;
use crate::table::state::DeltaTableState;
use crate::writer::utils::arrow_schema_without_partitions;
use crate::{DeltaTable, ObjectMeta, PartitionFilter, to_kernel_predicate};

/// Planner used by optimize.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PlannerStrategy {
    /// Older metrics with no planner field.
    #[default]
    UnknownLegacy,
    /// Compact planner.
    PreserveLocality,
    /// Z order planner.
    ZOrder,
}

/// Metrics from Optimize
#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "MetricsSerde")]
pub struct Metrics {
    /// Number of optimized files added
    pub num_files_added: u64,
    /// Number of unoptimized files removed
    pub num_files_removed: u64,
    /// Detailed metrics for the add operation
    #[serde(
        serialize_with = "serialize_metric_details",
        deserialize_with = "deserialize_metric_details"
    )]
    pub files_added: MetricDetails,
    /// Detailed metrics for the remove operation
    #[serde(
        serialize_with = "serialize_metric_details",
        deserialize_with = "deserialize_metric_details"
    )]
    pub files_removed: MetricDetails,
    /// Number of partitions that had at least one file optimized
    pub partitions_optimized: u64,
    /// The number of batches written
    pub num_batches: u64,
    /// How many files were considered during optimization. Not every file considered is optimized
    pub total_considered_files: usize,
    /// How many files were considered for optimization but were skipped
    pub total_files_skipped: usize,
    /// Compatibility field for `preserved_stable_order`
    pub preserve_insertion_order: bool,
    /// Planner used for this run
    pub planner_strategy: PlannerStrategy,
    /// True when file order is kept within a partition
    pub preserved_stable_order: bool,
    /// Largest count of adjacent input files in one bin
    pub max_bin_span_files: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetricsSerde {
    num_files_added: u64,
    num_files_removed: u64,
    #[serde(deserialize_with = "deserialize_metric_details")]
    files_added: MetricDetails,
    #[serde(deserialize_with = "deserialize_metric_details")]
    files_removed: MetricDetails,
    partitions_optimized: u64,
    num_batches: u64,
    total_considered_files: usize,
    total_files_skipped: usize,
    #[serde(default)]
    preserve_insertion_order: Option<bool>,
    #[serde(default)]
    planner_strategy: PlannerStrategy,
    #[serde(default)]
    preserved_stable_order: Option<bool>,
    #[serde(default)]
    max_bin_span_files: usize,
}

impl From<MetricsSerde> for Metrics {
    fn from(value: MetricsSerde) -> Self {
        let preserve_insertion_order = value
            .preserve_insertion_order
            .or(value.preserved_stable_order)
            .unwrap_or(false);
        let preserved_stable_order = value.preserved_stable_order.unwrap_or(false);

        Self {
            num_files_added: value.num_files_added,
            num_files_removed: value.num_files_removed,
            files_added: value.files_added,
            files_removed: value.files_removed,
            partitions_optimized: value.partitions_optimized,
            num_batches: value.num_batches,
            total_considered_files: value.total_considered_files,
            total_files_skipped: value.total_files_skipped,
            preserve_insertion_order,
            planner_strategy: value.planner_strategy,
            preserved_stable_order,
            max_bin_span_files: value.max_bin_span_files,
        }
    }
}

// Custom serialization function that serializes metric details as a string
fn serialize_metric_details<S>(value: &MetricDetails, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

// Custom deserialization that parses a JSON string into MetricDetails
fn deserialize_metric_details<'de, D>(deserializer: D) -> Result<MetricDetails, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(DeError::custom)
}

/// Statistics on files for a particular operation
/// Operation can be remove or add
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricDetails {
    /// Average file size of a operation
    pub avg: f64,
    /// Maximum file size of a operation
    pub max: i64,
    /// Minimum file size of a operation
    pub min: i64,
    /// Number of files encountered during operation
    pub total_files: usize,
    /// Sum of file sizes of a operation
    pub total_size: i64,
}

impl MetricDetails {
    /// Add a partial metric to the metrics
    pub fn add(&mut self, partial: &MetricDetails) {
        self.min = std::cmp::min(self.min, partial.min);
        self.max = std::cmp::max(self.max, partial.max);
        self.total_files += partial.total_files;
        self.total_size += partial.total_size;
        self.avg = self.total_size as f64 / self.total_files as f64;
    }
}

impl fmt::Display for MetricDetails {
    /// Display the metric details using serde serialization
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        serde_json::to_string(self).map_err(|_| fmt::Error)?.fmt(f)
    }
}

#[derive(Debug)]
/// Metrics for a single partition
pub struct PartialMetrics {
    /// Number of optimized files added
    pub num_files_added: u64,
    /// Number of unoptimized files removed
    pub num_files_removed: u64,
    /// Detailed metrics for the add operation
    pub files_added: MetricDetails,
    /// Detailed metrics for the remove operation
    pub files_removed: MetricDetails,
    /// The number of batches written
    pub num_batches: u64,
}

impl Metrics {
    /// Add a partial metric to the metrics
    pub fn add(&mut self, partial: &PartialMetrics) {
        self.num_files_added += partial.num_files_added;
        self.num_files_removed += partial.num_files_removed;
        self.files_added.add(&partial.files_added);
        self.files_removed.add(&partial.files_removed);
        self.num_batches += partial.num_batches;
    }

    fn apply_planner_stats(&mut self, planner_stats: &PlannerStats) {
        self.planner_strategy = planner_stats.planner_strategy;
        self.preserved_stable_order = planner_stats.preserved_stable_order;
        self.preserve_insertion_order = planner_stats.preserved_stable_order;
        self.max_bin_span_files = planner_stats.max_bin_span_files;
    }
}

impl Default for MetricDetails {
    fn default() -> Self {
        MetricDetails {
            min: i64::MAX,
            max: 0,
            avg: 0.0,
            total_files: 0,
            total_size: 0,
        }
    }
}

/// Type of optimization to perform.
#[derive(Debug)]
pub enum OptimizeType {
    /// Compact files into pre-determined bins
    Compact,
    /// Z-order files based on provided columns
    ZOrder(Vec<String>),
}

/// Optimize a Delta table with given options
///
/// If a target file size is not provided then `delta.targetFileSize` from the
/// table's configuration is read. Otherwise a default value is used.
pub struct OptimizeBuilder<'a> {
    /// A snapshot of the to-be-optimized table's state
    snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Filters to select specific table partitions to be optimized
    filters: &'a [PartitionFilter],
    /// Desired file size after bin-packing files
    target_size: Option<NonZeroU64>,
    /// Properties passed to underlying parquet writer
    writer_properties: Option<WriterProperties>,
    /// Commit properties and configuration
    commit_properties: CommitProperties,
    /// Maximum number of concurrent tasks (default is number of cpus)
    max_concurrent_tasks: usize,
    /// Optimize type
    optimize_type: OptimizeType,
    /// Datafusion session state relevant for executing the input plan
    session: Option<Arc<dyn Session>>,
    session_fallback_policy: SessionFallbackPolicy,
    min_commit_interval: Option<Duration>,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for OptimizeBuilder<'_> {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl<'a> OptimizeBuilder<'a> {
    /// Create a new [`OptimizeBuilder`]
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            filters: &[],
            target_size: None,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
            max_concurrent_tasks: num_cpus::get(),
            optimize_type: OptimizeType::Compact,
            min_commit_interval: None,
            session: None,
            session_fallback_policy: SessionFallbackPolicy::default(),
            custom_execute_handler: None,
        }
    }

    /// Choose the type of optimization to perform. Defaults to [OptimizeType::Compact].
    pub fn with_type(mut self, optimize_type: OptimizeType) -> Self {
        self.optimize_type = optimize_type;
        self
    }

    /// Only optimize files that return true for the specified partition filter
    pub fn with_filters(mut self, filters: &'a [PartitionFilter]) -> Self {
        self.filters = filters;
        self
    }

    /// Set the target file size
    pub fn with_target_size(mut self, target: NonZeroU64) -> Self {
        self.target_size = Some(target);
        self
    }

    /// Writer properties passed to parquet writer
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Additional information to write to the commit
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Deprecated. This setting has no effect.
    #[deprecated(
        since = "0.32.0",
        note = "compact always keeps partition file order, and z order does not; this setting has no effect"
    )]
    pub fn with_preserve_insertion_order(self, _preserve_insertion_order: bool) -> Self {
        self
    }

    /// Max number of concurrent tasks
    pub fn with_max_concurrent_tasks(mut self, max_concurrent_tasks: usize) -> Self {
        self.max_concurrent_tasks = max_concurrent_tasks;
        self
    }

    /// Min commit interval
    pub fn with_min_commit_interval(mut self, min_commit_interval: Duration) -> Self {
        self.min_commit_interval = Some(min_commit_interval);
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }

    /// Set the DataFusion session used for planning and execution.
    ///
    /// The provided `session` should wrap a concrete `datafusion::execution::context::SessionState`.
    ///
    /// If `session` is not a `SessionState`, the default policy is to log a warning and fall back to
    /// internal defaults. To make this strict (error instead), set
    /// `with_session_fallback_policy(SessionFallbackPolicy::RequireSessionState)`.
    ///
    /// Example: `Arc::new(create_session().state())`.
    pub fn with_session_state(mut self, session: Arc<dyn Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Control how delta-rs resolves the provided session when it is not a concrete `SessionState`.
    ///
    /// Defaults to `SessionFallbackPolicy::InternalDefaults` to preserve existing behavior.
    pub fn with_session_fallback_policy(mut self, policy: SessionFallbackPolicy) -> Self {
        self.session_fallback_policy = policy;
        self
    }
}

impl<'a> std::future::IntoFuture for OptimizeBuilder<'a> {
    type Output = DeltaResult<(DeltaTable, Metrics)>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), true, None).await?;
            if snapshot.table_configuration().column_mapping_mode() != ColumnMappingMode::None {
                return Err(unsupported_column_mapping_write("OPTIMIZE"));
            }
            PROTOCOL.can_write_to(&snapshot)?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let writer_properties = this.writer_properties.unwrap_or_else(|| {
                default_writer_properties(Compression::ZSTD(ZstdLevel::try_new(4).unwrap()))
            });
            let (session, _) = resolve_session_state(
                this.session.as_deref(),
                this.session_fallback_policy,
                || create_session_state_with_spill_config(None, None),
                SessionResolveContext {
                    operation: "optimize",
                    table_uri: Some(this.log_store.root_url()),
                    cdc: false,
                },
            )?;
            let plan = create_merge_plan(
                &this.log_store,
                this.optimize_type,
                &snapshot,
                this.filters,
                this.target_size.to_owned(),
                writer_properties,
                session,
            )
            .await?;

            let metrics = plan
                .execute(
                    this.log_store.clone(),
                    &snapshot,
                    this.max_concurrent_tasks,
                    this.min_commit_interval,
                    this.commit_properties.clone(),
                    operation_id,
                    this.custom_execute_handler.as_ref(),
                )
                .await?;

            if let Some(handler) = this.custom_execute_handler {
                handler.post_execute(&this.log_store, operation_id).await?;
            }
            let mut table =
                DeltaTable::new_with_state(this.log_store, DeltaTableState::new(snapshot));
            table.update_state().await?;
            Ok((table, metrics))
        })
    }
}

#[derive(Debug, Clone)]
struct OptimizeInput {
    target_size: NonZeroU64,
    predicate: Option<String>,
}

const MAX_OPTIMIZE_TARGET_SIZE: u64 = i64::MAX as u64;

fn optimize_target_size_to_i64(target_size: NonZeroU64) -> Result<i64, DeltaTableError> {
    i64::try_from(target_size.get()).map_err(|_| {
        DeltaTableError::Generic(format!(
            "optimize target_size {} exceeds i64::MAX ({MAX_OPTIMIZE_TARGET_SIZE})",
            target_size.get()
        ))
    })
}

impl TryFrom<OptimizeInput> for DeltaOperation {
    type Error = DeltaTableError;

    fn try_from(opt_input: OptimizeInput) -> Result<Self, Self::Error> {
        Ok(DeltaOperation::Optimize {
            target_size: optimize_target_size_to_i64(opt_input.target_size)?,
            predicate: opt_input.predicate,
        })
    }
}

/// Generate an appropriate remove action for the optimization task
fn create_remove(add: &Add) -> Action {
    // NOTE unwrap is safe since UNIX_EPOCH will always be earlier then now.
    let deletion_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let deletion_time = deletion_time.as_millis() as i64;

    Action::Remove(Remove {
        path: add.path.clone(),
        deletion_timestamp: Some(deletion_time),
        data_change: false,
        extended_file_metadata: Some(true),
        partition_values: Some(add.partition_values.clone()),
        size: Some(add.size),
        deletion_vector: add.deletion_vector.clone(),
        tags: add.tags.clone(),
        base_row_id: add.base_row_id,
        default_row_commit_version: add.default_row_commit_version,
    })
}

/// Layout for optimizing a plan
///
/// Within each partition, we identify a set of files that need to be merged
/// together and/or sorted together.
#[derive(Debug)]
enum OptimizeOperations {
    /// Plan to compact files into bins
    ///
    /// Bins keep partition file order, stop at ordinal gaps, and stop at
    /// skipped large files. Bins of size 1 are dropped.
    Compact(HashMap<String, (IndexMap<String, Scalar>, Vec<MergeBin>)>),
    /// Plan to Z-order each partition
    ZOrder(
        Vec<String>,
        HashMap<String, (IndexMap<String, Scalar>, MergeBin)>,
    ),
    // TODO: Sort
}

impl Default for OptimizeOperations {
    fn default() -> Self {
        OptimizeOperations::Compact(HashMap::new())
    }
}

#[derive(Debug)]
/// Encapsulates the operations required to optimize a Delta Table
pub struct MergePlan {
    operations: OptimizeOperations,
    /// Metrics collected during operation
    metrics: Metrics,
    /// Planner metadata copied into buffered and total metrics
    planner_stats: PlannerStats,
    /// Parameters passed down to merge tasks
    task_parameters: Arc<MergeTaskParameters>,
    /// Version of the table at beginning of optimization. Used for conflict resolution.
    read_table_version: Version,
    /// Session state used for provider owned rewrite scans.
    read_session: Arc<SessionState>,
}

#[derive(Debug, Clone, Default)]
struct PlannerStats {
    planner_strategy: PlannerStrategy,
    preserved_stable_order: bool,
    max_bin_span_files: usize,
}

impl PlannerStats {
    fn preserve_locality() -> Self {
        Self {
            planner_strategy: PlannerStrategy::PreserveLocality,
            preserved_stable_order: true,
            max_bin_span_files: 0,
        }
    }

    fn z_order(max_bin_span_files: usize) -> Self {
        Self {
            planner_strategy: PlannerStrategy::ZOrder,
            preserved_stable_order: false,
            max_bin_span_files,
        }
    }

    fn absorb(&mut self, other: &PlannerStats) {
        self.max_bin_span_files = self.max_bin_span_files.max(other.max_bin_span_files);
    }
}

/// Parameters passed to individual merge tasks
#[derive(Debug)]
pub struct MergeTaskParameters {
    /// Schema of written files
    file_schema: SchemaRef,
    /// Properties passed to parquet writer
    writer_properties: WriterProperties,
    /// Input parameters for the optimize operation
    input_parameters: OptimizeInput,
    /// Num index cols to collect stats for
    num_indexed_cols: DataSkippingNumIndexedCols,
    /// Stats columns, specific columns to collect stats from, takes precedence over num_indexed_cols
    stats_columns: Option<Vec<String>>,
}

/// A stream of record batches, with a ParquetError on failure.
type ParquetReadStream = BoxStream<'static, Result<RecordBatch, ParquetError>>;

#[derive(Clone)]
struct SelectedFileScanFactory {
    snapshot: EagerSnapshot,
    log_store: LogStoreRef,
    scan_config: DeltaScanConfig,
    read_operation_id: Option<Uuid>,
}

impl SelectedFileScanFactory {
    fn try_new(
        snapshot: &EagerSnapshot,
        log_store: LogStoreRef,
        session: &dyn Session,
        read_operation_id: Option<Uuid>,
    ) -> Result<Self, DeltaTableError> {
        Ok(Self {
            snapshot: snapshot.clone(),
            log_store,
            // Mirror the caller's DataFusion session flags so rewrite scans keep
            // the same parquet/view type behavior as the rest of optimize.
            scan_config: DeltaScanConfig::new_from_session(session)
                .with_schema(snapshot.input_schema()),
            read_operation_id,
        })
    }

    fn provider_for(
        &self,
        adds: impl IntoIterator<Item = Add>,
    ) -> Result<DeltaScanNext, DeltaTableError> {
        let provider = DeltaScanNext::new(self.snapshot.clone(), self.scan_config.clone())?
            .with_log_store(self.log_store.clone());
        let provider = if let Some(operation_id) = self.read_operation_id {
            provider.with_operation_id(operation_id)
        } else {
            provider
        };
        provider.with_selected_adds(adds)
    }
}

impl MergePlan {
    /// Rewrites files in a single partition.
    ///
    /// Returns a vector of add and remove actions, as well as the partial metrics
    /// collected during the operation.
    async fn rewrite_files<F>(
        task_parameters: Arc<MergeTaskParameters>,
        partition_values: IndexMap<String, Scalar>,
        files: MergeBin,
        object_store: ObjectStoreRef,
        read_stream: F,
        ignore_target_size: bool,
    ) -> Result<(Vec<Action>, PartialMetrics), DeltaTableError>
    where
        F: Future<Output = Result<ParquetReadStream, DeltaTableError>> + Send + 'static,
    {
        debug!("Rewriting files in partition: {partition_values:?}");
        // First, initialize metrics
        let mut partial_actions = files.iter().map(create_remove).collect::<Vec<_>>();

        let files_removed = files
            .iter()
            .fold(MetricDetails::default(), |mut curr, file| {
                curr.total_files += 1;
                curr.total_size += file.size;
                curr.max = std::cmp::max(curr.max, file.size);
                curr.min = std::cmp::min(curr.min, file.size);
                curr
            });

        let mut partial_metrics = PartialMetrics {
            num_files_added: 0,
            num_files_removed: files.len() as u64,
            files_added: MetricDetails::default(),
            files_removed,
            num_batches: 0,
        };

        // Next, initialize the writer
        let writer_config = PartitionWriterConfig::try_new(
            task_parameters.file_schema.clone(),
            partition_values.clone(),
            Some(task_parameters.writer_properties.clone()),
            // Since we know the total size of the bin, we can set the target file size to None.
            if ignore_target_size {
                None
            } else {
                Some(task_parameters.input_parameters.target_size)
            },
            None,
            None,
        )?;
        let mut writer = PartitionWriter::try_with_config(
            object_store,
            writer_config,
            task_parameters.num_indexed_cols,
            task_parameters.stats_columns.clone(),
        )?;

        let mut read_stream = read_stream.await?;

        while let Some(maybe_batch) = read_stream.next().await {
            let mut batch = maybe_batch?;

            batch = crate::kernel::schema::cast::cast_record_batch(
                &batch,
                task_parameters.file_schema.clone(),
                false,
                true,
            )?;
            partial_metrics.num_batches += 1;
            writer.write(&batch).await?;
        }

        let add_actions = writer.close().await?.into_iter().map(|mut add| {
            add.data_change = false;

            let size = add.size;

            partial_metrics.num_files_added += 1;
            partial_metrics.files_added.total_files += 1;
            partial_metrics.files_added.total_size += size;
            partial_metrics.files_added.max = std::cmp::max(partial_metrics.files_added.max, size);
            partial_metrics.files_added.min = std::cmp::min(partial_metrics.files_added.min, size);

            Action::Add(add)
        });
        partial_actions.extend(add_actions);

        debug!("Finished rewriting files in partition: {partition_values:?}");

        Ok((partial_actions, partial_metrics))
    }

    async fn read_selected_files(
        files: MergeBin,
        context: Arc<SessionContext>,
        scan_factory: SelectedFileScanFactory,
    ) -> Result<ParquetReadStream, DeltaTableError> {
        let provider = scan_factory.provider_for(files.iter().cloned())?;
        let df = context.read_table(Arc::new(provider))?;
        let stream = df
            .execute_stream()
            .await?
            .map_err(|err| {
                ParquetError::General(format!(
                    "Optimize selected-file scan failed while scanning data: {err}"
                ))
            })
            .boxed();
        Ok(stream)
    }

    /// Datafusion-based z-order read.
    async fn read_zorder(
        files: MergeBin,
        context: Arc<zorder::ZOrderExecContext>,
        scan_factory: SelectedFileScanFactory,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ParquetError>>, DeltaTableError> {
        use datafusion::functions::core::expr_ext::FieldAccessor;
        use datafusion::logical_expr::expr::ScalarFunction;
        use datafusion::logical_expr::{Expr, ScalarUDF, ident};

        let provider = scan_factory.provider_for(files.iter().cloned())?;
        let df = context.ctx.read_table(Arc::new(provider))?;

        let cols = context
            .columns
            .iter()
            .map(|col_name| {
                let mut segments = col_name.split('.');
                let first = segments.next().expect("column name cannot be empty");
                let mut expr = ident(first);
                for segment in segments {
                    expr = expr.field(segment);
                }
                expr
            })
            .collect_vec();
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::from(zorder::datafusion::ZOrderUDF)),
            cols,
        ));
        let df = df.sort(vec![expr.sort(true, true)])?;

        let stream = df
            .execute_stream()
            .await?
            .map_err(|err| {
                ParquetError::General(format!("Z-order failed while scanning data: {err}"))
            })
            .boxed();

        Ok(stream)
    }

    /// Perform the operations outlined in the plan.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all, fields(operation = "optimize", version = snapshot.version()))]
    pub async fn execute(
        mut self,
        log_store: LogStoreRef,
        snapshot: &EagerSnapshot,
        max_concurrent_tasks: usize,
        min_commit_interval: Option<Duration>,
        commit_properties: CommitProperties,
        operation_id: Uuid,
        handle: Option<&Arc<dyn CustomExecuteHandler>>,
    ) -> Result<Metrics, DeltaTableError> {
        let operations = std::mem::take(&mut self.operations);
        let read_session = self.read_session.clone();
        info!("starting optimize execution");
        let object_store = log_store.object_store(Some(operation_id));
        update_datafusion_session(
            read_session.as_ref(),
            log_store.as_ref(),
            Some(operation_id),
        )?;

        let mut stream = match operations {
            OptimizeOperations::Compact(bins) => {
                let read_context = Arc::new(SessionContext::new_with_state(
                    read_session.as_ref().clone(),
                ));
                let scan_factory = SelectedFileScanFactory::try_new(
                    snapshot,
                    log_store.clone(),
                    read_session.as_ref(),
                    Some(operation_id),
                )?;
                let task_parameters = self.task_parameters.clone();

                futures::stream::iter(bins)
                    .flat_map(|(_, (partition, bins))| {
                        futures::stream::iter(bins).map(move |bin| (partition.clone(), bin))
                    })
                    .map(move |(partition, files)| {
                        debug!(
                            "merging a group of {} files in partition {partition:?}",
                            files.len(),
                        );
                        for file in files.iter() {
                            debug!("  file {}", file.path);
                        }

                        let batch_stream = Self::read_selected_files(
                            files.clone(),
                            read_context.clone(),
                            scan_factory.clone(),
                        );

                        let rewrite_result = tokio::task::spawn(Self::rewrite_files(
                            task_parameters.clone(),
                            partition,
                            files,
                            object_store.clone(),
                            batch_stream,
                            true,
                        ));
                        util::flatten_join_error(rewrite_result)
                    })
                    .buffered(max_concurrent_tasks)
                    .boxed()
            }
            OptimizeOperations::ZOrder(zorder_columns, bins) => {
                debug!("Starting zorder with the columns: {zorder_columns:?} {bins:?}");

                let exec_context = Arc::new(zorder::ZOrderExecContext::new(
                    zorder_columns,
                    read_session.as_ref().clone(),
                    object_store,
                )?);
                let task_parameters = self.task_parameters.clone();
                let scan_factory = SelectedFileScanFactory::try_new(
                    snapshot,
                    log_store.clone(),
                    read_session.as_ref(),
                    Some(operation_id),
                )?;

                // For each rewrite evaluate the predicate and then modify each expression
                // to either compute the new value or obtain the old one then write these batches
                let log_store = log_store.clone();
                futures::stream::iter(bins)
                    .map(move |(_, (partition, files))| {
                        let batch_stream = Self::read_zorder(
                            files.clone(),
                            exec_context.clone(),
                            scan_factory.clone(),
                        );
                        let rewrite_result = tokio::task::spawn(Self::rewrite_files(
                            task_parameters.clone(),
                            partition,
                            files,
                            log_store.object_store(Some(operation_id)),
                            batch_stream,
                            false,
                        ));
                        util::flatten_join_error(rewrite_result)
                    })
                    .buffer_unordered(max_concurrent_tasks)
                    .boxed()
            }
        };

        let mut table =
            DeltaTable::new_with_state(log_store.clone(), DeltaTableState::new(snapshot.clone()));

        // Actions buffered so far. These will be flushed either at the end
        // or when we reach the commit interval.
        let mut actions = vec![];

        // Each time we commit, we'll reset buffered_metrics to orig_metrics.
        let mut orig_metrics = std::mem::take(&mut self.metrics);
        orig_metrics.apply_planner_stats(&self.planner_stats);
        let mut buffered_metrics = orig_metrics.clone();
        let mut total_metrics = orig_metrics.clone();

        let mut last_commit = Instant::now();
        let mut commits_made = 0;
        let mut snapshot = snapshot.clone();
        loop {
            let next = stream.next().await.transpose()?;

            let end = next.is_none();

            if let Some((partial_actions, partial_metrics)) = next {
                debug!("Recording metrics for a completed partition");
                actions.extend(partial_actions);
                buffered_metrics.add(&partial_metrics);
                total_metrics.add(&partial_metrics);
            }

            let now = Instant::now();
            let mature = match min_commit_interval {
                None => false,
                Some(i) => now.duration_since(last_commit) > i,
            };
            if !actions.is_empty() && (mature || end) {
                let actions = std::mem::take(&mut actions);
                last_commit = now;

                let mut properties = CommitProperties::default();
                properties.app_metadata = commit_properties.app_metadata.clone();
                properties
                    .app_metadata
                    .insert("readVersion".to_owned(), self.read_table_version.into());
                let maybe_map_metrics = serde_json::to_value(std::mem::replace(
                    &mut buffered_metrics,
                    orig_metrics.clone(),
                ));
                if let Ok(map) = maybe_map_metrics {
                    properties
                        .app_metadata
                        .insert("operationMetrics".to_owned(), map);
                }

                debug!("committing {} actions", actions.len());

                let commit = CommitBuilder::from(properties)
                    .with_actions(actions)
                    .with_operation_id(operation_id)
                    .with_post_commit_hook_handler(handle.cloned())
                    .with_max_retries(DEFAULT_RETRIES + commits_made)
                    .build(
                        Some(&snapshot),
                        log_store.clone(),
                        self.task_parameters.input_parameters.clone().try_into()?,
                    )
                    .await?;
                snapshot = commit.snapshot().snapshot;
                commits_made += 1;
            }

            if end {
                break;
            }
        }

        if total_metrics.num_files_added == 0 {
            total_metrics.files_added.min = 0;
        }
        if total_metrics.num_files_removed == 0 {
            total_metrics.files_removed.min = 0;
        }

        table.state = Some(DeltaTableState::new(snapshot));

        Ok(total_metrics)
    }
}

/// Build a Plan on which files to merge together. See [OptimizeBuilder]
#[instrument(skip_all, fields(operation = "create_merge_plan", version = snapshot.version()))]
pub async fn create_merge_plan(
    log_store: &dyn LogStore,
    optimize_type: OptimizeType,
    snapshot: &EagerSnapshot,
    filters: &[PartitionFilter],
    target_size: Option<NonZeroU64>,
    writer_properties: WriterProperties,
    session: SessionState,
) -> Result<MergePlan, DeltaTableError> {
    let target_size = target_size.unwrap_or_else(|| snapshot.table_properties().target_file_size());
    let _ = optimize_target_size_to_i64(target_size)?;
    let partitions_keys = snapshot.metadata().partition_columns();

    let (operations, metrics, planner_stats) = match optimize_type {
        OptimizeType::Compact => {
            info!("building compaction plan");
            build_compaction_plan(log_store, snapshot, filters, target_size).await?
        }
        OptimizeType::ZOrder(zorder_columns) => {
            info!("building z-order plan");
            build_zorder_plan(
                log_store,
                zorder_columns,
                snapshot,
                partitions_keys,
                filters,
            )
            .await?
        }
    };

    info!(
        partitions_optimized = metrics.partitions_optimized,
        total_considered_files = metrics.total_considered_files,
        "merge plan created"
    );

    let input_parameters = OptimizeInput {
        target_size,
        predicate: serde_json::to_string(filters).ok(),
    };
    let file_schema = arrow_schema_without_partitions(
        &Arc::new(snapshot.schema().as_ref().try_into_arrow()?),
        partitions_keys,
    );

    Ok(MergePlan {
        operations,
        metrics,
        planner_stats,
        task_parameters: Arc::new(MergeTaskParameters {
            file_schema,
            writer_properties,
            input_parameters,
            num_indexed_cols: snapshot.table_properties().num_indexed_cols(),
            stats_columns: snapshot
                .table_properties()
                .data_skipping_stats_columns
                .as_ref()
                .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()),
        }),
        read_table_version: snapshot.version(),
        read_session: Arc::new(session),
    })
}

/// A collection of bins for a particular partition
#[derive(Debug, Clone)]
struct MergeBin {
    files: Vec<Add>,
    size_bytes: u64,
}

impl MergeBin {
    pub fn new() -> Self {
        MergeBin {
            files: Vec::new(),
            size_bytes: 0,
        }
    }

    fn total_file_size(&self) -> u64 {
        self.size_bytes
    }

    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    fn len(&self) -> usize {
        self.files.len()
    }

    fn from_file(add: Add) -> Self {
        let mut bin = Self::new();
        bin.add(add);
        bin
    }

    fn add(&mut self, add: Add) {
        self.size_bytes += add.size as u64;
        self.files.push(add);
    }

    fn iter(&self) -> impl Iterator<Item = &Add> {
        self.files.iter()
    }
}

impl IntoIterator for MergeBin {
    type Item = Add;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.files.into_iter()
    }
}

#[derive(Debug, Clone)]
struct OrderedFileCandidate {
    add: Add,
    stable_ordinal: usize,
    size_bytes: u64,
}

fn plan_compaction_bins_in_stable_order(
    files: Vec<OrderedFileCandidate>,
    target_size: u64,
) -> (Vec<MergeBin>, PlannerStats) {
    let mut bins = Vec::new();
    let mut current = MergeBin::new();
    let mut current_first_ordinal = None;
    let mut current_last_ordinal = None;
    let mut planner_stats = PlannerStats::preserve_locality();

    for file in files {
        if current.is_empty() {
            current = MergeBin::from_file(file.add);
            current_first_ordinal = Some(file.stable_ordinal);
            current_last_ordinal = Some(file.stable_ordinal);
            continue;
        }

        let extends_contiguous_span = current_last_ordinal
            .map(|last| file.stable_ordinal == last + 1)
            .unwrap_or(false);
        if !extends_contiguous_span {
            if let (Some(first), Some(last)) = (current_first_ordinal, current_last_ordinal) {
                planner_stats.max_bin_span_files =
                    planner_stats.max_bin_span_files.max(last - first + 1);
            }

            bins.push(current);
            current = MergeBin::from_file(file.add);
            current_first_ordinal = Some(file.stable_ordinal);
            current_last_ordinal = Some(file.stable_ordinal);
            continue;
        }

        if current.total_file_size() + file.size_bytes <= target_size {
            current.add(file.add);
            current_last_ordinal = Some(file.stable_ordinal);
            continue;
        }

        if let (Some(first), Some(last)) = (current_first_ordinal, current_last_ordinal) {
            planner_stats.max_bin_span_files =
                planner_stats.max_bin_span_files.max(last - first + 1);
        }

        bins.push(current);
        current = MergeBin::from_file(file.add);
        current_first_ordinal = Some(file.stable_ordinal);
        current_last_ordinal = Some(file.stable_ordinal);
    }

    if !current.is_empty() {
        if let (Some(first), Some(last)) = (current_first_ordinal, current_last_ordinal) {
            planner_stats.max_bin_span_files =
                planner_stats.max_bin_span_files.max(last - first + 1);
        }
        bins.push(current);
    }

    (bins, planner_stats)
}

async fn build_compaction_plan(
    log_store: &dyn LogStore,
    snapshot: &EagerSnapshot,
    filters: &[PartitionFilter],
    target_size: NonZeroU64,
) -> Result<(OptimizeOperations, Metrics, PlannerStats), DeltaTableError> {
    type PartitionFileEntry = (IndexMap<String, Scalar>, usize, Vec<OrderedFileCandidate>);

    let mut metrics = Metrics::default();
    let mut planner_stats = PlannerStats::preserve_locality();
    let mut partition_files: HashMap<String, PartitionFileEntry> = HashMap::new();

    let predicate = if filters.is_empty() {
        None
    } else {
        Some(Arc::new(to_kernel_predicate(
            filters,
            snapshot.schema().as_ref(),
        )?))
    };

    // `file_views` returns active files with the newest first.
    // We use that order within each partition when building compact bins.
    let mut file_stream = snapshot.file_views(log_store, predicate);
    while let Some(file) = file_stream.next().await {
        let file = file?;
        metrics.total_considered_files += 1;
        let object_meta = ObjectMeta::try_from(&file)?;
        let partition_values = file
            .partition_values()
            .map(|v| {
                v.fields()
                    .iter()
                    .zip(v.values().iter())
                    .map(|(k, v)| (k.name().to_string(), v.clone()))
                    .collect::<IndexMap<_, _>>()
            })
            .unwrap_or_default();
        let partition_path = partition_values.hive_partition_path();
        let entry = partition_files
            .entry(partition_path)
            .or_insert_with(|| (partition_values, 0, vec![]));
        let stable_ordinal = entry.1;
        entry.1 += 1;

        if object_meta.size > target_size.get() {
            metrics.total_files_skipped += 1;
            continue;
        }

        entry.2.push(OrderedFileCandidate {
            add: file.to_add(),
            stable_ordinal,
            size_bytes: object_meta.size,
        });
    }

    let mut operations: HashMap<String, (IndexMap<String, Scalar>, Vec<MergeBin>)> = HashMap::new();
    for (part, (partition, _, files)) in partition_files {
        let (merge_bins, partition_stats) =
            plan_compaction_bins_in_stable_order(files, target_size.get());
        planner_stats.absorb(&partition_stats);

        operations.insert(part, (partition, merge_bins));
    }

    // Prune merge bins with only 1 file, since they have no effect
    for (_, (_, bins)) in operations.iter_mut() {
        bins.retain(|bin| {
            if bin.len() == 1 {
                metrics.total_files_skipped += 1;
                false
            } else {
                true
            }
        });
        planner_stats.max_bin_span_files = planner_stats
            .max_bin_span_files
            .max(bins.iter().map(MergeBin::len).max().unwrap_or(0));
    }
    operations.retain(|_, (_, files)| !files.is_empty());

    metrics.partitions_optimized = operations.len() as u64;

    if operations.is_empty() {
        planner_stats.max_bin_span_files = 0;
    }

    Ok((
        OptimizeOperations::Compact(operations),
        metrics,
        planner_stats,
    ))
}

/// Validates that a z-order column path exists in the schema, supporting nested
/// struct fields via dot notation (e.g., "meta.field_a").
fn validate_zorder_column(schema: &StructType, column: &str) -> Result<(), DeltaTableError> {
    let mut segments = column.split('.').peekable();
    let mut current_struct = schema;
    while let Some(segment) = segments.next() {
        let field = current_struct.field(segment).ok_or_else(|| {
            DeltaTableError::Generic(format!(
                "Z-order column \"{column}\": field \"{segment}\" not found in schema"
            ))
        })?;
        if segments.peek().is_some() {
            match field.data_type() {
                DataType::Struct(inner) => current_struct = inner,
                _ => {
                    return Err(DeltaTableError::Generic(format!(
                        "Z-order column \"{column}\": \"{segment}\" is not a struct type"
                    )));
                }
            }
        }
    }
    Ok(())
}

async fn build_zorder_plan(
    log_store: &dyn LogStore,
    zorder_columns: Vec<String>,
    snapshot: &EagerSnapshot,
    partition_keys: &[String],
    filters: &[PartitionFilter],
) -> Result<(OptimizeOperations, Metrics, PlannerStats), DeltaTableError> {
    if zorder_columns.is_empty() {
        return Err(DeltaTableError::Generic(
            "Z-order requires at least one column".to_string(),
        ));
    }
    let zorder_partition_cols = zorder_columns
        .iter()
        .filter(|col| partition_keys.contains(col))
        .collect_vec();
    if !zorder_partition_cols.is_empty() {
        return Err(DeltaTableError::Generic(format!(
            "Z-order columns cannot be partition columns. Found: {zorder_partition_cols:?}"
        )));
    }
    for col in &zorder_columns {
        validate_zorder_column(snapshot.schema().as_ref(), col)?;
    }

    // For now, just be naive and optimize all files in each selected partition.
    let mut metrics = Metrics::default();

    let mut partition_files: HashMap<String, (IndexMap<String, Scalar>, MergeBin)> = HashMap::new();

    let predicate = if filters.is_empty() {
        None
    } else {
        Some(Arc::new(to_kernel_predicate(
            filters,
            snapshot.schema().as_ref(),
        )?))
    };

    let mut file_stream = snapshot.file_views(log_store, predicate);
    while let Some(file) = file_stream.next().await {
        let file = file?;
        let partition_values = file
            .partition_values()
            .map(|v| {
                v.fields()
                    .iter()
                    .zip(v.values().iter())
                    .map(|(k, v)| (k.name().to_string(), v.clone()))
                    .collect::<IndexMap<_, _>>()
            })
            .unwrap_or_default();
        metrics.total_considered_files += 1;
        partition_files
            .entry(partition_values.hive_partition_path())
            .or_insert_with(|| (partition_values, MergeBin::new()))
            .1
            .add(file.to_add());
        debug!("partition_files inside the zorder plan: {partition_files:?}");
    }

    let max_bin_span_files = partition_files
        .values()
        .map(|(_, bin)| bin.len())
        .max()
        .unwrap_or(0);
    let operation = OptimizeOperations::ZOrder(zorder_columns, partition_files);
    Ok((
        operation,
        metrics,
        PlannerStats::z_order(max_bin_span_files),
    ))
}

#[cfg(test)]
mod compact_planner_tests {
    use super::*;
    use std::collections::HashMap;

    fn candidate(stable_ordinal: usize, size_bytes: u64) -> OrderedFileCandidate {
        OrderedFileCandidate {
            add: Add {
                path: format!("part-{stable_ordinal}.parquet"),
                partition_values: HashMap::new(),
                size: size_bytes as i64,
                modification_time: stable_ordinal as i64,
                data_change: false,
                stats: None,
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            },
            stable_ordinal,
            size_bytes,
        }
    }

    fn ordinals(bin: &MergeBin) -> Vec<usize> {
        bin.iter()
            .map(|add| add.modification_time as usize)
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_ordered_compact_bins_are_contiguous() {
        let (bins, stats) = plan_compaction_bins_in_stable_order(
            vec![
                candidate(0, 6),
                candidate(1, 3),
                candidate(2, 6),
                candidate(3, 3),
            ],
            10,
        );

        let planned_ordinals = bins.iter().map(ordinals).collect::<Vec<_>>();

        assert_eq!(planned_ordinals, vec![vec![0, 1], vec![2, 3]]);
        assert_eq!(stats.max_bin_span_files, 2);
    }

    #[test]
    fn test_ordered_compact_bins_do_not_merge_non_adjacent_files() {
        let (bins, _) = plan_compaction_bins_in_stable_order(
            vec![
                candidate(0, 8),
                candidate(1, 8),
                candidate(2, 2),
                candidate(3, 2),
            ],
            10,
        );

        let planned_ordinals = bins.iter().map(ordinals).collect::<Vec<_>>();

        assert_eq!(planned_ordinals, vec![vec![0], vec![1, 2], vec![3]]);
        assert!(
            planned_ordinals
                .iter()
                .all(|bin| { bin.windows(2).all(|window| window[1] == window[0] + 1) })
        );
    }

    #[test]
    fn test_ordered_compact_bins_respect_ordinal_gaps() {
        let (bins, stats) =
            plan_compaction_bins_in_stable_order(vec![candidate(0, 3), candidate(2, 3)], 10);

        let planned_ordinals = bins.iter().map(ordinals).collect::<Vec<_>>();

        assert_eq!(planned_ordinals, vec![vec![0], vec![2]]);
        assert_eq!(stats.max_bin_span_files, 1);
    }

    #[test]
    fn test_ordered_compact_bins_track_span_and_displacement() {
        let (_, stats) = plan_compaction_bins_in_stable_order(
            vec![
                candidate(0, 3),
                candidate(1, 3),
                candidate(2, 3),
                candidate(3, 9),
            ],
            10,
        );

        assert_eq!(stats.planner_strategy, PlannerStrategy::PreserveLocality);
        assert!(stats.preserved_stable_order);
        assert_eq!(stats.max_bin_span_files, 3);
    }

    #[test]
    fn test_optimize_input_target_size_must_fit_i64() {
        let input = OptimizeInput {
            target_size: std::num::NonZeroU64::new(i64::MAX as u64 + 1).unwrap(),
            predicate: None,
        };

        let err = crate::protocol::DeltaOperation::try_from(input).unwrap_err();
        assert!(err.to_string().contains("optimize target_size"));
        assert!(err.to_string().contains("i64::MAX"));
    }
}

pub(super) mod util {
    use super::*;
    use futures::Future;
    use tokio::task::JoinError;

    pub async fn flatten_join_error<T, E>(
        future: impl Future<Output = Result<Result<T, E>, JoinError>>,
    ) -> Result<T, DeltaTableError>
    where
        E: Into<DeltaTableError>,
    {
        match future.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(error.into()),
            Err(error) => Err(DeltaTableError::GenericError {
                source: Box::new(error),
            }),
        }
    }
}

/// Z-order utilities
pub(super) mod zorder {
    use super::*;

    use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
    use arrow_array::{Array, ArrayRef, BinaryArray};
    use arrow_buffer::bit_util::{get_bit_raw, set_bit_raw, unset_bit_raw};
    use arrow_row::{Row, RowConverter, SortField};
    use arrow_schema::ArrowError;
    // use arrow_schema::Schema as ArrowSchema;

    pub use self::datafusion::ZOrderExecContext;

    pub(super) mod datafusion {
        use super::*;
        use url::Url;

        use ::datafusion::common::DataFusionError;
        use ::datafusion::logical_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
            Volatility,
        };
        use ::datafusion::prelude::SessionContext;
        use arrow_schema::DataType;
        use itertools::Itertools;
        use std::any::Any;

        pub const ZORDER_UDF_NAME: &str = "zorder_key";

        pub struct ZOrderExecContext {
            pub columns: Arc<[String]>,
            pub ctx: SessionContext,
        }

        impl ZOrderExecContext {
            pub fn new(
                columns: Vec<String>,
                session: SessionState,
                object_store_ref: ObjectStoreRef,
            ) -> Result<Self, DataFusionError> {
                let columns = columns.into();

                let ctx = SessionContext::new_with_state(session);
                ctx.register_udf(ScalarUDF::from(datafusion::ZOrderUDF));
                ctx.register_object_store(&Url::parse("delta-rs://").unwrap(), object_store_ref);
                Ok(Self { columns, ctx })
            }
        }

        // DataFusion UDF impl for zorder_key
        #[derive(Debug, Hash, PartialEq, Eq)]
        pub struct ZOrderUDF;

        impl ScalarUDFImpl for ZOrderUDF {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                ZORDER_UDF_NAME
            }

            fn signature(&self) -> &Signature {
                static SIGNATURE: std::sync::LazyLock<Signature> =
                    std::sync::LazyLock::new(|| Signature {
                        type_signature: TypeSignature::VariadicAny,
                        volatility: Volatility::Immutable,
                        parameter_names: Some(vec![]),
                    });
                &SIGNATURE
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
                Ok(DataType::Binary)
            }

            fn invoke_with_args(
                &self,
                args: ScalarFunctionArgs,
            ) -> ::datafusion::common::Result<ColumnarValue> {
                zorder_key_datafusion(&args.args)
            }
        }

        /// Datafusion zorder UDF body
        fn zorder_key_datafusion(
            columns: &[ColumnarValue],
        ) -> Result<ColumnarValue, DataFusionError> {
            debug!("zorder_key_datafusion: {columns:#?}");
            let length = columns
                .iter()
                .map(|col| match col {
                    ColumnarValue::Array(array) => array.len(),
                    ColumnarValue::Scalar(_) => 1,
                })
                .max()
                .ok_or(DataFusionError::NotImplemented(
                    "z-order on zero columns.".to_string(),
                ))?;
            let columns: Vec<ArrayRef> = columns
                .iter()
                .map(|col| col.clone().into_array(length))
                .try_collect()?;
            let array = zorder_key(&columns)?;
            Ok(ColumnarValue::Array(array))
        }

        #[cfg(test)]
        mod tests {
            use super::*;
            use ::datafusion::assert_batches_eq;
            use arrow_array::{Int32Array, StringArray};
            use arrow_ord::sort::sort_to_indices;
            use arrow_schema::Field;
            use arrow_select::take::take;
            use rand::RngExt;

            #[test]
            fn test_order() {
                let int: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
                let str: ArrayRef = Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("x"),
                    Some("a"),
                    Some("x"),
                    None,
                ]));
                let int_large: ArrayRef = Arc::new(Int32Array::from(vec![10000, 2000, 300, 40, 5]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("int", int),
                    ("str", str),
                    ("int_large", int_large),
                ])
                .unwrap();

                let expected_1 = vec![
                    "+-----+-----+-----------+",
                    "| int | str | int_large |",
                    "+-----+-----+-----------+",
                    "| 1   | a   | 10000     |",
                    "| 2   | x   | 2000      |",
                    "| 3   | a   | 300       |",
                    "| 4   | x   | 40        |",
                    "| 5   |     | 5         |",
                    "+-----+-----+-----------+",
                ];
                let expected_2 = vec![
                    "+-----+-----+-----------+",
                    "| int | str | int_large |",
                    "+-----+-----+-----------+",
                    "| 5   |     | 5         |",
                    "| 1   | a   | 10000     |",
                    "| 3   | a   | 300       |",
                    "| 2   | x   | 2000      |",
                    "| 4   | x   | 40        |",
                    "+-----+-----+-----------+",
                ];
                let expected_3 = vec![
                    "+-----+-----+-----------+",
                    "| int | str | int_large |",
                    "+-----+-----+-----------+",
                    "| 5   |     | 5         |",
                    "| 4   | x   | 40        |",
                    "| 2   | x   | 2000      |",
                    "| 3   | a   | 300       |",
                    "| 1   | a   | 10000     |",
                    "+-----+-----+-----------+",
                ];

                let expected = [expected_1, expected_2, expected_3];

                let indices = Int32Array::from(shuffled_indices().to_vec());
                let shuffled_columns = batch
                    .columns()
                    .iter()
                    .map(|c| take(c, &indices, None).unwrap())
                    .collect::<Vec<_>>();
                let shuffled_batch =
                    RecordBatch::try_new(batch.schema(), shuffled_columns).unwrap();

                for i in 1..=batch.num_columns() {
                    let columns = (0..i)
                        .map(|idx| shuffled_batch.column(idx).clone())
                        .collect::<Vec<_>>();

                    let order_keys = zorder_key(&columns).unwrap();
                    let indices = sort_to_indices(order_keys.as_ref(), None, None).unwrap();
                    let sorted_columns = shuffled_batch
                        .columns()
                        .iter()
                        .map(|c| take(c, &indices, None).unwrap())
                        .collect::<Vec<_>>();
                    let sorted_batch =
                        RecordBatch::try_new(batch.schema(), sorted_columns).unwrap();

                    assert_batches_eq!(expected[i - 1], &[sorted_batch]);
                }
            }
            fn shuffled_indices() -> [i32; 5] {
                let mut rng = rand::rng();
                let mut array = [0, 1, 2, 3, 4];
                for i in (1..array.len()).rev() {
                    let j = rng.random_range(0..=i);
                    array.swap(i, j);
                }
                array
            }

            #[tokio::test]
            async fn test_zorder_mixed_case() {
                use arrow_schema::Schema as ArrowSchema;
                let schema = Arc::new(ArrowSchema::new(vec![
                    Field::new("moDified", DataType::Utf8, true),
                    Field::new("ID", DataType::Utf8, true),
                    Field::new("vaLue", DataType::Int32, true),
                ]));

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(arrow::array::StringArray::from(vec![
                            "2021-02-01",
                            "2021-02-01",
                            "2021-02-02",
                            "2021-02-02",
                        ])),
                        Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
                        Arc::new(arrow::array::Int32Array::from(vec![1, 10, 20, 100])),
                    ],
                )
                .unwrap();
                // write some data
                let table = crate::DeltaTable::new_in_memory()
                    .write(vec![batch.clone()])
                    .with_save_mode(crate::protocol::SaveMode::Append)
                    .await
                    .unwrap();

                let res = table
                    .optimize()
                    .with_type(OptimizeType::ZOrder(vec!["moDified".into()]))
                    .await;
                assert!(res.is_ok());
            }

            /// Issue <https://github.com/delta-io/delta-rs/issues/2834>
            #[tokio::test]
            async fn test_zorder_space_in_partition_value() {
                use arrow_schema::Schema as ArrowSchema;
                let _ = pretty_env_logger::try_init();
                let schema = Arc::new(ArrowSchema::new(vec![
                    Field::new("modified", DataType::Utf8, true),
                    Field::new("country", DataType::Utf8, true),
                    Field::new("value", DataType::Int32, true),
                ]));

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(arrow::array::StringArray::from(vec![
                            "2021-02-01",
                            "2021-02-01",
                            "2021-02-02",
                            "2021-02-02",
                        ])),
                        Arc::new(arrow::array::StringArray::from(vec![
                            "Germany",
                            "China",
                            "Canada",
                            "Dominican Republic",
                        ])),
                        Arc::new(arrow::array::Int32Array::from(vec![1, 10, 20, 100])),
                        //Arc::new(arrow::array::StringArray::from(vec!["Dominican Republic"])),
                        //Arc::new(arrow::array::Int32Array::from(vec![100])),
                    ],
                )
                .unwrap();
                // write some data
                let table = DeltaTable::new_in_memory()
                    .write(vec![batch.clone()])
                    .with_partition_columns(vec!["country"])
                    .with_save_mode(crate::protocol::SaveMode::Overwrite)
                    .await
                    .unwrap();

                let res = table
                    .optimize()
                    .with_type(OptimizeType::ZOrder(vec!["modified".into()]))
                    .await;
                assert!(res.is_ok(), "Failed to optimize: {res:#?}");
            }

            #[tokio::test]
            async fn test_zorder_space_in_partition_value_garbage() {
                use arrow_schema::Schema as ArrowSchema;
                let _ = pretty_env_logger::try_init();
                let schema = Arc::new(ArrowSchema::new(vec![
                    Field::new("modified", DataType::Utf8, true),
                    Field::new("country", DataType::Utf8, true),
                    Field::new("value", DataType::Int32, true),
                ]));

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(arrow::array::StringArray::from(vec![
                            "2021-02-01",
                            "2021-02-01",
                            "2021-02-02",
                            "2021-02-02",
                        ])),
                        Arc::new(arrow::array::StringArray::from(vec![
                            "Germany", "China", "Canada", "USA$$!",
                        ])),
                        Arc::new(arrow::array::Int32Array::from(vec![1, 10, 20, 100])),
                    ],
                )
                .unwrap();
                // write some data
                let table = DeltaTable::new_in_memory()
                    .write(vec![batch.clone()])
                    .with_partition_columns(vec!["country"])
                    .with_save_mode(crate::protocol::SaveMode::Overwrite)
                    .await
                    .unwrap();

                let res = table
                    .optimize()
                    .with_type(OptimizeType::ZOrder(vec!["modified".into()]))
                    .await;
                assert!(res.is_ok(), "Failed to optimize: {res:#?}");
            }
        }
    }

    /// Creates a new binary array containing the zorder keys for the given columns
    ///
    /// Each value is 16 bytes * number of columns. Each column is converted into
    /// its row binary representation, and then the first 16 bytes are taken.
    /// These truncated values are interleaved in the array values.
    pub fn zorder_key(columns: &[ArrayRef]) -> Result<ArrayRef, ArrowError> {
        if columns.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "Cannot zorder empty columns".to_string(),
            ));
        }

        // length is length of first array or 1 if all scalars
        let out_length = columns[0].len();

        if columns.iter().any(|col| col.len() != out_length) {
            return Err(ArrowError::InvalidArgumentError(
                "All columns must have the same length".to_string(),
            ));
        }

        // We are taking 128 bits (16 bytes) from each value. Shorter values will be padded.
        let value_size: usize = columns.len() * 16;

        // Initialize with zeros
        let mut out: Vec<u8> = vec![0; out_length * value_size];

        for (col_pos, col) in columns.iter().enumerate() {
            set_bits_for_column(col.clone(), col_pos, columns.len(), &mut out)?;
        }

        let offsets = (0..=out_length)
            .map(|i| (i * value_size) as i32)
            .collect::<Vec<i32>>();

        let out_arr = BinaryArray::try_new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(out),
            None,
        )?;

        Ok(Arc::new(out_arr))
    }

    /// Given an input array, will set the bits in the output array
    ///
    /// Arguments:
    /// * `input` - The input array
    /// * `col_pos` - The position of the column. Used to determine position
    ///   when interleaving.
    /// * `num_columns` - The number of columns in the input array. Used to
    ///   determine offset when interleaving.
    fn set_bits_for_column(
        input: ArrayRef,
        col_pos: usize,
        num_columns: usize,
        out: &mut Vec<u8>,
    ) -> Result<(), ArrowError> {
        // Convert array to rows
        let converter = RowConverter::new(vec![SortField::new(input.data_type().clone())])?;
        let rows = converter.convert_columns(&[input])?;

        for (row_i, row) in rows.iter().enumerate() {
            // How many bytes to get to this row's out position
            let row_offset = row_i * num_columns * 16;
            for bit_i in 0..128 {
                let bit = row.get_bit(bit_i);
                // Position of bit within the value. We place a value every
                // `num_columns` bits, offset by `col_pos` when interleaving.
                // So if there are 3 columns, and we are the second column, then
                // we place values at index: 1, 4, 7, 10, etc.
                let bit_pos = (bit_i * num_columns) + col_pos;
                let out_pos = (row_offset * 8) + bit_pos;
                // Safety: we pre-sized the output vector in the outer function
                if bit {
                    unsafe { set_bit_raw(out.as_mut_ptr(), out_pos) };
                } else {
                    unsafe { unset_bit_raw(out.as_mut_ptr(), out_pos) };
                }
            }
        }

        Ok(())
    }

    trait RowBitUtil {
        fn get_bit(&self, bit_i: usize) -> bool;
    }

    impl RowBitUtil for Row<'_> {
        /// Get the bit at the given index, or just give false if the index is out of bounds
        fn get_bit(&self, bit_i: usize) -> bool {
            let byte_i = bit_i / 8;
            let bytes = self.as_ref();
            if byte_i >= bytes.len() {
                return false;
            }
            // Safety: we just did a bounds check above
            unsafe { get_bit_raw(bytes.as_ptr(), bit_i) }
        }
    }

    #[cfg(test)]
    mod test {
        use arrow_array::{
            StringArray, UInt8Array, cast::as_generic_binary_array, new_empty_array,
        };
        use arrow_schema::DataType;

        use super::*;
        use crate::ensure_table_uri;

        #[test]
        fn test_rejects_no_columns() {
            let columns = vec![];
            let result = zorder_key(&columns);
            assert!(result.is_err());
        }

        #[test]
        fn test_handles_no_rows() {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(new_empty_array(&DataType::Int64)),
                Arc::new(new_empty_array(&DataType::Utf8)),
            ];
            let result = zorder_key(columns.as_slice());
            assert!(result.is_ok());
            let result = result.unwrap();
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_basics() {
            let columns: Vec<ArrayRef> = vec![
                // Small strings
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
                // Strings of various sizes
                Arc::new(StringArray::from(vec![
                    "delta-rs: A native Rust library for Delta Lake, with bindings into Python",
                    "cat",
                    "",
                ])),
                Arc::new(UInt8Array::from(vec![Some(1), Some(4), None])),
            ];
            let result = zorder_key(columns.as_slice()).unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result.data_type(), &DataType::Binary);
            assert_eq!(result.null_count(), 0);

            let data: &BinaryArray = as_generic_binary_array(result.as_ref());
            assert_eq!(data.value_data().len(), 3 * 16 * 3);
            assert!(data.iter().all(|x| x.unwrap().len() == 3 * 16));
        }

        #[tokio::test]
        async fn works_on_spark_table() {
            use tempfile::TempDir;
            // Create a temporary directory
            let tmp_dir = TempDir::new().expect("Failed to make temp dir");
            let table_name = "delta-1.2.1-only-struct-stats";

            // Copy recursively from the test data directory to the temporary directory
            let source_path = format!("../test/tests/data/{table_name}");
            fs_extra::dir::copy(source_path, tmp_dir.path(), &Default::default()).unwrap();

            let table_uri =
                ensure_table_uri(tmp_dir.path().join(table_name).to_str().unwrap()).unwrap();
            // Run optimize
            let (_, metrics) = DeltaTable::try_from_url(table_uri)
                .await
                .unwrap()
                .optimize()
                .await
                .unwrap();

            // Verify it worked
            assert_eq!(metrics.num_files_added, 1);
        }
    }
}
