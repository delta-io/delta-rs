//! Delete records from a Delta Table that satisfy a predicate
//!
//! When a predicate is not provided then all records are deleted from the Delta
//! Table. Otherwise a scan of the Delta table is performed to mark any files
//! that contain records that satisfy the predicate. Once files are determined
//! they are rewritten without the records.
//!
//! Predicates MUST be deterministic otherwise undefined behaviour may occur during the
//! scanning and rewriting phase.
//!
//! # Example
//! ```
//! # use datafusion::logical_expr::{col, lit};
//! # use deltalake_core::{DeltaTable, kernel::{DataType, PrimitiveType, StructType, StructField}};
//! # use deltalake_core::operations::delete::DeleteBuilder;
//! # tokio_test::block_on(async {
//! #  let schema = StructType::try_new(vec![
//! #      StructField::new(
//! #          "id".to_string(),
//! #          DataType::Primitive(PrimitiveType::String),
//! #          true,
//! #      )]).expect("Failed to generate schema for test");
//! # let table = DeltaTable::try_from_url(url::Url::parse("memory://").unwrap())
//! #               .await.expect("Failed to construct DeltaTable instance for test")
//! #        .create()
//! #        .with_columns(schema.fields().cloned())
//! #        .await
//! #        .expect("Failed to create test table");
//! let (table, metrics) = table.delete()
//!     .with_predicate(col("id").eq(lit(102)))
//!     .await
//!     .expect("Failed to delete");
//! # })
//! ````
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{ToDFSchema as _, exec_datafusion_err};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode, lit};
use datafusion::optimizer::simplify_expressions::simplify_predicates;
use datafusion::physical_plan::{ExecutionPlan, metrics::MetricBuilder};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;
use futures::future::BoxFuture;
use futures::{StreamExt as _, TryStreamExt, stream};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use uuid::Uuid;

use super::Operation;
use super::cdc::should_write_cdc;
use crate::DeltaTable;
use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::DeltaSessionExt;
use crate::delta_datafusion::SessionFallbackPolicy;
use crate::delta_datafusion::SessionResolveContext;
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::logical::{
    LogicalPlanBuilderExt as _, LogicalPlanExt, MetricObserver,
};
use crate::delta_datafusion::physical::{MetricObserverExec, find_metric_node, get_metric};
use crate::delta_datafusion::{
    Expression, add_actions_partition_mem_table, create_session, resolve_session_state,
    scan_files_where_matches, update_datafusion_session,
};
use crate::errors::DeltaResult;
use crate::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL};
use crate::kernel::{Action, EagerSnapshot, resolve_snapshot};
use crate::logstore::{LogStore, LogStoreRef};
use crate::operations::CustomExecuteHandler;
use crate::operations::cdc::CDC_COLUMN_NAME;
use crate::operations::write::execution::write_exec_plan;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;

const SOURCE_COUNT_ID: &str = "delete_source_count";
const SOURCE_COUNT_METRIC: &str = "num_source_rows";

/// Delete Records from the Delta Table.
/// See this module's documentation for more information
#[derive(Clone)]
pub struct DeleteBuilder {
    /// Which records to delete
    predicate: Option<Expression>,
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    session: Option<Arc<dyn Session>>,
    session_fallback_policy: SessionFallbackPolicy,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Commit properties and configuration
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl std::fmt::Debug for DeleteBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteBuilder")
            .field("predicate", &self.predicate)
            .field("snapshot", &self.snapshot)
            .field("log_store", &self.log_store)
            .field("commit_properties", &self.commit_properties)
            .finish()
    }
}

#[derive(Default, Debug, Serialize)]
/// Metrics for the Delete Operation
pub struct DeleteMetrics {
    /// Number of files added
    pub num_added_files: usize,
    /// Number of files removed
    pub num_removed_files: usize,
    /// Number of rows removed
    pub num_deleted_rows: usize,
    /// Number of rows copied in the process of deleting files
    pub num_copied_rows: usize,
    /// Time taken to execute the entire operation
    pub execution_time_ms: u64,
    /// Time taken to scan the file for matches
    pub scan_time_ms: u64,
    /// Time taken to rewrite the matched files
    pub rewrite_time_ms: u64,
}

impl super::Operation for DeleteBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl DeleteBuilder {
    /// Create a new [`DeleteBuilder`]
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            predicate: None,
            snapshot,
            log_store,
            session: None,
            session_fallback_policy: SessionFallbackPolicy::default(),
            commit_properties: CommitProperties::default(),
            writer_properties: None,
            custom_execute_handler: None,
        }
    }

    /// A predicate that determines if a record is deleted
    pub fn with_predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
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

    /// Additional information to write to the commit
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Writer properties passed to parquet writer for when files are rewritten
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

impl std::future::IntoFuture for DeleteBuilder {
    type Output = DeltaResult<(DeltaTable, DeleteMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), true, None).await?;
            PROTOCOL.check_append_only(&snapshot)?;
            PROTOCOL.can_write_to(&snapshot)?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            let (session, _) = resolve_session_state(
                this.session.as_deref(),
                this.session_fallback_policy,
                || create_session().state(),
                SessionResolveContext {
                    operation: "delete",
                    table_uri: Some(this.log_store.root_url()),
                    cdc: false,
                },
            )?;
            update_datafusion_session(&session, &this.log_store, Some(operation_id))?;
            session.ensure_log_store_registered(this.log_store.as_ref())?;

            let predicate = this
                .predicate
                .map(|p| {
                    let scan_config = DeltaScanConfig::new_from_session(&session);
                    let predicate_schema = scan_config
                        .table_schema(snapshot.table_configuration())?
                        .to_dfschema_ref()?;
                    p.resolve(&session, predicate_schema)
                })
                .transpose()?;

            let operation = DeltaOperation::Delete {
                predicate: predicate.as_ref().map(|p| fmt_expr_to_sql(p)).transpose()?,
            };

            let (actions, metrics) = execute(
                predicate,
                this.log_store.clone(),
                snapshot.clone(),
                &session,
                operation_id,
            )
            .await?;

            // Do not make a commit when there are zero updates to the state
            if actions.is_empty() {
                return Ok((
                    DeltaTable::new_with_state(this.log_store, DeltaTableState { snapshot }),
                    metrics,
                ));
            }

            let mut props = this.commit_properties;
            props
                .app_metadata
                .insert("readVersion".to_owned(), snapshot.version().into());
            props.app_metadata.insert(
                "operationMetrics".to_owned(),
                serde_json::to_value(&metrics)?,
            );

            let handle = this.custom_execute_handler.take();
            let commit = CommitBuilder::from(props)
                .with_actions(actions)
                .with_operation_id(operation_id)
                .with_post_commit_hook_handler(handle.clone())
                .build(Some(&snapshot), this.log_store.clone(), operation)
                .await?;

            if let Some(handler) = handle {
                handler.post_execute(&this.log_store, operation_id).await?;
            }

            Ok((
                DeltaTable::new_with_state(this.log_store, commit.snapshot()),
                metrics,
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DeleteMetricExtensionPlanner {}

impl DeleteMetricExtensionPlanner {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ExtensionPlanner for DeleteMetricExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(metric_observer) = node.as_any().downcast_ref::<MetricObserver>()
            && metric_observer.id.eq(SOURCE_COUNT_ID)
        {
            return Ok(Some(MetricObserverExec::try_new(
                SOURCE_COUNT_ID.into(),
                physical_inputs,
                |batch, metrics| {
                    MetricBuilder::new(metrics)
                        .global_counter(SOURCE_COUNT_METRIC)
                        .add(batch.num_rows());
                },
            )?));
        }
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        operation = "delete",
        version = snapshot.version(),
        table_uri = %log_store.root_url(),
    )
)]
async fn execute(
    predicate: Option<Expr>,
    log_store: LogStoreRef,
    snapshot: EagerSnapshot,
    session: &dyn Session,
    operation_id: Uuid,
) -> DeltaResult<(Vec<Action>, DeleteMetrics)> {
    let exec_start = Instant::now();
    let mut metrics = DeleteMetrics::default();
    metrics.num_removed_files = 0;
    metrics.num_added_files = 0;

    let scan_start = Instant::now();

    let Some(predicate) = predicate else {
        // no predicate, so we are just dropping all files.
        // we also don't need to write cdc actions if we are fropping all files.
        let removes: Vec<_> = snapshot
            .file_views(log_store.as_ref(), None)
            .map_ok(|f| f.remove_action(true).into())
            .try_collect()
            .await?;
        metrics.num_removed_files = removes.len();
        metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_millis() as u64;
        metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;
        return Ok((removes, metrics));
    };

    let skipping_pred = simplify_predicates(split_conjunction_owned(predicate.clone()))?;
    let partition_columns = snapshot
        .table_configuration()
        .metadata()
        .partition_columns()
        .clone();
    let mut props = crate::delta_datafusion::FindFilesExprProperties {
        partition_columns,
        partition_only: true,
        result: Ok(()),
    };
    for term in &skipping_pred {
        term.visit(&mut props)?;
        std::mem::replace(&mut props.result, Ok(()))?;
    }

    if props.partition_only {
        let partition_predicate = conjunction(skipping_pred).unwrap_or(lit(true));
        let removes: Vec<_> = match crate::delta_datafusion::engine::to_delta_predicate(
            &partition_predicate,
        ) {
            Ok(delta_predicate) => {
                // `Snapshot::files` documents predicate filtering as "best effort" file skipping
                // because, in general, files may contain a mix of matching and non-matching rows.
                //
                // For partition-only predicates, partition values are constant per file, so
                // evaluating `partition_predicate` against `partitionValues_parsed` is exact:
                // a file either fully matches or does not. It is therefore safe to treat this as
                // the authoritative match set for DELETE.
                snapshot
                    .file_views(log_store.as_ref(), Some(Arc::new(delta_predicate)))
                    .map_ok(|f| f.remove_action(true).into())
                    .try_collect()
                    .await?
            }
            Err(err) => {
                tracing::debug!(
                    ?err,
                    "Partition-only delete predicate not convertible to kernel; falling back to DataFusion evaluation"
                );

                let matching_paths = Arc::new(
                    find_file_paths_by_partition_predicate_datafusion(
                        session,
                        &snapshot,
                        &partition_predicate,
                    )
                    .await?,
                );
                snapshot
                    .file_views(log_store.as_ref(), None)
                    .try_filter_map(|f| {
                        let matching_paths = Arc::clone(&matching_paths);
                        async move {
                            Ok(matching_paths
                                .contains(f.path_raw())
                                .then(|| f.remove_action(true).into()))
                        }
                    })
                    .try_collect()
                    .await?
            }
        };

        metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_millis() as u64;
        metrics.num_removed_files = removes.len();
        metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;

        return Ok((removes, metrics));
    }

    let maybe_scan_plan = scan_files_where_matches(session, &snapshot, predicate).await?;
    metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_millis() as u64;

    let Some(files_scan) = maybe_scan_plan else {
        // no files contain data matching the predicate, so nothing more todo.
        metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;
        return Ok((vec![], metrics));
    };

    let root_url = Arc::new(snapshot.table_configuration().table_root().clone());
    let removes: Vec<_> = snapshot
        .file_views(log_store.as_ref(), Some(files_scan.delta_predicate.clone()))
        .zip(stream::iter(std::iter::repeat((
            root_url,
            Arc::new(files_scan.files_set()),
        ))))
        .map(|(f, u)| f.map(|f| (f, u)))
        .try_filter_map(|(f, (root, valid))| async move {
            let url = root
                .clone()
                .join(f.path_raw())
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            let is_valid = valid.contains(url.as_ref());
            Ok(is_valid.then(|| Action::Remove(f.remove_action(true))))
        })
        .try_collect()
        .await?;
    metrics.num_removed_files = removes.len();

    let counted_scan = LogicalPlan::Extension(Extension {
        node: Arc::new(MetricObserver {
            id: SOURCE_COUNT_ID.into(),
            input: files_scan.scan().clone(),
            enable_pushdown: false,
        }),
    });

    // We only want to delete data where our predicate evaluates to `true`,
    // so we need to retain all data from the removed files where the predicate
    // is `false` or `NULL` (i.e. is not true)
    let rescued_data = counted_scan
        .into_builder()
        .filter(files_scan.predicate.clone().is_not_true())?
        .build()?;

    let (write_plan, write_cdc) = if should_write_cdc(&snapshot)? {
        // create change set entries for all records we deleted
        let cdc_deletes = files_scan
            .scan()
            .clone()
            .into_builder()
            .filter(files_scan.predicate)?
            .with_column(CDC_COLUMN_NAME, lit("delete"))?
            .build()?;
        (
            rescued_data
                .into_builder()
                .with_column(CDC_COLUMN_NAME, lit(""))?
                .union(cdc_deletes)?
                .build()?,
            true,
        )
    } else {
        (rescued_data, false)
    };

    let exec = session.create_physical_plan(&write_plan).await?;
    let (mut actions, _) = write_exec_plan(
        session,
        log_store.as_ref(),
        snapshot.table_configuration(),
        exec.clone(),
        Some(operation_id),
        write_cdc,
    )
    .await?;

    if let Some(source_count) = find_metric_node(SOURCE_COUNT_ID, &exec) {
        let source_count_metrics = source_count.metrics().unwrap();
        let read_records = get_metric(&source_count_metrics, SOURCE_COUNT_METRIC);
        let filter_records = exec.metrics().and_then(|m| m.output_rows()).unwrap_or(0);
        metrics.num_copied_rows = filter_records;
        metrics.num_deleted_rows = read_records - filter_records;
    };

    metrics.num_added_files = actions.len();
    actions.extend(removes);

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_millis() as u64;
    Ok((actions, metrics))
}

async fn find_file_paths_by_partition_predicate_datafusion(
    session: &dyn Session,
    snapshot: &EagerSnapshot,
    predicate: &Expr,
) -> DeltaResult<std::collections::HashSet<String>> {
    use arrow_array::StringArray;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::col;

    use crate::delta_datafusion::PATH_COLUMN;
    use crate::errors::DeltaTableError;
    use datafusion::datasource::provider_as_source;
    use datafusion::physical_plan::collect;

    let Some(mem_table) = add_actions_partition_mem_table(snapshot)? else {
        return Ok(std::collections::HashSet::new());
    };

    let plan = LogicalPlanBuilder::scan(
        "partition_predicate",
        provider_as_source(Arc::new(mem_table)),
        None,
    )?
    .filter(predicate.to_owned())?
    .project([col(PATH_COLUMN)])?
    .build()?;

    let exec = session.create_physical_plan(&plan).await?;
    let batches = collect(exec, session.task_ctx()).await?;

    let mut paths = std::collections::HashSet::new();
    for batch in batches {
        let array = batch
            .column_by_name(PATH_COLUMN)
            .ok_or_else(|| DeltaTableError::Generic(format!("Column `{PATH_COLUMN}` missing")))?;
        let array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DeltaTableError::Generic(format!("Column `{PATH_COLUMN}` was not Utf8"))
            })?;

        for path in array.iter().flatten() {
            paths.insert(path.to_string());
        }
    }

    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::kernel::DataType as DeltaDataType;
    use crate::operations::collect_sendable_stream;
    use crate::protocol::*;
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::datafusion::write_batch;
    use crate::writer::test_utils::{
        get_arrow_schema, get_delta_schema, get_record_batch, setup_table_with_configuration,
    };
    use crate::{DeltaResult, DeltaTable, TableProperty};
    use arrow::array::Int32Array;
    use arrow::datatypes::TimestampMicrosecondType;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::ArrayRef;
    use arrow_array::BinaryArray;
    use arrow_array::StringArray;
    use arrow_array::StructArray;
    use arrow_buffer::NullBuffer;
    use arrow_schema::DataType;
    use arrow_schema::Fields;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::*;
    use datafusion::scalar::ScalarValue;
    use delta_kernel::engine::arrow_conversion::TryIntoKernel;
    use delta_kernel::schema::PrimitiveType;
    use delta_kernel::schema::StructType;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::sync::Arc;

    async fn setup_table(partitions: Option<Vec<&str>>) -> DeltaTable {
        let table_schema = get_delta_schema();

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(partitions.unwrap_or_default())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        table
    }

    #[tokio::test]
    async fn test_delete_on_empty_table() -> DeltaResult<()> {
        let table = setup_table(None).await;
        let batch = get_record_batch(None, false);
        let table = write_batch(table, batch).await;
        assert_eq!(Some(1), table.version());

        let (table, _metrics) = DeleteBuilder::new(table.log_store(), None).await?;
        assert_eq!(Some(2), table.version());

        let (table, _metrics) = DeleteBuilder::new(table.log_store(), None).await?;
        assert_eq!(Some(2), table.version());

        let actual = get_data(&table).await;
        assert!(actual.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_when_delta_table_is_append_only() {
        let table = setup_table_with_configuration(TableProperty::AppendOnly, Some("true")).await;
        let batch = get_record_batch(None, false);
        // append some data
        let table = write_batch(table, batch).await;
        // delete
        let _err = table
            .delete()
            .await
            .expect_err("Remove action is included when Delta table is append-only. Should error");
    }

    #[tokio::test]
    async fn test_delete_default() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = table
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 1);
        assert_eq!(state.log_data().num_files(), 1);

        let (table, metrics) = table.delete().await.unwrap();
        let state = table.snapshot().unwrap();

        assert_eq!(state.version(), 2);
        assert_eq!(state.log_data().num_files(), 0);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);

        // Deletes with no changes to state must not commit
        let (table, metrics) = table.delete().await.unwrap();
        assert_eq!(table.version(), Some(2));
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);
    }

    #[tokio::test]
    async fn test_delete_on_nonpartition_column() {
        // Delete based on a nonpartition column
        // Only rewrite files that match the predicate
        // Test data designed to force a scan of the underlying data

        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 1);
        assert_eq!(state.log_data().num_files(), 1);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 2);
        assert_eq!(state.log_data().num_files(), 2);

        let (table, metrics) = table
            .delete()
            .with_predicate(col("value").eq(lit(1)))
            .await
            .unwrap();
        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 3);
        assert_eq!(state.log_data().num_files(), 2);

        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert!(metrics.scan_time_ms > 0);
        assert_eq!(metrics.num_deleted_rows, 1);
        assert_eq!(metrics.num_copied_rows, 3);

        let last_commit = table.last_commit().await.unwrap();
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("value = 1"));

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| A  | 100   | 2021-02-02 |",
            "| A  | 100   | 2021-02-02 |",
            "| B  | 10    | 2021-02-02 |",
            "| B  | 20    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_null() {
        // Demonstrate deletion of null

        async fn prepare_table() -> DeltaTable {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                arrow::datatypes::DataType::Int32,
                true,
            )]));

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![
                    Some(0),
                    None,
                    Some(2),
                    None,
                    Some(4),
                ]))],
            )
            .unwrap();

            DeltaTable::new_in_memory()
                .write(vec![batch])
                .await
                .unwrap()
        }

        // Validate behaviour of greater than
        let table = prepare_table().await;
        let (table, _) = table
            .delete()
            .with_predicate(col("value").gt(lit(2)))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "| 0     |",
            "| 2     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate behaviour of less than
        let table = prepare_table().await;
        let (table, _) = table
            .delete()
            .with_predicate(col("value").lt(lit(2)))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "| 2     |",
            "| 4     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate behaviour of less plus not null
        let table = prepare_table().await;
        let (table, _) = table
            .delete()
            .with_predicate(col("value").lt(lit(2)).or(col("value").is_null()))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "| 2     |",
            "| 4     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_on_partition_column() {
        // Perform a delete where the predicate only contains partition columns

        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(["modified"].to_vec())).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-02",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 1);
        assert_eq!(state.log_data().num_files(), 2);

        let (table, metrics) = table
            .delete()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .await
            .unwrap();
        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 2);
        assert_eq!(state.log_data().num_files(), 1);

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);
        assert!(metrics.scan_time_ms > 0);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_partition_only_removes_empty_files_in_matching_partitions()
    -> DeltaResult<()> {
        use std::collections::HashMap;
        use std::sync::Arc;

        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use chrono::Utc;
        use object_store::PutPayload;
        use object_store::path::Path as ObjectStorePath;

        use crate::DeltaTable;
        use crate::kernel::{
            Action, Add, DataType as DeltaDataType, PrimitiveType, StructField, StructType,
        };

        // Partition columns match the issue report shape: dt + hour.
        let table_schema = StructType::try_new(vec![
            StructField::new(
                "dt".to_string(),
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "hour".to_string(),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "value".to_string(),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            ),
        ])?;

        let file_empty = "dt=2025-11-12/hour=0/empty.parquet";
        let file_nonempty = "dt=2025-11-12/hour=0/nonempty.parquet";
        let file_other = "dt=2025-11-13/hour=0/other.parquet";

        let now_ms = Utc::now().timestamp_millis();

        let add = |path: &str, dt: &str, hour: &str, size: i64| Add {
            path: path.to_string(),
            partition_values: HashMap::from([
                ("dt".to_string(), Some(dt.to_string())),
                ("hour".to_string(), Some(hour.to_string())),
            ]),
            size,
            modification_time: now_ms,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        // Prepare parquet bytes up front so the Add actions can have accurate sizes.
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Utf8, true),
            Field::new("hour", DataType::Int32, true),
            Field::new("value", DataType::Int32, true),
        ]));

        let empty_batch = RecordBatch::new_empty(Arc::clone(&arrow_schema));
        let empty_bytes = crate::test_utils::get_parquet_bytes(&empty_batch).unwrap();

        let nonempty_batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(StringArray::from(vec!["2025-11-12"])),
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )?;
        let nonempty_bytes = crate::test_utils::get_parquet_bytes(&nonempty_batch).unwrap();

        let other_batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(StringArray::from(vec!["2025-11-13"])),
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )?;
        let other_bytes = crate::test_utils::get_parquet_bytes(&other_batch).unwrap();

        // Create the table with 3 files in the log (2 in the matching partition, 1 outside).
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(vec!["dt", "hour"])
            .with_actions(vec![
                Action::Add(add(file_empty, "2025-11-12", "0", empty_bytes.len() as i64)),
                Action::Add(add(
                    file_nonempty,
                    "2025-11-12",
                    "0",
                    nonempty_bytes.len() as i64,
                )),
                Action::Add(add(file_other, "2025-11-13", "0", other_bytes.len() as i64)),
            ])
            .await?;

        // Write one empty parquet file and one non-empty parquet file for the matching partition.
        // The current (broken) behavior removes only the non-empty file because `distinct(file_id)`
        // never returns a row for the empty file.
        let store = table.object_store();
        store
            .put(
                &ObjectStorePath::from(file_empty),
                PutPayload::from(empty_bytes),
            )
            .await?;
        store
            .put(
                &ObjectStorePath::from(file_nonempty),
                PutPayload::from(nonempty_bytes),
            )
            .await?;
        store
            .put(
                &ObjectStorePath::from(file_other),
                PutPayload::from(other_bytes),
            )
            .await?;

        let state = table.snapshot()?;
        assert_eq!(state.log_data().num_files(), 3);

        let (table, metrics) = table.delete().with_predicate("dt < '2025-11-13'").await?;

        // Correct behavior: both files in dt=2025-11-12 should be removed, even if one is empty.
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 2);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);

        let state = table.snapshot()?;
        assert_eq!(state.log_data().num_files(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_partition_only_does_not_delete_null_partition_values() -> DeltaResult<()> {
        use std::collections::HashMap;
        use std::sync::Arc;

        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use chrono::Utc;
        use object_store::PutPayload;
        use object_store::path::Path as ObjectStorePath;

        use crate::DeltaTable;
        use crate::kernel::{
            Action, Add, DataType as DeltaDataType, PrimitiveType, StructField, StructType,
        };

        let table_schema = StructType::try_new(vec![
            StructField::new(
                "dt".to_string(),
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "value".to_string(),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            ),
        ])?;

        let file_match = "dt=2025-11-12/match.parquet";
        let file_null = "dt=__HIVE_DEFAULT_PARTITION__/null.parquet";
        let file_other = "dt=2025-11-13/other.parquet";

        let now_ms = Utc::now().timestamp_millis();

        let add = |path: &str, dt: Option<&str>, size: i64| Add {
            path: path.to_string(),
            partition_values: HashMap::from([("dt".to_string(), dt.map(|dt| dt.to_string()))]),
            size,
            modification_time: now_ms,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));

        let match_batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("2025-11-12")])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )?;
        let match_bytes = crate::test_utils::get_parquet_bytes(&match_batch).unwrap();

        let null_batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )?;
        let null_bytes = crate::test_utils::get_parquet_bytes(&null_batch).unwrap();

        let other_batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("2025-11-13")])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )?;
        let other_bytes = crate::test_utils::get_parquet_bytes(&other_batch).unwrap();

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(vec!["dt"])
            .with_actions(vec![
                Action::Add(add(
                    file_match,
                    Some("2025-11-12"),
                    match_bytes.len() as i64,
                )),
                Action::Add(add(file_null, None, null_bytes.len() as i64)),
                Action::Add(add(
                    file_other,
                    Some("2025-11-13"),
                    other_bytes.len() as i64,
                )),
            ])
            .await?;

        // Write parquet files so this test remains valid even if the implementation regresses.
        let store = table.object_store();
        store
            .put(
                &ObjectStorePath::from(file_match),
                PutPayload::from(match_bytes),
            )
            .await?;
        store
            .put(
                &ObjectStorePath::from(file_null),
                PutPayload::from(null_bytes),
            )
            .await?;
        store
            .put(
                &ObjectStorePath::from(file_other),
                PutPayload::from(other_bytes),
            )
            .await?;

        let state = table.snapshot()?;
        assert_eq!(state.log_data().num_files(), 3);

        let (table, metrics) = table.delete().with_predicate("dt < '2025-11-13'").await?;

        // SQL NULL semantics: NULL < '2025-11-13' is NULL (not true), so the NULL-partition file
        // should not be selected for deletion.
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);

        let state = table.snapshot()?;
        assert_eq!(state.log_data().num_files(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_partition_only_does_not_require_data_file_scan() -> DeltaResult<()> {
        use std::collections::HashMap;

        use chrono::Utc;

        use crate::DeltaTable;
        use crate::kernel::{
            Action, Add, DataType as DeltaDataType, PrimitiveType, StructField, StructType,
        };

        let table_schema = StructType::try_new(vec![
            StructField::new(
                "dt".to_string(),
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "hour".to_string(),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "value".to_string(),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            ),
        ])?;

        let file_missing_0 = "dt=2025-11-12/hour=0/missing0.parquet";
        let file_missing_1 = "dt=2025-11-12/hour=1/missing1.parquet";
        let file_other = "dt=2025-11-13/hour=0/other.parquet";

        let now_ms = Utc::now().timestamp_millis();

        let add = |path: &str, dt: &str, hour: &str| Add {
            path: path.to_string(),
            partition_values: HashMap::from([
                ("dt".to_string(), Some(dt.to_string())),
                ("hour".to_string(), Some(hour.to_string())),
            ]),
            size: 0,
            modification_time: now_ms,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(vec!["dt", "hour"])
            .with_actions(vec![
                Action::Add(add(file_missing_0, "2025-11-12", "0")),
                Action::Add(add(file_missing_1, "2025-11-12", "1")),
                Action::Add(add(file_other, "2025-11-13", "0")),
            ])
            .await?;

        // Intentionally DO NOT write the parquet files to the object store. If partition-only
        // deletes regress to a row-producing scan, this test should fail trying to read
        // missing files.
        let (table, metrics) = table.delete().with_predicate("dt < '2025-11-13'").await?;

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 2);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);

        let state = table.snapshot()?;
        assert_eq!(state.log_data().num_files(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_partition_only_fallback_multibatch_ignores_missing_files()
    -> DeltaResult<()> {
        use chrono::Utc;

        use crate::DeltaTable;
        use crate::kernel::{
            Action, DataType as DeltaDataType, PrimitiveType, StructField, StructType,
        };
        use crate::test_utils::make_test_add;

        let table_schema = StructType::try_new(vec![
            StructField::new(
                "dt".to_string(),
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "hour".to_string(),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "value".to_string(),
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
            ),
        ])?;

        let action_count = 9000;
        let expected_removed = action_count / 2;
        let now_ms = Utc::now().timestamp_millis();

        let adds = (0..action_count)
            .map(|idx| {
                let hour = if idx % 2 == 0 { 10 } else { 20 };
                let path = format!("dt=2025-11-12/hour={hour}/file-{idx:05}.parquet");
                let hour_str = if hour == 10 { "10" } else { "20" };
                Action::Add(make_test_add(
                    path,
                    &[("dt", "2025-11-12"), ("hour", hour_str)],
                    now_ms,
                ))
            })
            .collect::<Vec<_>>();

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_partition_columns(vec!["dt", "hour"])
            .with_actions(adds)
            .await?;

        // Intentionally do not write parquet files. This validates that the partition-only
        // DataFusion fallback path can resolve file candidates from metadata only.
        let (table, metrics) = table
            .delete()
            .with_predicate("CAST(hour AS STRING) LIKE '1%'")
            .await?;

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, expected_removed);
        assert_eq!(metrics.num_deleted_rows, 0);
        assert_eq!(metrics.num_copied_rows, 0);

        let state = table.snapshot()?;
        assert_eq!(
            state.log_data().num_files(),
            action_count - expected_removed
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_on_mixed_columns() {
        // Test predicates that contain non-partition and partition column
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(["modified"].to_vec())).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-02",
                    "2021-02-04",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 1);
        assert_eq!(state.log_data().num_files(), 3);

        let (table, metrics) = table
            .delete()
            .with_predicate(
                col("modified")
                    .eq(lit("2021-02-04"))
                    .and(col("value").eq(lit(100))),
            )
            .await
            .unwrap();

        let state = table.snapshot().unwrap();
        assert_eq!(state.version(), 2);
        assert_eq!(state.log_data().num_files(), 2);

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, 1);
        assert_eq!(metrics.num_copied_rows, 0);
        assert!(metrics.scan_time_ms > 0);

        let expected = [
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| B  | 20    | 2021-02-03 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_nested() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        // Test Delete with a predicate that references struct fields
        // See #2019
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new(
                "props",
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Utf8, true)])),
                true,
            ),
        ]));

        let struct_array = StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Utf8, true)]),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                Some("2021-02-01"),
                Some("2021-02-02"),
                None,
                None,
            ])) as ArrayRef],
            Some(NullBuffer::from_iter(vec![true, true, true, false])),
        );

        let data = vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
        ];
        let batches = vec![RecordBatch::try_new(schema.clone(), data).unwrap()];

        let table = DeltaTable::new_in_memory().write(batches).await.unwrap();

        let (table, _metrics) = table
            .delete()
            .with_predicate("props['a'] = '2021-02-02'")
            .await
            .unwrap();

        let expected = [
            "+----+-----------------+",
            "| id | props           |",
            "+----+-----------------+",
            "| A  | {a: 2021-02-01} |",
            "| C  | {a: }           |",
            "| D  |                 |",
            "+----+-----------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_partition_string_predicate_dictionary_formatting() -> DeltaResult<()> {
        // Regression test: resolving predicates against the execution scan schema can
        // dictionary-encode partition columns, so fmt_expr_to_sql must support
        // ScalarValue::Dictionary scalars.
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(vec!["id"])).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "C"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )?;

        let table = write_batch(table, batch).await;

        let (table, _metrics) = table.delete().with_predicate("id = 'A'").await?;

        let last_commit = table.last_commit().await.unwrap();
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("id = 'A'"));

        let expected = [
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| B  | 2     | 2021-02-02 |",
            "| C  | 4     | 2021-02-02 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_binary_equality_non_partition() -> DeltaResult<()> {
        // Regression test: DF52 view types can cause predicate evaluation to compare
        // Binary against BinaryView for equality predicates.
        let schema = Arc::new(Schema::new(vec![
            Field::new("data", DataType::Binary, true),
            Field::new("int32", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(b"aaa".as_slice()),
                    Some(b"bbb".as_slice()),
                    Some(b"ccc".as_slice()),
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef,
            ],
        )?;

        let table = DeltaTable::new_in_memory().write(vec![batch]).await?;

        let (table, _metrics) = table
            .delete()
            .with_predicate(col("data").eq(lit(ScalarValue::Binary(Some(b"bbb".to_vec())))))
            .await?;

        let last_commit = table.last_commit().await.unwrap();
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(
            parameters["predicate"],
            json!("data = decode('626262', 'hex')")
        );

        let mut values = Vec::new();
        for batch in get_data(&table).await {
            let int32 = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for idx in 0..batch.num_rows() {
                values.push(int32.value(idx));
            }
        }
        values.sort();
        assert_eq!(values, vec![0, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_string_equality_utf8view_regression_4125() -> DeltaResult<()> {
        // Direct regression test for GitHub issue #4125:
        // https://github.com/delta-io/delta-rs/issues/4125
        //
        // Non-partition string column with default session (view types enabled in DF52+).
        // Was failing with: Invalid comparison operation: Utf8 == Utf8View
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )?;

        let table = DeltaTable::new_in_memory().write(vec![batch]).await?;

        // This was the failing operation in issue #4125
        let (table, _metrics) = table.delete().with_predicate("name = 'bob'").await?;

        let last_commit = table.last_commit().await.unwrap();
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("name = 'bob'"));

        let expected = [
            "+---------+-------+",
            "| name    | value |",
            "+---------+-------+",
            "| alice   | 1     |",
            "| charlie | 3     |",
            "+---------+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_custom_session_schema_force_view_types_disabled() -> DeltaResult<()> {
        // Integration guardrail: user-supplied DataFusion sessions may disable view types.
        // Predicate resolution must respect the provided session's config.
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )?;

        let table = DeltaTable::new_in_memory().write(vec![batch]).await?;

        let config: datafusion::prelude::SessionConfig =
            crate::delta_datafusion::DeltaSessionConfig::default().into();
        let config = config.set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        );
        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .build_arc()
            .unwrap();
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_query_planner(crate::delta_datafusion::planner::DeltaPlanner::new())
            .build();
        let session = Arc::new(state);

        let (table, _metrics) = table
            .delete()
            .with_session_state(session)
            .with_predicate("name = 'bob'")
            .await?;

        let last_commit = table.last_commit().await.unwrap();
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("name = 'bob'"));

        let expected = [
            "+---------+-------+",
            "| name    | value |",
            "+---------+-------+",
            "| alice   | 1     |",
            "| charlie | 3     |",
            "+---------+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_failure_nondeterministic_query() {
        // Deletion requires a deterministic predicate

        let table = setup_table(None).await;

        let res = table
            .delete()
            .with_predicate(col("value").eq(cast(
                random() * lit(20.0),
                arrow::datatypes::DataType::Int32,
            )))
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_delete_cdc_enabled() {
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        )
        .unwrap();
        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));

        let (table, _metrics) = table
            .delete()
            .with_predicate(col("value").eq(lit(2)))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));

        let ctx = SessionContext::new();
        let table = table
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");

        let mut batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await
        .expect("Failed to collect batches");

        // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(3)).collect();

        assert_batches_sorted_eq! {[
        "+-------+--------------+-----------------+",
        "| value | _change_type | _commit_version |",
        "+-------+--------------+-----------------+",
        "| 1     | insert       | 1               |",
        "| 2     | delete       | 2               |",
        "| 2     | insert       | 1               |",
        "| 3     | insert       | 1               |",
        "+-------+--------------+-----------------+",
        ], &batches }
    }

    #[tokio::test]
    async fn test_delete_cdc_enabled_partitioned() {
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_column(
                "year",
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
                None,
            )
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_partition_columns(vec!["year"])
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("2020"),
                    Some("2020"),
                    Some("2024"),
                ])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));

        let (table, _metrics) = table
            .delete()
            .with_predicate(col("value").eq(lit(2)))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));

        let ctx = create_session().into_inner();
        let table = table
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");

        let mut batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await
        .expect("Failed to collect batches");

        // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(4)).collect();

        assert_batches_sorted_eq! {[
            "+-------+------+--------------+-----------------+",
            "| value | year | _change_type | _commit_version |",
            "+-------+------+--------------+-----------------+",
            "| 1     | 2020 | insert       | 1               |",
            "| 2     | 2020 | delete       | 2               |",
            "| 2     | 2020 | insert       | 1               |",
            "| 3     | 2024 | insert       | 1               |",
            "+-------+------+--------------+-----------------+",
        ], &batches }
    }

    #[tokio::test]
    async fn delete_partitioned_cdf() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let kernel_schema: StructType = schema.as_ref().try_into_kernel().unwrap();

        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_columns(kernel_schema.fields().cloned())
            .with_partition_columns(vec!["year"])
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("2020"),
                    Some("2020"),
                    Some("2024"),
                    Some("2025"),
                ])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
            ],
        )
        .unwrap();

        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));

        let (table, _metrics) = table
            .delete()
            .with_predicate(col("value").gt(lit(2)))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));

        let ctx = create_session().into_inner();
        let table = table
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");

        let mut batches = collect(table, ctx.task_ctx())
            .await
            .expect("Failed to collect batches");

        // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(4)).collect();

        assert_batches_sorted_eq! {[
            "+-------+------+--------------+-----------------+",
            "| value | year | _change_type | _commit_version |",
            "+-------+------+--------------+-----------------+",
            "| 1     | 2020 | insert       | 1               |",
            "| 2     | 2020 | insert       | 1               |",
            "| 3     | 2024 | delete       | 2               |",
            "| 3     | 2024 | insert       | 1               |",
            "| 4     | 2025 | delete       | 2               |",
            "| 4     | 2025 | insert       | 1               |",
            "+-------+------+--------------+-----------------+",
        ], &batches }
    }

    async fn collect_batches(
        num_partitions: usize,
        stream: Arc<dyn ExecutionPlan>,
        ctx: SessionContext,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let mut batches = vec![];
        for p in 0..num_partitions {
            let data: Vec<RecordBatch> =
                collect_sendable_stream(stream.execute(p, ctx.task_ctx())?).await?;
            batches.extend_from_slice(&data);
        }
        Ok(batches)
    }

    /// Test for expression simplification which is needed for casts with Datafusion, see:
    /// <https://github.com/delta-io/delta-rs/issues/4093>
    #[tokio::test]
    async fn test_delete_with_predicate() -> DeltaResult<()> {
        use arrow::array::ArrowPrimitiveType;
        use arrow::array::TimestampMicrosecondArray;
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_column(
                "id",
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
                None,
            )
            .with_column(
                "created_at",
                DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
                true,
                None,
            )
            .await?;
        assert_eq!(table.version(), Some(0));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("one")])),
                Arc::new(TimestampMicrosecondArray::new(
                    vec![TimestampMicrosecondType::default_value()].into(),
                    None,
                )),
            ],
        )?;
        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(
            table.version(),
            Some(1),
            "The first batch was not written successfully"
        );

        let predicate = "\"created_at\" < ARROW_CAST('2024-01-01 00:00:00+00:00', 'Timestamp(Microsecond, None)')";
        let (table, _metrics) = table.delete().with_predicate(predicate).await?;
        assert_eq!(table.version(), Some(2), "The delete failed to execute");
        Ok(())
    }
}
