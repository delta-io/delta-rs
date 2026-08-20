//! Upsert data from a source DataFrame into a target Delta Table.
//! For each conflicting record (e.g., matching on primary key), only the source record is kept.
//! All non-conflicting records are appended.

use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::{DataFusionMixins, DeltaSessionConfig};
use crate::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL};
use crate::kernel::{Action, EagerSnapshot};
use crate::logstore::{LogStore, LogStoreRef};
use crate::operations::cdc::should_write_cdc;
use crate::operations::write::WriterStatsConfig;
use crate::operations::write::execution::write_execution_plan_v2;
use crate::operations::{CustomExecuteHandler, Operation};
use crate::protocol::{DeltaOperation, MergePredicate, SaveMode};
use crate::table::config::TablePropertiesExt;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use arrow_array::Array;
use datafusion::common::{JoinType, ScalarValue};
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{DataFrame, SessionContext};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[derive(Default, Debug, Clone, Serialize)]
/// Metrics collected during the Upsert operation
pub struct UpsertMetrics {
    /// Number of files added to the target table
    pub num_added_files: usize,
    /// Number of files removed from the target table
    pub num_removed_files: usize,
    /// Number of conflicting records detected
    pub num_conflicting_records: usize,
    /// Time taken to execute the entire operation
    pub write_time_ms: u64,
    /// Time taken to scan the target files
    pub scan_time_ms: u64,
    /// Total execution time for the upsert operation
    pub execution_time_ms: u64,
}

/// Builder for configuring and executing an upsert operation
pub struct UpsertBuilder {
    /// The join keys used to identify conflicts between source and target records
    join_keys: Vec<String>,
    /// The source data to upsert into the target table
    source: DataFrame,
    /// The current state of the target table
    snapshot: EagerSnapshot,
    /// Delta log store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state for executing the plans
    state: Option<Arc<SessionState>>,
    /// Properties for Parquet writer configuration
    writer_properties: Option<WriterProperties>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// Handler invoked around execution and after the commit
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl UpsertBuilder {
    /// Create a new UpsertBuilder with required parameters
    pub fn new(
        log_store: LogStoreRef,
        snapshot: EagerSnapshot,
        join_keys: Vec<String>,
        source: DataFrame,
    ) -> Self {
        Self {
            join_keys,
            source,
            snapshot,
            log_store,
            state: None,
            writer_properties: None,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Set the Datafusion session state to use for plan execution
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(Arc::from(state));
        self
    }

    /// Set the Parquet writer properties for output files
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    /// Set additional commit properties for the transaction
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set a handler to invoke around execution and after the commit
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

impl super::Operation for UpsertBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }

    fn get_custom_execute_handler(&self) -> Option<Arc<dyn super::CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl std::future::IntoFuture for UpsertBuilder {
    type Output = DeltaResult<(DeltaTable, UpsertMetrics)>;
    type IntoFuture = futures::future::BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let exec_start = Instant::now();

            // Validate table state and protocol
            Self::validate_table_state(&self.snapshot)?;

            let operation_id = self.get_operation_id();
            self.pre_execute(operation_id).await?;

            // Get or create session state
            let state = self.get_or_create_session_state(operation_id);

            // Execute the upsert operation
            let (actions, mut metrics, predicate) =
                self.execute_upsert(state, operation_id).await?;

            // Commit the changes
            let table = self
                .commit_changes(actions, &metrics, predicate, operation_id)
                .await?;

            metrics.execution_time_ms =
                Instant::now().duration_since(exec_start).as_millis() as u64;

            self.post_execute(operation_id).await?;
            Ok((table, metrics))
        })
    }
}

const FILE_PATH_COLUMN: &str = "__delta_rs_path";

impl UpsertBuilder {
    /// Validate that the table is in a valid state for upsert operations
    fn validate_table_state(snapshot: &EagerSnapshot) -> DeltaResult<()> {
        PROTOCOL.can_write_to(snapshot)?;

        if !snapshot.load_config().require_files {
            return Err(DeltaTableError::NotInitializedWithFiles("UPSERT".into()));
        }

        if should_write_cdc(snapshot)? {
            return Err(DeltaTableError::Generic(
                "UPSERT is not supported on tables with change data feed enabled".into(),
            ));
        }

        Ok(())
    }

    /// Get the existing session state or create a new one
    fn get_or_create_session_state(&self, operation_id: Uuid) -> Arc<SessionState> {
        match &self.state {
            Some(state) => Arc::clone(state),
            None => {
                let config: datafusion::execution::context::SessionConfig =
                    DeltaSessionConfig::default().into();
                let session = SessionContext::new_with_config(config);
                let url = self.log_store.object_store_url();
                session.register_object_store(
                    url.as_ref(),
                    self.log_store.object_store(Some(operation_id)),
                );
                Arc::new(session.state())
            }
        }
    }

    /// Execute the main upsert logic
    async fn execute_upsert(
        &self,
        state: Arc<SessionState>,
        operation_id: Uuid,
    ) -> DeltaResult<(Vec<Action>, UpsertMetrics, Option<String>)> {
        let relevant_partition_cols: Vec<String> = self
            .snapshot
            .metadata()
            .partition_columns()
            .iter()
            .filter(|c| self.join_keys.contains(c))
            .cloned()
            .collect();

        // Get unique partition values from source to limit scan scope
        let partition_values = self
            .extract_partition_filters(&relevant_partition_cols)
            .await?;
        let partition_filters =
            Self::partition_filter_exprs(&relevant_partition_cols, &partition_values);

        // The scan scope and the predicate recorded on the commit are derived from the same
        // expressions, so the commit can never claim to have read less than it did.
        let commit_predicate = self.commit_predicate(&partition_filters, &state);

        // Create target DataFrame with partition filtering
        let target_df = self
            .create_target_dataframe(&state, &partition_filters)
            .await?;

        // Check for conflicts between source and target and cache the result for reuse
        let conflicts_df =
            Self::extract_conflicts_dataframe(&target_df, &self.source, &self.join_keys)
                .await?
                .cache()
                .await?;

        let has_conflicts = conflicts_df
            .clone()
            .limit(0, Some(1))?
            .collect()
            .await?
            .is_empty()
            .not();

        let (actions, metrics) = if has_conflicts {
            self.execute_upsert_with_conflicts(&state, &target_df, conflicts_df, operation_id)
                .await?
        } else {
            self.execute_simple_append(&state, operation_id).await?
        };

        Ok((actions, metrics, commit_predicate))
    }

    /// Turn the source's partition values into the filter expressions that scope the target scan.
    ///
    /// Ordering is pinned by `columns` and by each value's rendering, so the expressions -- and
    /// the predicate string derived from them for the commit -- are stable across runs.
    fn partition_filter_exprs(
        columns: &[String],
        partition_values: &HashMap<String, HashSet<ScalarValue>>,
    ) -> Vec<Expr> {
        columns
            .iter()
            .filter_map(|column| {
                let values = partition_values.get(column)?;
                if values.is_empty() {
                    return None;
                }
                let mut list: Vec<Expr> = values.iter().map(|v| lit(v.clone())).collect();
                list.sort_by_cached_key(|value| value.to_string());
                Some(Expr::InList(InList {
                    expr: Box::new(col(column)),
                    list,
                    negated: false,
                }))
            })
            .collect()
    }

    /// Render the partition filters as the predicate to record on the commit.
    fn commit_predicate(&self, filters: &[Expr], state: &SessionState) -> Option<String> {
        let predicate = conjunction(filters.iter().cloned())?;

        let sql = match fmt_expr_to_sql(&predicate) {
            Ok(sql) => sql,
            Err(e) => {
                tracing::warn!(
                    "upsert: partition filters could not be rendered as SQL, committing without a \
                     read predicate: {e}"
                );
                return None;
            }
        };

        match self.snapshot.parse_predicate_expression(&sql, state) {
            Ok(_) => Some(sql),
            Err(e) => {
                tracing::warn!(
                    "upsert: read predicate {sql:?} does not parse against the table schema, \
                     committing without one: {e}"
                );
                None
            }
        }
    }

    /// Render the join keys as the `ON` clause of the equivalent `MERGE`, for the commit's
    /// `mergePredicate` parameter.
    fn merge_predicate(&self) -> String {
        self.join_keys
            .iter()
            .map(|key| format!("target.{key} = source.{key}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    /// Collect the distinct non-null values present in the source for each of `columns`,
    /// so the target scan can be narrowed to the partitions the source actually touches.
    async fn extract_partition_filters(
        &self,
        columns: &[String],
    ) -> DeltaResult<HashMap<String, HashSet<ScalarValue>>> {
        if columns.is_empty() {
            return Ok(HashMap::new());
        }

        let select_exprs: Vec<Expr> = columns.iter().map(col).collect();
        let batches = self.source.clone().select(select_exprs)?.collect().await?;

        let mut seen: Vec<HashSet<ScalarValue>> = vec![HashSet::new(); columns.len()];

        for batch in &batches {
            for (col_idx, seen_set) in seen.iter_mut().enumerate() {
                let column = batch.column(col_idx);
                for row_idx in 0..column.len() {
                    if column.is_null(row_idx) {
                        continue;
                    }
                    seen_set.insert(ScalarValue::try_from_array(column.as_ref(), row_idx)?);
                }
            }
        }

        Ok(columns
            .iter()
            .zip(seen)
            .filter(|(_, values)| !values.is_empty())
            .map(|(name, values)| (name.clone(), values))
            .collect())
    }

    /// Create a DataFrame for the target table with partition filtering
    async fn create_target_dataframe(
        &self,
        state: &SessionState,
        filters: &[Expr],
    ) -> DeltaResult<DataFrame> {
        let mut builder = crate::delta_datafusion::DeltaScanNext::builder()
            .with_eager_snapshot(self.snapshot.clone())
            .with_log_store(self.log_store.clone())
            .with_session(Arc::new(state.clone()))
            .with_file_column(FILE_PATH_COLUMN);

        // Limit the scan scope to files matching the source partition values.
        if !filters.is_empty() {
            builder = builder.with_file_skipping_predicates(filters.to_vec());
        }

        let target_provider = datafusion::datasource::provider_as_source(builder.await?);

        let target_df = DataFrame::new(
            state.clone(),
            datafusion::logical_expr::LogicalPlanBuilder::scan(
                datafusion::common::TableReference::bare("target"),
                target_provider,
                None,
            )?
            .build()?,
        );

        Ok(target_df)
    }

    /// Prepare a DataFrame containing only the join key columns from the source.
    /// This does not perform any matching or filtering against the target; it simply selects the relevant columns.
    /// The resulting DataFrame is used later in an anti-join operation to filter out conflicting rows from the target DataFrame.
    fn find_conflicts_keys_only(&self) -> DeltaResult<DataFrame> {
        // Simply select join keys from source - we'll use this for the anti-join
        let source_keys: Vec<_> = self.join_keys.iter().map(col).collect();
        let source_subset =
            self.source.clone().select(source_keys).map_err(|e| {
                DeltaTableError::Generic(format!("Error selecting source keys: {}", e))
            })?;

        Ok(source_subset)
    }

    /// Collapse the rewrite plan to a single output partition before handing it to the
    /// shared write path.
    fn coalesce_for_write(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        if plan.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        }
    }

    /// Execute upsert when there are no conflicts - simple append
    async fn execute_simple_append(
        &self,
        state: &SessionState,
        operation_id: Uuid,
    ) -> DeltaResult<(Vec<Action>, UpsertMetrics)> {
        let logical_plan = self.source.clone().into_unoptimized_plan();
        let physical_plan =
            Self::coalesce_for_write(state.create_physical_plan(&logical_plan).await?);

        // Get partition columns for writing
        let partition_columns: Vec<String> = self.snapshot.metadata().partition_columns().to_vec();

        let (add_actions, write_metrics) = write_execution_plan_v2(
            Some(&self.snapshot),
            state,
            physical_plan,
            partition_columns,
            self.log_store.object_store(Some(operation_id)),
            Some(self.snapshot.table_properties().target_file_size()),
            None,
            self.writer_properties.clone(),
            WriterStatsConfig::new(self.snapshot.table_properties().num_indexed_cols(), None),
            None,
            false,
            None,
        )
        .await?;

        let metrics = UpsertMetrics {
            num_added_files: add_actions.len(),
            scan_time_ms: write_metrics.scan_time_ms,
            write_time_ms: write_metrics.write_time_ms,
            ..Default::default()
        };

        Ok((add_actions, metrics))
    }

    /// Execute upsert when conflicts exist - need to remove old files and write new ones
    async fn execute_upsert_with_conflicts(
        &self,
        state: &SessionState,
        target_df: &DataFrame,
        conflicts_df: DataFrame,
        operation_id: Uuid,
    ) -> DeltaResult<(Vec<Action>, UpsertMetrics)> {
        // Extract the file names from the conflicts DataFrame
        let conflicting_file_names = Self::extract_file_paths_from_conflicts(&conflicts_df).await?;
        let remove_actions = self.files_to_remove(&conflicting_file_names).await?;

        // Count the number of conflicting records
        let num_conflicting_records = conflicts_df.count().await?;

        // Filter to only conflicting files and drop the file path column
        // The filtered_target_df now only contains table columns (no __delta_rs_path)
        let filtered_target_df =
            Self::filter_conflicting_files(target_df, &conflicting_file_names)?;

        // Create a conflicts query for the anti-join (only join keys, no file path)
        // This ensures schema consistency
        let conflicts_for_antijoin = self.find_conflicts_keys_only()?;

        let non_conflicting_target =
            self.get_non_conflicting_target_rows(&filtered_target_df, &conflicts_for_antijoin)?;
        let result_df = self.union_source_with_target(&non_conflicting_target)?;

        // Write the combined data
        let logical_plan = result_df.into_unoptimized_plan();
        let physical_plan =
            Self::coalesce_for_write(state.create_physical_plan(&logical_plan).await?);

        // Get partition columns for writing
        let partition_columns: Vec<String> = self.snapshot.metadata().partition_columns().to_vec();

        let (add_actions, write_metrics) = write_execution_plan_v2(
            Some(&self.snapshot),
            state,
            physical_plan,
            partition_columns,
            self.log_store.object_store(Some(operation_id)),
            Some(self.snapshot.table_properties().target_file_size()),
            None,
            self.writer_properties.clone(),
            WriterStatsConfig::new(self.snapshot.table_properties().num_indexed_cols(), None),
            None,
            false,
            None,
        )
        .await?;

        // Store metrics before moving add_actions
        let metrics = UpsertMetrics {
            num_added_files: add_actions.len(),
            num_removed_files: remove_actions.len(),
            num_conflicting_records,
            scan_time_ms: write_metrics.scan_time_ms,
            write_time_ms: write_metrics.write_time_ms,
            ..Default::default()
        };

        // Combine add and remove actions
        let mut all_actions = add_actions;
        all_actions.extend(remove_actions);

        Ok((all_actions, metrics))
    }

    fn filter_conflicting_files(
        target_df: &DataFrame,
        conflicting_file_names: &HashSet<String>,
    ) -> Result<DataFrame, DeltaTableError> {
        let filtered_target_df = target_df
            .clone()
            .filter(
                col(FILE_PATH_COLUMN)
                    .in_list(conflicting_file_names.iter().map(lit).collect(), false),
            )?
            .drop_columns(&[FILE_PATH_COLUMN])?;
        Ok(filtered_target_df)
    }

    async fn files_to_remove(
        &self,
        conflicting_file_names: &HashSet<String>,
    ) -> DeltaResult<Vec<Action>> {
        use crate::delta_datafusion::normalize_path_as_file_id;
        use futures::stream::StreamExt;

        let table_root = self.snapshot.table_configuration().table_root();

        let mut remove_actions = Vec::new();
        let mut file_stream = self.snapshot.file_views(&self.log_store, None);

        while let Some(file_view) = file_stream.next().await {
            let file_view = file_view?;
            let path = file_view.path();
            let file_id = normalize_path_as_file_id(path.as_ref(), table_root, "upsert remove")?;
            if conflicting_file_names.contains(&file_id) || conflicting_file_names.contains(&*path)
            {
                remove_actions.push(Action::Remove(file_view.remove_action(true)));
            }
        }

        Ok(remove_actions)
    }

    /// Extract conflicting records as a DataFrame by performing a join.
    ///
    /// This method performs an inner join between target and source on join keys, which produces
    /// a SMALL DataFrame containing only rows that conflict (same join key values in both source
    /// and target). The result contains join keys + file path - NOT full row data.
    ///
    /// Memory footprint: Only conflicting rows with minimal columns (join keys + file path).
    /// For a table with billions of rows but only thousands of conflicts, this is tiny.
    ///
    /// Returns a DataFrame with the join keys and the file path column for all conflicting records.
    async fn extract_conflicts_dataframe(
        target_df: &DataFrame,
        source: &DataFrame,
        join_keys: &[String],
    ) -> Result<DataFrame, DeltaTableError> {
        // Select only join keys and file path from target (not full rows)
        let mut target_keys: Vec<_> = join_keys.iter().map(col).collect();
        target_keys.push(col(FILE_PATH_COLUMN));
        let target_subset = target_df.clone().select(target_keys)?;

        // Select only join keys from source (not full rows)
        let source_keys: Vec<_> = join_keys
            .iter()
            .map(|k| col(k).alias(format!("source_{k}")))
            .collect();
        let source_subset = source.clone().select(source_keys)?;

        let source_key_cols: Vec<_> = join_keys.iter().map(|s| format!("source_{}", s)).collect();
        let target_key_cols: Vec<_> = join_keys.iter().map(|s| s.to_string()).collect();

        // Perform inner join to find conflicts
        // The result is SMALL: only rows where join keys match (actual conflicts)
        let conflicts = source_subset.join(
            target_subset,
            JoinType::Inner,
            &source_key_cols
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
            &target_key_cols
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
            None,
        )?;

        Ok(conflicts)
    }

    /// Extract the set of file paths referenced by the conflicts DataFrame.
    async fn extract_file_paths_from_conflicts(
        conflicts_df: &DataFrame,
    ) -> Result<HashSet<String>, DeltaTableError> {
        let conflicting_paths = conflicts_df
            .clone()
            .select(vec![col(FILE_PATH_COLUMN)])?
            .distinct()?
            .collect()
            .await?;

        let mut conflicting_files = HashSet::new();
        for batch in &conflicting_paths {
            let as_utf8 =
                arrow::compute::cast(batch.column(0).as_ref(), &arrow::datatypes::DataType::Utf8)
                    .map_err(|e| {
                    DeltaTableError::Generic(format!(
                        "Failed to cast file path column to Utf8: {e}"
                    ))
                })?;
            let str_array = as_utf8
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("cast to Utf8 yields a StringArray");
            conflicting_files.extend(str_array.iter().flatten().map(|v| v.to_string()));
        }

        Ok(conflicting_files)
    }

    /// Get target rows that don't conflict with source (using anti-join)
    fn get_non_conflicting_target_rows(
        &self,
        target_df: &DataFrame,
        conflicts_df: &DataFrame,
    ) -> DeltaResult<DataFrame> {
        // Anti join: target rows NOT in source (non-conflicting target rows)
        let non_conflicting_target = conflicts_df.clone().join(
            target_df.clone(),
            JoinType::RightAnti,
            &self
                .join_keys
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
            &self
                .join_keys
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
            None,
        )?;

        Ok(non_conflicting_target)
    }

    /// Union source data with non-conflicting target rows
    fn union_source_with_target(&self, target_no_conflict: &DataFrame) -> DeltaResult<DataFrame> {
        fn reorder_to_schema(
            df: DataFrame,
            reference: &arrow_schema::Schema,
        ) -> Result<DataFrame, DeltaTableError> {
            let exprs: Vec<Expr> = reference.fields().iter().map(|f| col(f.name())).collect();
            df.select(exprs).map_err(|e| {
                DeltaTableError::Generic(format!(
                    "Failed to reorder DataFrame to reference schema: {e}"
                ))
            })
        }

        // Use the table snapshot arrow schema as canonical ordering
        let canonical_schema = self.snapshot.arrow_schema();

        // Reorder both sides
        let source_aligned = reorder_to_schema(self.source.clone(), canonical_schema.as_ref())?;
        let target_aligned =
            reorder_to_schema(target_no_conflict.clone(), canonical_schema.as_ref())?;

        // Union after alignment
        let result_df = source_aligned.union(target_aligned).map_err(|e| {
            DeltaTableError::Generic(format!("Union failed after schema alignment: {e}"))
        })?;
        Ok(result_df)
    }

    /// Choose how to describe this upsert in the commit, given the read predicate it managed to
    /// declare.
    fn commit_operation(&self, predicate: Option<String>) -> DeltaOperation {
        let Some(predicate) = predicate else {
            let partition_columns: Vec<String> =
                self.snapshot.metadata().partition_columns().to_vec();

            return DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: if partition_columns.is_empty() {
                    None
                } else {
                    Some(partition_columns)
                },
                predicate: None,
            };
        };

        DeltaOperation::Merge {
            predicate: Some(predicate),
            merge_predicate: Some(self.merge_predicate()),
            matched_predicates: vec![MergePredicate {
                action_type: "update".to_string(),
                predicate: None,
            }],
            not_matched_predicates: vec![MergePredicate {
                action_type: "insert".to_string(),
                predicate: None,
            }],
            not_matched_by_source_predicates: vec![],
        }
    }

    /// Commit all changes to the Delta log
    async fn commit_changes(
        &self,
        actions: Vec<Action>,
        metrics: &UpsertMetrics,
        predicate: Option<String>,
        operation_id: Uuid,
    ) -> DeltaResult<DeltaTable> {
        // Nothing changed, so don't burn a table version on an empty commit.
        if actions.is_empty() {
            return Ok(DeltaTable::new_with_state(
                self.log_store.clone(),
                DeltaTableState::new(self.snapshot.clone()),
            ));
        }

        // Add metrics to commit metadata
        let mut app_metadata = self.commit_properties.app_metadata.clone();
        app_metadata.insert("readVersion".to_owned(), self.snapshot.version().into());

        if let Ok(metrics_json) = serde_json::to_value(metrics) {
            app_metadata.insert("operationMetrics".to_owned(), metrics_json);
        }

        let mut commit_properties = self.commit_properties.clone();
        commit_properties.app_metadata = app_metadata;

        let operation = self.commit_operation(predicate);

        let commit = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .with_operation_id(operation_id)
            .with_post_commit_hook_handler(self.custom_execute_handler.clone())
            .build(Some(&self.snapshot), self.log_store.clone(), operation)
            .await?;

        Ok(DeltaTable::new_with_state(
            self.log_store.clone(),
            commit.snapshot(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;
    use delta_kernel::schema::{PrimitiveType, StructField};
    use std::sync::Arc;

    const DAY_1: &str = "2023-01-01";
    const DAY_2: &str = "2023-01-02";

    /// One row of the test table.
    ///
    /// Built through [`row`] and narrowed with [`Row::in_workspace`] / [`Row::on`], so a test states
    /// only the fields it actually cares about and the rest stay out of the way. The four columns are
    /// otherwise easy to transpose when written positionally.
    #[derive(Clone, Copy)]
    struct Row {
        date: &'static str,
        id: &'static str,
        value: i32,
        workspace_id: i32,
    }

    /// A row in workspace 1 on [`DAY_1`] -- the shape most tests want.
    fn row(id: &'static str, value: i32) -> Row {
        Row {
            date: DAY_1,
            id,
            value,
            workspace_id: 1,
        }
    }

    impl Row {
        fn in_workspace(self, workspace_id: i32) -> Self {
            Self {
                workspace_id,
                ..self
            }
        }

        fn on(self, date: &'static str) -> Self {
            Self { date, ..self }
        }
    }

    /// The columns every test table and source batch uses. Kept next to [`delta_schema`], which must
    /// describe the same four columns -- a mismatch surfaces as a failed write rather than silently.
    fn arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("date", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
            Field::new("workspace_id", DataType::Int32, false),
        ]))
    }

    fn delta_schema() -> Vec<StructField> {
        use delta_kernel::schema::DataType as DeltaType;

        vec![
            StructField::new("date", DeltaType::Primitive(PrimitiveType::String), false),
            StructField::new("id", DeltaType::Primitive(PrimitiveType::String), false),
            StructField::new("value", DeltaType::Primitive(PrimitiveType::Integer), false),
            StructField::new(
                "workspace_id",
                DeltaType::Primitive(PrimitiveType::Integer),
                false,
            ),
        ]
    }

    fn batch(rows: &[Row]) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema(),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.date))) as ArrayRef,
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.id))),
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.value))),
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|r| r.workspace_id),
                )),
            ],
        )
        .expect("row data matches the test schema")
    }

    fn source_from(batch: RecordBatch) -> DataFrame {
        SessionContext::new()
            .read_batches(vec![batch])
            .expect("in-memory batch is a valid source")
    }

    /// A source DataFrame over `rows`, in the table's own column order.
    fn source(rows: &[Row]) -> DataFrame {
        source_from(batch(rows))
    }

    /// The same rows with the columns permuted into `order`, for sources whose column order differs
    /// from the target's.
    fn source_with_column_order(order: &[&str], rows: &[Row]) -> DataFrame {
        let full = batch(rows);
        let schema = full.schema();

        let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = order
            .iter()
            .map(|name| {
                let (index, field) = schema
                    .column_with_name(name)
                    .unwrap_or_else(|| panic!("no column named {name:?} in the test schema"));
                (field.clone(), full.column(index).clone())
            })
            .unzip();

        source_from(
            RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
                .expect("permuting columns preserves the batch"),
        )
    }

    fn keys(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// The join keys every partition-aware test uses: the partition column plus the record key.
    fn join_keys() -> Vec<String> {
        keys(&["workspace_id", "id"])
    }

    /// A table holding `files` -- one Parquet file per inner slice, so a test controls how many files
    /// a conflict has to span -- partitioned by `partition_columns`.
    async fn table_with_files(files: &[&[Row]], partition_columns: &[&str]) -> DeltaTable {
        let mut table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema())
            .with_partition_columns(keys(partition_columns))
            .await
            .unwrap();

        for rows in files {
            table = table.write([batch(rows)]).await.unwrap();
        }

        table
    }

    /// Five rows in one workspace across two files: ids A-C, then D-E.
    async fn setup_test_table() -> DeltaTable {
        table_with_files(
            &[
                &[row("A", 1), row("B", 2), row("C", 3)],
                &[row("D", 4), row("E", 5)],
            ],
            &["workspace_id"],
        )
        .await
    }

    /// Two workspaces, one file each, so a per-workspace upsert rewrites exactly one file.
    async fn setup_two_workspace_table() -> DeltaTable {
        table_with_files(
            &[
                &[row("A", 1), row("B", 2)],
                &[row("A", 3).in_workspace(2), row("B", 4).in_workspace(2)],
            ],
            &["workspace_id"],
        )
        .await
    }

    /// Source updating id "A" inside a single workspace, i.e. guaranteed to conflict with the
    /// existing row and therefore to emit a remove for that workspace's file.
    fn single_workspace_source(workspace_id: i32, value: i32) -> DataFrame {
        source(&[row("A", value).in_workspace(workspace_id)])
    }

    async fn get_table_data(table: &DeltaTable) -> Vec<RecordBatch> {
        use datafusion::physical_plan::common::collect;
        let (_table, stream) = table.scan_table().await.unwrap();
        collect(stream).await.unwrap()
    }

    /// Total rows currently in the table.
    async fn row_count(table: &DeltaTable) -> usize {
        table_rows(&get_table_data(table).await)
    }

    fn table_rows(data: &[RecordBatch]) -> usize {
        data.iter().map(|batch| batch.num_rows()).sum()
    }

    fn assert_record(data: &[RecordBatch], expected: (&str, i32)) {
        let (expected_id, expected_value) = expected;
        let mut found = false;

        for batch in data {
            // The scan may hand back `Utf8`, `Utf8View` or `LargeUtf8` for `id` depending on how it
            // encoded the column, so cast rather than enumerating the variants.
            let ids = arrow::compute::cast(batch.column_by_name("id").unwrap(), &DataType::Utf8)
                .expect("id column casts to Utf8");
            let ids = ids
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("cast to Utf8 yields a StringArray");
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("value column is Int32");

            for i in 0..batch.num_rows() {
                if ids.value(i) == expected_id {
                    found = true;
                    assert_eq!(
                        values.value(i),
                        expected_value,
                        "value mismatch for id {expected_id:?}"
                    );
                }
            }
        }

        assert!(found, "expected record {expected_id:?} not found");
    }

    async fn table_version_now(table: &DeltaTable) -> Option<u64> {
        let mut reloaded = table.clone();
        reloaded.load().await.unwrap();
        reloaded.version()
    }

    #[tokio::test]
    async fn test_upsert_no_conflicts() {
        let table = setup_test_table().await;
        let input_rows = row_count(&table).await;

        let (updated_table, metrics) = table
            .upsert(source(&[row("F", 6), row("G", 7)]), join_keys())
            .await
            .unwrap();

        // Should have added files but no removed files since no conflicts
        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_conflicting_records, 0);

        assert_eq!(row_count(&updated_table).await, input_rows + 2);
    }

    #[tokio::test]
    async fn test_upsert_with_conflicts() {
        let table = setup_test_table().await;
        let input_rows = row_count(&table).await;

        // "A" conflicts, "F" doesn't.
        let (updated_table, metrics) = table
            .upsert(source(&[row("A", 10), row("F", 6)]), join_keys())
            .await
            .unwrap();

        // Note: The write operation may combine files into a single output file
        assert!(metrics.num_added_files >= 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_conflicting_records, 1);

        let data = get_table_data(&updated_table).await;
        assert_record(&data, ("A", 10)); // Updated record
        assert_record(&data, ("F", 6)); // New record
        assert_eq!(table_rows(&data), input_rows + 1);
    }

    #[tokio::test]
    async fn test_upsert_with_multifile_conflicts() {
        let table = setup_test_table().await;
        let input_rows = row_count(&table).await;

        // "A" conflicts with the first file, "E" with the second, "F" with neither.
        let (updated_table, metrics) = table
            .upsert(
                source(&[row("A", 10), row("E", 50), row("F", 6)]),
                join_keys(),
            )
            .await
            .unwrap();

        assert!(metrics.num_added_files >= 1);
        assert_eq!(metrics.num_removed_files, 2);
        assert_eq!(metrics.num_conflicting_records, 2);

        let data = get_table_data(&updated_table).await;
        assert_record(&data, ("A", 10));
        assert_record(&data, ("E", 50));
        assert_record(&data, ("F", 6));
        assert_eq!(table_rows(&data), input_rows + 1);
    }

    #[tokio::test]
    async fn test_upsert_with_duplicate_conflicts() {
        // "A" exists in both files, so one source row conflicts twice.
        let table = table_with_files(
            &[&[row("A", 1), row("B", 2)], &[row("A", 3), row("C", 4)]],
            &["workspace_id"],
        )
        .await;

        let input_rows = row_count(&table).await;

        let (updated_table, metrics) = table
            .upsert(source(&[row("A", 10)]), join_keys())
            .await
            .unwrap();

        assert!(metrics.num_added_files >= 1);
        assert_eq!(metrics.num_removed_files, 2);
        assert_eq!(metrics.num_conflicting_records, 2);

        let data = get_table_data(&updated_table).await;
        assert_record(&data, ("A", 10));
        // The duplicate "A" is collapsed into the single updated row.
        assert_eq!(table_rows(&data), input_rows - 1);
    }

    #[tokio::test]
    async fn test_upsert_empty_source() {
        let table = setup_test_table().await;
        let input_rows = row_count(&table).await;

        let (updated_table, metrics) = table.upsert(source(&[]), keys(&["id"])).await.unwrap();

        // No changes should be made for empty source
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_conflicting_records, 0);

        assert_eq!(row_count(&updated_table).await, input_rows);
    }

    #[tokio::test]
    async fn test_upsert_with_another_partition() {
        let table = setup_test_table().await;
        let input_rows = row_count(&table).await;

        // Same ids, different workspace, so nothing conflicts.
        let (updated_table, metrics) = table
            .upsert(
                source(&[row("A", 1).in_workspace(2), row("E", 4).in_workspace(2)]),
                join_keys(),
            )
            .await
            .unwrap();

        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 0);

        assert_eq!(row_count(&updated_table).await, input_rows + 2);
    }

    #[tokio::test]
    async fn test_upsert_with_custom_properties() {
        let table = setup_test_table().await;

        let mut commit_props = CommitProperties::default();
        commit_props
            .app_metadata
            .insert("test_key".to_string(), serde_json::json!("test_value"));

        let (updated_table, _) = table
            .upsert(source(&[row("F", 6)]), join_keys())
            .with_commit_properties(commit_props)
            .await
            .unwrap();

        // Verify the commit contains our custom properties
        let history: Vec<_> = updated_table.history(None).await.unwrap().collect();
        assert!(history[0].operation_parameters.is_some());
    }

    #[tokio::test]
    async fn test_upsert_with_two_partition_columns() {
        let table = table_with_files(
            &[
                &[row("A", 1), row("B", 2).on(DAY_2)],
                &[row("C", 3), row("D", 4).on(DAY_2)],
            ],
            &["workspace_id", "date"],
        )
        .await;

        let input_rows = row_count(&table).await;

        // Updates A in place, moves B into DAY_1, and inserts F.
        let (updated_table, metrics) = table
            .upsert(
                source(&[row("A", 10), row("B", 11), row("F", 6)]),
                join_keys(),
            )
            .await
            .unwrap();

        // One remove for A's partition, one for B's original partition.
        assert_eq!(metrics.num_removed_files, 2);
        assert_eq!(metrics.num_added_files, 1);

        let data = get_table_data(&updated_table).await;
        assert_record(&data, ("A", 10)); // Updated
        assert_record(&data, ("B", 11)); // Moved with updated value
        assert_record(&data, ("F", 6)); // New
        assert_eq!(table_rows(&data), input_rows + 1);
    }

    #[tokio::test]
    async fn test_upsert_with_source_column_order_difference() {
        // Target schema order is date, id, value, workspace_id.
        let table = setup_test_table().await;
        let input_rows = row_count(&table).await;

        let (updated_table, _) = table
            .upsert(
                source_with_column_order(
                    &["workspace_id", "value", "id", "date"],
                    &[row("A", 10), row("Z", 99)],
                ),
                join_keys(),
            )
            .await
            .unwrap();

        let data = get_table_data(&updated_table).await;
        assert_record(&data, ("A", 10));
        assert_record(&data, ("Z", 99));
        assert_eq!(table_rows(&data), input_rows + 1);
    }

    #[tokio::test]
    async fn test_concurrent_upserts_in_different_partitions_do_not_conflict() {
        let table = setup_two_workspace_table().await;
        let base_version = table.version();

        let (first, first_metrics) = table
            .clone()
            .upsert(single_workspace_source(1, 10), join_keys())
            .await
            .unwrap();

        assert_eq!(
            first_metrics.num_removed_files, 1,
            "first upsert must remove a pre-existing file for the race to be meaningful"
        );
        assert!(first.version() > base_version);

        // `table` is still pinned to `base_version`, so this commit races the one above.
        let (second, _) = table
            .upsert(single_workspace_source(2, 30), join_keys())
            .await
            .expect("upsert into a different partition must not conflict");

        assert!(second.version() > first.version());
        assert_eq!(row_count(&second).await, 4);
    }

    #[tokio::test]
    async fn test_concurrent_upserts_in_same_partition_still_conflict() {
        let table = setup_two_workspace_table().await;

        let (first, _) = table
            .clone()
            .upsert(single_workspace_source(1, 10), join_keys())
            .await
            .unwrap();

        let result = table
            .upsert(single_workspace_source(1, 20), join_keys())
            .await;

        let err = result.expect_err("same-partition race must be reported");
        assert!(
            matches!(
                err,
                DeltaTableError::Transaction {
                    source: crate::kernel::transaction::TransactionError::CommitConflict(_)
                }
            ),
            "expected a commit conflict, got: {err:?}"
        );

        // The losing upsert left the table exactly as the winner did.
        assert_eq!(first.version(), table_version_now(&first).await);
    }
}
