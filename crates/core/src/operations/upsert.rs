//! Upsert data from a source DataFrame into a target Delta Table.
//! For each conflicting record (e.g., matching on primary key), only the source record is kept.
//! All non-conflicting records are appended. This operation is memory bound and optimized for performance.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::logical_expr::{Expr, col};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::physical_plan::ExecutionPlan;
use itertools::Itertools;
use parquet::file::properties::WriterProperties;

use crate::table::state::DeltaTableState;
use crate::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL};
use crate::logstore::LogStoreRef;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use crate::delta_datafusion::planner::DeltaPlanner;
use crate::delta_datafusion::physical::find_metric_node;
use crate::operations::write::execution::write_execution_plan_v2;
use crate::operations::write::WriterStatsConfig;
use crate::delta_datafusion::{register_store, DataFusionMixins};
use crate::delta_datafusion::DeltaSessionConfig;
use crate::protocol::SaveMode;

pub struct UpsertBuilder {
    /// The join keys used to identify conflicts
    join_keys: Vec<String>,
    /// The source data
    source: DataFrame,
    /// The target table state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    state: Option<datafusion::execution::session_state::SessionState>,
    /// Properties for Parquet writer
    writer_properties: Option<WriterProperties>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl UpsertBuilder {
    pub fn new(
        log_store: LogStoreRef,
        snapshot: DeltaTableState,
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
        }
    }

    pub fn with_session_state(mut self, state: datafusion::execution::session_state::SessionState) -> Self {
        self.state = Some(state);
        self
    }

    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }

    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }
}

impl super::Operation<()> for UpsertBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn super::CustomExecuteHandler>> {
        None
    }
}

impl std::future::IntoFuture for UpsertBuilder {
    type Output = DeltaResult<(DeltaTable, usize /*num records written*/ )>;
    type IntoFuture = futures::future::BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;
        Box::pin(async move {
            PROTOCOL.can_write_to(&this.snapshot.snapshot)?;

            if !this.snapshot.load_config().require_files {
                return Err(DeltaTableError::NotInitializedWithFiles("UPSERT".into()));
            }

            let state = this.state.unwrap_or_else(|| {
                let config: datafusion::execution::context::SessionConfig = DeltaSessionConfig::default().into();
                let session = SessionContext::new_with_config(config);
                register_store(this.log_store.clone(), session.runtime_env());
                session.state()
            });

            let exec_start = Instant::now();

            // --- 1. Load target as DataFrame ---
            let scan_config = crate::delta_datafusion::DeltaScanConfigBuilder::default()
                .with_file_column(false)
                .with_parquet_pushdown(true)
                .with_schema(this.snapshot.input_schema().unwrap())
                .build(&this.snapshot)?;

            let target_provider = Arc::new(crate::delta_datafusion::DeltaTableProvider::try_new(
                this.snapshot.clone(),
                this.log_store.clone(),
                scan_config.clone(),
            )?);

            let target = DataFrame::new(
                state.clone(),
                datafusion::logical_expr::LogicalPlanBuilder::scan(
                    datafusion::common::TableReference::bare("target"),
                    datafusion::datasource::provider_as_source(target_provider),
                    None,
                )?.build()?
            );

            // --- 2. Perform anti-join to filter out conflicting target records ---
            // let join_exprs: Vec<(Expr, Expr)> = this.join_keys
            //     .iter()
            //     .map(|key| (col(format!("target.{key}")), col(format!("source.{key}"))))
            //     .collect();

            let source_df = this.source.clone().with_column("source_marker", datafusion::logical_expr::lit(true))?;
            let target_df = target.with_column("target_marker", datafusion::logical_expr::lit(true))?;

            // Left anti join: target rows NOT in source (conflicting target rows removed)
            let target_no_conflict = target_df.join(
                source_df.clone(),
                datafusion::logical_expr::JoinType::LeftAnti,
                &this.join_keys.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                &this.join_keys.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                None
            )?;

            // --- 3. Union: (conflict-resolved source + non-conflicting target) ---
            // Source wins on conflicts, so just append all source records + non-conflicting target
            let append_df = source_df.union(target_no_conflict)?;

            // --- 4. Write out the union as new files ---
            let logical_plan = append_df.into_unoptimized_plan();
            let physical_plan = state.create_physical_plan(&logical_plan).await?;

            let (actions, metrics) = write_execution_plan_v2(
                Some(&this.snapshot),
                state.clone(),
                physical_plan,
                this.snapshot.metadata().partition_columns().clone(),
                this.log_store.object_store(None),
                Some(this.snapshot.table_config().target_file_size() as usize),
                None,
                this.writer_properties.clone(),
                WriterStatsConfig::new(
                    this.snapshot.table_config().num_indexed_cols(),
                    None,
                ),
                None,
                false
            ).await?;

            //TODO fix this
            let num_records = 0;

            let app_metadata = &mut this.commit_properties.app_metadata;
            app_metadata.insert("readVersion".to_owned(), this.snapshot.version().into());
            app_metadata.insert("operationMetrics".to_owned(), serde_json::json!({ "outputRows": num_records }));

            let operation = crate::protocol::DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: Some(this.snapshot.metadata().partition_columns().clone()),
                predicate: None,
            };

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions)
                .build(Some(&this.snapshot), this.log_store.clone(), operation)
                .await?;

            Ok((DeltaTable::new_with_state(this.log_store, commit.snapshot()), num_records))
        })
    }
}