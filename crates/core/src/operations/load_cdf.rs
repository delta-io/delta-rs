use std::sync::Arc;
use std::time::SystemTime;

use arrow_schema::{ArrowError, Field};
use chrono::{DateTime, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{ScalarValue, Statistics};
use tracing::log;

use crate::delta_datafusion::cdf::*;
use crate::delta_datafusion::{register_store, DataFusionMixins};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, AddCDCFile, CommitInfo, Remove};
use crate::logstore::{get_actions, LogStoreRef};
use crate::table::state::DeltaTableState;
use crate::DeltaTableError;

#[derive(Clone)]
pub struct CdfLoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// A sub-selection of columns to be loaded
    columns: Option<Vec<String>>,
    /// Version to read from
    starting_version: i64,
    /// Version to stop reading at
    ending_version: Option<i64>,
    /// Starting timestamp of commits to accept 
    starting_timestamp: Option<DateTime<Utc>>,
    /// Ending timestamp of commits to accept 
    ending_timestamp: Option<DateTime<Utc>>,
    ctx: SessionContext,
}

impl CdfLoadBuilder {
    /// Create a new [`LoadBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            columns: None,
            starting_version: 0,
            ending_version: None,
            starting_timestamp: None,
            ending_timestamp: None,
            ctx: SessionContext::new(),
        }
    }

    /// Specify column selection to load
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    pub fn with_starting_version(mut self, starting_version: i64) -> Self {
        self.starting_version = starting_version;
        self
    }

    pub fn with_ending_version(mut self, ending_version: i64) -> Self {
        self.ending_version = Some(ending_version);
        self
    }

    pub fn with_session_ctx(mut self, ctx: SessionContext) -> Self {
        self.ctx = ctx;
        self
    }

    pub fn with_ending_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.ending_timestamp = Some(timestamp);
        self
    }

    pub fn with_starting_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.starting_timestamp = Some(timestamp);
        self
    }

    /// This is a rust version of https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala#L418
    /// Which iterates through versions of the delta table collects the relevant actions / commit info and returns those
    /// groupings for later use. The scala implementation has a lot more edge case handling and read schema checking (and just error checking in general)
    /// than I have right now. I plan to extend the checks once we have a stable state of the initial implementation.
    async fn determine_files_to_read(
        &self,
    ) -> DeltaResult<(
        Vec<CdcDataSpec<AddCDCFile>>,
        Vec<CdcDataSpec<Add>>,
        Vec<CdcDataSpec<Remove>>,
    )> {
        let start = self.starting_version;
        let end = self
            .ending_version
            .unwrap_or(self.log_store.get_latest_version(start).await?);

        let starting_timestamp = self.starting_timestamp.unwrap_or(DateTime::UNIX_EPOCH);
        let ending_timestamp = self
            .ending_timestamp
            .unwrap_or(DateTime::from(SystemTime::now()));

        log::debug!(
            "starting timestamp = {:?}, ending timestamp = {:?}",
            &starting_timestamp,
            &ending_timestamp
        );
        log::debug!("starting version = {}, ending version = {:?}", start, end);

        let mut change_files = vec![];
        let mut add_files = vec![];
        let mut remove_files = vec![];

        for version in start..=end {
            let snapshot_bytes = self
                .log_store
                .read_commit_entry(version)
                .await?
                .ok_or(DeltaTableError::InvalidVersion(version))?;
            let version_actions = get_actions(version, snapshot_bytes).await?;

            let mut commit_info = None;
            let mut ts = 0;
            let mut cdc_actions = vec![];

            if self.starting_timestamp.is_some() || self.ending_timestamp.is_some() {
                let version_commit = version_actions
                    .iter()
                    .find(|a| matches!(a, Action::CommitInfo(_)));
                if let Some(Action::CommitInfo(CommitInfo {
                    timestamp: Some(t), ..
                })) = version_commit
                {
                    if starting_timestamp.timestamp_millis() > *t
                        || *t > ending_timestamp.timestamp_millis()
                    {
                        log::debug!("Version: {} skipped, due to commit timestamp", version);
                        continue;
                    }
                }
            }

            for action in &version_actions {
                match action {
                    Action::Cdc(f) => cdc_actions.push(f.clone()),
                    Action::CommitInfo(ci) => {
                        ts = ci.timestamp.unwrap_or(0);
                        commit_info.replace(ci);
                    }
                    _ => {}
                }
            }

            if !cdc_actions.is_empty() {
                log::debug!(
                    "Located {} cdf actions for version: {}",
                    cdc_actions.len(),
                    version
                );
                change_files.push(CdcDataSpec::new(
                    version,
                    ts,
                    cdc_actions,
                    commit_info.cloned(),
                ))
            } else {
                let add_actions = version_actions
                    .iter()
                    .filter_map(|a| match a {
                        Action::Add(a) if a.data_change => Some(a.clone()),
                        _ => None,
                    })
                    .collect::<Vec<Add>>();

                let remove_actions = version_actions
                    .iter()
                    .filter_map(|a| match a {
                        Action::Remove(r) if r.data_change => Some(r.clone()),
                        _ => None,
                    })
                    .collect::<Vec<Remove>>();

                if !add_actions.is_empty() {
                    log::debug!(
                        "Located {} cdf actions for version: {}",
                        add_actions.len(),
                        version
                    );
                    add_files.push(CdcDataSpec::new(
                        version,
                        ts,
                        add_actions,
                        commit_info.cloned(),
                    ));
                }

                if !remove_actions.is_empty() {
                    log::debug!(
                        "Located {} cdf actions for version: {}",
                        remove_actions.len(),
                        version
                    );
                    remove_files.push(CdcDataSpec::new(
                        version,
                        ts,
                        remove_actions,
                        commit_info.cloned(),
                    ));
                }
            }
        }

        Ok((change_files, add_files, remove_files))
    }

    #[inline]
    fn get_add_action_type() -> Option<ScalarValue> {
        Some(ScalarValue::Utf8(Some(String::from("insert"))))
    }

    /// Executes the scan
    pub async fn build(&self) -> DeltaResult<DeltaCdfScan> {
        let (cdc, add, _remove) = self.determine_files_to_read().await?;
        register_store(
            self.log_store.clone(),
            self.ctx.state().runtime_env().clone(),
        );

        let partition_values = self.snapshot.metadata().partition_columns.clone();
        let schema = self.snapshot.arrow_schema()?;
        let schema_fields: Vec<Field> = self
            .snapshot
            .arrow_schema()?
            .all_fields()
            .into_iter()
            .filter(|f| !partition_values.contains(f.name()))
            .cloned()
            .collect();

        let this_partition_values = partition_values
            .iter()
            .map(|name| schema.field_with_name(name).map(|f| f.to_owned()))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        // Setup for the Read Schemas of each kind of file, CDC files include commit action type so they need a slightly
        // different schema than standard add file reads
        let cdc_file_schema = create_cdc_schema(schema_fields.clone(), true);
        let add_file_schema = create_cdc_schema(schema_fields, false);

        // Set up the mapping of partition columns to be projected into the final output batch
        // cdc for example has timestamp, version, and any table partitions mapped here.
        // add on the other hand has action type, timestamp, version and any additional table partitions because adds do
        // not include their actions
        let mut cdc_partition_cols = CDC_PARTITION_SCHEMA.clone();
        let mut add_partition_cols = ADD_PARTITION_SCHEMA.clone();
        cdc_partition_cols.extend_from_slice(&this_partition_values);
        add_partition_cols.extend_from_slice(&this_partition_values);

        // Set up the partition to physical file mapping, this is a mostly unmodified version of what is done in load
        let cdc_file_groups =
            create_partition_values(schema.clone(), cdc, &partition_values, None)?;
        let add_file_groups = create_partition_values(
            schema.clone(),
            add,
            &partition_values,
            Self::get_add_action_type(),
        )?;

        // Create the parquet scans for each associated type of file. I am not sure when we would use removes yet, but
        // they would be here if / when they are necessary
        let cdc_scan = ParquetFormat::new()
            .create_physical_plan(
                &self.ctx.state(),
                FileScanConfig {
                    object_store_url: self.log_store.object_store_url(),
                    file_schema: cdc_file_schema.clone(),
                    file_groups: cdc_file_groups.into_values().collect(),
                    statistics: Statistics::new_unknown(&cdc_file_schema),
                    projection: None,
                    limit: None,
                    table_partition_cols: cdc_partition_cols,
                    output_ordering: vec![],
                },
                None,
            )
            .await?;

        let add_scan = ParquetFormat::new()
            .create_physical_plan(
                &self.ctx.state(),
                FileScanConfig {
                    object_store_url: self.log_store.object_store_url(),
                    file_schema: add_file_schema.clone(),
                    file_groups: add_file_groups.into_values().collect(),
                    statistics: Statistics::new_unknown(&add_file_schema),
                    projection: None,
                    limit: None,
                    table_partition_cols: add_partition_cols,
                    output_ordering: vec![],
                },
                None,
            )
            .await?;

        // The output batches are then unioned to create a single output. Coalesce partitions is only here for the time
        // being for development. I plan to parallelize the reads once the base idea is correct.
        let union_scan: Arc<dyn ExecutionPlan> = Arc::new(UnionExec::new(vec![cdc_scan, add_scan]));
        Ok(DeltaCdfScan::new(union_scan, schema))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use arrow_array::RecordBatch;
    use arrow_cast::pretty::print_batches;
    use chrono::NaiveDateTime;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    use crate::delta_datafusion::cdf::DeltaCdfScan;
    use crate::operations::collect_sendable_stream;
    use crate::writer::test_utils::TestResult;
    use crate::DeltaOps;

    async fn collect_and_print(
        num_partitions: usize,
        stream: DeltaCdfScan,
        ctx: SessionContext,
    ) -> TestResult {
        let mut batches = vec![];
        for p in 0..num_partitions {
            let data: Vec<RecordBatch> =
                collect_sendable_stream(stream.execute(p, ctx.task_ctx())?).await?;
            batches.extend_from_slice(&data);
        }

        print_batches(&batches).map_err(Into::into)
    }

    #[tokio::test]
    async fn test_load_local() -> TestResult {
        let ctx = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table")
            .await?
            .load_cdf()
            .with_session_ctx(ctx.clone())
            .with_starting_version(0)
            .build()
            .await?;

        collect_and_print(table.output_partitioning().partition_count(), table, ctx).await
    }

    #[tokio::test]
    async fn test_load_local_datetime() -> TestResult {
        pretty_env_logger::init();
        let ctx = SessionContext::new();
        let starting_timestamp = NaiveDateTime::from_str("2023-12-22T17:10:21.675").unwrap();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table")
            .await?
            .load_cdf()
            .with_session_ctx(ctx.clone())
            .with_ending_timestamp(starting_timestamp.and_utc())
            .build()
            .await?;

        collect_and_print(table.output_partitioning().partition_count(), table, ctx).await
    }
}
