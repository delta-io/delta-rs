//! Module for reading the change datafeed of delta tables
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let builder = CdfLoadBuilder::new(table.log_store(), table.snapshot())
//!     .with_starting_version(3);
//!
//! let ctx = SessionContext::new();
//! let provider = DeltaCdfTableProvider::try_new(builder)?;
//! let df = ctx.read_table(provider).await?;

use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Field, Schema};
use chrono::{DateTime, Utc};
use datafusion::common::config::TableParquetOptions;
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::execution::SessionState;
use datafusion::physical_expr::{expressions, PhysicalExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use tracing::log;

use crate::delta_datafusion::{register_store, DataFusionMixins};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, AddCDCFile, CommitInfo};
use crate::logstore::{get_actions, LogStoreRef};
use crate::table::state::DeltaTableState;
use crate::DeltaTableError;
use crate::{delta_datafusion::cdf::*, kernel::Remove};

/// Builder for create a read of change data feeds for delta tables
#[derive(Clone, Debug)]
pub struct CdfLoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    pub snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Version to read from
    starting_version: Option<i64>,
    /// Version to stop reading at
    ending_version: Option<i64>,
    /// Starting timestamp of commits to accept
    starting_timestamp: Option<DateTime<Utc>>,
    /// Ending timestamp of commits to accept
    ending_timestamp: Option<DateTime<Utc>>,
    /// Enable ending version or timestamp exceeding the last commit
    allow_out_of_range: bool,
}

impl CdfLoadBuilder {
    /// Create a new [`LoadBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            starting_version: None,
            ending_version: None,
            starting_timestamp: None,
            ending_timestamp: None,
            allow_out_of_range: false,
        }
    }

    /// Version to start at (version 0 if not provided)
    pub fn with_starting_version(mut self, starting_version: i64) -> Self {
        self.starting_version = Some(starting_version);
        self
    }

    /// Version (inclusive) to end at
    pub fn with_ending_version(mut self, ending_version: i64) -> Self {
        self.ending_version = Some(ending_version);
        self
    }

    /// Timestamp (inclusive) to end at
    pub fn with_ending_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.ending_timestamp = Some(timestamp);
        self
    }

    /// Timestamp to start from
    pub fn with_starting_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.starting_timestamp = Some(timestamp);
        self
    }

    /// Enable ending version or timestamp exceeding the last commit
    pub fn with_allow_out_of_range(mut self) -> Self {
        self.allow_out_of_range = true;
        self
    }

    async fn calculate_earliest_version(&self) -> DeltaResult<i64> {
        let ts = self.starting_timestamp.unwrap_or(DateTime::UNIX_EPOCH);
        for v in 0..self.snapshot.version() {
            if let Ok(Some(bytes)) = self.log_store.read_commit_entry(v).await {
                if let Ok(actions) = get_actions(v, bytes).await {
                    if actions.iter().any(|action| {
                        matches!(action, Action::CommitInfo(CommitInfo {
                            timestamp: Some(t), ..
                        }) if ts.timestamp_millis() < *t)
                    }) {
                        return Ok(v);
                    }
                }
            }
        }
        Ok(0)
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
        if self.starting_version.is_none() && self.starting_timestamp.is_none() {
            return Err(DeltaTableError::NoStartingVersionOrTimestamp);
        }
        let start = if let Some(s) = self.starting_version {
            s
        } else {
            self.calculate_earliest_version().await?
        };

        let mut change_files: Vec<CdcDataSpec<AddCDCFile>> = vec![];
        let mut add_files: Vec<CdcDataSpec<Add>> = vec![];
        let mut remove_files: Vec<CdcDataSpec<Remove>> = vec![];

        // Start from 0 since if start > latest commit, the returned commit is not a valid commit
        let latest_version = match self.log_store.get_latest_version(start).await {
            Ok(latest_version) => latest_version,
            Err(DeltaTableError::InvalidVersion(_)) if self.allow_out_of_range => {
                return Ok((change_files, add_files, remove_files));
            }
            Err(e) => return Err(e),
        };

        let mut end = self.ending_version.unwrap_or(latest_version);

        if end > latest_version {
            end = latest_version;
        }

        if end < start {
            return if self.allow_out_of_range {
                Ok((change_files, add_files, remove_files))
            } else {
                Err(DeltaTableError::ChangeDataInvalidVersionRange { start, end })
            };
        }
        if start > latest_version {
            return if self.allow_out_of_range {
                Ok((change_files, add_files, remove_files))
            } else {
                Err(DeltaTableError::InvalidVersion(start))
            };
        }

        let starting_timestamp = self.starting_timestamp.unwrap_or(DateTime::UNIX_EPOCH);
        let ending_timestamp = self
            .ending_timestamp
            .unwrap_or(DateTime::from(SystemTime::now()));

        // Check that starting_timestamp is within boundaries of the latest version
        let latest_snapshot_bytes = self
            .log_store
            .read_commit_entry(latest_version)
            .await?
            .ok_or(DeltaTableError::InvalidVersion(latest_version))?;

        let latest_version_actions: Vec<Action> =
            get_actions(latest_version, latest_snapshot_bytes).await?;
        let latest_version_commit = latest_version_actions
            .iter()
            .find(|a| matches!(a, Action::CommitInfo(_)));

        if let Some(Action::CommitInfo(CommitInfo {
            timestamp: Some(latest_timestamp),
            ..
        })) = latest_version_commit
        {
            if starting_timestamp.timestamp_millis() > *latest_timestamp {
                return if self.allow_out_of_range {
                    Ok((change_files, add_files, remove_files))
                } else {
                    Err(DeltaTableError::ChangeDataTimestampGreaterThanCommit { ending_timestamp })
                };
            }
        }

        log::debug!(
            "starting timestamp = {starting_timestamp:?}, ending timestamp = {ending_timestamp:?}"
        );
        log::debug!("starting version = {start}, ending version = {end:?}");

        for version in start..=end {
            let snapshot_bytes = self
                .log_store
                .read_commit_entry(version)
                .await?
                .ok_or(DeltaTableError::InvalidVersion(version));

            let version_actions: Vec<Action> = get_actions(version, snapshot_bytes?).await?;

            let mut ts = 0;
            let mut cdc_actions = vec![];

            if self.starting_timestamp.is_some() || self.ending_timestamp.is_some() {
                // TODO: fallback on other actions for timestamps because CommitInfo action is optional
                // theoretically.
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
                        log::debug!("Version: {version} skipped, due to commit timestamp");
                        continue;
                    }
                }
            }

            for action in &version_actions {
                match action {
                    Action::Cdc(f) => cdc_actions.push(f.clone()),
                    Action::Metadata(md) => {
                        log::info!("Metadata: {md:?}");
                        if let Some(key) = &md.configuration().get("delta.enableChangeDataFeed") {
                            let key = key.to_lowercase();
                            // Check here to ensure the CDC function is enabled for the first version of the read
                            // and check in subsequent versions only that it was not disabled.
                            if (version == start && key != "true") || key == "false" {
                                return Err(DeltaTableError::ChangeDataNotRecorded {
                                    version,
                                    start,
                                    end,
                                });
                            }
                        } else if version == start {
                            return Err(DeltaTableError::ChangeDataNotEnabled { version });
                        };
                    }
                    Action::CommitInfo(ci) => {
                        ts = ci.timestamp.unwrap_or(0);
                    }
                    _ => {}
                }
            }

            if !cdc_actions.is_empty() {
                log::debug!(
                    "Located {} cdf actions for version: {version}",
                    cdc_actions.len(),
                );
                change_files.push(CdcDataSpec::new(version, ts, cdc_actions))
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
                    .filter_map(|r| match r {
                        Action::Remove(r) if r.data_change => Some(r.clone()),
                        _ => None,
                    })
                    .collect::<Vec<Remove>>();

                if !add_actions.is_empty() {
                    log::debug!(
                        "Located {} cdf actions for version: {version}",
                        add_actions.len(),
                    );
                    add_files.push(CdcDataSpec::new(version, ts, add_actions));
                }

                if !remove_actions.is_empty() {
                    log::debug!(
                        "Located {} cdf actions for version: {version}",
                        remove_actions.len(),
                    );
                    remove_files.push(CdcDataSpec::new(version, ts, remove_actions));
                }
            }
        }

        Ok((change_files, add_files, remove_files))
    }

    #[inline]
    fn get_add_action_type() -> Option<ScalarValue> {
        Some(ScalarValue::Utf8(Some(String::from("insert"))))
    }

    #[inline]
    fn get_remove_action_type() -> Option<ScalarValue> {
        Some(ScalarValue::Utf8(Some(String::from("delete"))))
    }

    /// Executes the scan
    pub(crate) async fn build(
        &self,
        session_sate: &SessionState,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> DeltaResult<Arc<dyn ExecutionPlan>> {
        let (cdc, add, remove) = self.determine_files_to_read().await?;
        register_store(self.log_store.clone(), session_sate.runtime_env().clone());

        let partition_values = self.snapshot.metadata().partition_columns().clone();
        let schema = self.snapshot.input_schema()?;
        let schema_fields: Vec<Arc<Field>> = self
            .snapshot
            .input_schema()?
            .fields()
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
        let add_remove_file_schema = create_cdc_schema(schema_fields, false);

        // Set up the mapping of partition columns to be projected into the final output batch
        // cdc for example has timestamp, version, and any table partitions mapped here.
        // add on the other hand has action type, timestamp, version and any additional table partitions because adds do
        // not include their actions
        let mut cdc_partition_cols = CDC_PARTITION_SCHEMA.clone();
        let mut add_remove_partition_cols = ADD_PARTITION_SCHEMA.clone();
        cdc_partition_cols.extend_from_slice(&this_partition_values);
        add_remove_partition_cols.extend_from_slice(&this_partition_values);

        // Set up the partition to physical file mapping, this is a mostly unmodified version of what is done in load
        let cdc_file_groups =
            create_partition_values(schema.clone(), cdc, &partition_values, None)?;
        let add_file_groups = create_partition_values(
            schema.clone(),
            add,
            &partition_values,
            Self::get_add_action_type(),
        )?;
        let remove_file_groups = create_partition_values(
            schema.clone(),
            remove,
            &partition_values,
            Self::get_remove_action_type(),
        )?;

        // Create the parquet scans for each associated type of file.
        let mut parquet_source = ParquetSource::new(TableParquetOptions::new());
        if let Some(filters) = filters {
            parquet_source = parquet_source.with_predicate(Arc::clone(filters));
        }
        let parquet_source: Arc<dyn FileSource> = Arc::new(parquet_source);
        let cdc_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(
                self.log_store.object_store_url(),
                Arc::clone(&cdc_file_schema),
                Arc::clone(&parquet_source),
            )
            .with_file_groups(cdc_file_groups.into_values().map(FileGroup::from).collect())
            .with_table_partition_cols(cdc_partition_cols)
            .build(),
        );

        let add_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(
                self.log_store.object_store_url(),
                Arc::clone(&add_remove_file_schema),
                Arc::clone(&parquet_source),
            )
            .with_file_groups(add_file_groups.into_values().map(FileGroup::from).collect())
            .with_table_partition_cols(add_remove_partition_cols.clone())
            .build(),
        );

        let remove_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(
                self.log_store.object_store_url(),
                Arc::clone(&add_remove_file_schema),
                parquet_source,
            )
            .with_file_groups(
                remove_file_groups
                    .into_values()
                    .map(FileGroup::from)
                    .collect(),
            )
            .with_table_partition_cols(add_remove_partition_cols)
            .build(),
        );

        // The output batches are then unioned to create a single output. Coalesce partitions is only here for the time
        // being for development. I plan to parallelize the reads once the base idea is correct.
        let union_scan: Arc<dyn ExecutionPlan> =
            Arc::new(UnionExec::new(vec![cdc_scan, add_scan, remove_scan]));

        // We project the union in the order of the input_schema + cdc cols at the end
        // This is to ensure the DeltaCdfTableProvider uses the correct schema construction.
        let mut fields = schema.fields().to_vec();
        for f in ADD_PARTITION_SCHEMA.clone() {
            fields.push(f.into());
        }
        let project_schema = Schema::new(fields);

        let union_schema = union_scan.schema();

        let expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = project_schema
            .fields()
            .into_iter()
            .map(|f| -> (Arc<dyn PhysicalExpr>, String) {
                let field_name = f.name();
                let expr = Arc::new(expressions::Column::new(
                    field_name,
                    union_schema.index_of(field_name).unwrap(),
                ));
                (expr, field_name.to_owned())
            })
            .collect();

        let scan = Arc::new(ProjectionExec::try_new(expressions, union_scan)?);

        Ok(scan)
    }
}

#[allow(unused)]
/// Helper function to collect batches associated with reading CDF data
pub(crate) async fn collect_batches(
    num_partitions: usize,
    stream: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let mut batches = vec![];
    for p in 0..num_partitions {
        let data: Vec<RecordBatch> =
            crate::operations::collect_sendable_stream(stream.execute(p, ctx.task_ctx())?).await?;
        batches.extend_from_slice(&data);
    }
    Ok(batches)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::str::FromStr;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::Schema;
    use chrono::NaiveDateTime;
    use datafusion::common::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use itertools::Itertools;

    use crate::test_utils::TestSchemas;
    use crate::writer::test_utils::TestResult;
    use crate::{DeltaOps, DeltaTable, TableProperty};

    #[tokio::test]
    async fn test_load_local() -> TestResult {
        let ctx: SessionContext = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table")
            .await?
            .load_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await?;

        let batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await?;
        assert_batches_sorted_eq! {
            [
                "+----+--------+------------+------------------+-----------------+-------------------------+",
                "| id | name   | birthday   | _change_type     | _commit_version | _commit_timestamp       |",
                "+----+--------+------------+------------------+-----------------+-------------------------+",
                "| 1  | Steve  | 2023-12-22 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 10 | Borb   | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 2  | Bob    | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675 |",
                "| 2  | Bob    | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 2  | Bob    | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675 |",
                "| 3  | Dave   | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675 |",
                "| 3  | Dave   | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 3  | Dave   | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675 |",
                "| 4  | Kate   | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675 |",
                "| 4  | Kate   | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 4  | Kate   | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675 |",
                "| 5  | Emily  | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 5  | Emily  | 2023-12-24 | update_preimage  | 2               | 2023-12-29T21:41:33.785 |",
                "| 5  | Emily  | 2023-12-29 | update_postimage | 2               | 2023-12-29T21:41:33.785 |",
                "| 6  | Carl   | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 6  | Carl   | 2023-12-24 | update_preimage  | 2               | 2023-12-29T21:41:33.785 |",
                "| 6  | Carl   | 2023-12-29 | update_postimage | 2               | 2023-12-29T21:41:33.785 |",
                "| 7  | Dennis | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 7  | Dennis | 2023-12-24 | update_preimage  | 2               | 2023-12-29T21:41:33.785 |",
                "| 7  | Dennis | 2023-12-29 | delete           | 3               | 2024-01-06T16:44:59.570 |",
                "| 7  | Dennis | 2023-12-29 | update_postimage | 2               | 2023-12-29T21:41:33.785 |",
                "| 8  | Claire | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 9  | Ada    | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "+----+--------+------------+------------------+-----------------+-------------------------+",
        ], &batches }
        Ok(())
    }

    #[tokio::test]
    async fn test_load_local_datetime() -> TestResult {
        let ctx = SessionContext::new();
        let starting_timestamp = NaiveDateTime::from_str("2023-12-22T17:10:21.675").unwrap();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table")
            .await?
            .load_cdf()
            .with_starting_version(0)
            .with_ending_timestamp(starting_timestamp.and_utc())
            .build(&ctx.state(), None)
            .await
            .unwrap();

        let batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await?;

        assert_batches_sorted_eq! {
            [
                "+----+--------+------------+------------------+-----------------+-------------------------+",
                "| id | name   | birthday   | _change_type     | _commit_version | _commit_timestamp       |",
                "+----+--------+------------+------------------+-----------------+-------------------------+",
                "| 1  | Steve  | 2023-12-22 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 10 | Borb   | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 2  | Bob    | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675 |",
                "| 2  | Bob    | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 2  | Bob    | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675 |",
                "| 3  | Dave   | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675 |",
                "| 3  | Dave   | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 3  | Dave   | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675 |",
                "| 4  | Kate   | 2023-12-22 | update_postimage | 1               | 2023-12-22T17:10:21.675 |",
                "| 4  | Kate   | 2023-12-23 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 4  | Kate   | 2023-12-23 | update_preimage  | 1               | 2023-12-22T17:10:21.675 |",
                "| 5  | Emily  | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 6  | Carl   | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 7  | Dennis | 2023-12-24 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 8  | Claire | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "| 9  | Ada    | 2023-12-25 | insert           | 0               | 2023-12-22T17:10:18.828 |",
                "+----+--------+------------+------------------+-----------------+-------------------------+",
            ],
            &batches
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_load_local_non_partitioned() -> TestResult {
        let ctx = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table-non-partitioned")
            .await?
            .load_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await?;

        let batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await?;

        assert_batches_sorted_eq! {
            ["+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+-------------------------+",
             "| id | name   | birthday   | long_field        | boolean_field | double_field | smallint_field | _change_type     | _commit_version | _commit_timestamp       |",
             "+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+-------------------------+",
             "| 7  | Dennis | 2024-04-14 | 6                 | true          | 3.14         | 1              | delete           | 3               | 2024-04-14T15:58:32.495 |",
             "| 3  | Dave   | 2024-04-15 | 2                 | true          | 3.14         | 1              | update_preimage  | 1               | 2024-04-14T15:58:29.393 |",
             "| 3  | Dave   | 2024-04-14 | 2                 | true          | 3.14         | 1              | update_postimage | 1               | 2024-04-14T15:58:29.393 |",
             "| 4  | Kate   | 2024-04-15 | 3                 | true          | 3.14         | 1              | update_preimage  | 1               | 2024-04-14T15:58:29.393 |",
             "| 4  | Kate   | 2024-04-14 | 3                 | true          | 3.14         | 1              | update_postimage | 1               | 2024-04-14T15:58:29.393 |",
             "| 2  | Bob    | 2024-04-15 | 1                 | true          | 3.14         | 1              | update_preimage  | 1               | 2024-04-14T15:58:29.393 |",
             "| 2  | Bob    | 2024-04-14 | 1                 | true          | 3.14         | 1              | update_postimage | 1               | 2024-04-14T15:58:29.393 |",
             "| 7  | Dennis | 2024-04-16 | 6                 | true          | 3.14         | 1              | update_preimage  | 2               | 2024-04-14T15:58:31.257 |",
             "| 7  | Dennis | 2024-04-14 | 6                 | true          | 3.14         | 1              | update_postimage | 2               | 2024-04-14T15:58:31.257 |",
             "| 5  | Emily  | 2024-04-16 | 4                 | true          | 3.14         | 1              | update_preimage  | 2               | 2024-04-14T15:58:31.257 |",
             "| 5  | Emily  | 2024-04-14 | 4                 | true          | 3.14         | 1              | update_postimage | 2               | 2024-04-14T15:58:31.257 |",
             "| 6  | Carl   | 2024-04-16 | 5                 | true          | 3.14         | 1              | update_preimage  | 2               | 2024-04-14T15:58:31.257 |",
             "| 6  | Carl   | 2024-04-14 | 5                 | true          | 3.14         | 1              | update_postimage | 2               | 2024-04-14T15:58:31.257 |",
             "| 1  | Alex   | 2024-04-14 | 1                 | true          | 3.14         | 1              | insert           | 4               | 2024-04-14T15:58:33.444 |",
             "| 2  | Alan   | 2024-04-15 | 1                 | true          | 3.14         | 1              | insert           | 4               | 2024-04-14T15:58:33.444 |",
             "| 1  | Steve  | 2024-04-14 | 1                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 2  | Bob    | 2024-04-15 | 1                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 3  | Dave   | 2024-04-15 | 2                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 4  | Kate   | 2024-04-15 | 3                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 5  | Emily  | 2024-04-16 | 4                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 6  | Carl   | 2024-04-16 | 5                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 7  | Dennis | 2024-04-16 | 6                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 8  | Claire | 2024-04-17 | 7                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 9  | Ada    | 2024-04-17 | 8                 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "| 10 | Borb   | 2024-04-17 | 99999999999999999 | true          | 3.14         | 1              | insert           | 0               | 2024-04-14T15:58:26.249 |",
             "+----+--------+------------+-------------------+---------------+--------------+----------------+------------------+-----------------+-------------------------+"],
            &batches
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_load_bad_version_range() -> TestResult {
        let ctx = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table-non-partitioned")
            .await?
            .load_cdf()
            .with_starting_version(4)
            .with_ending_version(1)
            .build(&ctx.state(), None)
            .await;

        assert!(table.is_err());
        assert!(matches!(
            table.unwrap_err(),
            DeltaTableError::ChangeDataInvalidVersionRange { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_load_version_out_of_range() -> TestResult {
        let ctx = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table-non-partitioned")
            .await?
            .load_cdf()
            .with_starting_version(5)
            .build(&ctx.state(), None)
            .await;

        assert!(table.is_err());
        assert!(matches!(
            table.unwrap_err(),
            DeltaTableError::InvalidVersion(5)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_load_version_out_of_range_with_flag() -> TestResult {
        let ctx = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table-non-partitioned")
            .await?
            .load_cdf()
            .with_starting_version(5)
            .with_allow_out_of_range()
            .build(&ctx.state(), None)
            .await?;

        let batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table.clone(),
            ctx,
        )
        .await?;

        assert!(batches.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_load_timestamp_out_of_range() -> TestResult {
        let ending_timestamp = NaiveDateTime::from_str("2033-12-22T17:10:21.675").unwrap();
        let ctx = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table-non-partitioned")
            .await?
            .load_cdf()
            .with_starting_timestamp(ending_timestamp.and_utc())
            .build(&ctx.state(), None)
            .await;

        assert!(table.is_err());
        assert!(matches!(
            table.unwrap_err(),
            DeltaTableError::ChangeDataTimestampGreaterThanCommit { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_load_timestamp_out_of_range_with_flag() -> TestResult {
        let ctx = SessionContext::new();
        let ending_timestamp = NaiveDateTime::from_str("2033-12-22T17:10:21.675").unwrap();
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table-non-partitioned")
            .await?
            .load_cdf()
            .with_starting_timestamp(ending_timestamp.and_utc())
            .with_allow_out_of_range()
            .build(&ctx.state(), None)
            .await?;

        let batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table.clone(),
            ctx,
        )
        .await?;

        assert!(batches.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_load_non_cdf() -> TestResult {
        let ctx = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/simple_table")
            .await?
            .load_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await;

        assert!(table.is_err());
        assert!(matches!(
            table.unwrap_err(),
            DeltaTableError::ChangeDataNotEnabled { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_load_vacuumed_table() -> TestResult {
        let ending_timestamp = NaiveDateTime::from_str("2024-01-06T15:44:59.570")?;
        let ctx = SessionContext::new();
        let table = DeltaOps::try_from_uri("../test/tests/data/checkpoint-cdf-table")
            .await?
            .load_cdf()
            .with_starting_timestamp(ending_timestamp.and_utc())
            .build(&ctx.state(), None)
            .await?;

        let batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await?;

        assert_batches_sorted_eq! {
            [
                "+----+--------+------------+------------------+-----------------+-------------------------+",
                "| id | name   | birthday   | _change_type     | _commit_version | _commit_timestamp       |",
                "+----+--------+------------+------------------+-----------------+-------------------------+",
                "| 11 | Ossama | 2024-12-30 | insert           | 4               | 2025-01-06T16:33:18.167 |",
                "| 11 | Ossama | 2024-12-30 | update_preimage  | 5               | 2025-01-06T16:38:19.623 |",
                "| 12 | Nick   | 2023-12-29 | insert           | 4               | 2025-01-06T16:33:18.167 |",
                "| 12 | Nick   | 2023-12-29 | update_preimage  | 5               | 2025-01-06T16:38:19.623 |",
                "| 12 | Ossama | 2024-12-30 | update_postimage | 5               | 2025-01-06T16:38:19.623 |",
                "| 13 | Nick   | 2023-12-29 | update_postimage | 5               | 2025-01-06T16:38:19.623 |",
                "| 13 | Ryan   | 2023-12-22 | insert           | 4               | 2025-01-06T16:33:18.167 |",
                "| 13 | Ryan   | 2023-12-22 | update_preimage  | 5               | 2025-01-06T16:38:19.623 |",
                "| 14 | Ryan   | 2023-12-22 | update_postimage | 5               | 2025-01-06T16:38:19.623 |",
                "| 14 | Zach   | 2023-12-25 | insert           | 4               | 2025-01-06T16:33:18.167 |",
                "| 14 | Zach   | 2023-12-25 | update_preimage  | 5               | 2025-01-06T16:38:19.623 |",
                "| 15 | Zach   | 2023-12-25 | update_postimage | 5               | 2025-01-06T16:38:19.623 |",
                "| 7  | Dennis | 2023-12-29 | delete           | 3               | 2024-01-06T16:44:59.570 |",
                "+----+--------+------------+------------------+-----------------+-------------------------+",
             ],
            &batches
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_use_remove_actions_for_deletions() -> TestResult {
        let delta_schema = TestSchemas::simple();
        let table: DeltaTable = DeltaOps::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_partition_columns(["id"])
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

        let schema: Arc<Schema> = Arc::new(delta_schema.try_into_arrow()?);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("1"), Some("2"), Some("3")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("yes"),
                    Some("yes"),
                    Some("no"),
                ])),
            ],
        )
        .unwrap();

        let second_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("3")])),
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("yes")])),
            ],
        )
        .unwrap();

        let table = DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));

        let table = DeltaOps(table)
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2));

        let ctx = SessionContext::new();
        let cdf_scan = DeltaOps(table.clone())
            .load_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");

        let mut batches = collect_batches(
            cdf_scan
                .properties()
                .output_partitioning()
                .partition_count(),
            cdf_scan,
            ctx,
        )
        .await
        .expect("Failed to collect batches");

        // The batches will contain a current _commit_timestamp which shouldn't be check_append_only
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(5)).collect();

        assert_batches_sorted_eq! {[
            "+-------+----------+----+--------------+-----------------+",
            "| value | modified | id | _change_type | _commit_version |",
            "+-------+----------+----+--------------+-----------------+",
            "| 1     | yes      | 1  | delete       | 2               |",
            "| 1     | yes      | 1  | insert       | 1               |",
            "| 10    | yes      | 3  | insert       | 2               |",
            "| 2     | yes      | 2  | delete       | 2               |",
            "| 2     | yes      | 2  | insert       | 1               |",
            "| 3     | no       | 3  | delete       | 2               |",
            "| 3     | no       | 3  | insert       | 1               |",
            "+-------+----------+----+--------------+-----------------+",
        ], &batches }

        let snapshot_bytes = table
            .log_store
            .read_commit_entry(2)
            .await?
            .expect("failed to get snapshot bytes");
        let version_actions = get_actions(2, snapshot_bytes).await?;

        let cdc_actions = version_actions
            .iter()
            .filter(|action| matches!(action, &&Action::Cdc(_)))
            .collect_vec();
        assert!(cdc_actions.is_empty());
        Ok(())
    }
}
