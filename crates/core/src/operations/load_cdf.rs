//! Module for reading the change datafeed of delta tables

use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Field};
use chrono::{DateTime, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::prelude::SessionContext;
use datafusion_common::{ScalarValue, Statistics};
use datafusion_physical_expr::expressions;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::ExecutionPlan;
use tracing::log;

use crate::delta_datafusion::{register_store, DataFusionMixins};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, AddCDCFile, CommitInfo};
use crate::logstore::{get_actions, LogStoreRef};
use crate::table::state::DeltaTableState;
use crate::DeltaTableError;
use crate::{delta_datafusion::cdf::*, kernel::Remove};

/// Builder for create a read of change data feeds for delta tables
#[derive(Clone)]
pub struct CdfLoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Columns to project
    columns: Option<Vec<String>>,
    /// Version to read from
    starting_version: i64,
    /// Version to stop reading at
    ending_version: Option<i64>,
    /// Starting timestamp of commits to accept
    starting_timestamp: Option<DateTime<Utc>>,
    /// Ending timestamp of commits to accept
    ending_timestamp: Option<DateTime<Utc>>,
    /// Provided Datafusion context
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

    /// Version to start at (version 0 if not provided)
    pub fn with_starting_version(mut self, starting_version: i64) -> Self {
        self.starting_version = starting_version;
        self
    }

    /// Version (inclusive) to end at
    pub fn with_ending_version(mut self, ending_version: i64) -> Self {
        self.ending_version = Some(ending_version);
        self
    }

    /// Provide a datafusion session context
    pub fn with_session_ctx(mut self, ctx: SessionContext) -> Self {
        self.ctx = ctx;
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

    /// Columns to select
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
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

        if end < start {
            return Err(DeltaTableError::ChangeDataInvalidVersionRange { start, end });
        }

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

        let mut change_files: Vec<CdcDataSpec<AddCDCFile>> = vec![];
        let mut add_files: Vec<CdcDataSpec<Add>> = vec![];
        let mut remove_files: Vec<CdcDataSpec<Remove>> = vec![];

        for version in start..=end {
            let snapshot_bytes = self
                .log_store
                .read_commit_entry(version)
                .await?
                .ok_or(DeltaTableError::InvalidVersion(version))?;
            let version_actions = get_actions(version, snapshot_bytes).await?;

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
                        log::debug!("Version: {} skipped, due to commit timestamp", version);
                        continue;
                    }
                }
            }

            for action in &version_actions {
                match action {
                    Action::Cdc(f) => cdc_actions.push(f.clone()),
                    Action::Metadata(md) => {
                        log::info!("Metadata: {:?}", &md);
                        if let Some(Some(key)) = &md.configuration.get("delta.enableChangeDataFeed")
                        {
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
                    "Located {} cdf actions for version: {}",
                    cdc_actions.len(),
                    version
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
                        "Located {} cdf actions for version: {}",
                        add_actions.len(),
                        version
                    );
                    add_files.push(CdcDataSpec::new(version, ts, add_actions));
                }

                if !remove_actions.is_empty() {
                    log::debug!(
                        "Located {} cdf actions for version: {}",
                        remove_actions.len(),
                        version
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

    fn get_remove_action_type() -> Option<ScalarValue> {
        Some(ScalarValue::Utf8(Some(String::from("delete"))))
    }

    /// Executes the scan
    pub async fn build(&self) -> DeltaResult<DeltaCdfScan> {
        let (cdc, add, remove) = self.determine_files_to_read().await?;
        register_store(
            self.log_store.clone(),
            self.ctx.state().runtime_env().clone(),
        );

        let partition_values = self.snapshot.metadata().partition_columns.clone();
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
                    file_schema: add_remove_file_schema.clone(),
                    file_groups: add_file_groups.into_values().collect(),
                    statistics: Statistics::new_unknown(&add_remove_file_schema.clone()),
                    projection: None,
                    limit: None,
                    table_partition_cols: add_remove_partition_cols.clone(),
                    output_ordering: vec![],
                },
                None,
            )
            .await?;

        let remove_scan = ParquetFormat::new()
            .create_physical_plan(
                &self.ctx.state(),
                FileScanConfig {
                    object_store_url: self.log_store.object_store_url(),
                    file_schema: add_remove_file_schema.clone(),
                    file_groups: remove_file_groups.into_values().collect(),
                    statistics: Statistics::new_unknown(&add_remove_file_schema),
                    projection: None,
                    limit: None,
                    table_partition_cols: add_remove_partition_cols,
                    output_ordering: vec![],
                },
                None,
            )
            .await?;

        // The output batches are then unioned to create a single output. Coalesce partitions is only here for the time
        // being for development. I plan to parallelize the reads once the base idea is correct.
        let mut union_scan: Arc<dyn ExecutionPlan> =
            Arc::new(UnionExec::new(vec![cdc_scan, add_scan, remove_scan]));

        if let Some(columns) = &self.columns {
            let expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = union_scan
                .schema()
                .fields()
                .into_iter()
                .enumerate()
                .map(|(idx, field)| -> (Arc<dyn PhysicalExpr>, String) {
                    let field_name = field.name();
                    let expr = Arc::new(expressions::Column::new(field_name, idx));
                    (expr, field_name.to_owned())
                })
                .filter(|(_, field_name)| columns.contains(field_name))
                .collect();
            union_scan = Arc::new(ProjectionExec::try_new(expressions, union_scan)?);
        }
        Ok(DeltaCdfScan::new(union_scan))
    }
}

#[allow(unused)]
/// Helper function to collect batches associated with reading CDF data
pub(crate) async fn collect_batches(
    num_partitions: usize,
    stream: DeltaCdfScan,
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
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_sorted_eq;
    use itertools::Itertools;

    use crate::test_utils::TestSchemas;
    use crate::writer::test_utils::TestResult;
    use crate::{DeltaOps, DeltaTable, TableProperty};

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

        let batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await?;
        assert_batches_sorted_eq! {
            ["+----+--------+------------------+-----------------+-------------------------+------------+",
             "| id | name   | _change_type     | _commit_version | _commit_timestamp       | birthday   |",
             "+----+--------+------------------+-----------------+-------------------------+------------+",
             "| 7  | Dennis | delete           | 3               | 2024-01-06T16:44:59.570 | 2023-12-29 |",
             "| 3  | Dave   | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |",
             "| 4  | Kate   | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |",
             "| 2  | Bob    | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |",
             "| 7  | Dennis | update_preimage  | 2               | 2023-12-29T21:41:33.785 | 2023-12-24 |",
             "| 5  | Emily  | update_preimage  | 2               | 2023-12-29T21:41:33.785 | 2023-12-24 |",
             "| 6  | Carl   | update_preimage  | 2               | 2023-12-29T21:41:33.785 | 2023-12-24 |",
             "| 7  | Dennis | update_postimage | 2               | 2023-12-29T21:41:33.785 | 2023-12-29 |",
             "| 5  | Emily  | update_postimage | 2               | 2023-12-29T21:41:33.785 | 2023-12-29 |",
             "| 6  | Carl   | update_postimage | 2               | 2023-12-29T21:41:33.785 | 2023-12-29 |",
             "| 3  | Dave   | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |",
             "| 4  | Kate   | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |",
             "| 2  | Bob    | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |",
             "| 2  | Bob    | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |",
             "| 3  | Dave   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |",
             "| 4  | Kate   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |",
             "| 5  | Emily  | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |",
             "| 6  | Carl   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |",
             "| 7  | Dennis | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |",
             "| 1  | Steve  | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-22 |",
             "| 8  | Claire | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |",
             "| 9  | Ada    | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |",
             "| 10 | Borb   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |",
             "+----+--------+------------------+-----------------+-------------------------+------------+"
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
            .with_session_ctx(ctx.clone())
            .with_ending_timestamp(starting_timestamp.and_utc())
            .build()
            .await?;

        let batches = collect_batches(
            table.properties().output_partitioning().partition_count(),
            table,
            ctx,
        )
        .await?;

        assert_batches_sorted_eq! {
            ["+----+--------+------------------+-----------------+-------------------------+------------+",
             "| id | name   | _change_type     | _commit_version | _commit_timestamp       | birthday   |",
             "+----+--------+------------------+-----------------+-------------------------+------------+",
             "| 3  | Dave   | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |",
             "| 4  | Kate   | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |",
             "| 2  | Bob    | update_preimage  | 1               | 2023-12-22T17:10:21.675 | 2023-12-23 |",
             "| 3  | Dave   | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |",
             "| 4  | Kate   | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |",
             "| 2  | Bob    | update_postimage | 1               | 2023-12-22T17:10:21.675 | 2023-12-22 |",
             "| 2  | Bob    | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |",
             "| 3  | Dave   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |",
             "| 4  | Kate   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-23 |",
             "| 8  | Claire | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |",
             "| 9  | Ada    | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |",
             "| 10 | Borb   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-25 |",
             "| 1  | Steve  | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-22 |",
             "| 5  | Emily  | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |",
             "| 6  | Carl   | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |",
             "| 7  | Dennis | insert           | 0               | 2023-12-22T17:10:18.828 | 2023-12-24 |",
             "+----+--------+------------------+-----------------+-------------------------+------------+"
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
            .with_session_ctx(ctx.clone())
            .with_starting_version(0)
            .build()
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
        let table = DeltaOps::try_from_uri("../test/tests/data/cdf-table-non-partitioned")
            .await?
            .load_cdf()
            .with_starting_version(4)
            .with_ending_version(1)
            .build()
            .await;

        assert!(table.is_err());
        assert!(matches!(
            table.unwrap_err(),
            DeltaTableError::ChangeDataInvalidVersionRange { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_load_non_cdf() -> TestResult {
        let table = DeltaOps::try_from_uri("../test/tests/data/simple_table")
            .await?
            .load_cdf()
            .with_starting_version(0)
            .build()
            .await;

        assert!(table.is_err());
        assert!(matches!(
            table.unwrap_err(),
            DeltaTableError::ChangeDataNotEnabled { .. }
        ));

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
        assert_eq!(table.version(), 0);

        let schema = Arc::new(Schema::try_from(delta_schema)?);

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
        assert_eq!(table.version(), 1);

        let table = DeltaOps(table)
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap();
        assert_eq!(table.version(), 2);

        let ctx = SessionContext::new();
        let cdf_scan = DeltaOps(table.clone())
            .load_cdf()
            .with_session_ctx(ctx.clone())
            .with_starting_version(0)
            .build()
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
        let _: Vec<_> = batches.iter_mut().map(|b| b.remove_column(4)).collect();

        assert_batches_sorted_eq! {[
        "+-------+----------+--------------+-----------------+----+",
        "| value | modified | _change_type | _commit_version | id |",
        "+-------+----------+--------------+-----------------+----+",
        "| 1     | yes      | delete       | 2               | 1  |",
        "| 1     | yes      | insert       | 1               | 1  |",
        "| 10    | yes      | insert       | 2               | 3  |",
        "| 2     | yes      | delete       | 2               | 2  |",
        "| 2     | yes      | insert       | 1               | 2  |",
        "| 3     | no       | delete       | 2               | 3  |",
        "| 3     | no       | insert       | 1               | 3  |",
        "+-------+----------+--------------+-----------------+----+",
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
