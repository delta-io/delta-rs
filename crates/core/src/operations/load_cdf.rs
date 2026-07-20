//! Module for reading the change datafeed of delta tables
//!
//! # Example
//! ```rust ignore
//! let table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
//! let builder = CdfLoadBuilder::new(table.log_store(), table.snapshot())
//!     .with_starting_version(3);
//!
//! let ctx = SessionContext::new();
//! let provider = DeltaCdfTableProvider::try_new(builder)?;
//! let df = ctx.read_table(provider).await?;

use std::sync::Arc;
use std::time::SystemTime;

use arrow_schema::{ArrowError, Field, Schema};
use chrono::{DateTime, Utc};
use datafusion::catalog::Session;
use datafusion::common::DFSchema;
use datafusion::common::ScalarValue;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{PhysicalExpr, expressions};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use delta_kernel::table_features::ColumnMappingMode;
use tracing::log;

use crate::DeltaTableError;
use crate::delta_datafusion::{
    DataFusionMixins, DeltaSessionExt, extract_partition_only_predicate,
};
use crate::errors::{ColumnMappingOperation, DeltaResult};
use crate::kernel::transaction::PROTOCOL;
use crate::kernel::{
    Action, Add, AddCDCFile, CommitInfo, EagerSnapshot, Version, resolve_snapshot,
};
use crate::logstore::{LogStoreRef, get_actions};
use crate::{delta_datafusion::cdf::*, kernel::Remove};

/// Builder for create a read of change data feeds for delta tables
#[derive(Clone)]
pub struct CdfLoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    pub(crate) snapshot: Option<EagerSnapshot>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Version to read from
    starting_version: Option<Version>,
    /// Version to stop reading at
    ending_version: Option<Version>,
    /// Starting timestamp of commits to accept
    starting_timestamp: Option<DateTime<Utc>>,
    /// Ending timestamp of commits to accept
    ending_timestamp: Option<DateTime<Utc>>,
    /// Enable ending version or timestamp exceeding the last commit
    allow_out_of_range: bool,
    /// Datafusion session state relevant for executing the input plan
    session: Option<Arc<dyn Session>>,
    /// Optional logical predicate used ONLY to prune files by their partition
    /// values. This is never applied as a row-level filter, so any non-partition
    /// conjuncts are ignored here and row-level correctness must be enforced by a
    /// separate `FilterExec` wrapped around the resulting plan.
    filter: Option<Expr>,
}

impl std::fmt::Debug for CdfLoadBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdfLoadBuilder")
            .field("snapshot", &self.snapshot)
            .field("log_store", &self.log_store)
            .field("starting_version", &self.starting_version)
            .field("ending_version", &self.ending_version)
            .field("starting_timestamp", &self.starting_timestamp)
            .field("ending_timestamp", &self.ending_timestamp)
            .field("allow_out_of_range", &self.allow_out_of_range)
            .finish()
    }
}

impl CdfLoadBuilder {
    /// Create a new [`CdfLoadBuilder`]
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            starting_version: None,
            ending_version: None,
            starting_timestamp: None,
            ending_timestamp: None,
            allow_out_of_range: false,
            session: None,
            filter: None,
        }
    }

    /// Version to start at (version 0 if not provided)
    pub fn with_starting_version(mut self, starting_version: Version) -> Self {
        self.starting_version = Some(starting_version);
        self
    }

    /// Version (inclusive) to end at
    pub fn with_ending_version(mut self, ending_version: Version) -> Self {
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

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, session: Arc<dyn Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// A logical predicate used ONLY to prune files by their partition values.
    ///
    /// This does NOT apply a row-level filter: only the conjuncts that reference
    /// partition columns exclusively are used, and they merely decide which files
    /// to scan. Any data-column predicate is ignored for correctness purposes, so
    /// callers that need the rows actually filtered must wrap the resulting plan in
    /// a `FilterExec` (as `DeltaCdfTableProvider::scan` does). It is `pub(crate)`
    /// to prevent external callers from mistaking it for a row-level filter.
    pub(crate) fn with_partition_pruning_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    #[inline]
    fn timestamp_in_range(action: &Action, ts: DateTime<Utc>) -> bool {
        matches!(action, Action::CommitInfo(CommitInfo { in_commit_timestamp: Some(t), .. }) if ts.timestamp_millis() <= *t)
            || matches!(action, Action::CommitInfo(CommitInfo { timestamp: Some(t), .. }) if ts.timestamp_millis() <= *t)
    }

    async fn calculate_earliest_version(&self, snapshot: &EagerSnapshot) -> DeltaResult<Version> {
        let ts = self.starting_timestamp.unwrap_or(DateTime::UNIX_EPOCH);
        for v in 0..snapshot.version() {
            if let Ok(Some(bytes)) = self.log_store.read_commit_entry(v).await
                && let Ok(actions) = get_actions(v, &bytes)
                && actions
                    .iter()
                    .any(|action| Self::timestamp_in_range(action, ts))
            {
                return Ok(v);
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
        snapshot: &EagerSnapshot,
        partition_pruning: Option<&PartitionPruningPredicate>,
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
            self.calculate_earliest_version(snapshot).await?
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
            get_actions(latest_version, &latest_snapshot_bytes)?;
        let latest_version_commit = latest_version_actions
            .iter()
            .find(|a| matches!(a, Action::CommitInfo(_)));

        if let Some(Action::CommitInfo(CommitInfo {
            timestamp: Some(latest_timestamp),
            ..
        })) = latest_version_commit
            && starting_timestamp.timestamp_millis() > *latest_timestamp
        {
            return if self.allow_out_of_range {
                Ok((change_files, add_files, remove_files))
            } else {
                Err(DeltaTableError::ChangeDataTimestampGreaterThanCommit { ending_timestamp })
            };
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

            let version_actions: Vec<Action> = get_actions(version, &snapshot_bytes?)?;

            let mut ts = 0;
            let mut cdc_actions = vec![];

            if self.starting_timestamp.is_some() || self.ending_timestamp.is_some() {
                // TODO: fallback on other actions for timestamps because CommitInfo action is optional
                // theoretically.
                let version_commit = version_actions
                    .iter()
                    .find(|a| matches!(a, Action::CommitInfo(_)));

                match version_commit {
                    Some(Action::CommitInfo(CommitInfo {
                        in_commit_timestamp: Some(t),
                        ..
                    })) if starting_timestamp.timestamp_millis() > *t
                        || *t > ending_timestamp.timestamp_millis() =>
                    {
                        log::debug!("Version: {version} skipped, due to in commit timestamp");
                        continue;
                    }
                    Some(Action::CommitInfo(CommitInfo {
                        timestamp: Some(t), ..
                    })) if starting_timestamp.timestamp_millis() > *t
                        || *t > ending_timestamp.timestamp_millis() =>
                    {
                        log::debug!("Version: {version} skipped, due to commit timestamp");
                        continue;
                    }
                    _ => {}
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
                        ts = ci.in_commit_timestamp.or(ci.timestamp).unwrap_or(0);
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

        if let Some(partition_pruning) = partition_pruning {
            change_files = prune_specs_by_partition(change_files, partition_pruning)?;
            add_files = prune_specs_by_partition(add_files, partition_pruning)?;
            remove_files = prune_specs_by_partition(remove_files, partition_pruning)?;
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

    /// Extract the partition-only conjuncts of [`Self::filter`] and compile them
    /// into a physical predicate that can be evaluated against a file's
    /// `partitionValues`. Returns `None` when there is no filter, no partition
    /// columns, or no conjunct restricted to partition columns -- in which case
    /// the file set is left untouched and correctness still relies on the
    /// row-level `FilterExec`.
    ///
    /// Pruning is purely an optimization, so it **fails open**: if simplifying
    /// the predicate, building the partition schema, or compiling the physical
    /// expression fails for any reason, this logs at debug and returns `Ok(None)`
    /// rather than propagating the error. A pruning failure must never change the
    /// query result or error a read that would otherwise succeed.
    fn partition_pruning_predicate(
        &self,
        session: &dyn Session,
        schema: &arrow_schema::SchemaRef,
        partition_columns: &[String],
    ) -> DeltaResult<Option<PartitionPruningPredicate>> {
        let Some(filter) = self.filter.clone() else {
            return Ok(None);
        };
        if partition_columns.is_empty() {
            return Ok(None);
        }

        match self.try_build_partition_pruning_predicate(session, schema, partition_columns, filter)
        {
            Ok(predicate) => Ok(predicate),
            Err(e) => {
                // Fail open: pruning is an optimization. Keep every file and let
                // the row-level FilterExec enforce correctness.
                log::debug!(
                    "load_cdf: skipping partition pruning, failed to build pruning predicate: {e}"
                );
                Ok(None)
            }
        }
    }

    /// Fallible inner builder for [`Self::partition_pruning_predicate`]. Any error
    /// returned here is swallowed by the caller (pruning falls open), so it is
    /// safe for the steps below to propagate errors with `?`.
    fn try_build_partition_pruning_predicate(
        &self,
        session: &dyn Session,
        schema: &arrow_schema::SchemaRef,
        partition_columns: &[String],
        filter: Expr,
    ) -> DeltaResult<Option<PartitionPruningPredicate>> {
        let Some((partition_predicate, referenced_columns)) =
            extract_partition_only_predicate(filter, partition_columns)?
        else {
            return Ok(None);
        };

        // Materialize only the partition columns the predicate actually references.
        // Coercing every partition column would waste work on columns the predicate
        // never reads, and a malformed/missing value in an unreferenced column would
        // make the file fail open and silently weaken pruning.
        let partition_fields = referenced_columns
            .iter()
            .map(|name| schema.field_with_name(name).cloned())
            .collect::<Result<Vec<_>, ArrowError>>()?;
        let partition_schema: arrow_schema::SchemaRef = Arc::new(Schema::new(partition_fields));
        let df_schema: DFSchema = partition_schema.as_ref().clone().try_into()?;
        let predicate = session.create_physical_expr(partition_predicate, &df_schema)?;

        Ok(Some(PartitionPruningPredicate {
            predicate,
            partition_schema,
            table_schema: schema.clone(),
        }))
    }

    /// Executes the scan
    pub async fn build(
        &self,
        session: &dyn Session,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> DeltaResult<Arc<dyn ExecutionPlan>> {
        let snapshot = resolve_snapshot(&self.log_store, self.snapshot.clone(), true, None).await?;
        PROTOCOL.can_read_from(&snapshot)?;
        if snapshot.table_configuration().column_mapping_mode() != ColumnMappingMode::None {
            return Err(DeltaTableError::unsupported_column_mapping(
                ColumnMappingOperation::Read,
                "CHANGE DATA FEED scans",
            ));
        }

        let partition_values = snapshot.metadata().partition_columns();
        let schema = snapshot.input_schema();

        let partition_pruning =
            self.partition_pruning_predicate(session, &schema, partition_values)?;
        let (cdc, add, remove) = self
            .determine_files_to_read(&snapshot, partition_pruning.as_ref())
            .await?;
        session.ensure_log_store_registered(self.log_store.as_ref())?;

        let schema_fields: Vec<Arc<Field>> = schema
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
        let cdc_file_groups = create_partition_values(schema.clone(), cdc, partition_values, None)?;
        let add_file_groups = create_partition_values(
            schema.clone(),
            add,
            partition_values,
            Self::get_add_action_type(),
        )?;
        let remove_file_groups = create_partition_values(
            schema.clone(),
            remove,
            partition_values,
            Self::get_remove_action_type(),
        )?;

        let cdc_partition_fields: Vec<Arc<Field>> =
            cdc_partition_cols.into_iter().map(Arc::new).collect();
        let add_remove_partition_fields: Vec<Arc<Field>> = add_remove_partition_cols
            .into_iter()
            .map(Arc::new)
            .collect();

        let cdc_table_schema = TableSchema::new(Arc::clone(&cdc_file_schema), cdc_partition_fields);
        let add_table_schema = TableSchema::new(
            Arc::clone(&add_remove_file_schema),
            add_remove_partition_fields.clone(),
        );
        let remove_table_schema = TableSchema::new(
            Arc::clone(&add_remove_file_schema),
            add_remove_partition_fields,
        );

        let parquet_options = TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let mut cdc_source = ParquetSource::new(cdc_table_schema)
            .with_table_parquet_options(parquet_options.clone());
        let mut add_source = ParquetSource::new(add_table_schema)
            .with_table_parquet_options(parquet_options.clone());
        let mut remove_source =
            ParquetSource::new(remove_table_schema).with_table_parquet_options(parquet_options);

        if let Some(filters) = filters {
            cdc_source = cdc_source.with_predicate(Arc::clone(filters));
            add_source = add_source.with_predicate(Arc::clone(filters));
            remove_source = remove_source.with_predicate(Arc::clone(filters));
        }

        let cdc_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(self.log_store.object_store_url(), Arc::new(cdc_source))
                .with_file_groups(cdc_file_groups.into_values().map(FileGroup::from).collect())
                .build(),
        );

        let add_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(self.log_store.object_store_url(), Arc::new(add_source))
                .with_file_groups(add_file_groups.into_values().map(FileGroup::from).collect())
                .build(),
        );

        let remove_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(self.log_store.object_store_url(), Arc::new(remove_source))
                .with_file_groups(
                    remove_file_groups
                        .into_values()
                        .map(FileGroup::from)
                        .collect(),
                )
                .build(),
        );

        let union_scan = UnionExec::try_new(vec![cdc_scan, add_scan, remove_scan])?;

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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::str::FromStr;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::Schema;
    use chrono::NaiveDateTime;
    use datafusion::common::assert_batches_sorted_eq;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use itertools::Itertools;

    use crate::delta_datafusion::cdf::scan::DeltaCdfTableProvider;
    use crate::test_utils::TestSchemas;
    use crate::writer::test_utils::TestResult;
    use crate::{DeltaTable, TableProperty};
    use std::path::Path;
    use url::Url;

    /// Regression test for partition pruning in `load_cdf`.
    ///
    /// The cdf-table fixture is partitioned by `birthday`. A WHERE clause on `birthday`
    /// should narrow the physical plan's file_groups to only the matching partition.
    /// Until partition pruning is implemented, `determine_files_to_read` enumerates every
    /// Add/AddCDCFile/Remove action in the version range without consulting partition
    /// predicates, so all partitions' files end up in the plan and only a row-level
    /// FilterExec rescues correctness.
    #[tokio::test]
    async fn cdf_partition_predicate_prunes_file_groups() -> TestResult {
        let ctx: SessionContext = SessionContext::new();
        let table_path = Path::new("../test/tests/data/cdf-table");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri).await?;

        let builder = table.scan_cdf().with_starting_version(0);
        let provider = DeltaCdfTableProvider::try_new(builder)?;
        ctx.register_table("cdf", Arc::new(provider))?;

        let df = ctx
            .sql("SELECT * FROM cdf WHERE birthday = DATE '2023-12-25'")
            .await?;
        let plan = df.create_physical_plan().await?;
        let txt = displayable(plan.as_ref()).indent(true).to_string();

        let mut paths: Vec<&str> = txt
            .split(|c: char| c == ',' || c == '[' || c == ']' || c.is_whitespace())
            .filter(|s| s.contains("part-") && s.ends_with(".parquet"))
            .collect();
        paths.sort();
        paths.dedup();
        let non_matching: Vec<&&str> = paths
            .iter()
            .filter(|p| !p.contains("birthday=2023-12-25"))
            .collect();

        assert!(
            non_matching.is_empty(),
            "load_cdf should prune non-matching partitions, but found {} file(s) from other partitions:\n{}\n\nFull plan:\n{}",
            non_matching.len(),
            non_matching
                .iter()
                .map(|p| format!("  {p}"))
                .collect::<Vec<_>>()
                .join("\n"),
            txt,
        );
        Ok(())
    }

    /// Pruning files by partition value must not drop any matching change rows.
    #[tokio::test]
    async fn cdf_partition_predicate_keeps_matching_rows() -> TestResult {
        let ctx: SessionContext = SessionContext::new();
        let table_path = Path::new("../test/tests/data/cdf-table");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri).await?;

        let provider = DeltaCdfTableProvider::try_new(table.scan_cdf().with_starting_version(0))?;
        ctx.register_table("cdf", Arc::new(provider))?;

        let batches = ctx
            .sql("SELECT id, name, birthday, _change_type FROM cdf WHERE birthday = DATE '2023-12-25'")
            .await?
            .collect()
            .await?;

        assert_batches_sorted_eq! {
        [
            "+----+--------+------------+--------------+",
            "| id | name   | birthday   | _change_type |",
            "+----+--------+------------+--------------+",
            "| 10 | Borb   | 2023-12-25 | insert       |",
            "| 8  | Claire | 2023-12-25 | insert       |",
            "| 9  | Ada    | 2023-12-25 | insert       |",
            "+----+--------+------------+--------------+",
        ], &batches }
        Ok(())
    }

    /// Open the `cdf-table` fixture (partitioned by `birthday`) and register it as
    /// a CDF table provider so partition pruning can be exercised through SQL.
    async fn register_cdf_table(ctx: &SessionContext) -> TestResult {
        let table_path = Path::new("../test/tests/data/cdf-table");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri).await?;
        let provider = DeltaCdfTableProvider::try_new(table.scan_cdf().with_starting_version(0))?;
        ctx.register_table("cdf", Arc::new(provider))?;
        Ok(())
    }

    async fn copied_column_mapping_cdf_table()
    -> Result<(tempfile::TempDir, DeltaTable), Box<dyn std::error::Error + 'static>> {
        let fixture = Path::new("../test/tests/data/table_with_column_mapping");
        let temp_dir = tempfile::tempdir()?;
        fs_extra::dir::copy(fixture, temp_dir.path(), &Default::default())?;
        let table_path = temp_dir.path().join("table_with_column_mapping");
        let log_path = table_path.join("_delta_log/00000000000000000000.json");
        let log = std::fs::read_to_string(&log_path)?;
        let updated_log = log.replace(
            "\"delta.columnMapping.mode\":\"name\"",
            "\"delta.enableChangeDataFeed\":\"true\",\"delta.columnMapping.mode\":\"name\"",
        );
        assert_ne!(
            log, updated_log,
            "expected column mapping table fixture metadata to contain column mapping mode"
        );
        std::fs::write(log_path, updated_log)?;

        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri).await?;
        Ok((temp_dir, table))
    }

    #[tokio::test]
    async fn cdf_scan_rejects_column_mapping_tables() -> TestResult {
        let ctx: SessionContext = SessionContext::new();
        let (_temp_dir, table) = copied_column_mapping_cdf_table().await?;

        let err = table
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect_err("cdf scan should reject column-mapped tables before planning files");

        match err {
            DeltaTableError::UnsupportedColumnMapping {
                mode: _,
                operation: _,
            } => {}
            _ => panic!("Unexpected error: {err:?}"),
        }

        Ok(())
    }

    /// Collect the distinct `birthday=YYYY-MM-DD` partition segments referenced by
    /// every parquet file (both `part-` add/remove files and `cdc-` change files)
    /// in a rendered physical plan. This reflects exactly which partitions survived
    /// file-group pruning.
    fn partitions_in_plan(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let txt = displayable(plan.as_ref()).indent(true).to_string();
        let mut parts: Vec<String> = txt
            .split(|c: char| c == ',' || c == '[' || c == ']' || c.is_whitespace() || c == '/')
            .filter(|s| s.starts_with("birthday="))
            .map(|s| s.to_string())
            .collect();
        parts.sort();
        parts.dedup();
        parts
    }

    /// Build the physical plan for `query` against the registered `cdf` table and
    /// return the distinct partitions remaining in its file groups.
    async fn pruned_partitions(ctx: &SessionContext, query: &str) -> DeltaResult<Vec<String>> {
        let plan = ctx.sql(query).await?.create_physical_plan().await?;
        Ok(partitions_in_plan(&plan))
    }

    /// Equality on the partition column should keep only the `2023-12-23` partition,
    /// which is materialized as CDC files (`_change_data/birthday=2023-12-23/...`)
    /// for the update at version 1 in addition to the original add at version 0.
    #[tokio::test]
    async fn cdf_partition_predicate_prunes_to_cdc_partition() -> TestResult {
        let ctx = SessionContext::new();
        register_cdf_table(&ctx).await?;

        let partitions =
            pruned_partitions(&ctx, "SELECT * FROM cdf WHERE birthday = DATE '2023-12-23'").await?;
        assert_eq!(
            partitions,
            vec!["birthday=2023-12-23".to_string()],
            "equality on partition column must prune to the single matching CDC partition",
        );

        // Results must still be correct: the 2023-12-23 partition carries the
        // version-0 inserts and the version-1 update preimages.
        let batches = ctx
            .sql("SELECT id, name, birthday, _change_type FROM cdf WHERE birthday = DATE '2023-12-23'")
            .await?
            .collect()
            .await?;
        assert_batches_sorted_eq! {
        [
            "+----+------+------------+-----------------+",
            "| id | name | birthday   | _change_type    |",
            "+----+------+------------+-----------------+",
            "| 2  | Bob  | 2023-12-23 | insert          |",
            "| 2  | Bob  | 2023-12-23 | update_preimage |",
            "| 3  | Dave | 2023-12-23 | insert          |",
            "| 3  | Dave | 2023-12-23 | update_preimage |",
            "| 4  | Kate | 2023-12-23 | insert          |",
            "| 4  | Kate | 2023-12-23 | update_preimage |",
            "+----+------+------------+-----------------+",
        ], &batches }
        Ok(())
    }

    /// The `2023-12-29` partition exercises the Remove-action fallback: version 3
    /// deletes id 7 via a `Remove` action (the partition also has version-2 adds
    /// and CDC files). Pruning to it must keep that Remove file so the `delete`
    /// change row is still produced.
    #[tokio::test]
    async fn cdf_partition_predicate_prunes_to_remove_fallback_partition() -> TestResult {
        let ctx = SessionContext::new();
        register_cdf_table(&ctx).await?;

        let partitions =
            pruned_partitions(&ctx, "SELECT * FROM cdf WHERE birthday = DATE '2023-12-29'").await?;
        assert_eq!(
            partitions,
            vec!["birthday=2023-12-29".to_string()],
            "equality on partition column must prune to the single matching partition, \
             including its Remove-action file",
        );

        // The delete (sourced from the version-3 Remove action) must survive.
        let batches = ctx
            .sql("SELECT id, name, birthday, _change_type FROM cdf WHERE birthday = DATE '2023-12-29'")
            .await?
            .collect()
            .await?;
        assert_batches_sorted_eq! {
        [
            "+----+--------+------------+------------------+",
            "| id | name   | birthday   | _change_type     |",
            "+----+--------+------------+------------------+",
            "| 5  | Emily  | 2023-12-29 | update_postimage |",
            "| 6  | Carl   | 2023-12-29 | update_postimage |",
            "| 7  | Dennis | 2023-12-29 | delete           |",
            "| 7  | Dennis | 2023-12-29 | update_postimage |",
            "+----+--------+------------+------------------+",
        ], &batches }
        Ok(())
    }

    /// `IS NULL` on the partition column matches no partition (every partition has a
    /// concrete `birthday`), so all files should be pruned away and no rows returned.
    #[tokio::test]
    async fn cdf_partition_predicate_is_null_prunes_everything() -> TestResult {
        let ctx = SessionContext::new();
        register_cdf_table(&ctx).await?;

        let partitions =
            pruned_partitions(&ctx, "SELECT * FROM cdf WHERE birthday IS NULL").await?;
        assert!(
            partitions.is_empty(),
            "IS NULL on a fully-populated partition column must prune every partition, got {partitions:?}",
        );

        let batches = ctx
            .sql("SELECT * FROM cdf WHERE birthday IS NULL")
            .await?
            .collect()
            .await?;
        assert!(
            batches.iter().all(|b| b.num_rows() == 0),
            "IS NULL must return no rows",
        );
        Ok(())
    }

    /// `IN (...)` over partition values should keep exactly the listed partitions.
    #[tokio::test]
    async fn cdf_partition_predicate_in_list_prunes_to_listed_partitions() -> TestResult {
        let ctx = SessionContext::new();
        register_cdf_table(&ctx).await?;

        let partitions = pruned_partitions(
            &ctx,
            "SELECT * FROM cdf WHERE birthday IN (DATE '2023-12-22', DATE '2023-12-25')",
        )
        .await?;
        assert_eq!(
            partitions,
            vec![
                "birthday=2023-12-22".to_string(),
                "birthday=2023-12-25".to_string(),
            ],
            "IN list must keep exactly the listed partitions",
        );

        let batches = ctx
            .sql("SELECT id, name, birthday, _change_type FROM cdf WHERE birthday IN (DATE '2023-12-22', DATE '2023-12-25')")
            .await?
            .collect()
            .await?;
        assert_batches_sorted_eq! {
        [
            "+----+--------+------------+------------------+",
            "| id | name   | birthday   | _change_type     |",
            "+----+--------+------------+------------------+",
            "| 1  | Steve  | 2023-12-22 | insert           |",
            "| 10 | Borb   | 2023-12-25 | insert           |",
            "| 2  | Bob    | 2023-12-22 | update_postimage |",
            "| 3  | Dave   | 2023-12-22 | update_postimage |",
            "| 4  | Kate   | 2023-12-22 | update_postimage |",
            "| 8  | Claire | 2023-12-25 | insert           |",
            "| 9  | Ada    | 2023-12-25 | insert           |",
            "+----+--------+------------+------------------+",
        ], &batches }
        Ok(())
    }

    /// An `OR` of two partition equalities should keep both matching partitions.
    #[tokio::test]
    async fn cdf_partition_predicate_or_keeps_both_partitions() -> TestResult {
        let ctx = SessionContext::new();
        register_cdf_table(&ctx).await?;

        let partitions = pruned_partitions(
            &ctx,
            "SELECT * FROM cdf WHERE birthday = DATE '2023-12-22' OR birthday = DATE '2023-12-25'",
        )
        .await?;
        assert_eq!(
            partitions,
            vec![
                "birthday=2023-12-22".to_string(),
                "birthday=2023-12-25".to_string(),
            ],
            "OR of partition equalities must keep both partitions",
        );
        Ok(())
    }

    /// `NOT (birthday = ...)` should keep every partition except the negated one.
    #[tokio::test]
    async fn cdf_partition_predicate_not_excludes_partition() -> TestResult {
        let ctx = SessionContext::new();
        register_cdf_table(&ctx).await?;

        let partitions = pruned_partitions(
            &ctx,
            "SELECT * FROM cdf WHERE NOT (birthday = DATE '2023-12-22')",
        )
        .await?;
        assert_eq!(
            partitions,
            vec![
                "birthday=2023-12-23".to_string(),
                "birthday=2023-12-24".to_string(),
                "birthday=2023-12-25".to_string(),
                "birthday=2023-12-29".to_string(),
            ],
            "NOT equality must keep every partition except the excluded one",
        );
        assert!(
            !partitions.contains(&"birthday=2023-12-22".to_string()),
            "the negated partition must be pruned away",
        );
        Ok(())
    }

    /// A range predicate (`>`/`<`) should keep only partitions inside the range.
    #[tokio::test]
    async fn cdf_partition_predicate_range_prunes_outside_bounds() -> TestResult {
        let ctx = SessionContext::new();
        register_cdf_table(&ctx).await?;

        // 2023-12-23 < birthday < 2023-12-29 leaves only 2023-12-24/25.
        let partitions = pruned_partitions(
            &ctx,
            "SELECT * FROM cdf WHERE birthday > DATE '2023-12-23' AND birthday < DATE '2023-12-29'",
        )
        .await?;
        assert_eq!(
            partitions,
            vec![
                "birthday=2023-12-24".to_string(),
                "birthday=2023-12-25".to_string(),
            ],
            "range predicate must keep only partitions strictly inside the bounds",
        );
        Ok(())
    }

    /// A predicate mixing a partition column and a data column must only prune on
    /// the partition part. The data conjunct (`id > 5`) is NOT a pruning criterion,
    /// so the partition must still be kept whole and correctness is left to the
    /// row-level `FilterExec`.
    #[tokio::test]
    async fn cdf_mixed_partition_and_data_predicate_only_prunes_partition() -> TestResult {
        let ctx = SessionContext::new();
        register_cdf_table(&ctx).await?;

        let partitions = pruned_partitions(
            &ctx,
            "SELECT * FROM cdf WHERE birthday = DATE '2023-12-24' AND id > 5",
        )
        .await?;
        // Only the partition conjunct prunes; the whole 2023-12-24 partition is kept
        // and no other partition leaks in because of the data predicate.
        assert_eq!(
            partitions,
            vec!["birthday=2023-12-24".to_string()],
            "mixed predicate must prune on the partition part only, keeping the matching partition whole",
        );

        // The data part is still applied by the row-level filter, so only id > 5 rows
        // from the 2023-12-24 partition are returned.
        let batches = ctx
            .sql("SELECT id, name, birthday, _change_type FROM cdf WHERE birthday = DATE '2023-12-24' AND id > 5")
            .await?
            .collect()
            .await?;
        assert_batches_sorted_eq! {
        [
            "+----+--------+------------+-----------------+",
            "| id | name   | birthday   | _change_type    |",
            "+----+--------+------------+-----------------+",
            "| 6  | Carl   | 2023-12-24 | insert          |",
            "| 6  | Carl   | 2023-12-24 | update_preimage |",
            "| 7  | Dennis | 2023-12-24 | insert          |",
            "| 7  | Dennis | 2023-12-24 | update_preimage |",
            "+----+--------+------------+-----------------+",
        ], &batches }
        Ok(())
    }

    /// A predicate on a non-partition column must not prune any files; the
    /// matching is left to the row-level filter, so every partition's files
    /// still appear in the plan.
    #[tokio::test]
    async fn cdf_non_partition_predicate_does_not_prune() -> TestResult {
        let ctx: SessionContext = SessionContext::new();
        let table_path = Path::new("../test/tests/data/cdf-table");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri).await?;

        let provider = DeltaCdfTableProvider::try_new(table.scan_cdf().with_starting_version(0))?;
        ctx.register_table("cdf", Arc::new(provider))?;

        let collect_paths = |txt: &str| -> Vec<String> {
            let mut paths: Vec<String> = txt
                .split(|c: char| c == ',' || c == '[' || c == ']' || c.is_whitespace())
                .filter(|s| s.contains("part-") && s.ends_with(".parquet"))
                .map(|s| s.to_string())
                .collect();
            paths.sort();
            paths.dedup();
            paths
        };

        let baseline_plan = ctx
            .sql("SELECT * FROM cdf")
            .await?
            .create_physical_plan()
            .await?;
        let baseline_paths =
            collect_paths(&displayable(baseline_plan.as_ref()).indent(true).to_string());

        let filtered_plan = ctx
            .sql("SELECT * FROM cdf WHERE id > 5")
            .await?
            .create_physical_plan()
            .await?;
        let filtered_paths =
            collect_paths(&displayable(filtered_plan.as_ref()).indent(true).to_string());

        assert_eq!(
            baseline_paths, filtered_paths,
            "non-partition predicate must not prune files"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_respect_ict() -> TestResult {
        let ctx: SessionContext = SessionContext::new();
        let table_path = Path::new("../test/tests/data/cdc_ict_table");
        let starting_timestamp = NaiveDateTime::from_str("2026-07-11T16:36:53.881")?;
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
            // .with_starting_version(0)
            .with_starting_timestamp(starting_timestamp.and_utc())
            .build(&ctx.state(), None)
            .await?;

        let batches = collect(table, ctx.task_ctx()).await?;
        assert_batches_sorted_eq! {
                            [
            "+-------+-----+-----------+------------------+-----------------+-------------------------+",
            "| name  | age | birthyear | _change_type     | _commit_version | _commit_timestamp       |",
            "+-------+-----+-----------+------------------+-----------------+-------------------------+",
            "| Dan   | 14  | 1995      | delete           | 2               | 2026-07-12T16:36:52.175 |",
            "| Dave  | 22  | 1995      | delete           | 2               | 2026-07-12T16:36:52.175 |",
            "| Kate  | 36  | 1995      | update_preimage  | 3               | 2026-07-12T16:36:53.881 |",
            "| Kate  | 37  | 1995      | update_postimage | 3               | 2026-07-12T16:36:53.881 |",
            "| Steve | 40  | 1986      | update_preimage  | 3               | 2026-07-12T16:36:53.881 |",
            "| Steve | 41  | 1986      | update_postimage | 3               | 2026-07-12T16:36:53.881 |",
            "+-------+-----+-----------+------------------+-----------------+-------------------------+",
        ], &batches }
        Ok(())
    }

    #[tokio::test]
    async fn test_load_local() -> TestResult {
        let ctx: SessionContext = SessionContext::new();
        let table_path = Path::new("../test/tests/data/cdf-table");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await?;

        let batches = collect(table, ctx.task_ctx()).await?;
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
        let starting_timestamp = NaiveDateTime::from_str("2023-12-22T17:10:21.675")?;
        let table_path = Path::new("../test/tests/data/cdf-table");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
            .with_starting_version(0)
            .with_ending_timestamp(starting_timestamp.and_utc())
            .build(&ctx.state(), None)
            .await?;

        let batches = collect(table, ctx.task_ctx()).await?;

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
        let table_path = Path::new("../test/tests/data/cdf-table-non-partitioned");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await?;

        let batches = collect(table, ctx.task_ctx()).await?;

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
        let table_path = Path::new("../test/tests/data/cdf-table-non-partitioned");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
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
        let table_path = Path::new("../test/tests/data/cdf-table-non-partitioned");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
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
        let table_path = Path::new("../test/tests/data/cdf-table-non-partitioned");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
            .with_starting_version(5)
            .with_allow_out_of_range()
            .build(&ctx.state(), None)
            .await?;

        let batches = collect(table, ctx.task_ctx()).await?;

        assert!(batches.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_load_timestamp_out_of_range() -> TestResult {
        let ending_timestamp = NaiveDateTime::from_str("2033-12-22T17:10:21.675")?;
        let ctx = SessionContext::new();
        let table_path = Path::new("../test/tests/data/cdf-table-non-partitioned");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
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
        let ending_timestamp = NaiveDateTime::from_str("2033-12-22T17:10:21.675")?;
        let table_path = Path::new("../test/tests/data/cdf-table-non-partitioned");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
            .with_starting_timestamp(ending_timestamp.and_utc())
            .with_allow_out_of_range()
            .build(&ctx.state(), None)
            .await?;

        let batches = collect(table, ctx.task_ctx()).await?;

        assert!(batches.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_load_non_cdf() -> TestResult {
        let ctx = SessionContext::new();
        let table_path = Path::new("../test/tests/data/simple_table");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
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
        let table_path = Path::new("../test/tests/data/checkpoint-cdf-table");
        let table_uri = Url::from_directory_path(std::fs::canonicalize(table_path)?).unwrap();
        let table = DeltaTable::try_from_url(table_uri)
            .await?
            .scan_cdf()
            .with_starting_timestamp(ending_timestamp.and_utc())
            .build(&ctx.state(), None)
            .await?;

        let batches = collect(table, ctx.task_ctx()).await?;

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
        let table: DeltaTable = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .with_partition_columns(["id"])
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await?;
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
        )?;

        let second_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("3")])),
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("yes")])),
            ],
        )?;

        let table = table
            .write(vec![batch])
            .await
            .expect("Failed to write first batch");
        assert_eq!(table.version(), Some(1));

        let table = table
            .write([second_batch])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await?;
        assert_eq!(table.version(), Some(2));

        let ctx = SessionContext::new();
        let cdf_scan = table
            .clone()
            .scan_cdf()
            .with_starting_version(0)
            .build(&ctx.state(), None)
            .await
            .expect("Failed to load CDF");

        let mut batches = collect(cdf_scan, ctx.task_ctx())
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
        let version_actions = get_actions(2, &snapshot_bytes)?;

        let cdc_actions = version_actions
            .iter()
            .filter(|action| matches!(action, &&Action::Cdc(_)))
            .collect_vec();
        assert!(cdc_actions.is_empty());
        Ok(())
    }
}
