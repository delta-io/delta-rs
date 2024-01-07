use std::sync::Arc;

use arrow_schema::{ArrowError, DataType, Field, SchemaRef, TimeUnit};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{ScalarValue, Statistics};
use lazy_static::lazy_static;

use crate::delta_datafusion::cdf::scan_utils::*;
use crate::delta_datafusion::register_store;
use crate::kernel::{Action, Add, AddCDCFile, Remove};
use crate::logstore::{get_actions, LogStoreRef};
use crate::{DeltaResult, DeltaTableError};

use super::*;

pub const CHANGE_TYPE_COL: &str = "_change_type";
pub const COMMIT_VERSION_COL: &str = "_commit_version";
pub const COMMIT_TIMESTAMP_COL: &str = "_commit_timestamp";

lazy_static! {
    static ref CDC_PARTITION_SCHEMA: Vec<Field> = vec![
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true
        )
    ];
    static ref ADD_PARTITION_SCHEMA: Vec<Field> = vec![
        Field::new(CHANGE_TYPE_COL, DataType::Utf8, true),
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true
        ),
    ];
}

pub struct DeltaCdfScan {
    log_store: LogStoreRef,
    starting_version: i64,
    schema: SchemaRef,
    partition_values: Vec<String>,
}

impl DeltaCdfScan {
    pub fn new(
        log_store: LogStoreRef,
        starting_version: i64,
        schema: SchemaRef,
        partition_values: Vec<String>,
    ) -> Self {
        Self {
            log_store,
            starting_version,
            schema,
            partition_values,
        }
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
        let end = self.log_store.get_latest_version(start).await?;
        dbg!(start, end);
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
            let mut ts = None;
            let mut cdc_actions = vec![];
            for action in &version_actions {
                match action {
                    Action::Cdc(f) => cdc_actions.push(f.clone()),
                    Action::CommitInfo(ci) => {
                        ts = ci.timestamp;
                        commit_info.replace(ci);
                    }
                    _ => {}
                }
            }

            if !cdc_actions.is_empty() {
                change_files.push(CdcDataSpec {
                    version,
                    timestamp: ts.unwrap_or(0),
                    actions: cdc_actions,
                    commit_info: commit_info.cloned(),
                })
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
                    add_files.push(CdcDataSpec {
                        version,
                        timestamp: ts.unwrap_or(0),
                        actions: add_actions,
                        commit_info: commit_info.cloned(),
                    });
                }

                if !remove_actions.is_empty() {
                    remove_files.push(CdcDataSpec {
                        version,
                        timestamp: ts.unwrap_or(0),
                        actions: remove_actions,
                        commit_info: commit_info.cloned(),
                    });
                }
            }
        }

        Ok((change_files, add_files, remove_files))
    }

    fn get_add_action_type() -> Option<ScalarValue> {
        Some(ScalarValue::Utf8(Some(String::from("insert"))))
    }

    pub async fn scan(&self) -> DeltaResult<SendableRecordBatchStream> {
        let (cdc, add, _remove) = self.determine_files_to_read().await?;
        let ctx = SessionContext::new();
        register_store(self.log_store.clone(), ctx.state().runtime_env().clone());

        let schema_fields: Vec<Field> = self
            .schema
            .all_fields()
            .into_iter()
            .filter(|f| !self.partition_values.contains(f.name()))
            .cloned()
            .collect();

        let this_partition_values = self
            .partition_values
            .iter()
            .map(|name| self.schema.field_with_name(name).map(|f| f.to_owned()))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        // Setup for the Read Schemas of each kind of file, CDC files include commit action type so they need a slightly
        // different schema than standard add file reads
        let cdc_file_schema = create_cdc_schema(schema_fields.clone(), true);
        let add_file_schema = create_cdc_schema(schema_fields, false);

        // Setup the mapping of partition columns to be projected into the final output batch
        // cdc for example has timestamp, version, and any table partitions mapped here.
        // add on the other hand has action type, timestamp, version and any additional table partitions because adds do
        // not include their actions
        let mut cdc_partition_cols = CDC_PARTITION_SCHEMA.clone();
        let mut add_partition_cols = ADD_PARTITION_SCHEMA.clone();
        cdc_partition_cols.extend_from_slice(&this_partition_values);
        add_partition_cols.extend_from_slice(&this_partition_values);

        // Setup the partition to physical file mapping, this is a mostly unmodified version of what is done in load
        let cdc_file_groups =
            create_partition_values(self.schema.clone(), cdc, &self.partition_values, None)?;
        let add_file_groups = create_partition_values(
            self.schema.clone(),
            add,
            &self.partition_values,
            Self::get_add_action_type(),
        )?;

        // Create the parquet scans for each associated type of file. I am not sure when we would use removes yet, but
        // they would be here if / when they are necessary
        let cdc_scan = ParquetFormat::new()
            .create_physical_plan(
                &ctx.state(),
                FileScanConfig {
                    object_store_url: self.log_store.object_store_url(),
                    file_schema: cdc_file_schema.clone(),
                    file_groups: cdc_file_groups.into_values().collect(),
                    statistics: Statistics::new_unknown(&cdc_file_schema),
                    projection: None,
                    limit: None,
                    table_partition_cols: cdc_partition_cols.clone(),
                    output_ordering: vec![],
                    infinite_source: false,
                },
                None,
            )
            .await?;

        let add_scan = ParquetFormat::new()
            .create_physical_plan(
                &ctx.state(),
                FileScanConfig {
                    object_store_url: self.log_store.object_store_url(),
                    file_schema: add_file_schema.clone(),
                    file_groups: add_file_groups.into_values().collect(),
                    statistics: Statistics::new_unknown(&add_file_schema),
                    projection: None,
                    limit: None,
                    table_partition_cols: add_partition_cols.clone(),
                    output_ordering: vec![],
                    infinite_source: false,
                },
                None,
            )
            .await?;

        // The output batches are then unioned to create a single output. Coalesce partitions is only here for the time
        // being for development. I plan to parallelize the reads once the base idea is correct.
        let union_scan =
            CoalescePartitionsExec::new(Arc::new(UnionExec::new(vec![cdc_scan, add_scan])));
        let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
        Ok(union_scan.execute(0, task_ctx)?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use arrow_cast::pretty::print_batches;

    use crate::operations::collect_sendable_stream;
    use crate::writer::test_utils::TestResult;
    use crate::DeltaOps;

    use super::DeltaCdfScan;

    /**
    This is the output from a cdf read on the test table using spark. It's the reference point I used to work from.

    ```scala
        val table = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load("./cdf-table")
      table.show(truncate = false, numRows = 100)
    ```

    +---+------+----------+----------------+---------------+-----------------------+
    |id |name  |birthday  |_change_type    |_commit_version|_commit_timestamp      |
    +---+------+----------+----------------+---------------+-----------------------+
    |7  |Dennis|2023-12-29|update_postimage|2              |2023-12-29 16:41:33.843|
    |5  |Emily |2023-12-29|update_postimage|2              |2023-12-29 16:41:33.843|
    |7  |Dennis|2023-12-24|update_preimage |2              |2023-12-29 16:41:33.843|
    |5  |Emily |2023-12-24|update_preimage |2              |2023-12-29 16:41:33.843|
    |6  |Carl  |2023-12-29|update_postimage|2              |2023-12-29 16:41:33.843|
    |3  |Dave  |2023-12-22|update_postimage|1              |2023-12-22 12:10:21.688|
    |4  |Kate  |2023-12-22|update_postimage|1              |2023-12-22 12:10:21.688|
    |6  |Carl  |2023-12-24|update_preimage |2              |2023-12-29 16:41:33.843|
    |2  |Bob   |2023-12-22|update_postimage|1              |2023-12-22 12:10:21.688|
    |3  |Dave  |2023-12-23|update_preimage |1              |2023-12-22 12:10:21.688|
    |4  |Kate  |2023-12-23|update_preimage |1              |2023-12-22 12:10:21.688|
    |2  |Bob   |2023-12-23|update_preimage |1              |2023-12-22 12:10:21.688|
    |7  |Dennis|2023-12-29|delete          |3              |2024-01-06 11:44:59.624|
    |8  |Claire|2023-12-25|insert          |0              |2023-12-22 12:10:18.922|
    |7  |Dennis|2023-12-24|insert          |0              |2023-12-22 12:10:18.922|
    |1  |Steve |2023-12-22|insert          |0              |2023-12-22 12:10:18.922|
    |5  |Emily |2023-12-24|insert          |0              |2023-12-22 12:10:18.922|
    |3  |Dave  |2023-12-23|insert          |0              |2023-12-22 12:10:18.922|
    |4  |Kate  |2023-12-23|insert          |0              |2023-12-22 12:10:18.922|
    |10 |Borb  |2023-12-25|insert          |0              |2023-12-22 12:10:18.922|
    |6  |Carl  |2023-12-24|insert          |0              |2023-12-22 12:10:18.922|
    |2  |Bob   |2023-12-23|insert          |0              |2023-12-22 12:10:18.922|
    |9  |Ada   |2023-12-25|insert          |0              |2023-12-22 12:10:18.922|
    +---+------+----------+----------------+---------------+-----------------------+
    **/

    #[tokio::test]
    async fn test_load_local() -> TestResult {
        // I checked in a pre-built table from spark, once the writing side is finished I will circle back to make these
        // tests self contained and not rely on a physical table with them
        let _table = DeltaOps::try_from_uri("./tests/data/cdf-table").await?;
        let schema = _table.0.state.arrow_schema()?;
        let partition_cols = _table.0.metadata()?.clone().partition_columns.clone();

        let scan = DeltaCdfScan::new(_table.0.log_store.clone(), 0, schema, partition_cols);

        let results = scan.scan().await?;
        let data: Vec<RecordBatch> = collect_sendable_stream(results).await?;
        print_batches(&data)?;

        Ok(())
    }
}
