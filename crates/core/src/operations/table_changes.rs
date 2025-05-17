use crate::{DeltaResult, DeltaTableError};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use arrow_select::filter::filter_record_batch;
use chrono::{DateTime, Utc};
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{Engine, Table};
use std::collections::HashMap;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

#[derive(Default, Clone)]
pub struct TableChangesBuilder {
    starting_version: Option<i64>,
    ending_version: Option<i64>,
    starting_timestamp: Option<DateTime<Utc>>,
    ending_timestamp: Option<DateTime<Utc>>,
    engine: Option<Arc<dyn Engine>>,
    table_options: HashMap<String, String>,
    allow_out_of_range: bool,
    table_root: String,
    version_limit: Option<usize>,
}

impl std::fmt::Debug for TableChangesBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl TableChangesBuilder {
    pub fn new(table_root: String) -> Self {
        Self {
            table_root,
            ..Default::default()
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

    pub fn with_engine(mut self, engine: Arc<dyn Engine>) -> Self {
        self.engine = Some(engine);
        self
    }

    pub fn with_table_options(mut self, table_options: HashMap<String, String>) -> Self {
        self.table_options.extend(table_options.into_iter());
        self
    }

    pub fn with_version_limit(mut self, limit: usize) -> Self {
        self.version_limit = Some(limit);
        self
    }
    pub fn build(self) -> DeltaResult<Arc<dyn ExecutionPlan>> {
        if self.starting_version.is_none() && self.starting_timestamp.is_none() {
            return Err(DeltaTableError::NoStartingVersionOrTimestamp);
        }
        let root_url = Url::from_str(&self.table_root)
            .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;

        let engine = self.engine.unwrap_or(Arc::new(DefaultEngine::try_new(
            &root_url,
            self.table_options,
            Arc::new(TokioBackgroundExecutor::new()),
        )?));

        let table = Table::try_from_uri(&self.table_root)?;

        let snapshot = Arc::new(table.snapshot(engine.as_ref(), None)?);
        let history_manager =
            table.history_manager_from_snapshot(engine.as_ref(), snapshot, self.version_limit)?;
        let start_time = self.starting_timestamp.unwrap_or(DateTime::<Utc>::MIN_UTC);
        let end_time = self.ending_timestamp.map(|ts| ts.timestamp());
        let (start, end) = history_manager.timestamp_range_to_versions(
            engine.as_ref(),
            start_time.timestamp(),
            end_time,
        )?;

        let table_changes = table
            .table_changes(engine.as_ref(), start, end)?
            .into_scan_builder()
            .build()?;

        let schema_ref = table_changes.schema().clone();
        let schema = Arc::new(Schema::try_from(schema_ref.as_ref())?);
        let changes = table_changes.execute(engine)?;

        let source = changes
            .map(|cr| -> DeltaResult<_> {
                let scan_result = cr?;
                let mask = scan_result.full_mask();
                let data = scan_result.raw_data?;

                let arrow_data = data
                    .into_any()
                    .downcast::<ArrowEngineData>()
                    .map_err(|_| {
                        delta_kernel::Error::EngineDataType("ArrowEngineData".to_string())
                    })?
                    .into();
                if let Some(m) = mask {
                    Ok(filter_record_batch(&arrow_data, &m.into())?)
                } else {
                    Ok(arrow_data)
                }
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        let memory_source = MemorySourceConfig::try_new_from_batches(schema, source)?;
        Ok(memory_source)
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
