//! Datafusion TableProvider implementation for Delta Lake tables.
//!
//! <div class="warning">
//!
//! The table provider is based on Snapshots of a Delta Table. Therefore, it represents
//! a static view of the table at a specific point in time. Changes to the underlying
//! Delta Table after the snapshot was taken will not be reflected in queries executed.
//!
//! To work with a dynamic view of the table that reflects ongoing changes, consider using
//! the catalog abstractions in this crate, which provide a higher-level interface for managing
//! Delta Tables within DataFusion sessions.
//!
//! </div>
//!
//! # Overview
//!
//! The [`DeltaScan`] struct integrates Delta Tables with DataFusion by implementing the
//! `TableProvider` trait. It encapsulates all delta table-specific logic required to translate
//! DataFusion's logical and physical plans into operations on Delta Lake data. This includes
//! - table scans
//! - insert operations
//!
//! ## Table Scans
//!
//! Scanning a Delta Table involves two major steps:
//! - planning the physical data file reads based on Datafusion's abstractions
//! - applying Delta features by transforming the physical data into the table's logical schema
//!
use std::any::Any;
use std::{borrow::Cow, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::prelude::Expr;
use datafusion::{
    catalog::{Session, TableProvider},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
};
use delta_kernel::PredicateRef;
use delta_kernel::table_configuration::TableConfiguration;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

pub use self::scan::DeltaScanExec;
use self::scan::KernelScanPlan;
use crate::DeltaResult;
use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::engine::DataFusionEngine;
use crate::delta_datafusion::table_provider::TableProviderBuilder;
use crate::kernel::{EagerSnapshot, LogicalFileView, Snapshot};
use crate::logstore::LogStore;

mod scan;

/// Default column name for the file id column we add to files read from disk.
const FILE_ID_COLUMN_DEFAULT: &str = "__delta_rs_file_id__";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SnapshotWrapper {
    Snapshot(Arc<Snapshot>),
    EagerSnapshot(Arc<EagerSnapshot>),
}

impl From<Arc<Snapshot>> for SnapshotWrapper {
    fn from(snap: Arc<Snapshot>) -> Self {
        SnapshotWrapper::Snapshot(snap)
    }
}

impl From<Snapshot> for SnapshotWrapper {
    fn from(snap: Snapshot) -> Self {
        SnapshotWrapper::Snapshot(snap.into())
    }
}

impl From<Arc<EagerSnapshot>> for SnapshotWrapper {
    fn from(esnap: Arc<EagerSnapshot>) -> Self {
        SnapshotWrapper::EagerSnapshot(esnap)
    }
}

impl From<EagerSnapshot> for SnapshotWrapper {
    fn from(esnap: EagerSnapshot) -> Self {
        SnapshotWrapper::EagerSnapshot(esnap.into())
    }
}

impl SnapshotWrapper {
    pub(crate) fn table_configuration(&self) -> &TableConfiguration {
        match self {
            SnapshotWrapper::Snapshot(snap) => snap.table_configuration(),
            SnapshotWrapper::EagerSnapshot(esnap) => esnap.snapshot().table_configuration(),
        }
    }

    pub(crate) fn snapshot(&self) -> &Snapshot {
        match self {
            SnapshotWrapper::Snapshot(snap) => snap.as_ref(),
            SnapshotWrapper::EagerSnapshot(esnap) => esnap.snapshot(),
        }
    }

    pub(crate) fn file_views(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        match self {
            SnapshotWrapper::Snapshot(snap) => snap.file_views(log_store, predicate),
            SnapshotWrapper::EagerSnapshot(esnap) => esnap.file_views(log_store, predicate),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaScan {
    snapshot: SnapshotWrapper,
    config: DeltaScanConfig,
    scan_schema: SchemaRef,
    /// Full schema including file_id column if configured
    full_schema: SchemaRef,
}

impl DeltaScan {
    // create new delta scan
    pub fn new(snapshot: impl Into<SnapshotWrapper>, config: DeltaScanConfig) -> Result<Self> {
        let snapshot = snapshot.into();
        let scan_schema = config.table_schema(snapshot.table_configuration())?;
        let full_schema = if config.retain_file_id() {
            let mut fields = scan_schema.fields().to_vec();
            fields.push(config.file_id_field());
            Arc::new(Schema::new(fields))
        } else {
            scan_schema.clone()
        };
        Ok(Self {
            snapshot,
            config,
            scan_schema,
            full_schema,
        })
    }

    pub fn builder() -> TableProviderBuilder {
        TableProviderBuilder::new()
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let engine = DataFusionEngine::new_from_session(session);

        // Filter out file_id column from projection if present
        let file_id_idx = self
            .config
            .file_column_name
            .as_ref()
            .map(|_| self.scan_schema.fields().len());
        let kernel_projection = projection.map(|proj| {
            proj.iter()
                .filter(|&&idx| Some(idx) != file_id_idx)
                .copied()
                .collect::<Vec<_>>()
        });

        let scan_plan = KernelScanPlan::try_new(
            self.snapshot.snapshot(),
            kernel_projection.as_ref(),
            filters,
            &self.config,
        )?;

        let stream = match &self.snapshot {
            SnapshotWrapper::Snapshot(_) => scan_plan.scan.scan_metadata(engine.clone()),
            SnapshotWrapper::EagerSnapshot(esn) => {
                if let Ok(files) = esn.files() {
                    scan_plan.scan.scan_metadata_from(
                        engine.clone(),
                        esn.snapshot().version() as u64,
                        Box::new(files.to_vec().into_iter()),
                        None,
                    )
                } else {
                    scan_plan.scan.scan_metadata(engine.clone())
                }
            }
        };

        scan::execution_plan(&self.config, session, scan_plan, stream, engine, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(scan::supports_filters_pushdown(
            filter,
            self.snapshot.table_configuration(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        datasource::{physical_plan::FileScanConfig, source::DataSource},
        error::DataFusionError,
        physical_plan::{ExecutionPlanVisitor, collect_partitioned, visit_execution_plan},
    };
    use datafusion_datasource::source::DataSourceExec;

    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::session::create_session,
        kernel::Snapshot,
        test_utils::{TestResult, TestTables},
    };

    use super::*;

    /// Extracts fields from the parquet scan
    #[derive(Default)]
    struct DeltaScanVisitor {
        num_scanned: Option<usize>,
        total_bytes_scanned: Option<usize>,
    }

    impl DeltaScanVisitor {
        fn pre_visit_delta_scan(
            &mut self,
            delta_scan_exec: &scan::DeltaScanExec,
        ) -> Result<bool, DataFusionError> {
            let Some(metrics) = delta_scan_exec.metrics() else {
                return Ok(true);
            };

            self.num_scanned = metrics
                .sum_by_name("count_files_scanned")
                .map(|v| v.as_usize());

            Ok(true)
        }

        fn pre_visit_data_source(
            &mut self,
            datasource_exec: &DataSourceExec,
        ) -> Result<bool, DataFusionError> {
            let Some(scan_config) = datasource_exec
                .data_source()
                .as_any()
                .downcast_ref::<FileScanConfig>()
            else {
                return Ok(true);
            };

            let pq_metrics = scan_config
                .metrics()
                .clone_inner()
                .sum_by_name("bytes_scanned");
            self.total_bytes_scanned = pq_metrics.map(|v| v.as_usize());

            Ok(true)
        }
    }

    impl ExecutionPlanVisitor for DeltaScanVisitor {
        type Error = DataFusionError;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if let Some(delta_scan_exec) = plan.as_any().downcast_ref::<scan::DeltaScanExec>() {
                return self.pre_visit_delta_scan(delta_scan_exec);
            };

            if let Some(datasource_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
                return self.pre_visit_data_source(datasource_exec);
            }

            Ok(true)
        }
    }

    #[tokio::test]
    async fn test_query_simple_table() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::builder().with_snapshot(snapshot).await?;

        let session = Arc::new(create_session().into_inner());
        session.register_table("delta_table", provider).unwrap();

        let df = session.sql("SELECT * FROM delta_table").await.unwrap();
        let batches = df.collect().await?;

        let expected = vec![
            "+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_simple_table() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::builder().with_snapshot(snapshot).await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let plan = provider.scan(&state, None, &[], None).await?;

        let batches: Vec<_> = collect_partitioned(plan.clone(), session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();

        assert_eq!(visitor.num_scanned, Some(5));
        assert_eq!(visitor.total_bytes_scanned, Some(231));

        let expected = vec![
            "+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }
}
