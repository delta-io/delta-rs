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
use std::collections::VecDeque;
use std::pin::Pin;
use std::{borrow::Cow, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_schema::FieldRef;
use chrono::{TimeZone, Utc};
use dashmap::DashMap;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::stats::Precision;
use datafusion::common::{
    ColumnStatistics, DataFusionError, HashMap, HashSet, Result, Statistics, plan_datafusion_err,
    plan_err,
};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::TableType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::planner::logical2physical;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::{
    catalog::{Session, TableProvider},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
};
use datafusion_datasource::compute_all_files_statistics;
use datafusion_datasource::file_groups::FileGroup;
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::expressions::StructData;
use delta_kernel::scan::ScanMetadata;
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_features::TableFeature;
use delta_kernel::{Engine, Expression};
use futures::future::ready;
use futures::{Stream, TryStreamExt as _};
use itertools::Itertools;
use object_store::ObjectMeta;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::DeltaTableError;
use crate::delta_datafusion::engine::{
    AsObjectStoreUrl as _, DataFusionEngine, predicate_to_df, to_datafusion_scalar,
    to_delta_predicate,
};
use crate::delta_datafusion::table_provider::TableProviderBuilder;
use crate::delta_datafusion::table_provider::next::predicate::can_pushdown_filters;
use crate::delta_datafusion::table_provider::next::replay::{ScanFileContext, ScanFileStream};
pub use crate::delta_datafusion::table_provider::next::scan::DeltaScanExec;
use crate::delta_datafusion::table_provider::next::scan_meta::DeltaScanMetaExec;
use crate::delta_datafusion::{DataFusionMixins as _, DeltaScanConfig};
use crate::kernel::{EagerSnapshot, Scan, Snapshot};

mod predicate;
mod replay;
mod scan;
mod scan_meta;

pub type ScanMetadataStream =
    Pin<Box<dyn Stream<Item = Result<ScanMetadata, DeltaTableError>> + Send>>;

/// Default column name for the file id column we add to files read from disk.
const FILE_ID_COLUMN_DEFAULT: &str = "__delta_rs_file_id__";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum SnapshotWrapper {
    Snapshot(Arc<Snapshot>),
    EagerSnapshot(Arc<EagerSnapshot>),
}

impl SnapshotWrapper {
    fn table_configuration(&self) -> &TableConfiguration {
        match self {
            SnapshotWrapper::Snapshot(snap) => snap.table_configuration(),
            SnapshotWrapper::EagerSnapshot(esnap) => esnap.snapshot().table_configuration(),
        }
    }

    fn snapshot(&self) -> &Snapshot {
        match self {
            SnapshotWrapper::Snapshot(snap) => snap.as_ref(),
            SnapshotWrapper::EagerSnapshot(esnap) => esnap.snapshot(),
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
    pub(super) fn new(snapshot: SnapshotWrapper, config: DeltaScanConfig) -> Result<Self> {
        let scan_schema = snapshot.snapshot().read_schema();
        let full_schema = if let Some(file_column_name) = &config.file_column_name {
            let mut fields = scan_schema.fields().to_vec();
            fields.push(Arc::new(Field::new(
                file_column_name.clone(),
                DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
                false,
            )));
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

    async fn execution_plan(
        &self,
        session: &dyn Session,
        mut scan_plan: KernelScanPlan,
        stream: ScanMetadataStream,
        engine: Arc<dyn Engine>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (files, transforms, dvs, metrics) = replay_files(engine, &scan_plan, stream).await?;

        let (file_id_column, retain_file_id) = self
            .config
            .file_column_name
            .as_ref()
            .map(|name| (name.clone(), true))
            .unwrap_or_else(|| (FILE_ID_COLUMN_DEFAULT.to_string(), false));
        let file_id_field = Arc::new(Field::new(
            file_id_column.clone(),
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        // Add file_id column to output schema if configured
        if retain_file_id {
            let mut fields = scan_plan.output_schema.fields().to_vec();
            fields.push(file_id_field.clone());
            scan_plan.output_schema = Arc::new(Schema::new(fields));
        }

        if scan_plan.is_metadata_only() {
            let maybe_file_rows = files
                .iter()
                .map(|f| {
                    Ok((
                        f.file_url.to_string(),
                        match &f.stats.num_rows {
                            Precision::Exact(n) => *n,
                            _ => {
                                return plan_err!(
                                    "Metadata-only scan requires exact row counts for file {}",
                                    f.file_url
                                );
                            }
                        },
                    ))
                })
                .try_collect::<_, VecDeque<_>, _>();

            if let Ok(file_rows) = maybe_file_rows {
                let exec = DeltaScanMetaExec::new(
                    Arc::new(scan_plan),
                    vec![file_rows],
                    Arc::new(transforms),
                    Arc::new(dvs),
                    retain_file_id.then_some(file_id_field),
                    metrics,
                );
                return Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>);
            }
        }

        get_data_scan_plan(
            session,
            scan_plan,
            files,
            transforms,
            dvs,
            metrics,
            limit,
            file_id_field,
            retain_file_id,
        )
        .await
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

        let scan_plan = KernelScanPlan::new(
            self.snapshot.snapshot(),
            kernel_projection.as_ref(),
            filters,
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

        self.execution_plan(session, scan_plan, stream, engine, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(can_pushdown_filters(
            filter,
            self.snapshot.table_configuration(),
        ))
    }
}

/// Internal representation of a scan plan based on Kernel's Scan abstraction
///
/// Holds the inner scan implementation along with information
/// about how to project the output to match the requested schema
#[derive(Clone, Debug)]
struct KernelScanPlan {
    /// Inner kernel based scan implementation
    scan: Arc<Scan>,
    /// The resulting schema exposed to the caller (used for expression evaluation)
    result_schema: SchemaRef,
    /// The final output schema (includes file_id column if configured)
    output_schema: SchemaRef,
    /// If set, indicates a projection to apply to the
    /// scan output to obtain the result schema
    result_projection: Option<Vec<usize>>,
}

impl KernelScanPlan {
    fn new(snapshot: &Snapshot, projection: Option<&Vec<usize>>, filters: &[Expr]) -> Result<Self> {
        let Some(projection) = projection else {
            let mut builder = snapshot.scan_builder();
            if !filters.is_empty() {
                builder = builder.with_predicate(Arc::new(to_delta_predicate(filters)?));
            }
            let schema = snapshot.read_schema();
            return Ok(Self {
                scan: Arc::new(builder.build()?),
                result_schema: schema.clone(),
                output_schema: schema,
                result_projection: None,
            });
        };
        let mut projection = projection.clone();

        let result_schema = Arc::new(snapshot.read_schema().project(&projection)?);

        let columns_referenced: HashSet<_> = filters
            .iter()
            .flat_map(|f| f.column_refs().iter().map(|c| c.name()).collect_vec())
            .collect();
        let data_columns: HashSet<_> = result_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let missing_columns: Vec<_> = columns_referenced
            .difference(&data_columns)
            .cloned()
            .collect();

        for col in missing_columns {
            let (idx, _) = snapshot
                .read_schema()
                .column_with_name(col)
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "Column '{col}' referenced in filter but not found in schema"
                    )
                })?;
            projection.push(idx);
        }

        let kernel_scan_schema =
            Arc::new((&snapshot.read_schema().project(&projection)?).try_into_kernel()?);

        let scan = Arc::new(
            snapshot
                .scan_builder()
                .with_schema(kernel_scan_schema)
                .with_predicate(Arc::new(to_delta_predicate(filters)?))
                .build()?,
        );

        let logical_columns: HashSet<_> = scan
            .logical_schema()
            .fields()
            .map(|f| f.name().as_str())
            .collect();
        let excess_columns = logical_columns.difference(&data_columns).collect_vec();
        let result_projection = if !excess_columns.is_empty() {
            let mut result_projection = Vec::with_capacity(result_schema.fields().len());
            for (i, field) in scan.logical_schema().fields().enumerate() {
                if data_columns.contains(field.name().as_str()) {
                    result_projection.push(i);
                }
            }
            Some(result_projection)
        } else {
            None
        };

        drop(data_columns);
        drop(logical_columns);

        Ok(Self {
            scan,
            result_schema: result_schema.clone(),
            output_schema: result_schema,
            result_projection,
        })
    }

    /// Denotes if the scan can be resolved using only file metadata
    ///
    /// It may still be impossible to perform a metadata-only scan if the
    /// file statistics are not sufficient to satisfy the query.
    fn is_metadata_only(&self) -> bool {
        self.scan.physical_schema().fields().len() == 0
    }

    fn table_configuration(&self) -> &TableConfiguration {
        self.scan.snapshot().table_configuration()
    }
}

async fn get_data_scan_plan(
    session: &dyn Session,
    scan_plan: KernelScanPlan,
    files: Vec<ScanFileContext>,
    transforms: HashMap<String, Arc<Expression>>,
    dvs: DashMap<String, Vec<bool>>,
    metrics: ExecutionPlanMetricsSet,
    limit: Option<usize>,
    file_id_field: FieldRef,
    retain_file_ids: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut partition_stats = HashMap::new();

    // Convert the files into datafusions `PartitionedFile`s grouped by the object store they are stored in
    // this is used to create a DataSourceExec plan for each store
    // To correlate the data with the original file, we add the file url as a partition value
    // This is required to apply the correct transform to the data in downstream processing.
    let to_partitioned_file = |f: ScanFileContext| {
        if let Some(part_stata) = &f.partitions {
            update_partition_stats(part_stata, &f.stats, &mut partition_stats)?;
        }
        // We create a PartitionedFile from the ObjectMeta to avoid any surprises in path encoding
        // that may arise from using the 'new' method directly. i.e. the 'new' method encodes paths
        // segments again, which may lead to double-encoding in some cases.
        let mut partitioned_file: PartitionedFile = ObjectMeta {
            location: Path::from_url_path(f.file_url.path())?,
            size: f.size,
            last_modified: Utc.timestamp_nanos(0),
            e_tag: None,
            version: None,
        }
        .into();
        partitioned_file = partitioned_file.with_statistics(Arc::new(f.stats));
        partitioned_file.partition_values = vec![ScalarValue::Utf8(Some(f.file_url.to_string()))];
        Ok::<_, DataFusionError>((
            f.file_url.as_object_store_url(),
            (partitioned_file, None::<Vec<bool>>),
        ))
    };

    // Group the files by their object store url. Since datafusion assumes that all files in a
    // DataSourceExec are stored in the same object store, we need to create one plan per store
    let files_by_store = files
        .into_iter()
        .map(to_partitioned_file)
        .try_collect::<_, Vec<_>, _>()?
        .into_iter()
        .into_group_map();

    let physical_schema: SchemaRef =
        Arc::new(scan_plan.scan.physical_schema().as_ref().try_into_arrow()?);

    // TODO(roeap); not sure exactly how row tracking is implemented in kernel right now
    // so leaving predicate as None for now until we are sure this is safe to do.
    let table_config = scan_plan.table_configuration();
    let predicate = if table_config.is_feature_enabled(&TableFeature::RowTracking) {
        None
    } else {
        scan_plan.scan.physical_predicate().and_then(|expr| {
            // TODO(roeap): we should do some more advanced analysis here to see if we
            // can rewrite the predicate to only include non-partition columns.
            // Most likely we need to analyze the expression list passed to scan on
            // TableProvider since these are not yet concatenated.
            let no_part = expr.references().iter().all(|col| {
                !table_config
                    .metadata()
                    .partition_columns()
                    .contains(&col.to_string())
            });
            no_part
                .then(|| predicate_to_df(expr.as_ref(), &KernelDataType::BOOLEAN).ok())
                .flatten()
        })
    };

    let file_id_column = file_id_field.name().clone();
    let pq_plan = get_read_plan(
        session,
        files_by_store,
        &physical_schema,
        limit,
        &file_id_field,
        predicate.as_ref(),
    )
    .await?;

    let exec = DeltaScanExec::new(
        Arc::new(scan_plan),
        pq_plan,
        Arc::new(transforms),
        Arc::new(dvs),
        partition_stats,
        file_id_column,
        retain_file_ids,
        metrics,
    );

    Ok(Arc::new(exec))
}

fn update_partition_stats(
    data: &StructData,
    stats: &Statistics,
    part_stats: &mut HashMap<String, ColumnStatistics>,
) -> Result<()> {
    for (field, stat) in data.fields().iter().zip(data.values().iter()) {
        let (null_count, value) = if stat.is_null() {
            (stats.num_rows.clone(), Precision::Absent)
        } else {
            (
                Precision::Exact(0),
                Precision::Exact(to_datafusion_scalar(stat)?),
            )
        };
        if let Some(part_stat) = part_stats.get_mut(field.name()) {
            part_stat.null_count = part_stat.null_count.add(&null_count);
            part_stat.min_value = part_stat.min_value.min(&value);
            part_stat.max_value = part_stat.max_value.max(&value);
        } else {
            part_stats.insert(
                field.name().clone(),
                ColumnStatistics {
                    null_count,
                    min_value: value.clone(),
                    max_value: value,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            );
        }
    }

    Ok(())
}

async fn replay_files(
    engine: Arc<dyn Engine>,
    scan_plan: &KernelScanPlan,
    stream: ScanMetadataStream,
) -> Result<(
    Vec<ScanFileContext>,
    HashMap<String, Arc<Expression>>,
    DashMap<String, Vec<bool>>,
    ExecutionPlanMetricsSet,
)> {
    let mut stream = ScanFileStream::new(engine, &scan_plan.scan, stream);
    let mut files = Vec::new();
    while let Some(file) = stream.try_next().await? {
        files.extend(file);
    }

    let transforms: HashMap<_, _> = files
        .iter_mut()
        .flat_map(|file| {
            file.transform
                .take()
                .map(|t| (file.file_url.to_string(), t))
        })
        .collect();

    let dv_stream = stream.dv_stream.build();
    let dvs: DashMap<_, _> = dv_stream
        .try_filter_map(|(url, dv)| ready(Ok(dv.map(|dv| (url.to_string(), dv)))))
        .try_collect()
        .await?;

    let metrics = ExecutionPlanMetricsSet::new();
    MetricBuilder::new(&metrics)
        .global_counter("count_files_scanned")
        .add(stream.metrics.num_scanned);

    Ok((files, transforms, dvs, metrics))
}

type FilesByStore = (ObjectStoreUrl, Vec<(PartitionedFile, Option<Vec<bool>>)>);
async fn get_read_plan(
    state: &dyn Session,
    files_by_store: impl IntoIterator<Item = FilesByStore>,
    physical_schema: &SchemaRef,
    limit: Option<usize>,
    file_id_field: &FieldRef,
    predicate: Option<&Expr>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut plans = Vec::new();

    let pq_options = TableParquetOptions {
        global: state.config().options().execution.parquet.clone(),
        ..Default::default()
    };

    for (store_url, files) in files_by_store.into_iter() {
        let reader_factory = Arc::new(CachedParquetFileReaderFactory::new(
            state.runtime_env().object_store(&store_url)?,
            state.runtime_env().cache_manager.get_file_metadata_cache(),
        ));
        let mut file_source =
            ParquetSource::new(pq_options.clone()).with_parquet_file_reader_factory(reader_factory);

        // TODO(roeap); we might be able to also push selection vectors into the read plan
        // by creating parquet access plans. However we need to make sure this does not
        // interfere with other delta features like row ids.
        let has_selection_vectors = files.iter().any(|(_, sv)| sv.is_some());
        if !has_selection_vectors && let Some(pred) = predicate {
            let physical = logical2physical(pred, physical_schema.as_ref());
            file_source = file_source
                .with_predicate(physical)
                .with_pushdown_filters(true);
        }

        let file_group: FileGroup = files.into_iter().map(|file| file.0).collect();
        let (file_groups, statistics) =
            compute_all_files_statistics(vec![file_group], physical_schema.clone(), true, false)?;

        let config =
            FileScanConfigBuilder::new(store_url, physical_schema.clone(), Arc::new(file_source))
                .with_file_groups(file_groups)
                .with_statistics(statistics)
                .with_table_partition_cols(vec![file_id_field.as_ref().clone()])
                .with_limit(limit)
                .build();

        plans.push(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>);
    }

    Ok(match plans.len() {
        0 => Arc::new(EmptyExec::new(physical_schema.clone())),
        1 => plans.remove(0),
        _ => UnionExec::try_new(plans)?,
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, RecordBatch, StringArray, StructArray};
    use arrow_schema::Fields;
    use datafusion::{
        datasource::{physical_plan::FileScanConfig, source::DataSource},
        physical_plan::{ExecutionPlanVisitor, collect, collect_partitioned, visit_execution_plan},
        prelude::{col, lit},
    };
    use object_store::{ObjectStore as _, memory::InMemory};
    use parquet::arrow::ArrowWriter;
    use url::Url;

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
            delta_scan_exec: &DeltaScanExec,
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
            if let Some(delta_scan_exec) = plan.as_any().downcast_ref::<DeltaScanExec>() {
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

    #[tokio::test]
    async fn test_parquet_plan() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_data.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values.push(ScalarValue::Utf8(Some(
            "memory:///test_data.parquet".to_string(),
        )));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            None,
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 1  | a     | memory:///test_data.parquet |",
            "| 2  | b     | memory:///test_data.parquet |",
            "| 3  | c     | memory:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        // respect limits
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            Some(1),
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 1  | a     | memory:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        // extended schema with missing column
        let arrow_schema_extended = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("value2", DataType::Utf8, true),
        ]));
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema_extended,
            Some(1),
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+--------+-----------------------------+",
            "| id | value | value2 | __delta_rs_file_id__        |",
            "+----+-------+--------+-----------------------------+",
            "| 1  | a     |        | memory:///test_data.parquet |",
            "+----+-------+--------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        // extended schema with missing column and different data types
        let arrow_schema_extended = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8View, true),
            Field::new("value2", DataType::Utf8View, true),
        ]));
        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &arrow_schema_extended,
            Some(1),
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+--------+-----------------------------+",
            "| id | value | value2 | __delta_rs_file_id__        |",
            "+----+-------+--------+-----------------------------+",
            "| 1  | a     |        | memory:///test_data.parquet |",
            "+----+-------+--------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);
        assert!(matches!(
            batches[0].column(1).data_type(),
            DataType::Utf8View
        ));
        assert!(matches!(
            batches[0].column(2).data_type(),
            DataType::Utf8View
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_plan_nested() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let nested_fields: Fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]
        .into();
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", DataType::Struct(nested_fields.clone()), true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::try_new(
                    nested_fields,
                    vec![
                        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
                        Arc::new(StringArray::from(vec![Some("aa"), Some("bb"), Some("cc")])),
                    ],
                    None,
                )?),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_data.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values.push(ScalarValue::Utf8(Some(
            "memory:///test_data.parquet".to_string(),
        )));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            None,
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+---------------+-----------------------------+",
            "| id | nested        | __delta_rs_file_id__        |",
            "+----+---------------+-----------------------------+",
            "| 1  | {a: a, b: aa} | memory:///test_data.parquet |",
            "| 2  | {a: b, b: bb} | memory:///test_data.parquet |",
            "| 3  | {a: c, b: cc} | memory:///test_data.parquet |",
            "+----+---------------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        let nested_fields_extended: Fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]
        .into();
        let arrow_schema_extended = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "nested",
                DataType::Struct(nested_fields_extended.clone()),
                true,
            ),
        ]));
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema_extended,
            None,
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+--------------------+-----------------------------+",
            "| id | nested             | __delta_rs_file_id__        |",
            "+----+--------------------+-----------------------------+",
            "| 1  | {a: a, b: aa, c: } | memory:///test_data.parquet |",
            "| 2  | {a: b, b: bb, c: } | memory:///test_data.parquet |",
            "| 3  | {a: c, b: cc, c: } | memory:///test_data.parquet |",
            "+----+--------------------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_plan_multiple_stores() -> TestResult {
        let store_1 = Arc::new(InMemory::new());
        let store_url_1 = Url::parse("first:///")?;
        let store_2 = Arc::new(InMemory::new());
        let store_url_2 = Url::parse("second:///")?;

        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url_1, store_1.clone());
        session
            .runtime_env()
            .register_object_store(&store_url_2, store_2.clone());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let data_1 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )?;
        let data_2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec![Some("b")])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data_1)?;
        arrow_writer.close()?;
        let path = Path::from("test_data.parquet");
        store_1.put(&path, buffer.into()).await?;
        let mut file_1: PartitionedFile = store_1.head(&path).await?.into();
        file_1.partition_values.push(ScalarValue::Utf8(Some(
            "first:///test_data.parquet".to_string(),
        )));

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data_2)?;
        arrow_writer.close()?;
        let path = Path::from("test_data.parquet");
        store_2.put(&path, buffer.into()).await?;
        let mut file_2: PartitionedFile = store_2.head(&path).await?.into();
        file_2.partition_values.push(ScalarValue::Utf8(Some(
            "second:///test_data.parquet".to_string(),
        )));

        let files_by_store = vec![
            (
                store_url_1.as_object_store_url(),
                vec![(file_1, None::<Vec<bool>>)],
            ),
            (
                store_url_2.as_object_store_url(),
                vec![(file_2, None::<Vec<bool>>)],
            ),
        ];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            None,
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 1  | a     | first:///test_data.parquet  |",
            "| 2  | b     | second:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_plan_predicate() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_data.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values.push(ScalarValue::Utf8(Some(
            "memory:///test_data.parquet".to_string(),
        )));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let predicate = col("id").eq(lit(2i32));
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            None,
            &file_id_field,
            Some(&predicate),
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 2  | b     | memory:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }
}
