use std::any::Any;
use std::pin::Pin;
use std::{borrow::Cow, sync::Arc};

use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{DataFusionError, HashMap, Result};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::{
    catalog::{Session, TableProvider},
    logical_expr::{dml::InsertOp, LogicalPlan},
    physical_plan::ExecutionPlan,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::scan::ScanMetadata;
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::Engine;
use futures::future::ready;
use futures::{Stream, TryStreamExt as _};
use itertools::Itertools;
use object_store::path::Path;

use crate::delta_datafusion::engine::{
    to_delta_predicate, AsObjectStoreUrl as _, DataFusionEngine,
};
use crate::delta_datafusion::table_provider::get_pushdown_filters;
use crate::delta_datafusion::table_provider::next::replay::{ScanFileContext, ScanFileStream};
pub use crate::delta_datafusion::table_provider::next::scan::DeltaScanExec;
use crate::delta_datafusion::DataFusionMixins as _;
use crate::kernel::{EagerSnapshot, Scan, Snapshot};
use crate::DeltaTableError;

mod replay;
mod scan;

pub type ScanMetadataStream =
    Pin<Box<dyn Stream<Item = Result<ScanMetadata, DeltaTableError>> + Send>>;

impl Snapshot {
    fn kernel_scan(&self, projection: Option<&Vec<usize>>, filters: &[Expr]) -> Result<Arc<Scan>> {
        let (_, projected_kernel_schema) = project_schema(self.read_schema(), projection)?;
        Ok(Arc::new(
            self.scan_builder()
                .with_schema(projected_kernel_schema.clone())
                .with_predicate(Arc::new(to_delta_predicate(filters)?))
                .build()?,
        ))
    }

    async fn execution_plan(
        &self,
        session: &dyn Session,
        scan: Arc<Scan>,
        stream: ScanMetadataStream,
        engine: Arc<dyn Engine>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // let (_, projected_kernel_schema) = project_schema(self.read_schema(), projection)?;

        let mut stream = ScanFileStream::new(engine, &scan, stream);
        let mut files = Vec::new();
        while let Some(file) = stream.try_next().await? {
            files.push(file);
        }

        let transforms: HashMap<_, _> = files
            .iter()
            .flatten()
            .flat_map(|file| {
                file.transform
                    .as_ref()
                    .map(|t| (file.file_url.to_string(), t.clone()))
            })
            .collect();
        let dv_stream = stream.dv_stream.build();
        let dvs: HashMap<_, _> = dv_stream
            .try_filter_map(|(url, dv)| ready(Ok(dv.map(|dv| (url.to_string(), dv)))))
            .try_collect()
            .await?;

        let metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics)
            .global_counter("count_files_skipped")
            .add(stream.metrics.num_skipped);
        MetricBuilder::new(&metrics)
            .global_counter("count_files_scanned")
            .add(stream.metrics.num_scanned);

        let file_id_column = "__delta_rs_file_id".to_string();

        // Convert the files into datafusions `PartitionedFile`s grouped by the object store they are stored in
        // this is used to create a DataSourceExec plan for each store
        // To correlate the data with the original file, we add the file url as a partition value
        // This is required to apply the correct transform to the data in downstream processing.
        let to_partitioned_file = |f: ScanFileContext| {
            let file_path = Path::from_url_path(f.file_url.path())?;
            let mut partitioned_file = PartitionedFile::new(file_path.to_string(), f.size)
                .with_statistics(Arc::new(f.stats));
            partitioned_file.partition_values =
                vec![ScalarValue::Utf8(Some(f.file_url.to_string()))];
            // NB: we need to reassign the location since the 'new' method does incompatible path encoding internally.
            partitioned_file.object_meta.location = file_path;
            Ok::<_, DataFusionError>((
                f.file_url.as_object_store_url(),
                (partitioned_file, None::<Vec<bool>>),
            ))
        };

        // Group the files by their object store url. Since datafusion assumes that all files in a
        // DataSourceExec are stored in the same object store, we need to create one plan per store
        let files_by_store = files
            .into_iter()
            .flat_map(|fs| fs.into_iter().map(to_partitioned_file))
            .try_collect::<_, Vec<_>, _>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .into_iter()
            .into_group_map();

        let physical_schema = Arc::new(scan.physical_schema().as_ref().try_into_arrow()?);
        let pq_plan = get_read_plan(
            files_by_store,
            &physical_schema,
            session,
            limit,
            Field::new(
                file_id_column.clone(),
                DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
                false,
            ),
            &metrics,
        )
        .await?;

        let (projected_arrow_schema, _) = project_schema(self.read_schema(), projection)?;
        let exec = DeltaScanExec::new(
            projected_arrow_schema,
            scan.logical_schema().clone(),
            pq_plan,
            Arc::new(transforms),
            Arc::new(dvs),
            file_id_column,
            metrics,
        );

        Ok(Arc::new(exec))
    }
}

#[async_trait::async_trait]
impl TableProvider for Snapshot {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_schema()
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
        let scan = self.kernel_scan(projection, filters)?;
        let engine = DataFusionEngine::new_from_session(session);
        let stream = scan.scan_metadata(engine.clone());
        self.execution_plan(session, scan, stream, engine, projection, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(get_pushdown_filters(
            filter,
            self.metadata().partition_columns(),
        ))
    }

    /// Insert the data into the delta table
    /// Insert operation is only supported for Append and Overwrite
    /// Return the execution plan
    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!("Implement insert_into method")
    }
}

#[async_trait::async_trait]
impl TableProvider for EagerSnapshot {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        TableProvider::schema(self.snapshot())
    }

    fn table_type(&self) -> TableType {
        self.snapshot().table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.snapshot().get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.snapshot().get_logical_plan()
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan = self.snapshot().kernel_scan(projection, filters)?;
        let engine = DataFusionEngine::new_from_session(session);
        let stream = if let Ok(files) = self.files() {
            scan.scan_metadata_from(
                engine.clone(),
                self.snapshot().version() as u64,
                Box::new(files.to_vec().into_iter()),
                None,
            )
        } else {
            scan.scan_metadata(engine.clone())
        };
        self.snapshot()
            .execution_plan(session, scan, stream, engine, projection, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.snapshot().supports_filters_pushdown(filter)
    }

    /// Insert the data into the delta table
    /// Insert operation is only supported for Append and Overwrite
    /// Return the execution plan
    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.snapshot().insert_into(state, input, insert_op).await
    }
}

async fn get_read_plan(
    files_by_store: impl IntoIterator<
        Item = (ObjectStoreUrl, Vec<(PartitionedFile, Option<Vec<bool>>)>),
    >,
    physical_schema: &SchemaRef,
    state: &dyn Session,
    limit: Option<usize>,
    file_id_field: Field,
    _metrics: &ExecutionPlanMetricsSet,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO: update parquet source.
    let source = ParquetSource::default();

    let mut plans = Vec::new();

    for (store_url, files) in files_by_store.into_iter() {
        // state.ensure_object_store(store_url.as_ref()).await?;

        let store = state.runtime_env().object_store(&store_url)?;
        let _reader_factory = source
            .parquet_file_reader_factory()
            .cloned()
            .unwrap_or_else(|| Arc::new(DefaultParquetFileReaderFactory::new(store)));

        // let file_group = compute_parquet_access_plans(&reader_factory, files, &metrics).await?;
        let file_group = files.into_iter().map(|file| file.0);

        // TODO: convert passed predicate to an expression in terms of physical columns
        // and add it to the FileScanConfig
        // let file_source =
        //     source.with_schema_adapter_factory(Arc::new(NestedSchemaAdapterFactory))?;
        let file_source = Arc::new(source.clone());
        let config = FileScanConfigBuilder::new(store_url, physical_schema.clone(), file_source)
            .with_file_group(file_group.into_iter().collect())
            .with_table_partition_cols(vec![file_id_field.clone()])
            .with_limit(limit)
            .build();
        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        plans.push(plan);
    }

    let plan = match plans.len() {
        1 => plans.remove(0),
        _ => Arc::new(UnionExec::new(plans)),
    };
    Ok(match plan.with_fetch(limit) {
        Some(limit) => limit,
        None => plan,
    })
}

fn project_schema(
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<(SchemaRef, KernelSchemaRef)> {
    let projected_arrow_schema = match projection {
        Some(p) => Arc::new(schema.project(p)?),
        None => schema,
    };
    let projected_kernel_schema: KernelSchemaRef = Arc::new(
        projected_arrow_schema
            .as_ref()
            .try_into_kernel()
            .map_err(DeltaTableError::from)?,
    );
    Ok((projected_arrow_schema, projected_kernel_schema))
}

#[cfg(test)]
mod tests {
    use datafusion::{
        datasource::{physical_plan::FileScanConfig, source::DataSource},
        physical_plan::{collect_partitioned, visit_execution_plan, ExecutionPlanVisitor},
    };

    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::create_test_session,
        kernel::Snapshot,
        test_utils::{TestResult, TestTables},
    };

    use super::*;

    /// Extracts fields from the parquet scan
    #[derive(Default)]
    struct DeltaScanVisitor {
        num_skipped: Option<usize>,
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

            self.num_skipped = metrics
                .sum_by_name("count_files_skipped")
                .map(|v| v.as_usize());
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

            // if let Some(parquet_source) = scan_config
            //     .file_source
            //     .as_any()
            //     .downcast_ref::<ParquetSource>()
            // {
            //     parquet_source
            // }

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
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);

        let session = Arc::new(create_test_session().into_inner());
        session.register_table("delta_table", snapshot).unwrap();

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

        let session = Arc::new(create_test_session().into_inner());
        let state = session.state_ref().read().clone();

        let plan = snapshot.scan(&state, None, &[], None).await?;

        let batches: Vec<_> = collect_partitioned(plan.clone(), session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();

        assert_eq!(visitor.num_scanned, Some(5));
        assert_eq!(visitor.num_skipped, Some(28));
        assert_eq!(visitor.total_bytes_scanned, Some(231));

        let expected = vec![
            "+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }
}
