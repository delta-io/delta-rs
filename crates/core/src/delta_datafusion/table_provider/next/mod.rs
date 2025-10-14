use std::any::Any;
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
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::{
    catalog::{Session, TableProvider},
    logical_expr::{dml::InsertOp, LogicalPlan},
    physical_plan::ExecutionPlan,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use futures::future::ready;
use futures::TryStreamExt as _;
use itertools::Itertools;
use object_store::path::Path;

use crate::delta_datafusion::engine::{AsObjectStoreUrl as _, DataFusionEngine};
use crate::delta_datafusion::table_provider::next::replay::{ScanFileContext, ScanFileStream};
use crate::delta_datafusion::table_provider::next::scan::DeltaScanExec;
use crate::delta_datafusion::DataFusionMixins as _;
use crate::kernel::Snapshot;
use crate::DeltaTableError;

mod replay;
mod scan;

#[derive(Debug)]
pub struct DeltaTableProvider {
    snapshot: Arc<Snapshot>,
    metrics: ExecutionPlanMetricsSet,
}

impl<T: Into<Arc<Snapshot>>> From<T> for DeltaTableProvider {
    fn from(snapshot: T) -> Self {
        DeltaTableProvider::new(snapshot)
    }
}

impl DeltaTableProvider {
    pub fn new(snapshot: impl Into<Arc<Snapshot>>) -> Self {
        Self {
            snapshot: snapshot.into(),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn into_parts(self) -> (Arc<Snapshot>, ExecutionPlanMetricsSet) {
        (self.snapshot, self.metrics)
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.snapshot.read_schema()
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
        let projected_schema: KernelSchemaRef = if let Some(projection) = projection {
            let projected_schema = self.schema().project(projection)?;
            Arc::new(
                (&projected_schema)
                    .try_into_kernel()
                    .map_err(DeltaTableError::from)?,
            )
        } else {
            Arc::new(
                self.schema()
                    .as_ref()
                    .try_into_kernel()
                    .map_err(DeltaTableError::from)?,
            )
        };

        let scan = Arc::new(
            self.snapshot
                .scan_builder()
                .with_schema(projected_schema.clone())
                .build()?,
        );
        let engine = DataFusionEngine::new_from_session(session);
        let stream = scan.scan_metadata(engine.clone());

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

        let _metrics = stream.metrics;

        let file_id_column = "__delta_rs_file_id".to_string();
        let metrics = ExecutionPlanMetricsSet::new();

        // Convert the files into datafusions `PartitionedFile`s grouped by the object store they are stored in
        // this is used to create a DataSourceExec plan for each store
        // To correlate the data with the original file, we add the file url as a partition value
        // This is required to apply the correct transform to the data in downstream processing.
        let to_partitioned_file = |f: ScanFileContext| {
            let file_path = Path::from_url_path(f.file_url.path())?;
            let mut partitioned_file = PartitionedFile::new(file_path.to_string(), f.size);
            partitioned_file.partition_values =
                vec![ScalarValue::Utf8(Some(f.file_url.to_string()))];
            // NB: we need to reassign the location since the 'new' method does incompatible path encoding internally.
            partitioned_file.object_meta.location = file_path;
            Ok::<_, DataFusionError>((
                f.file_url.as_object_store_url(),
                (partitioned_file, None::<Vec<bool>>),
            ))
        };

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

        let exec = DeltaScanExec::new(
            self.snapshot.read_schema(),
            projected_schema,
            pq_plan,
            Arc::new(transforms),
            Arc::new(dvs),
            file_id_column,
            metrics,
        );

        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        todo!()
        // Ok(get_pushdown_filters(
        //     filter,
        //     self.snapshot.metadata().partition_columns(),
        // ))
    }

    // fn statistics(&self) -> Option<Statistics> {
    //     self.snapshot.log_data().statistics()
    // }

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

#[cfg(test)]
mod tests {
    use datafusion::{physical_plan::collect_partitioned, prelude::SessionContext};

    use crate::{
        assert_batches_sorted_eq,
        kernel::Snapshot,
        test_utils::{TestResult, TestTables},
    };

    use super::*;

    #[tokio::test]
    async fn test_scan_simple_table() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaTableProvider::new(snapshot);

        let session = Arc::new(SessionContext::new());
        let state = session.state_ref().read().clone();

        let plan = provider.scan(&state, None, &[], None).await?;

        let batches: Vec<_> = collect_partitioned(plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let expected = vec![
            "+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }
}
