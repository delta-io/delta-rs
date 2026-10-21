//! Newline-delimited JSON files as a DataFusion file source, read with the kernel's reader settings.
//!
//! DataFusion's own `JsonSource` cannot turn on arrow-json's `coerce_primitive`. The kernel's
//! default engine reads with it, so a bare `false` or number still lands in a string column.
//! [`CoercingJsonSource`] is `JsonSource` with that one setting: projection and the per-file values
//! go through DataFusion's `ProjectionOpener` as in `JsonSource`, and the opener reads each file
//! whole. A file is never split by byte range, so its rows keep file order, as the kernel's
//! `JsonHandler` contract requires.

use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::json::ReaderBuilder;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::apply_expression_roots;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{PartitionedFile, TableSchema};
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::{ObjectStore, ObjectStoreExt as _};

use super::ScanLayout;

/// The declared file schema, plus the per-file value columns.
pub(super) fn table_schema(layout: &ScanLayout) -> TableSchema {
    TableSchema::builder(Arc::clone(&layout.file_schema))
        .with_table_partition_cols(layout.constant_fields.clone())
        .build()
}

/// A scan of `store_url`'s files through [`CoercingJsonSource`].
pub(super) fn scan_config(
    table_schema: &TableSchema,
    store_url: &ObjectStoreUrl,
) -> FileScanConfigBuilder {
    let source = CoercingJsonSource::new(table_schema.clone());
    FileScanConfigBuilder::new(store_url.clone(), Arc::new(source))
}

/// DataFusion's `JsonSource`, reading with `coerce_primitive`.
#[derive(Clone)]
pub(super) struct CoercingJsonSource {
    table_schema: TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
}

impl CoercingJsonSource {
    fn new(table_schema: TableSchema) -> Self {
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl FileSource for CoercingJsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> DFResult<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        let opener = Arc::new(CoercingJsonOpener {
            object_store,
            read_schema: Arc::new(file_schema.project(&self.projection.file_indices)?),
            batch_size: self
                .batch_size
                .expect("the file scan sets the batch size before it opens files"),
        });
        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size: Some(batch_size),
            ..self.clone()
        })
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> DFResult<Option<Arc<dyn FileSource>>> {
        let merged = self.projection.source.try_merge(projection)?;
        let projection = SplitProjection::new(self.table_schema.file_schema(), &merged);
        Ok(Some(Arc::new(Self {
            projection,
            ..self.clone()
        })))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "json"
    }

    /// A file is read whole, so its rows keep file order.
    fn supports_repartitioning(&self) -> bool {
        false
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        apply_expression_roots(self.projection.source.iter(), f)
    }
}

/// Reads one file whole and decodes it one batch per poll, in file order.
struct CoercingJsonOpener {
    object_store: Arc<dyn ObjectStore>,
    /// The file columns the projection reads.
    read_schema: SchemaRef,
    batch_size: usize,
}

impl FileOpener for CoercingJsonOpener {
    fn open(&self, file: PartitionedFile) -> DFResult<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let reader = ReaderBuilder::new(Arc::clone(&self.read_schema))
            .with_coerce_primitive(true)
            .with_batch_size(self.batch_size);
        Ok(Box::pin(async move {
            let bytes = store.get(&file.object_meta.location).await?.bytes().await?;
            let batches = reader.build(Cursor::new(bytes))?;
            Ok(futures::stream::iter(batches).map_err(Into::into).boxed())
        }))
    }
}
