use std::sync::Arc;

use dashmap::{mapref::one::Ref, DashMap};
use datafusion::execution::{
    object_store::{ObjectStoreRegistry, ObjectStoreUrl},
    TaskContext,
};
use delta_kernel::engine::parse_json as arrow_parse_json;
use delta_kernel::{
    engine::default::{
        executor::tokio::{TokioBackgroundExecutor, TokioMultiThreadExecutor},
        json::DefaultJsonHandler,
        parquet::DefaultParquetHandler,
    },
    error::DeltaResult as KernelResult,
    schema::SchemaRef,
    EngineData, FileDataReadResultIterator, FileMeta, FilteredEngineData, JsonHandler,
    ParquetHandler, PredicateRef,
};
use itertools::Itertools;
use tokio::runtime::{Handle, RuntimeFlavor};

use super::storage::{group_by_store, AsObjectStoreUrl};

#[derive(Clone)]
pub struct DataFusionFileFormatHandler {
    ctx: Arc<TaskContext>,
    pq_registry: Arc<DashMap<ObjectStoreUrl, Arc<dyn ParquetHandler>>>,
    json_registry: Arc<DashMap<ObjectStoreUrl, Arc<dyn JsonHandler>>>,
    handle: Handle,
}

impl DataFusionFileFormatHandler {
    /// Create a new [`DatafusionParquetHandler`] instance.
    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        Self {
            ctx,
            pq_registry: DashMap::new().into(),
            json_registry: DashMap::new().into(),
            handle,
        }
    }

    fn registry(&self) -> Arc<dyn ObjectStoreRegistry> {
        self.ctx.runtime_env().object_store_registry.clone()
    }

    fn get_or_create_pq(
        &self,
        url: ObjectStoreUrl,
    ) -> KernelResult<Ref<'_, ObjectStoreUrl, Arc<dyn ParquetHandler>>> {
        if let Some(handler) = self.pq_registry.get(&url) {
            return Ok(handler);
        }
        let store = self
            .registry()
            .get_store(url.as_ref())
            .map_err(delta_kernel::Error::generic_err)?;

        let handler: Arc<dyn ParquetHandler> = match self.handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => Arc::new(DefaultParquetHandler::new(
                store,
                Arc::new(TokioMultiThreadExecutor::new(self.handle.clone())),
            )),
            RuntimeFlavor::CurrentThread => Arc::new(DefaultParquetHandler::new(
                store,
                Arc::new(TokioBackgroundExecutor::new()),
            )),
            _ => panic!("unsupported runtime flavor"),
        };

        self.pq_registry.insert(url.clone(), handler);
        Ok(self.pq_registry.get(&url).unwrap())
    }

    fn get_or_create_json(
        &self,
        url: ObjectStoreUrl,
    ) -> KernelResult<Ref<'_, ObjectStoreUrl, Arc<dyn JsonHandler>>> {
        if let Some(handler) = self.json_registry.get(&url) {
            return Ok(handler);
        }
        let store = self
            .registry()
            .get_store(url.as_ref())
            .map_err(delta_kernel::Error::generic_err)?;

        let handler: Arc<dyn JsonHandler> = match self.handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => Arc::new(DefaultJsonHandler::new(
                store,
                Arc::new(TokioMultiThreadExecutor::new(self.handle.clone())),
            )),
            RuntimeFlavor::CurrentThread => Arc::new(DefaultJsonHandler::new(
                store,
                Arc::new(TokioBackgroundExecutor::new()),
            )),
            _ => panic!("unsupported runtime flavor"),
        };

        self.json_registry.insert(url.clone(), handler);
        Ok(self.json_registry.get(&url).unwrap())
    }
}

impl ParquetHandler for DataFusionFileFormatHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let grouped_files = group_by_store(files.to_vec());
        Ok(Box::new(
            grouped_files
                .into_iter()
                .map(|(url, files)| {
                    self.get_or_create_pq(url)?.read_parquet_files(
                        &files.to_vec(),
                        physical_schema.clone(),
                        predicate.clone(),
                    )
                })
                // TODO: this should not do any blocking operations, since this should
                // happen when the iterators are polled and we are just creating a vec of iterators.
                // Is this correct?
                .try_collect::<_, Vec<_>, _>()?
                .into_iter()
                .flatten(),
        ))
    }
}

impl JsonHandler for DataFusionFileFormatHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let grouped_files = group_by_store(files.to_vec());
        Ok(Box::new(
            grouped_files
                .into_iter()
                .map(|(url, files)| {
                    self.get_or_create_json(url)?.read_json_files(
                        &files.to_vec(),
                        physical_schema.clone(),
                        predicate.clone(),
                    )
                })
                // TODO: this should not do any blocking operations, since this should
                // happen when the iterators are polled and we are just creating a vec of iterators.
                // Is this correct?
                .try_collect::<_, Vec<_>, _>()?
                .into_iter()
                .flatten(),
        ))
    }

    fn write_json_file(
        &self,
        path: &url::Url,
        data: Box<dyn Iterator<Item = KernelResult<FilteredEngineData>> + Send + '_>,
        overwrite: bool,
    ) -> KernelResult<()> {
        self.get_or_create_json(path.as_object_store_url())?
            .write_json_file(path, data, overwrite)
    }
}
