use std::sync::Arc;

use dashmap::{DashMap, mapref::one::Ref};
use datafusion::execution::{
    TaskContext,
    object_store::{ObjectStoreRegistry, ObjectStoreUrl},
};
use delta_kernel::engine::parse_json as arrow_parse_json;
use delta_kernel::{
    EngineData, FileDataReadResultIterator, FileMeta, FilteredEngineData, JsonHandler,
    ParquetHandler, PredicateRef, error::DeltaResult as KernelResult, schema::SchemaRef,
};
use delta_kernel_default_engine::{
    executor::tokio::{TokioBackgroundExecutor, TokioMultiThreadExecutor},
    json::DefaultJsonHandler,
    parquet::DefaultParquetHandler,
};
use itertools::Itertools;
use tokio::runtime::{Handle, RuntimeFlavor};

use super::storage::{AsObjectStoreUrl, group_by_store};

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

    fn parquet_handler_for(
        &self,
        url: ObjectStoreUrl,
    ) -> KernelResult<Ref<'_, ObjectStoreUrl, Arc<dyn ParquetHandler>>> {
        if let Some(handler) = self.pq_registry.get(&url) {
            return Ok(handler);
        }
        // The entry holds the shard's write lock, so concurrent first calls build one handler.
        let handler = self.pq_registry.entry(url.clone()).or_try_insert_with(|| {
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
            Ok::<_, delta_kernel::Error>(handler)
        })?;
        Ok(handler.downgrade())
    }

    fn json_handler_for(
        &self,
        url: ObjectStoreUrl,
    ) -> KernelResult<Ref<'_, ObjectStoreUrl, Arc<dyn JsonHandler>>> {
        if let Some(handler) = self.json_registry.get(&url) {
            return Ok(handler);
        }
        // The entry holds the shard's write lock, so concurrent first calls build one handler.
        let handler = self
            .json_registry
            .entry(url.clone())
            .or_try_insert_with(|| {
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
                Ok::<_, delta_kernel::Error>(handler)
            })?;
        Ok(handler.downgrade())
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
                    self.parquet_handler_for(url)?.read_parquet_files(
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

    fn write_parquet_file(
        &self,
        location: url::Url,
        data: Box<dyn Iterator<Item = KernelResult<Box<dyn EngineData>>> + Send>,
    ) -> KernelResult<()> {
        self.parquet_handler_for(location.as_object_store_url())?
            .write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<delta_kernel::ParquetFooter> {
        self.parquet_handler_for(file.as_object_store_url())?
            .read_parquet_footer(file)
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
                    self.json_handler_for(url)?.read_json_files(
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
    ) -> KernelResult<u64> {
        self.json_handler_for(path.as_object_store_url())?
            .write_json_file(path, data, overwrite)
    }
}
