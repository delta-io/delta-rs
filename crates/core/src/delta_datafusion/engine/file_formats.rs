use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use datafusion::{
    config::TableParquetOptions,
    datasource::physical_plan::{
        JsonSource, ParquetSource, parquet::CachedParquetFileReaderFactory,
    },
    execution::{TaskContext, object_store::ObjectStoreUrl},
    physical_expr::planner::logical2physical,
    physical_plan::{ExecutionPlan, execute_stream},
    prelude::Expr,
};
use datafusion_datasource::{
    PartitionedFile, file_groups::FileGroup, file_scan_config::FileScanConfigBuilder,
    source::DataSourceExec,
};
use delta_kernel::{
    DeltaResult, EngineData, Error, FileDataReadResultIterator, FileMeta, FilteredEngineData,
    JsonHandler, ParquetHandler, PredicateRef,
    engine::{
        arrow_conversion::TryIntoArrow as _, arrow_data::ArrowEngineData,
        parse_json as arrow_parse_json, to_json_bytes,
    },
    schema::{DataType, SchemaRef},
};
use futures::TryStreamExt;
use itertools::Itertools as _;
use object_store::{PutMode, path::Path};
use tracing::instrument;
use url::Url;

use crate::delta_datafusion::engine::{
    AsObjectStoreUrl as _, BlockingStreamIterator, TracedHandle, predicate_to_df,
};

#[derive(Debug, Clone)]
pub struct DataFusionFileFormatHandler {
    handle: TracedHandle,
    ctx: Arc<TaskContext>,
}

impl DataFusionFileFormatHandler {
    pub fn new(ctx: Arc<TaskContext>, handle: super::TracedHandle) -> Self {
        Self { handle, ctx }
    }
}

impl ParquetHandler for DataFusionFileFormatHandler {
    #[instrument(skip(self, physical_schema))]
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let store_url = files
            .first()
            .expect("files is not empty")
            .location
            .as_object_store_url();
        let files = to_partitioned_files((store_url.clone(), files.iter().collect()))?.1;

        let arrow_schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into_arrow()?);
        let exec = parquet_exec(
            &self.ctx,
            files,
            arrow_schema,
            predicate
                .map(|p| predicate_to_df(p.as_ref(), &DataType::BOOLEAN))
                .transpose()
                .map_err(Error::generic_err)?,
            store_url,
        )?;

        execute_iter(exec, self.ctx.clone(), self.handle.clone())
    }

    fn read_parquet_footer(&self, _file: &FileMeta) -> DeltaResult<delta_kernel::ParquetFooter> {
        todo!()
    }

    fn write_parquet_file(
        &self,
        _location: url::Url,
        _data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
    ) -> DeltaResult<()> {
        todo!()
    }
}

impl JsonHandler for DataFusionFileFormatHandler {
    #[instrument(skip_all)]
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    #[instrument(skip(self, physical_schema))]
    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let store_url = files
            .first()
            .expect("files is not empty")
            .location
            .as_object_store_url();
        let files = to_partitioned_files((store_url.clone(), files.iter().collect()))?.1;

        let arrow_schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into_arrow()?);
        let exec = json_exec(store_url, files, arrow_schema);

        execute_iter(exec, self.ctx.clone(), self.handle.clone())
    }

    // note: for now we just buffer all the data and write it out all at once
    #[instrument(skip(self, data))]
    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        let buffer = to_json_bytes(data)?;
        let put_mode = if overwrite {
            PutMode::Overwrite
        } else {
            PutMode::Create
        };

        let store_url = path.as_object_store_url();
        let store = self
            .ctx
            .runtime_env()
            .object_store(store_url)
            .map_err(Error::generic_err)?;

        let path = Path::from_url_path(path.path())?;
        let path_str = path.to_string();
        self.handle
            .block_on(async move { store.put_opts(&path, buffer.into(), put_mode.into()).await })
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { .. } => Error::FileAlreadyExists(path_str),
                e => e.into(),
            })?;

        Ok(())
    }
}

fn execute_iter(
    exec: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
    task_executor: TracedHandle,
) -> DeltaResult<FileDataReadResultIterator> {
    let stream = execute_stream(exec, ctx)
        .map_err(Error::generic_err)?
        .map_err(Error::generic_err)
        .map_ok(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>);

    Ok(Box::new(BlockingStreamIterator {
        stream: Some(Box::pin(stream)),
        handle: task_executor,
    }))
}

fn parquet_exec(
    ctx: &Arc<TaskContext>,
    files: Vec<PartitionedFile>,
    read_schema: ArrowSchemaRef,
    predicate: Option<Expr>,
    store_url: ObjectStoreUrl,
) -> DeltaResult<Arc<dyn ExecutionPlan>> {
    let pq_options = TableParquetOptions {
        global: ctx.session_config().options().execution.parquet.clone(),
        ..Default::default()
    };

    let reader_factory = Arc::new(CachedParquetFileReaderFactory::new(
        ctx.runtime_env()
            .object_store(&store_url)
            .map_err(Error::generic_err)?,
        ctx.runtime_env().cache_manager.get_file_metadata_cache(),
    ));
    let mut file_source =
        ParquetSource::new(pq_options.clone()).with_parquet_file_reader_factory(reader_factory);

    if let Some(pred) = predicate {
        let physical = logical2physical(&pred, read_schema.as_ref());
        file_source = file_source
            .with_predicate(physical)
            .with_pushdown_filters(true);
    }

    let file_group: FileGroup = files.into_iter().collect();

    let config = FileScanConfigBuilder::new(store_url, read_schema.clone(), Arc::new(file_source))
        .with_file_group(file_group)
        .build();

    Ok(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>)
}

fn json_exec(
    store_url: ObjectStoreUrl,
    files: Vec<PartitionedFile>,
    arrow_schema: ArrowSchemaRef,
) -> Arc<dyn ExecutionPlan> {
    let config =
        FileScanConfigBuilder::new(store_url, arrow_schema, Arc::new(JsonSource::default()))
            .with_file_group(files.into_iter().collect())
            .build();
    DataSourceExec::from_data_source(config)
}

fn to_partitioned_files(
    arg: (ObjectStoreUrl, Vec<&FileMeta>),
) -> DeltaResult<(ObjectStoreUrl, Vec<PartitionedFile>)> {
    let (url, files) = arg;
    let part_files = files
        .into_iter()
        .map(|f| {
            let path = Path::from_url_path(f.location.path())?;
            let mut partitioned_file = PartitionedFile::new(path.to_string(), f.size);
            // NB: we need to reassign the location since the 'new' method does
            // incorrect or inconsistent encoding internally.
            partitioned_file.object_meta.location = path;
            Ok::<_, Error>(partitioned_file)
        })
        .try_collect::<_, Vec<_>, _>()?;
    Ok::<_, Error>((url, part_files))
}
