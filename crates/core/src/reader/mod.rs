use std::future::ready;
use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use futures::executor::block_on;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataLoader};
use parquet::errors::ParquetError;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ParquetMetaData;

/// Wrapper for ParquetObjectReader that does additional retries
#[derive(Clone, Debug)]
pub struct CloudParquetObjectReader {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    metadata_size_hint: Option<usize>,
    preload_column_index: bool,
    preload_offset_index: bool,
}
#[allow(dead_code)]
impl CloudParquetObjectReader {
    /// Creates a new [`CloudParquetObjectReader`] for the provided [`ObjectStore`] and [`ObjectMeta`]
    ///
    /// [`ObjectMeta`] can be obtained using [`ObjectStore::list`] or [`ObjectStore::head`]
    pub fn new(store: Arc<dyn ObjectStore>, meta: ObjectMeta) -> Self {
        Self {
            store,
            meta,
            metadata_size_hint: None,
            preload_column_index: false,
            preload_offset_index: false,
        }
    }

    /// Provide a hint as to the size of the parquet file's footer,
    /// see [fetch_parquet_metadata](crate::arrow::async_reader::fetch_parquet_metadata)
    pub fn with_footer_size_hint(self, hint: usize) -> Self {
        Self {
            metadata_size_hint: Some(hint),
            ..self
        }
    }

    /// Load the Column Index as part of [`Self::get_metadata`]
    pub fn with_preload_column_index(self, preload_column_index: bool) -> Self {
        Self {
            preload_column_index,
            ..self
        }
    }

    /// Load the Offset Index as part of [`Self::get_metadata`]
    pub fn with_preload_offset_index(self, preload_offset_index: bool) -> Self {
        Self {
            preload_offset_index,
            ..self
        }
    }
}

impl AsyncFileReader for CloudParquetObjectReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let mut retries = 5;
        loop {
            let future_result = self
                .store
                .get_range(&self.meta.location, range.clone())
                .map_err(|e| {
                    ParquetError::General(format!("AsyncChunkReader::get_bytes error: {e}"))
                })
                .boxed();

            let result = block_on(future_result);
            match result {
                Ok(bytes) => return Box::pin(ready(Ok(bytes))),
                Err(err) => {
                    if retries == 0 {
                        return Box::pin(ready(Err(err)));
                    }
                    retries -= 1;
                }
            }
        }
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>>
    where
        Self: Send,
    {
        async move {
            let mut retries = 5;
            loop {
                let future_result = self
                    .store
                    .get_ranges(&self.meta.location, &ranges)
                    .await
                    .map_err(|e| {
                        ParquetError::General(format!(
                            "ParquetObjectReader::get_byte_ranges error: {e}"
                        ))
                    });
                match future_result {
                    Ok(bytes) => return Ok(bytes),
                    Err(err) => {
                        if retries == 0 {
                            return Err(err);
                        }
                        retries -= 1;
                    }
                }
            }
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let preload_column_index = self.preload_column_index;
            let preload_offset_index = self.preload_offset_index;
            let file_size = self.meta.size;
            let prefetch = self.metadata_size_hint;
            let mut loader = MetadataLoader::load(self, file_size, prefetch).await?;
            loader
                .load_page_index(preload_column_index, preload_offset_index)
                .await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}
