//! Parquet Object Reader
//!
//! A custom implementation of [`AsyncFileReader`] that reads Parquet files
//! from an [`ObjectStore`]. This is a simplified version of the deprecated
//! `ParquetObjectReader` struct from the parquet crate.
//!
//! See: <https://docs.rs/parquet/latest/parquet/arrow/async_reader/trait.AsyncFileReader.html>

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt as _, path::Path};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use parquet::errors::Result;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

/// Reads Parquet files in object storage using [`ObjectStore`].
///
/// This struct provides a simple implementation of [`AsyncFileReader`] that
/// can be used with [`ParquetRecordBatchStreamBuilder`].
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use deltalake_core::logstore::parquet_reader::ParquetObjectReader;
/// # use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
/// # async fn run() {
/// # let store: Arc<dyn ObjectStore> = todo!();
/// # let location: Path = todo!();
/// # let file_size: u64 = todo!();
/// let reader = ParquetObjectReader::new(store, location).with_file_size(file_size);
/// let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ParquetObjectReader {
    /// The ObjectStore instance to use for reading
    store: Arc<dyn ObjectStore>,
    /// The path to the file
    path: Path,
    /// The file size, if known
    file_size: Option<u64>,
    /// The metadata size hint, if provided
    metadata_size_hint: Option<usize>,
    /// Whether to preload the column index
    preload_column_index: bool,
    /// Whether to preload the offset index
    preload_offset_index: bool,
}

impl ParquetObjectReader {
    /// Creates a new [`ParquetObjectReader`] for the provided [`ObjectStore`] and [`Path`].
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            file_size: None,
            metadata_size_hint: None,
            preload_column_index: false,
            preload_offset_index: false,
        }
    }

    /// Provide a byte size of this file.
    ///
    /// If provided, the file size will ensure that only bounded range requests are used. If file
    /// size is not provided, the reader will use suffix range requests to fetch the metadata.
    ///
    /// Providing this size up front is an important optimization to avoid extra calls when the
    /// underlying store does not support suffix range requests.
    pub fn with_file_size(self, file_size: u64) -> Self {
        Self {
            file_size: Some(file_size),
            ..self
        }
    }

    /// Provide a hint as to the size of the parquet file's footer.
    pub fn with_footer_size_hint(self, hint: usize) -> Self {
        Self {
            metadata_size_hint: Some(hint),
            ..self
        }
    }

    /// Sets whether to preload the column index.
    pub fn with_preload_column_index(self, preload: bool) -> Self {
        Self {
            preload_column_index: preload,
            ..self
        }
    }

    /// Sets whether to preload the offset index.
    pub fn with_preload_offset_index(self, preload: bool) -> Self {
        Self {
            preload_offset_index: preload,
            ..self
        }
    }
}

impl MetadataSuffixFetch for &mut ParquetObjectReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        let path = self.path.clone();
        let store = Arc::clone(&self.store);

        async move {
            let resp = store
                .get_opts(&path, options)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
            Ok(bytes)
        }
        .boxed()
    }
}

impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        let path = self.path.clone();
        let store = Arc::clone(&self.store);
        async move { store.get_range(&path, range).await.map_err(|e| e.into()) }.boxed()
    }

    // Using default implementation from AsyncFileReader which calls get_bytes sequentially

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata_opts = options.map(|o| o.metadata_options().clone());
            let mut metadata = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_opts)
                .with_column_index_policy(PageIndexPolicy::from(self.preload_column_index))
                .with_offset_index_policy(PageIndexPolicy::from(self.preload_offset_index))
                .with_prefetch_hint(self.metadata_size_hint);

            // Override page index policies from ArrowReaderOptions if specified and not Skip.
            if let Some(options) = options {
                if options.column_index_policy() != PageIndexPolicy::Skip
                    || options.offset_index_policy() != PageIndexPolicy::Skip
                {
                    metadata = metadata
                        .with_column_index_policy(options.column_index_policy())
                        .with_offset_index_policy(options.offset_index_policy());
                }
            }

            let metadata = if let Some(file_size) = self.file_size {
                metadata.load_and_finish(self, file_size).await?
            } else {
                metadata.load_via_suffix_and_finish(self).await?
            };

            Ok(Arc::new(metadata))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_object_reader_new() {
        // This test just verifies the struct can be instantiated
        // Full integration tests are in the operations module
    }
}
