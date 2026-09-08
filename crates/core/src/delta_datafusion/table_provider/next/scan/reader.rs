//! Parquet readers that validate file metadata and row counts from the full footer.

use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use datafusion::{
    common::{HashMap, Result, plan_err},
    datasource::physical_plan::parquet::{
        CachedParquetFileReaderFactory, ParquetFileReaderFactory,
    },
    execution::cache::cache_manager::FileMetadataCache,
    physical_plan::metrics::{ExecutionPlanMetricsSet, Gauge, MetricBuilder},
};
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use object_store::{ObjectStore, path::Path};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};

use super::metadata_cache::StoreMetadataCache;

#[derive(Clone, Debug)]
pub(super) struct FooterEvidence {
    pub records: u64,
}

pub(super) fn validate_footer(
    metadata: &ParquetMetaData,
) -> std::result::Result<FooterEvidence, String> {
    let records = u64::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| "negative footer row count")?;
    let mut total = 0_u64;
    let missing_ordinals = metadata
        .row_groups()
        .iter()
        .all(|group| group.ordinal().is_none());
    for (index, group) in metadata.row_groups().iter().enumerate() {
        if !missing_ordinals && group.ordinal().and_then(|n| usize::try_from(n).ok()) != Some(index)
        {
            return Err("inconsistent row group ordinals in complete footer".into());
        }
        let count = u64::try_from(group.num_rows()).map_err(|_| "negative row group count")?;
        total = total
            .checked_add(count)
            .ok_or("row group population overflows u64")?;
    }
    if total != records {
        return Err("row group population differs from footer row count".into());
    }
    Ok(FooterEvidence { records })
}

#[derive(Debug)]
pub(super) struct BoundParquetReaderFactory {
    store: Arc<dyn ObjectStore>,
    cache: Arc<FileMetadataCache>,
    files: HashMap<Path, (object_store::ObjectMeta, Option<u64>)>,
    hidden_position: Option<(String, String)>,
    footer_bounds: HashMap<Path, Vec<Arc<std::sync::OnceLock<u64>>>>,
}

impl BoundParquetReaderFactory {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        cache: Arc<FileMetadataCache>,
        files: HashMap<Path, (object_store::ObjectMeta, Option<u64>)>,
        hidden_position: Option<(String, String)>,
        footer_bounds: HashMap<Path, Vec<Arc<std::sync::OnceLock<u64>>>>,
    ) -> Self {
        Self {
            store,
            cache,
            files,
            hidden_position,
            footer_bounds,
        }
    }
}

impl ParquetFileReaderFactory for BoundParquetReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let Some((expected, count)) = self.files.get(&file.object_meta.location) else {
            return plan_err!("unplanned file in bound Parquet reader");
        };
        if &file.object_meta != expected
            || !file.extensions.is_empty()
            || file.arrow_schema.is_some()
        {
            return plan_err!(
                "bound Parquet reader received changed file metadata or reader extensions"
            );
        }
        let cache = Arc::new(StoreMetadataCache::new(
            Arc::clone(&self.cache),
            Arc::clone(&self.store),
            expected.clone(),
        ));
        let footer_bounds = self
            .footer_bounds
            .get(&file.object_meta.location)
            .cloned()
            .unwrap_or_default();
        let reader = CachedParquetFileReaderFactory::new(Arc::clone(&self.store), cache.clone())
            .create_reader(partition_index, file, metadata_size_hint, metrics)?;
        Ok(Box::new(BoundReader {
            active_metadata: MetricBuilder::new(metrics)
                .gauge("active_footer_reference_bytes", partition_index),
            reader,
            cache,
            count: *count,
            hidden_position: self.hidden_position.clone(),
            footer_bounds,
        }))
    }
}

struct BoundReader {
    // Estimated metadata bytes held by each reader. Shared allocations are counted
    // once per reader. Allocator overhead is excluded; this does not bound process memory.
    active_metadata: Gauge,
    reader: Box<dyn AsyncFileReader + Send>,
    cache: Arc<StoreMetadataCache>,
    count: Option<u64>,
    hidden_position: Option<(String, String)>,
    footer_bounds: Vec<Arc<std::sync::OnceLock<u64>>>,
}

impl Drop for BoundReader {
    fn drop(&mut self) {
        self.active_metadata.set(0);
    }
}

impl AsyncFileReader for BoundReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.reader.get_bytes(range)
    }
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        self.reader.get_byte_ranges(ranges)
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = self.reader.get_metadata(options).await?;
            self.active_metadata.set(metadata.memory_size());
            if let Some((hidden, file_id)) = &self.hidden_position {
                let evidence = self
                    .cache
                    .footer_evidence(&metadata)
                    .map_err(ParquetError::General)?;
                if self.count.is_some_and(|count| count != evidence.records) {
                    return Err(ParquetError::General(
                        "numRecords in the log differs from the footer row count".into(),
                    ));
                }
                for bound in &self.footer_bounds {
                    let count = bound.get_or_init(|| evidence.records);
                    if *count != evidence.records {
                        return Err(ParquetError::General(
                            "physical file population changed after planning".into(),
                        ));
                    }
                }
                if metadata
                    .file_metadata()
                    .schema_descr()
                    .root_schema()
                    .get_fields()
                    .iter()
                    .any(|field| field.name() == hidden || field.name() == file_id)
                {
                    return Err(ParquetError::General(
                        "Parquet field collides with physical input support column".into(),
                    ));
                }
            }
            Ok(metadata)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use chrono::{TimeZone, Utc};
    use datafusion::execution::cache::default_cache::DefaultCache;
    use object_store::{ObjectStoreExt, memory::InMemory};
    use parquet::arrow::ArrowWriter;

    async fn fixture(name: &str) -> (Arc<dyn ObjectStore>, object_store::ObjectMeta) {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("same.parquet");
        store.put(&path, bytes.into()).await.unwrap();
        let mut meta = store.head(&path).await.unwrap();
        meta.last_modified = Utc.timestamp_opt(0, 0).unwrap();
        meta.e_tag = None;
        meta.version = None;
        (store, meta)
    }

    async fn footer(
        store: Arc<dyn ObjectStore>,
        meta: object_store::ObjectMeta,
        cache: Arc<FileMetadataCache>,
    ) -> Arc<ParquetMetaData> {
        let factory = BoundParquetReaderFactory::new(
            store,
            cache,
            HashMap::from([(meta.location.clone(), (meta.clone(), Some(2)))]),
            Some(("position".into(), "__file_id".into())),
            HashMap::new(),
        );
        let mut reader = factory
            .create_reader(0, meta.into(), None, &ExecutionPlanMetricsSet::new())
            .unwrap();
        reader.get_metadata(None).await.unwrap()
    }

    #[tokio::test]
    async fn equal_validation_attributes_in_different_stores_do_not_alias() {
        let (a, meta_a) = fixture("aa").await;
        let (b, meta_b) = fixture("bb").await;
        assert_eq!(
            meta_a, meta_b,
            "fixture must match the path, size, and modification time"
        );
        let cache: Arc<FileMetadataCache> = Arc::new(DefaultCache::new(1 << 20));
        let first = footer(a.clone(), meta_a.clone(), cache.clone()).await;
        let second = footer(b, meta_b, cache.clone()).await;
        assert_eq!(
            first
                .file_metadata()
                .schema_descr()
                .root_schema()
                .get_fields()[0]
                .name(),
            "aa"
        );
        assert_eq!(
            second
                .file_metadata()
                .schema_descr()
                .root_schema()
                .get_fields()[0]
                .name(),
            "bb"
        );
        let fresh_query = footer(a, meta_a, cache).await;
        assert!(
            Arc::ptr_eq(&first, &fresh_query),
            "separately planned readers should reuse immutable metadata"
        );
    }

    #[tokio::test]
    async fn concurrent_fresh_readers_share_one_retention_allowance() {
        let cache: Arc<FileMetadataCache> = Arc::new(DefaultCache::new(4096));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let cache = cache.clone();
            tasks.spawn(async move {
                let (store, meta) = fixture("aa").await;
                footer(store, meta, cache).await
            });
        }
        let mut active_metadata = Vec::new();
        while let Some(result) = tasks.join_next().await {
            active_metadata.push(result.unwrap());
        }
        let retained: usize = cache
            .list_entries()
            .values()
            .map(|entry| entry.size_bytes)
            .sum();
        assert!(retained <= cache.cache_limit());
        assert!(
            cache.len() < active_metadata.len(),
            "fixture must evict while readers retain metadata"
        );
        assert!(
            active_metadata
                .iter()
                .all(|m| m.file_metadata().num_rows() == 2)
        );
    }

    #[tokio::test]
    async fn footer_evidence_rejects_bad_populations_and_ordinals() {
        let (store, meta) = fixture("aa").await;
        let original = footer(store, meta, Arc::new(DefaultCache::new(1 << 20))).await;
        let group = original.row_group(0);
        let missing = parquet::file::metadata::RowGroupMetaData::builder(group.schema_descr_ptr())
            .set_column_metadata(group.columns().to_vec())
            .set_num_rows(1)
            .build()
            .unwrap();
        let omitted = ParquetMetaData::new(
            original.file_metadata().clone(),
            vec![missing.clone(), missing.clone()],
        );
        assert_eq!(validate_footer(&omitted).unwrap().records, 2);
        let explicit = missing
            .clone()
            .into_builder()
            .set_ordinal(0)
            .build()
            .unwrap();
        let duplicate = ParquetMetaData::new(
            original.file_metadata().clone(),
            vec![explicit.clone(), explicit],
        );
        assert!(
            validate_footer(&duplicate)
                .unwrap_err()
                .contains("ordinals")
        );
        let mixed = ParquetMetaData::new(
            original.file_metadata().clone(),
            vec![
                missing.clone(),
                missing
                    .clone()
                    .into_builder()
                    .set_ordinal(1)
                    .build()
                    .unwrap(),
            ],
        );
        assert!(validate_footer(&mixed).is_err());
        let wrong_total = ParquetMetaData::new(original.file_metadata().clone(), vec![missing]);
        assert!(
            validate_footer(&wrong_total)
                .unwrap_err()
                .contains("population")
        );
    }

    #[tokio::test]
    async fn reader_rejects_unexpected_on_disk_support_fields() {
        for name in ["position", "__file_id"] {
            let (store, meta) = fixture(name).await;
            let factory = BoundParquetReaderFactory::new(
                store,
                Arc::new(DefaultCache::new(1 << 20)),
                HashMap::from([(meta.location.clone(), (meta.clone(), Some(2)))]),
                Some(("position".into(), "__file_id".into())),
                HashMap::new(),
            );
            let mut reader = factory
                .create_reader(0, meta.into(), None, &ExecutionPlanMetricsSet::new())
                .unwrap();
            assert!(
                reader
                    .get_metadata(None)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("support column")
            );
        }
    }

    #[tokio::test]
    async fn positional_reader_rejects_log_footer_count_mismatch() {
        let (store, meta) = fixture("aa").await;
        let factory = BoundParquetReaderFactory::new(
            store,
            Arc::new(DefaultCache::new(1 << 20)),
            HashMap::from([(meta.location.clone(), (meta.clone(), Some(3)))]),
            Some(("position".into(), "__file_id".into())),
            HashMap::new(),
        );
        let mut reader = factory
            .create_reader(0, meta.into(), None, &ExecutionPlanMetricsSet::new())
            .unwrap();
        assert!(
            reader
                .get_metadata(None)
                .await
                .unwrap_err()
                .to_string()
                .contains("numRecords")
        );
    }
}
