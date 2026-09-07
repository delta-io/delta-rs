//! Store-qualified views of the session cache. The backing cache owns the single
//! retention allowance, including entries from concurrent, independently planned scans.

use std::{
    any::Any,
    sync::{Arc, Weak},
    time::Duration,
};

use datafusion::{
    common::{HashMap, Result, TableReference},
    execution::cache::{
        Cache, CacheEntryInfo,
        cache_manager::{CachedFileMetadataEntry, FileMetadata, FileMetadataCache},
    },
};
use object_store::{ObjectMeta, ObjectStore, path::Path};

#[derive(Debug)]
pub(super) struct StoreMetadataCache {
    backing: Arc<FileMetadataCache>,
    store: Arc<dyn ObjectStore>,
    object: ObjectMeta,
}

struct TaggedMetadata {
    // Weak retains the allocation identity, preventing address reuse without
    // retaining an unregistered store's data for the cache entry's lifetime.
    store: Weak<dyn ObjectStore>,
    value: Arc<dyn FileMetadata>,
    object: ObjectMeta,
    footer: Option<std::result::Result<super::reader::FooterEvidence, String>>,
}

impl FileMetadata for TaggedMetadata {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn memory_size(&self) -> usize {
        self.value.memory_size()
            + std::mem::size_of::<Self>()
            + self.object.location.as_ref().len()
            + self.object.e_tag.as_ref().map_or(0, String::len)
            + self.object.version.as_ref().map_or(0, String::len)
            + self
                .footer
                .as_ref()
                .and_then(|proof| proof.as_ref().err())
                .map_or(0, String::len)
    }
    fn extra_info(&self) -> HashMap<String, String> {
        self.value.extra_info()
    }
}

impl StoreMetadataCache {
    pub(super) fn new(
        backing: Arc<FileMetadataCache>,
        store: Arc<dyn ObjectStore>,
        object: ObjectMeta,
    ) -> Self {
        Self {
            backing,
            store,
            object,
        }
    }

    pub(super) fn footer_evidence(
        &self,
        metadata: &Arc<parquet::file::metadata::ParquetMetaData>,
    ) -> std::result::Result<super::reader::FooterEvidence, String> {
        if let Some(entry) = self.backing.get(&self.key(&self.object.location))
            && let Some(tagged) = entry.file_metadata.as_any().downcast_ref::<TaggedMetadata>()
            && Weak::ptr_eq(&Arc::downgrade(&self.store), &tagged.store)
            && tagged.object == self.object
            && let Some(cached) = tagged.value.as_any().downcast_ref::<datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData>()
            && Arc::ptr_eq(cached.parquet_metadata(), metadata)
            && let Some(evidence) = &tagged.footer
        {
            return evidence.clone();
        }
        super::reader::validate_footer(metadata)
    }

    fn key(&self, path: &Path) -> Path {
        // The store allocation and canonical path qualify each key. Tagged values
        // distinguish these entries from bare-path entries and verify full identity.
        let address = Arc::as_ptr(&self.store) as *const () as usize;
        Path::from(format!("__delta_rs_metadata/{address:x}/{}", path.as_ref()))
    }

    fn untag(&self, entry: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        let tagged = entry
            .file_metadata
            .as_any()
            .downcast_ref::<TaggedMetadata>()?;
        if !Weak::ptr_eq(&tagged.store, &Arc::downgrade(&self.store))
            || tagged.object != self.object
        {
            return None;
        }
        Some(CachedFileMetadataEntry::new(
            tagged.object.clone(),
            Arc::clone(&tagged.value),
        ))
    }
}

impl Cache<Path, CachedFileMetadataEntry> for StoreMetadataCache {
    fn get(&self, path: &Path) -> Option<CachedFileMetadataEntry> {
        if path != &self.object.location {
            return None;
        }
        self.untag(self.backing.get(&self.key(path))?)
    }
    fn put(&self, path: &Path, value: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        if path != &self.object.location || value.meta != self.object {
            return None;
        }
        let footer = value.file_metadata.as_any().downcast_ref::<datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData>().map(|v| super::reader::validate_footer(v.parquet_metadata()));
        let tagged = Arc::new(TaggedMetadata {
            footer,
            store: Arc::downgrade(&self.store),
            object: value.meta.clone(),
            value: value.file_metadata,
        });
        self.backing
            .put(
                &self.key(path),
                CachedFileMetadataEntry::new(value.meta, tagged),
            )
            .and_then(|v| self.untag(v))
    }
    fn remove(&self, path: &Path) -> Option<CachedFileMetadataEntry> {
        self.backing
            .remove(&self.key(path))
            .and_then(|v| self.untag(v))
    }
    fn contains_key(&self, path: &Path) -> bool {
        self.get(path).is_some()
    }
    fn len(&self) -> usize {
        self.list_entries().len()
    }
    fn clear(&self) {
        self.remove(&self.object.location);
    }
    fn name(&self) -> String {
        self.backing.name()
    }
    fn cache_limit(&self) -> usize {
        self.backing.cache_limit()
    }
    fn update_cache_limit(&self, limit: usize) {
        self.backing.update_cache_limit(limit);
    }
    fn cache_ttl(&self) -> Option<Duration> {
        self.backing.cache_ttl()
    }
    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        self.backing.update_cache_ttl(ttl);
    }
    fn drop_table_entries(&self, table: &TableReference) -> Result<()> {
        self.backing.drop_table_entries(table)
    }
    fn list_entries(&self) -> HashMap<Path, CacheEntryInfo<CachedFileMetadataEntry>> {
        self.backing
            .list_entries()
            .into_iter()
            .filter_map(|(key, info)| {
                if key != self.key(&self.object.location) {
                    return None;
                }
                let value = self.untag(info.value)?;
                Some((
                    self.object.location.clone(),
                    CacheEntryInfo {
                        value,
                        size_bytes: info.size_bytes,
                        hits: info.hits,
                        expires: info.expires,
                    },
                ))
            })
            .collect()
    }
}
