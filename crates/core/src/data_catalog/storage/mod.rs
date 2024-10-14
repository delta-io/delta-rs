//! listing_schema contains a SchemaProvider that scans ObjectStores for tables automatically
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion_common::DataFusionError;
use futures::TryStreamExt;
use object_store::ObjectStore;

use crate::errors::DeltaResult;
use crate::table::builder::ensure_table_uri;
use crate::DeltaTableBuilder;
use crate::{storage::*, DeltaTable};

const DELTA_LOG_FOLDER: &str = "_delta_log";

/// A `SchemaProvider` that scans an `ObjectStore` to automatically discover delta tables.
///
/// A subfolder relationship is assumed, i.e. given:
/// authority = s3://host.example.com:3000
/// path = /data/tpch
///
/// A table called "customer" will be registered for the folder:
/// s3://host.example.com:3000/data/tpch/customer
///
/// assuming it contains valid deltalake data, i.e a `_delta_log` folder:
/// s3://host.example.com:3000/data/tpch/customer/_delta_log/
#[derive(Debug)]
pub struct ListingSchemaProvider {
    authority: String,
    /// Underlying object store
    store: Arc<dyn ObjectStore>,
    /// A map of table names to a fully quilfied storage location
    tables: DashMap<String, Arc<dyn TableProvider>>,
    /// Options used to create underlying object stores
    storage_options: StorageOptions,
}

impl ListingSchemaProvider {
    /// Create a new [`ListingSchemaProvider`]
    pub fn try_new(
        root_uri: impl AsRef<str>,
        storage_options: Option<HashMap<String, String>>,
    ) -> DeltaResult<Self> {
        let uri = ensure_table_uri(root_uri)?;
        let storage_options: StorageOptions = storage_options.unwrap_or_default().into();
        // We already parsed the url, so unwrapping is safe.
        let store = store_for(&uri, &storage_options)?;
        Ok(Self {
            authority: uri.to_string(),
            store,
            tables: DashMap::new(),
            storage_options,
        })
    }

    /// Reload table information from ObjectStore
    pub async fn refresh(&self) -> datafusion_common::Result<()> {
        let entries: Vec<_> = self.store.list(None).try_collect().await?;
        let mut tables = HashSet::new();
        for file in entries.iter() {
            let mut parent = Path::new(file.location.as_ref());
            while let Some(p) = parent.parent() {
                if parent.ends_with(DELTA_LOG_FOLDER) {
                    tables.insert(p);
                    break;
                }
                parent = p;
            }
        }

        for table in tables.into_iter() {
            let table_name = normalize_table_name(table)?;
            let table_path = table
                .to_str()
                .ok_or_else(|| DataFusionError::Internal("Cannot parse file name!".to_string()))?
                .to_string();
            if !self.table_exist(&table_name) {
                let table_url = format!("{}/{}", self.authority, table_path);
                let Ok(delta_table) = DeltaTableBuilder::from_uri(table_url)
                    .with_storage_options(self.storage_options.0.clone())
                    .build()
                else {
                    continue;
                };
                let _ = self.register_table(table_name, Arc::new(delta_table));
            }
        }
        Ok(())
    }

    /// Tables are not initialized but have a reference setup. To initialize the delta
    /// table, the `load()` function must be called on the delta table. This function helps with
    /// that and ensures the DashMap is updated
    pub async fn load_table(&self, table_name: &str) -> datafusion::common::Result<()> {
        if let Some(mut table) = self.tables.get_mut(&table_name.to_string()) {
            if let Some(delta_table) = table.value().as_any().downcast_ref::<DeltaTable>() {
                // If table has not yet been loaded, we remove it from the tables map and add it again
                if delta_table.state.is_none() {
                    let mut delta_table = delta_table.clone();
                    delta_table.load().await?;
                    *table = Arc::from(delta_table);
                }
            }
        }

        Ok(())
    }
}

// normalizes a path fragment to be a valida table name in datafusion
// - removes some reserved characters (-, +, ., " ")
// - lowercase ascii
fn normalize_table_name(path: &Path) -> Result<String, DataFusionError> {
    Ok(path
        .file_name()
        .ok_or_else(|| DataFusionError::Internal("Cannot parse file name!".to_string()))?
        .to_str()
        .ok_or_else(|| DataFusionError::Internal("Cannot parse file name!".to_string()))?
        .replace(['-', '.', ' '], "_")
        .to_ascii_lowercase())
}

#[async_trait]
impl SchemaProvider for ListingSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|t| t.key().clone()).collect()
    }

    async fn table(&self, name: &str) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        let Some(provider) = self.tables.get(name).map(|t| t.clone()) else {
            return Ok(None);
        };
        Ok(Some(provider))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        if !self.table_exist(name.as_str()) {
            self.tables.insert(name, table.clone());
        }
        Ok(Some(table))
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table) = self.tables.remove(name) {
            return Ok(Some(table.1));
        }
        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::catalog::CatalogProvider;
    use datafusion::catalog::MemoryCatalogProvider;
    use datafusion::execution::context::SessionContext;

    #[test]
    fn test_normalize_table_name() {
        let cases = vec![
            (Path::new("Table Name"), "table_name"),
            (Path::new("Table.Name"), "table_name"),
            (Path::new("Table-Name"), "table_name"),
        ];
        for (raw, expected) in cases {
            assert_eq!(normalize_table_name(raw).unwrap(), expected.to_string())
        }
    }

    #[tokio::test]
    async fn test_table_names() {
        let fs = ListingSchemaProvider::try_new("../test/tests/data/", None).unwrap();
        fs.refresh().await.unwrap();
        let table_names = fs.table_names();
        assert!(table_names.len() > 20);
        assert!(table_names.contains(&"simple_table".to_string()))
    }

    #[tokio::test]
    async fn test_query_table() {
        let schema = Arc::new(ListingSchemaProvider::try_new("../test/tests/data/", None).unwrap());
        schema.refresh().await.unwrap();
        schema.load_table("simple_table").await.unwrap();

        let ctx = SessionContext::new();
        let catalog = Arc::new(MemoryCatalogProvider::default());
        catalog.register_schema("test", schema).unwrap();
        ctx.register_catalog("delta", catalog);

        let data = ctx
            .sql("SELECT * FROM delta.test.simple_table")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+",
        ];

        assert_batches_sorted_eq!(&expected, &data);
    }
}
