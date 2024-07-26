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
use crate::open_table_with_storage_options;
use crate::storage::*;
use crate::table::builder::ensure_table_uri;

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
pub struct ListingSchemaProvider {
    authority: String,
    /// Underlying object store
    store: Arc<dyn ObjectStore>,
    /// A map of table names to a fully quilfied storage location
    tables: DashMap<String, String>,
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
        let storage_options = storage_options.unwrap_or_default().into();
        // We already parsed the url, so unwrapping is safe.
        let store = store_for(&uri)?;
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
                self.tables.insert(table_name.to_string(), table_url);
            }
        }
        Ok(())
    }
}

// noramalizes a path fragment to be a valida table name in datafusion
// - removes some reserved characters (-, +, ., " ")
// - lowecase ascii
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
        let Some(location) = self.tables.get(name).map(|t| t.clone()) else {
            return Ok(None);
        };
        let provider =
            open_table_with_storage_options(location, self.storage_options.0.clone()).await?;
        Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "schema provider does not support registering tables".to_owned(),
        ))
    }

    fn deregister_table(
        &self,
        _name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "schema provider does not support deregistering tables".to_owned(),
        ))
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
    use datafusion::catalog_common::MemoryCatalogProvider;
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
