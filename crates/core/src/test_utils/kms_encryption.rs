//! Mock KMS implementation for testing encryption via delta table properties.
//!
//! This module is **not part of the stable public API**. It lives in `test_utils` and is
//! compiled in non-test builds only to allow downstream integration-test crates to depend on it
//! without pulling in a separate crate. Do not rely on it for production use.
//!
//! # Usage
//!
//! 1. Create a [`MockKmsFactory`] and register it with a DataFusion session using
//!    [`datafusion::execution::RuntimeEnv::register_parquet_encryption_factory`].
//! 2. Create a Delta table with `delta.encryption.kms.id` set to the same ID.
//! 3. All subsequent read/write operations on the table will use the registered factory.
//!
//! ```rust,ignore
//! // Register factory at startup
//! let factory = Arc::new(MockKmsFactory::new());
//! session.runtime_env().register_parquet_encryption_factory("test-kms", factory.clone());
//!
//! // Create encrypted table
//! table.create()
//!     .with_property("delta.encryption.kms.id", "test-kms")
//!     .with_property("delta.encryption.footer.key", "my-key")
//!     .await?;
//! ```

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::config::EncryptionFactoryOptions;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use object_store::path::Path;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;

use crate::table::config::{
    FACTORY_OPT_COLUMN_KEYS, FACTORY_OPT_FOOTER_KEY, FACTORY_OPT_PLAINTEXT_FOOTER,
};

/// Mock encryption factory for use in tests.
///
/// Generates a unique key per (file, key-id) and stores it for later decryption.
/// Supports footer-only encryption, column-level encryption, and plaintext-footer mode
/// by reading the options forwarded from `delta.encryption.*` table properties.
///
/// Keys are keyed by (basename, key_id) so path-prefix differences between write and read
/// are handled correctly.
#[derive(Debug, Default)]
pub struct MockKmsFactory {
    /// Stores the actual encryption key for each (filename, key_id) pair.
    key_store: Mutex<HashMap<(Path, String), Vec<u8>>>,
    counter: AtomicU64,
}

impl MockKmsFactory {
    pub fn new() -> Self {
        Self {
            key_store: Mutex::new(HashMap::new()),
            counter: AtomicU64::new(0),
        }
    }

    /// Return the key for `(filename, key_id)`, creating a new one if it doesn't exist yet.
    fn get_or_create_key(&self, filename: &Path, key_id: &str) -> Vec<u8> {
        let mut store = self.key_store.lock().unwrap();
        store
            .entry((filename.clone(), key_id.to_string()))
            .or_insert_with(|| {
                let idx = self.counter.fetch_add(1, Ordering::Relaxed);
                let mut key = [0u8; 16];
                key[..8].copy_from_slice(&idx.to_le_bytes());
                key.to_vec()
            })
            .clone()
    }

    /// Look up the key for `(filename, key_id)`. Returns `None` if not found.
    fn lookup_key(&self, filename: &Path, key_id: &str) -> Option<Vec<u8>> {
        let store = self.key_store.lock().unwrap();
        store.get(&(filename.clone(), key_id.to_string())).cloned()
    }

    /// Parse `"keyId:col1,col2;keyId2:col3"` into `Vec<(key_id, Vec<col_name>)>`.
    fn parse_column_keys(value: &str) -> Vec<(String, Vec<String>)> {
        value
            .split(';')
            .filter_map(|seg| {
                let seg = seg.trim();
                let (key_id, cols) = seg.split_once(':')?;
                let cols: Vec<String> = cols
                    .split(',')
                    .map(|c| c.trim().to_string())
                    .filter(|c| !c.is_empty())
                    .collect();
                if cols.is_empty() {
                    None
                } else {
                    Some((key_id.trim().to_string(), cols))
                }
            })
            .collect()
    }
}

#[async_trait]
impl EncryptionFactory for MockKmsFactory {
    async fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        _schema: &Arc<ArrowSchema>,
        file_path: &Path,
    ) -> datafusion::error::Result<Option<Arc<FileEncryptionProperties>>> {
        let filename = Path::from(file_path.filename().unwrap_or(file_path.as_ref()));

        let footer_key_id = config
            .options
            .get(FACTORY_OPT_FOOTER_KEY)
            .map(|s| s.as_str())
            .unwrap_or("footer-key");
        let plaintext_footer = config
            .options
            .get(FACTORY_OPT_PLAINTEXT_FOOTER)
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);
        let column_keys_str = config
            .options
            .get(FACTORY_OPT_COLUMN_KEYS)
            .cloned()
            .unwrap_or_default();

        let footer_key = self.get_or_create_key(&filename, footer_key_id);

        let mut builder =
            FileEncryptionProperties::builder(footer_key).with_plaintext_footer(plaintext_footer);

        for (key_id, cols) in Self::parse_column_keys(&column_keys_str) {
            let col_key = self.get_or_create_key(&filename, &key_id);
            for col in &cols {
                builder = builder.with_column_key(col, col_key.clone());
            }
        }

        let props = builder.build()?;
        Ok(Some(props))
    }

    async fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> datafusion::error::Result<Option<Arc<FileDecryptionProperties>>> {
        let filename = Path::from(file_path.filename().unwrap_or(file_path.as_ref()));

        let footer_key_id = config
            .options
            .get(FACTORY_OPT_FOOTER_KEY)
            .map(|s| s.as_str())
            .unwrap_or("footer-key");
        let column_keys_str = config
            .options
            .get(FACTORY_OPT_COLUMN_KEYS)
            .cloned()
            .unwrap_or_default();

        let footer_key = self.lookup_key(&filename, footer_key_id).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "No encryption key found for file {file_path:?}"
            ))
        })?;

        let mut builder = FileDecryptionProperties::builder(footer_key);

        for (key_id, cols) in Self::parse_column_keys(&column_keys_str) {
            if let Some(col_key) = self.lookup_key(&filename, &key_id) {
                for col in &cols {
                    builder = builder.with_column_key(col, col_key.clone());
                }
            }
        }

        let props = builder.build()?;
        Ok(Some(props))
    }
}
