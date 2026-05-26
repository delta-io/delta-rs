//! Writer-side encryption support driven by Delta table properties.
//!
//! Encryption configuration is read from the table's `delta.encryption.*` properties
//! (stored in the Delta log) rather than passed as a runtime API parameter.
//! This means that once a table is created with encryption properties all subsequent
//! write operations automatically encrypt output files — no per-operation configuration
//! is required from the caller.
//!
//! # Write-time key flow
//!
//! 1. [`WriterEncryptionConfig::from_config`] reads `delta.encryption.*` from the
//!    table's [`TableConfiguration`].
//! 2. It looks up the user-registered [`EncryptionFactory`] from DataFusion's
//!    `RuntimeEnv` using the `delta.encryption.kms.id` property value.
//! 3. It wraps the factory in a [`KmsWriterPropertiesFactory`], which implements
//!    [`WriterPropertiesFactory`].
//! 4. Each new parquet file calls [`WriterPropertiesFactory::create_writer_properties`]
//!    **with the actual file path** so that the factory can derive the encryption key
//!    from the path (AAD — Additional Authenticated Data).

use std::sync::{Arc, LazyLock};

use dashmap::DashMap;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::config::EncryptionFactoryOptions;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::table::config::EncryptionConfig;
use delta_kernel::table_configuration::TableConfiguration;

// Re-export the factory types that are defined in the non-datafusion `writer_factory` module
// so callers can keep importing them from this module.
pub use crate::writer::writer_factory::{
    WriterPropertiesFactory, WriterPropertiesFactoryRef, default_writer_properties_factory,
    factory_from_writer_properties, snappy_writer_properties,
};

// ---------------------------------------------------------------------------
// KmsWriterPropertiesFactory — fetches per-file keys from a KMS via DataFusion
// ---------------------------------------------------------------------------

/// A [`WriterPropertiesFactory`] that derives per-file encryption keys from a KMS by
/// delegating to the DataFusion [`EncryptionFactory`] registered in `RuntimeEnv`.
///
/// Key material (footer key, column keys, plaintext-footer flag) is encoded in
/// `factory_options` and forwarded to the factory — see [`EncryptionConfig::factory_options`].
/// The factory itself is responsible for deriving the actual per-file key material.
#[derive(Debug)]
struct KmsWriterPropertiesFactory {
    base_properties: WriterProperties,
    encryption_factory: Arc<dyn EncryptionFactory>,
    factory_options: EncryptionFactoryOptions,
}

#[async_trait]
impl WriterPropertiesFactory for KmsWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.base_properties.compression(column_path)
    }

    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        let encryption_props = self
            .encryption_factory
            .get_file_encryption_properties(&self.factory_options, file_schema, file_path)
            .await?;

        let mut builder: WriterPropertiesBuilder = self.base_properties.clone().into();

        if let Some(enc_props) = encryption_props {
            builder = builder.with_file_encryption_properties(enc_props);
        }

        Ok(builder.build())
    }
}

// ---------------------------------------------------------------------------
// WriterEncryptionConfig — resolved from TableConfiguration + Session
// ---------------------------------------------------------------------------

/// Encryption configuration for the write path, resolved from Delta table properties.
///
/// Create via [`WriterEncryptionConfig::from_config`]; then pass
/// [`WriterEncryptionConfig::factory`] to [`WriterConfig::new`].
#[derive(Debug, Default)]
pub struct WriterEncryptionConfig {
    /// `None` when the table has no encryption properties.
    pub factory: Option<WriterPropertiesFactoryRef>,
}

impl WriterEncryptionConfig {
    /// Resolve from a [`TableConfiguration`] (used in `write_exec_plan` which receives
    /// `table_config: &TableConfiguration` directly).
    pub fn from_config(config: &TableConfiguration, session: &dyn Session) -> DeltaResult<Self> {
        // try_from_properties errors when kms.id is set but footer.key is missing,
        // preventing silent plaintext writes on partially-configured tables.
        let Some(enc) = EncryptionConfig::try_from_properties(config.table_properties())? else {
            return Ok(Self { factory: None });
        };
        // Check the session's RuntimeEnv first, then the global process-wide registry.
        // The global registry is needed because operations create their own internal sessions
        // that don't inherit the user's session factory registrations.
        let df_factory = resolve_encryption_factory_or_err(&enc.kms_id, session)?;
        Ok(Self {
            factory: Some(Self::build_factory(df_factory, enc.factory_options())),
        })
    }

    /// Resolve using only the global registry — for legacy writers that have no DataFusion session.
    ///
    /// Returns `Ok(None)` when the table has no encryption properties; errors if the
    /// table has `delta.encryption.kms.id` set but the factory is not registered.
    pub fn from_global_registry(enc: Option<EncryptionConfig>) -> DeltaResult<Self> {
        let Some(enc) = enc else {
            return Ok(Self { factory: None });
        };
        let df_factory = get_encryption_factory(&enc.kms_id)
            .ok_or_else(|| unregistered_factory_error(&enc.kms_id))?;
        Ok(Self {
            factory: Some(Self::build_factory(df_factory, enc.factory_options())),
        })
    }

    fn build_factory(
        encryption_factory: Arc<dyn EncryptionFactory>,
        factory_options: EncryptionFactoryOptions,
    ) -> WriterPropertiesFactoryRef {
        Arc::new(KmsWriterPropertiesFactory {
            base_properties: snappy_writer_properties(),
            encryption_factory,
            factory_options,
        })
    }
}

// ---------------------------------------------------------------------------
// Global EncryptionFactory registry
// ---------------------------------------------------------------------------

/// Process-wide registry for [`EncryptionFactory`] implementations.
///
/// Delta-rs operations create their own internal DataFusion sessions, which do not
/// automatically inherit factories registered in a user-created `SessionContext`.
/// This global registry bridges that gap: register your factory once here and all
/// delta-rs operations (write, read, optimize, etc.) will find it automatically.
///
/// ```rust,ignore
/// use deltalake_core::operations::write::encryption::register_encryption_factory;
///
/// register_encryption_factory("my-kms", Arc::new(MyFactory::new()));
/// ```
static GLOBAL_FACTORY_REGISTRY: LazyLock<DashMap<String, Arc<dyn EncryptionFactory>>> =
    LazyLock::new(DashMap::new);

/// Register an [`EncryptionFactory`] in the process-wide registry.
///
/// The `id` must match the value of `delta.encryption.kms.id` on any table that should
/// use this factory.  Registration persists for the lifetime of the process.
pub fn register_encryption_factory(id: impl Into<String>, factory: Arc<dyn EncryptionFactory>) {
    GLOBAL_FACTORY_REGISTRY.insert(id.into(), factory);
}

/// Look up a previously registered [`EncryptionFactory`] by id.
///
/// Returns `None` if no factory with that id has been registered.
pub fn get_encryption_factory(id: &str) -> Option<Arc<dyn EncryptionFactory>> {
    GLOBAL_FACTORY_REGISTRY
        .get(id)
        .map(|e| Arc::clone(e.value()))
}

/// Resolve an [`EncryptionFactory`] by looking in the session's `RuntimeEnv` first,
/// then falling back to the global registry.
pub fn resolve_encryption_factory(
    id: &str,
    session: &dyn datafusion::catalog::Session,
) -> Option<Arc<dyn EncryptionFactory>> {
    session
        .runtime_env()
        .parquet_encryption_factory(id)
        .ok()
        .or_else(|| get_encryption_factory(id))
}

/// Build the standard "factory not registered" error for a given `kms.id`.
pub(crate) fn unregistered_factory_error(id: &str) -> DeltaTableError {
    DeltaTableError::Generic(format!(
        "No EncryptionFactory registered for kms.id '{id}'. \
         Register one via `deltalake_core::operations::write::encryption::register_encryption_factory`."
    ))
}

/// Resolve an [`EncryptionFactory`] or return a descriptive error.
pub fn resolve_encryption_factory_or_err(
    id: &str,
    session: &dyn datafusion::catalog::Session,
) -> DeltaResult<Arc<dyn EncryptionFactory>> {
    resolve_encryption_factory(id, session).ok_or_else(|| unregistered_factory_error(id))
}
