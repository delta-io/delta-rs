//! High level operations API to interact with Delta tables
//!
//! At the heart of the high level operations APIs is the [`DeltaOps`] struct,
//! which consumes a [`DeltaTable`] and exposes methods to attain builders for
//! several high level operations. The specific builder structs allow fine-tuning
//! the operations' behaviors and will return an updated table potentially in conjunction
//! with a [data stream][datafusion::physical_plan::SendableRecordBatchStream],
//! if the operation returns data as well.
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "datafusion")]
use arrow::array::RecordBatch;
use async_trait::async_trait;
#[cfg(feature = "datafusion")]
pub use datafusion::physical_plan::common::collect as collect_sendable_stream;
use delta_kernel::table_properties::{DataSkippingNumIndexedCols, TableProperties};
use url::Url;
use uuid::Uuid;

use self::{
    add_column::AddColumnBuilder, add_feature::AddTableFeatureBuilder, create::CreateBuilder,
    filesystem_check::FileSystemCheckBuilder, restore::RestoreBuilder,
    set_tbl_properties::SetTablePropertiesBuilder,
    update_field_metadata::UpdateFieldMetadataBuilder,
    update_table_metadata::UpdateTableMetadataBuilder, vacuum::VacuumBuilder,
};
#[cfg(feature = "datafusion")]
use self::{
    constraints::ConstraintBuilder, delete::DeleteBuilder, drop_constraints::DropConstraintBuilder,
    load::LoadBuilder, load_cdf::CdfLoadBuilder, merge::MergeBuilder, optimize::OptimizeBuilder,
    update::UpdateBuilder, write::WriteBuilder,
};
use crate::DeltaTable;
#[cfg(feature = "datafusion")]
use crate::delta_datafusion::Expression;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::logstore::LogStoreRef;
use crate::operations::generate::GenerateBuilder;
use crate::table::builder::DeltaTableBuilder;
use crate::table::config::{DEFAULT_NUM_INDEX_COLS, TablePropertiesExt as _};

pub mod add_column;
pub mod add_feature;
pub mod convert_to_delta;
pub mod create;
pub mod drop_constraints;
pub mod filesystem_check;
pub mod generate;
pub mod restore;
pub mod update_field_metadata;
pub mod update_table_metadata;
pub mod vacuum;

#[cfg(feature = "datafusion")]
mod cdc;
#[cfg(feature = "datafusion")]
pub mod constraints;
#[cfg(feature = "datafusion")]
pub mod delete;
#[cfg(feature = "datafusion")]
mod load;
#[cfg(feature = "datafusion")]
pub mod load_cdf;
#[cfg(feature = "datafusion")]
pub mod merge;
#[cfg(feature = "datafusion")]
pub mod optimize;
pub mod set_tbl_properties;
#[cfg(feature = "datafusion")]
pub mod update;
#[cfg(feature = "datafusion")]
pub mod write;

#[cfg(all(test, feature = "datafusion"))]
mod session_fallback_policy_tests;

impl DeltaTable {
    /// Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given URL.
    ///
    /// ```
    /// use deltalake_core::DeltaTable;
    /// use url::Url;
    ///
    /// async {
    ///     let url = Url::parse("memory:///").unwrap();
    ///     let ops = DeltaTable::try_from_url(url).await.unwrap();
    /// };
    /// ```
    pub async fn try_from_url(uri: Url) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_url(uri)?.build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table),
            Err(DeltaTableError::NotATable(_)) => Ok(table),
            Err(err) => Err(err),
        }
    }

    /// Create a [`DeltaTable`] instance from URL with storage options
    pub async fn try_from_url_with_storage_options(
        uri: Url,
        storage_options: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_url(uri)?
            .with_storage_options(storage_options)
            .build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table),
            Err(DeltaTableError::NotATable(_)) => Ok(table),
            Err(err) => Err(err),
        }
    }

    #[must_use]
    pub fn create(&self) -> CreateBuilder {
        CreateBuilder::default().with_log_store(self.log_store())
    }

    #[must_use]
    pub fn restore(self) -> RestoreBuilder {
        RestoreBuilder::new(
            self.log_store(),
            self.state.clone().map(|state| state.snapshot),
        )
    }

    /// Vacuum stale files from delta table
    #[must_use]
    pub fn vacuum(self) -> VacuumBuilder {
        VacuumBuilder::new(
            self.log_store(),
            self.state.clone().map(|state| state.snapshot),
        )
    }

    /// Audit active files with files present on the filesystem
    #[must_use]
    pub fn filesystem_check(self) -> FileSystemCheckBuilder {
        FileSystemCheckBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Enable a table feature for a table
    #[must_use]
    pub fn add_feature(self) -> AddTableFeatureBuilder {
        AddTableFeatureBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Set table properties
    #[must_use]
    pub fn set_tbl_properties(self) -> SetTablePropertiesBuilder {
        SetTablePropertiesBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Add new columns
    #[must_use]
    pub fn add_columns(self) -> AddColumnBuilder {
        AddColumnBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Update field metadata
    #[must_use]
    pub fn update_field_metadata(self) -> UpdateFieldMetadataBuilder {
        UpdateFieldMetadataBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Update table metadata
    #[must_use]
    pub fn update_table_metadata(self) -> UpdateTableMetadataBuilder {
        UpdateTableMetadataBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Generate a symlink_format_manifest for other engines
    pub fn generate(self) -> GenerateBuilder {
        GenerateBuilder::new(self.log_store(), self.state.map(|s| s.snapshot))
    }
}

#[cfg(feature = "datafusion")]
impl DeltaTable {
    #[must_use]
    pub fn scan_table(&self) -> LoadBuilder {
        LoadBuilder::new(
            self.log_store(),
            self.state.clone().map(|state| state.snapshot),
        )
    }

    /// Load a table with CDF Enabled
    #[must_use]
    pub fn scan_cdf(self) -> CdfLoadBuilder {
        CdfLoadBuilder::new(self.log_store(), self.state.map(|s| s.snapshot))
    }

    #[must_use]
    pub fn write(self, batches: impl IntoIterator<Item = RecordBatch>) -> WriteBuilder {
        WriteBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
            .with_input_batches(batches)
    }

    /// Audit active files with files present on the filesystem
    #[must_use]
    pub fn optimize<'a>(self) -> OptimizeBuilder<'a> {
        OptimizeBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Delete data from Delta table
    #[must_use]
    pub fn delete(self) -> DeleteBuilder {
        DeleteBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Update data from Delta table
    #[must_use]
    pub fn update(self) -> UpdateBuilder {
        UpdateBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Update data from Delta table
    #[must_use]
    pub fn merge<E: Into<Expression>>(
        self,
        source: datafusion::prelude::DataFrame,
        predicate: E,
    ) -> MergeBuilder {
        MergeBuilder::new(
            self.log_store(),
            self.state.clone().map(|s| s.snapshot),
            predicate.into(),
            source,
        )
    }

    /// Add a check constraint to a table
    #[must_use]
    pub fn add_constraint(self) -> ConstraintBuilder {
        ConstraintBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }

    /// Drops constraints from a table
    #[must_use]
    pub fn drop_constraints(self) -> DropConstraintBuilder {
        DropConstraintBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
    }
}

#[async_trait]
pub trait CustomExecuteHandler: Send + Sync {
    // Execute arbitrary code at the start of a delta operation
    async fn pre_execute(&self, log_store: &LogStoreRef, operation_id: Uuid) -> DeltaResult<()>;

    // Execute arbitrary code at the end of a delta operation
    async fn post_execute(&self, log_store: &LogStoreRef, operation_id: Uuid) -> DeltaResult<()>;

    // Execute arbitrary code at the start of the post commit hook
    async fn before_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operation: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()>;

    // Execute arbitrary code at the end of the post commit hook
    async fn after_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operation: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()>;
}

#[allow(unused)]
/// The [Operation] trait defines common behaviors that all operations builders
/// should have consistent
pub(crate) trait Operation: std::future::IntoFuture {
    fn log_store(&self) -> &LogStoreRef;
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>>;
    async fn pre_execute(&self, operation_id: Uuid) -> DeltaResult<()> {
        if let Some(handler) = self.get_custom_execute_handler() {
            handler.pre_execute(self.log_store(), operation_id).await
        } else {
            Ok(())
        }
    }

    async fn post_execute(&self, operation_id: Uuid) -> DeltaResult<()> {
        if let Some(handler) = self.get_custom_execute_handler() {
            handler.post_execute(self.log_store(), operation_id).await
        } else {
            Ok(())
        }
    }

    fn get_operation_id(&self) -> uuid::Uuid {
        Uuid::new_v4()
    }
}

/// High level interface for executing commands against a DeltaTable
#[deprecated(note = "Use methods directly on DeltaTable instead, e.g. `delta_table.create()`")]
pub struct DeltaOps(pub DeltaTable);

#[allow(deprecated)]
impl DeltaOps {
    /// Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given URL.
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    /// use url::Url;
    ///
    /// async {
    ///     let url = Url::parse("memory:///").unwrap();
    ///     let ops = DeltaOps::try_from_url(url).await.unwrap();
    /// };
    /// ```
    pub async fn try_from_url(uri: Url) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_url(uri)?.build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// Create a [`DeltaOps`] instance from URL with storage options
    pub async fn try_from_url_with_storage_options(
        uri: Url,
        storage_options: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_url(uri)?
            .with_storage_options(storage_options)
            .build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// Create a new [`DeltaOps`] instance, backed by an un-initialized in memory table
    ///
    /// Using this will not persist any changes beyond the lifetime of the table object.
    /// The main purpose of in-memory tables is for use in testing.
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    ///
    /// let ops = DeltaOps::new_in_memory();
    /// ```
    #[must_use]
    pub fn new_in_memory() -> Self {
        let url = Url::parse("memory:///").unwrap();
        DeltaTableBuilder::from_url(url)
            .unwrap()
            .build()
            .unwrap()
            .into()
    }

    /// Create a new Delta table
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_url(url::Url::parse("memory://").unwrap()).await.unwrap();
    ///     let table = ops.create().with_table_name("my_table").await.unwrap();
    ///     assert_eq!(table.version(), Some(0));
    /// };
    /// ```
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::create`] instead")]
    pub fn create(self) -> CreateBuilder {
        CreateBuilder::default().with_log_store(self.0.log_store)
    }

    /// Generate a symlink_format_manifest for other engines
    #[deprecated(note = "Use [`DeltaTable::generate`] instead")]
    pub fn generate(self) -> GenerateBuilder {
        GenerateBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Load data from a DeltaTable
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::scan`] instead")]
    pub fn load(self) -> LoadBuilder {
        LoadBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Load a table with CDF Enabled
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::scan_cdf`] instead")]
    pub fn load_cdf(self) -> CdfLoadBuilder {
        CdfLoadBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Write data to Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::write`] instead")]
    pub fn write(self, batches: impl IntoIterator<Item = RecordBatch>) -> WriteBuilder {
        WriteBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
            .with_input_batches(batches)
    }

    /// Vacuum stale files from delta table
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::vacuum`] instead")]
    pub fn vacuum(self) -> VacuumBuilder {
        VacuumBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Audit and repair active files with files present on the filesystem
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::filesystem_check`] instead")]
    pub fn filesystem_check(self) -> FileSystemCheckBuilder {
        FileSystemCheckBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Audit active files with files present on the filesystem
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::optimize`] instead")]
    pub fn optimize<'a>(self) -> OptimizeBuilder<'a> {
        OptimizeBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Delete data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::delete`] instead")]
    pub fn delete(self) -> DeleteBuilder {
        DeleteBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::update`] instead")]
    pub fn update(self) -> UpdateBuilder {
        UpdateBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Restore delta table to a specified version or datetime
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::restore`] instead")]
    pub fn restore(self) -> RestoreBuilder {
        RestoreBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::merge`] instead")]
    pub fn merge<E: Into<Expression>>(
        self,
        source: datafusion::prelude::DataFrame,
        predicate: E,
    ) -> MergeBuilder {
        MergeBuilder::new(
            self.0.log_store,
            self.0.state.map(|s| s.snapshot),
            predicate.into(),
            source,
        )
    }

    /// Add a check constraint to a table
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::add_constraint`] instead")]
    pub fn add_constraint(self) -> ConstraintBuilder {
        ConstraintBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Enable a table feature for a table
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::add_feature`] instead")]
    pub fn add_feature(self) -> AddTableFeatureBuilder {
        AddTableFeatureBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Drops constraints from a table
    #[cfg(feature = "datafusion")]
    #[must_use]
    #[deprecated(note = "Use [`DeltaTable::drop_constraints`] instead")]
    pub fn drop_constraints(self) -> DropConstraintBuilder {
        DropConstraintBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Set table properties
    #[deprecated(note = "Use [`DeltaTable::set_tbl_properties`] instead")]
    pub fn set_tbl_properties(self) -> SetTablePropertiesBuilder {
        SetTablePropertiesBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Add new columns
    #[deprecated(note = "Use [`DeltaTable::add_columns`] instead")]
    pub fn add_columns(self) -> AddColumnBuilder {
        AddColumnBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Update field metadata
    #[deprecated(note = "Use [`DeltaTable::update_field_metadata`] instead")]
    pub fn update_field_metadata(self) -> UpdateFieldMetadataBuilder {
        UpdateFieldMetadataBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }

    /// Update table metadata
    #[deprecated(note = "Use [`DeltaTable::update_table_metadata`] instead")]
    pub fn update_table_metadata(self) -> UpdateTableMetadataBuilder {
        UpdateTableMetadataBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
    }
}

#[allow(deprecated)]
impl From<DeltaTable> for DeltaOps {
    fn from(table: DeltaTable) -> Self {
        Self(table)
    }
}

#[allow(deprecated)]
impl From<DeltaOps> for DeltaTable {
    fn from(ops: DeltaOps) -> Self {
        ops.0
    }
}

#[allow(deprecated)]
impl AsRef<DeltaTable> for DeltaOps {
    fn as_ref(&self) -> &DeltaTable {
        &self.0
    }
}

/// Get the num_idx_columns and stats_columns from the table configuration in the state
/// If table_config does not exist (only can occur in the first write action) it takes
/// the configuration that was passed to the writerBuilder.
pub fn get_num_idx_cols_and_stats_columns(
    config: Option<&TableProperties>,
    configuration: HashMap<String, Option<String>>,
) -> (DataSkippingNumIndexedCols, Option<Vec<String>>) {
    let (num_index_cols, stats_columns) = match &config {
        Some(conf) => (
            conf.num_indexed_cols(),
            conf.data_skipping_stats_columns
                .clone()
                .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()),
        ),
        _ => (
            configuration
                .get("delta.dataSkippingNumIndexedCols")
                .and_then(|v| {
                    v.as_ref()
                        .and_then(|vv| vv.parse::<u64>().ok())
                        .map(DataSkippingNumIndexedCols::NumColumns)
                })
                .unwrap_or(DataSkippingNumIndexedCols::NumColumns(
                    DEFAULT_NUM_INDEX_COLS,
                )),
            configuration
                .get("delta.dataSkippingStatsColumns")
                .and_then(|v| {
                    v.as_ref()
                        .map(|v| v.split(',').map(|s| s.to_string()).collect::<Vec<String>>())
                }),
        ),
    };
    (
        num_index_cols,
        stats_columns
            .clone()
            .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()),
    )
}

/// Get the target_file_size from the table configuration in the sates
/// If table_config does not exist (only can occur in the first write action) it takes
/// the configuration that was passed to the writerBuilder.
#[cfg(feature = "datafusion")]
pub(crate) fn get_target_file_size(
    config: Option<&TableProperties>,
    configuration: &HashMap<String, Option<String>>,
) -> u64 {
    match &config {
        Some(conf) => conf.target_file_size().get(),
        _ => configuration
            .get("delta.targetFileSize")
            .and_then(|v| v.clone().map(|v| v.parse::<u64>().unwrap()))
            .unwrap_or(crate::table::config::DEFAULT_TARGET_FILE_SIZE),
    }
}
