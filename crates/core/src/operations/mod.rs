//! High level operations API to interact with Delta tables
//!
//! The operations module provides builders for several high level operations.
//! The specific builder structs allow fine-tuning the operations' behaviors
//! and will return an updated table potentially in conjunction with a
//! [data stream][datafusion::physical_plan::SendableRecordBatchStream],
//! if the operation returns data as well.
//!
//! These operations are available directly on [`DeltaTable`] via methods like
//! [`DeltaTable::create`], [`DeltaTable::write`], [`DeltaTable::merge`], etc.
use std::collections::HashMap;
#[cfg(feature = "datafusion")]
use std::num::NonZeroU64;
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
    drop_column_not_null::DropColumnNotNullBuilder, filesystem_check::FileSystemCheckBuilder,
    restore::RestoreBuilder, set_tbl_properties::SetTablePropertiesBuilder,
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
pub mod drop_column_not_null;
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
    /// Create a new [`DeltaTable`] instance from a URL.
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

    /// Create a new Delta table at this location, returning a [`CreateBuilder`].
    #[must_use]
    pub fn create(&self) -> CreateBuilder {
        CreateBuilder::default().with_log_store(self.log_store())
    }

    /// Restore the table to an earlier version or timestamp, returning a [`RestoreBuilder`].
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

    /// Drop the `NOT NULL` constraint on a column, making it nullable
    #[must_use]
    pub fn drop_column_not_null(self) -> DropColumnNotNullBuilder {
        DropColumnNotNullBuilder::new(self.log_store(), self.state.clone().map(|s| s.snapshot))
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
    /// Read the table's data into Arrow record batches, returning a [`LoadBuilder`].
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

    /// Write the given record batches to the table, returning a [`WriteBuilder`].
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

/// Hook for embedding custom behavior into the lifecycle of a Delta operation.
///
/// Implementors can run arbitrary async code around an operation's execution and its post-commit
/// hook, e.g. to integrate external transaction coordination, metrics, or cleanup. Each callback
/// receives the operation's [`LogStoreRef`] and a unique `operation_id`.
#[async_trait]
pub trait CustomExecuteHandler: Send + Sync {
    /// Execute arbitrary code at the start of a delta operation.
    async fn pre_execute(&self, log_store: &LogStoreRef, operation_id: Uuid) -> DeltaResult<()>;

    /// Execute arbitrary code at the end of a delta operation.
    async fn post_execute(&self, log_store: &LogStoreRef, operation_id: Uuid) -> DeltaResult<()>;

    /// Execute arbitrary code at the start of the post commit hook.
    async fn before_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operation: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()>;

    /// Execute arbitrary code at the end of the post commit hook.
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
) -> NonZeroU64 {
    match &config {
        Some(conf) => conf.target_file_size(),
        _ => configuration
            .get("delta.targetFileSize")
            .and_then(|v| v.clone().and_then(|v| v.parse::<NonZeroU64>().ok()))
            .unwrap_or(crate::table::config::DEFAULT_TARGET_FILE_SIZE),
    }
}
