//! High level operations API to interact with Delta tables
//!
//! At the heart of the high level operations APIs is the [`DeltaOps`] struct,
//! which consumes a [`DeltaTable`] and exposes methods to attain builders for
//! several high level operations. The specific builder structs allow fine-tuning
//! the operations' behaviors and will return an updated table potentially in conjunction
//! with a [data stream][datafusion::physical_plan::SendableRecordBatchStream],
//! if the operation returns data as well.
use async_trait::async_trait;
use delta_kernel::table_properties::{DataSkippingNumIndexedCols, TableProperties};
use std::collections::HashMap;
use std::sync::Arc;
use update_field_metadata::UpdateFieldMetadataBuilder;
use uuid::Uuid;

use add_feature::AddTableFeatureBuilder;
#[cfg(feature = "datafusion")]
use arrow_array::RecordBatch;
#[cfg(feature = "datafusion")]
pub use datafusion::physical_plan::common::collect as collect_sendable_stream;

use self::add_column::AddColumnBuilder;
use self::create::CreateBuilder;
use self::filesystem_check::FileSystemCheckBuilder;
#[cfg(feature = "datafusion")]
use self::optimize::OptimizeBuilder;
use self::restore::RestoreBuilder;
use self::set_tbl_properties::SetTablePropertiesBuilder;
use self::update_table_metadata::UpdateTableMetadataBuilder;
use self::vacuum::VacuumBuilder;
#[cfg(feature = "datafusion")]
use self::{
    constraints::ConstraintBuilder, datafusion_utils::Expression, delete::DeleteBuilder,
    drop_constraints::DropConstraintBuilder, load::LoadBuilder, load_cdf::CdfLoadBuilder,
    merge::MergeBuilder, update::UpdateBuilder, write::WriteBuilder,
};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::logstore::LogStoreRef;
use crate::table::builder::ensure_table_uri;
use crate::table::builder::DeltaTableBuilder;
use crate::table::config::{TablePropertiesExt as _, DEFAULT_NUM_INDEX_COLS};
use crate::DeltaTable;
use url::Url;

pub mod add_column;
pub mod add_feature;
pub mod convert_to_delta;
pub mod create;
pub mod drop_constraints;
pub mod filesystem_check;
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
pub(crate) trait Operation<State>: std::future::IntoFuture {
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
pub struct DeltaOps(pub DeltaTable);

impl DeltaOps {
    /// Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given URL.
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    /// use url::Url;
    ///
    /// async {
    ///     let url = Url::parse("memory:///").unwrap();
    ///     let ops = DeltaOps::try_from_uri(url).await.unwrap();
    /// };
    /// ```
    pub async fn try_from_uri(uri: Url) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_uri(uri)?.build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given uri string (deprecated).
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_uri_str("memory:///").await.unwrap();
    /// };
    /// ```
    #[deprecated(note = "Use try_from_uri with url::Url instead")]
    pub async fn try_from_uri_str(uri: impl AsRef<str>) -> DeltaResult<Self> {
        let url = ensure_table_uri(uri)?;
        Self::try_from_uri(url).await
    }

    /// Create a [`DeltaOps`] instance from URL with storage options
    pub async fn try_from_uri_with_storage_options(
        uri: Url,
        storage_options: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_uri(uri)?
            .with_storage_options(storage_options)
            .build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// Create a [`DeltaOps`] instance from uri string with storage options (deprecated)
    #[deprecated(note = "Use try_from_uri_with_storage_options with url::Url instead")]
    pub async fn try_from_uri_str_with_storage_options(
        uri: impl AsRef<str>,
        storage_options: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        let url = ensure_table_uri(uri)?;
        Self::try_from_uri_with_storage_options(url, storage_options).await
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
        DeltaTableBuilder::from_uri(url)
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
    ///     let ops = DeltaOps::try_from_uri(url::Url::parse("memory://").unwrap()).await.unwrap();
    ///     let table = ops.create().with_table_name("my_table").await.unwrap();
    ///     assert_eq!(table.version(), Some(0));
    /// };
    /// ```
    #[must_use]
    pub fn create(self) -> CreateBuilder {
        CreateBuilder::default().with_log_store(self.0.log_store)
    }

    /// Load data from a DeltaTable
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn load(self) -> LoadBuilder {
        LoadBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Load a table with CDF Enabled
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn load_cdf(self) -> CdfLoadBuilder {
        CdfLoadBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Write data to Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn write(self, batches: impl IntoIterator<Item = RecordBatch>) -> WriteBuilder {
        WriteBuilder::new(self.0.log_store, self.0.state.map(|s| s.snapshot))
            .with_input_batches(batches)
    }

    /// Vacuum stale files from delta table
    #[must_use]
    pub fn vacuum(self) -> VacuumBuilder {
        VacuumBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Audit active files with files present on the filesystem
    #[must_use]
    pub fn filesystem_check(self) -> FileSystemCheckBuilder {
        FileSystemCheckBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Audit active files with files present on the filesystem
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn optimize<'a>(self) -> OptimizeBuilder<'a> {
        OptimizeBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Delete data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn delete(self) -> DeleteBuilder {
        DeleteBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn update(self) -> UpdateBuilder {
        UpdateBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Restore delta table to a specified version or datetime
    #[must_use]
    pub fn restore(self) -> RestoreBuilder {
        RestoreBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn merge<E: Into<Expression>>(
        self,
        source: datafusion::prelude::DataFrame,
        predicate: E,
    ) -> MergeBuilder {
        MergeBuilder::new(
            self.0.log_store,
            self.0.state.unwrap().snapshot,
            predicate.into(),
            source,
        )
    }

    /// Add a check constraint to a table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn add_constraint(self) -> ConstraintBuilder {
        ConstraintBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Enable a table feature for a table
    #[must_use]
    pub fn add_feature(self) -> AddTableFeatureBuilder {
        AddTableFeatureBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Drops constraints from a table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn drop_constraints(self) -> DropConstraintBuilder {
        DropConstraintBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Set table properties
    pub fn set_tbl_properties(self) -> SetTablePropertiesBuilder {
        SetTablePropertiesBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Add new columns
    pub fn add_columns(self) -> AddColumnBuilder {
        AddColumnBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Update field metadata
    pub fn update_field_metadata(self) -> UpdateFieldMetadataBuilder {
        UpdateFieldMetadataBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }

    /// Update table metadata
    pub fn update_table_metadata(self) -> UpdateTableMetadataBuilder {
        UpdateTableMetadataBuilder::new(self.0.log_store, self.0.state.unwrap().snapshot)
    }
}

impl From<DeltaTable> for DeltaOps {
    fn from(table: DeltaTable) -> Self {
        Self(table)
    }
}

impl From<DeltaOps> for DeltaTable {
    fn from(ops: DeltaOps) -> Self {
        ops.0
    }
}

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

pub(super) mod util {
    use super::*;
    use futures::Future;
    use tokio::{runtime::Handle, sync::Notify, task::JoinError};

    pub async fn flatten_join_error<T, E>(
        future: impl Future<Output = Result<Result<T, E>, JoinError>>,
    ) -> Result<T, DeltaTableError>
    where
        E: Into<DeltaTableError>,
    {
        match future.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(error.into()),
            Err(error) => Err(DeltaTableError::GenericError {
                source: Box::new(error),
            }),
        }
    }

    /// Creates a Tokio [`Runtime`] for use with CPU bound tasks
    ///
    /// Tokio forbids dropping `Runtime`s in async contexts, so creating a separate
    /// `Runtime` correctly is somewhat tricky. This structure manages the creation
    /// and shutdown of a separate thread.
    ///
    /// # Notes
    /// On drop, the thread will wait for all remaining tasks to complete.
    ///
    /// Depending on your application, more sophisticated shutdown logic may be
    /// required, such as ensuring that no new tasks are added to the runtime.
    ///
    /// # Credits
    /// This code is derived from code originally written for [InfluxDB 3.0]
    ///
    /// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
    /// Vendored from [datafusion-examples](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/thread_pools.rs)
    pub struct CpuRuntime {
        /// Handle is the tokio structure for interacting with a Runtime.
        handle: Handle,
        /// Signal to start shutting down
        notify_shutdown: Arc<Notify>,
        /// When thread is active, is Some
        thread_join_handle: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for CpuRuntime {
        fn drop(&mut self) {
            // Notify the thread to shutdown.
            self.notify_shutdown.notify_one();
            // In a production system you also need to ensure your code stops adding
            // new tasks to the underlying runtime after this point to allow the
            // thread to complete its work and exit cleanly.
            if let Some(thread_join_handle) = self.thread_join_handle.take() {
                // If the thread is still running, we wait for it to finish
                print!("Shutting down CPU runtime thread...");
                if let Err(e) = thread_join_handle.join() {
                    eprintln!("Error joining CPU runtime thread: {e:?}",);
                } else {
                    println!("CPU runtime thread shutdown successfully.");
                }
            }
        }
    }

    impl CpuRuntime {
        /// Create a new Tokio Runtime for CPU bound tasks
        pub fn try_new() -> DeltaResult<Self> {
            let cpu_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_time()
                .build()?;
            let handle = cpu_runtime.handle().clone();
            let notify_shutdown = Arc::new(Notify::new());
            let notify_shutdown_captured = Arc::clone(&notify_shutdown);

            // The cpu_runtime runs and is dropped on a separate thread
            let thread_join_handle = std::thread::spawn(move || {
                cpu_runtime.block_on(async move {
                    notify_shutdown_captured.notified().await;
                });
                // Note: cpu_runtime is dropped here, which will wait for all tasks
                // to complete
            });

            Ok(Self {
                handle,
                notify_shutdown,
                thread_join_handle: Some(thread_join_handle),
            })
        }

        /// Return a handle suitable for spawning CPU bound tasks
        ///
        /// # Notes
        ///
        /// If a task spawned on this handle attempts to do IO, it will error with a
        /// message such as:
        ///
        /// ```text
        ///A Tokio 1.x context was found, but IO is disabled.
        /// ```
        pub fn handle(&self) -> &Handle {
            &self.handle
        }
    }
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

#[cfg(feature = "datafusion")]
mod datafusion_utils {
    use datafusion::common::DFSchema;
    use datafusion::execution::context::SessionState;
    use datafusion::logical_expr::Expr;

    use crate::{delta_datafusion::expr::parse_predicate_expression, DeltaResult};

    /// Used to represent user input of either a Datafusion expression or string expression
    #[derive(Debug, Clone)]
    pub enum Expression {
        /// Datafusion Expression
        DataFusion(Expr),
        /// String Expression
        String(String),
    }

    impl From<Expr> for Expression {
        fn from(val: Expr) -> Self {
            Expression::DataFusion(val)
        }
    }

    impl From<&str> for Expression {
        fn from(val: &str) -> Self {
            Expression::String(val.to_string())
        }
    }
    impl From<String> for Expression {
        fn from(val: String) -> Self {
            Expression::String(val)
        }
    }

    pub(crate) fn into_expr(
        expr: Expression,
        schema: &DFSchema,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        match expr {
            Expression::DataFusion(expr) => Ok(expr),
            Expression::String(s) => parse_predicate_expression(schema, s, df_state),
        }
    }

    pub(crate) fn maybe_into_expr(
        expr: Option<Expression>,
        schema: &DFSchema,
        df_state: &SessionState,
    ) -> DeltaResult<Option<Expr>> {
        Ok(match expr {
            Some(predicate) => Some(into_expr(predicate, schema, df_state)?),
            None => None,
        })
    }
}
