//! High level operations API to interact with Delta tables
//!
//! At the heart of the high level operations APIs is the [`DeltaOps`] struct,
//! which consumes a [`DeltaTable`] and exposes methods to attain builders for
//! several high level operations. The specific builder structs allow fine-tuning
//! the operations' behaviors and will return an updated table potentially in conjunction
//! with a [data stream][datafusion::physical_plan::SendableRecordBatchStream],
//! if the operation returns data as well.
use std::collections::HashMap;

use add_feature::AddTableFeatureBuilder;
#[cfg(feature = "datafusion")]
use arrow_array::RecordBatch;
#[cfg(feature = "datafusion")]
pub use datafusion_physical_plan::common::collect as collect_sendable_stream;

use self::add_column::AddColumnBuilder;
use self::create::CreateBuilder;
use self::filesystem_check::FileSystemCheckBuilder;
use self::optimize::OptimizeBuilder;
use self::restore::RestoreBuilder;
use self::set_tbl_properties::SetTablePropertiesBuilder;
use self::vacuum::VacuumBuilder;
#[cfg(feature = "datafusion")]
use self::{
    constraints::ConstraintBuilder, datafusion_utils::Expression, delete::DeleteBuilder,
    drop_constraints::DropConstraintBuilder, load::LoadBuilder, load_cdf::CdfLoadBuilder,
    merge::MergeBuilder, update::UpdateBuilder, write::WriteBuilder,
};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::table::builder::DeltaTableBuilder;
use crate::DeltaTable;

pub mod add_column;
pub mod add_feature;
pub mod cast;
pub mod convert_to_delta;
pub mod create;
pub mod drop_constraints;
pub mod filesystem_check;
pub mod optimize;
pub mod restore;
pub mod transaction;
pub mod vacuum;

#[cfg(all(feature = "cdf", feature = "datafusion"))]
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
pub mod set_tbl_properties;
#[cfg(feature = "datafusion")]
pub mod update;
#[cfg(feature = "datafusion")]
pub mod write;
pub mod writer;

#[allow(unused)]
/// The [Operation] trait defines common behaviors that all operations builders
/// should have consistent
pub(crate) trait Operation<State>: std::future::IntoFuture {}

/// High level interface for executing commands against a DeltaTable
pub struct DeltaOps(pub DeltaTable);

impl DeltaOps {
    /// Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given uri.
    ///
    /// ```
    /// use deltalake_core::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_uri("memory://").await.unwrap();
    /// };
    /// ```
    pub async fn try_from_uri(uri: impl AsRef<str>) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_uri(uri).build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// try from uri with storage options
    pub async fn try_from_uri_with_storage_options(
        uri: impl AsRef<str>,
        storage_options: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_uri(uri)
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
        DeltaTableBuilder::from_uri("memory://")
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
    ///     let ops = DeltaOps::try_from_uri("memory://").await.unwrap();
    ///     let table = ops.create().with_table_name("my_table").await.unwrap();
    ///     assert_eq!(table.version(), 0);
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
        LoadBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Load a table with CDF Enabled
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn load_cdf(self) -> CdfLoadBuilder {
        CdfLoadBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Write data to Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn write(self, batches: impl IntoIterator<Item = RecordBatch>) -> WriteBuilder {
        WriteBuilder::new(self.0.log_store, self.0.state).with_input_batches(batches)
    }

    /// Vacuum stale files from delta table
    #[must_use]
    pub fn vacuum(self) -> VacuumBuilder {
        VacuumBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Audit active files with files present on the filesystem
    #[must_use]
    pub fn filesystem_check(self) -> FileSystemCheckBuilder {
        FileSystemCheckBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Audit active files with files present on the filesystem
    #[must_use]
    pub fn optimize<'a>(self) -> OptimizeBuilder<'a> {
        OptimizeBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Delete data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn delete(self) -> DeleteBuilder {
        DeleteBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn update(self) -> UpdateBuilder {
        UpdateBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Restore delta table to a specified version or datetime
    #[must_use]
    pub fn restore(self) -> RestoreBuilder {
        RestoreBuilder::new(self.0.log_store, self.0.state.unwrap())
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
            self.0.state.unwrap(),
            predicate.into(),
            source,
        )
    }

    /// Add a check constraint to a table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn add_constraint(self) -> ConstraintBuilder {
        ConstraintBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Enable a table feature for a table
    #[must_use]
    pub fn add_feature(self) -> AddTableFeatureBuilder {
        AddTableFeatureBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Drops constraints from a table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn drop_constraints(self) -> DropConstraintBuilder {
        DropConstraintBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Set table properties
    pub fn set_tbl_properties(self) -> SetTablePropertiesBuilder {
        SetTablePropertiesBuilder::new(self.0.log_store, self.0.state.unwrap())
    }

    /// Add new columns
    pub fn add_columns(self) -> AddColumnBuilder {
        AddColumnBuilder::new(self.0.log_store, self.0.state.unwrap())
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
    config: Option<crate::table::config::TableConfig<'_>>,
    configuration: HashMap<String, Option<String>>,
) -> (i32, Option<Vec<String>>) {
    let (num_index_cols, stats_columns) = match &config {
        Some(conf) => (conf.num_indexed_cols(), conf.stats_columns()),
        _ => (
            configuration
                .get("delta.dataSkippingNumIndexedCols")
                .and_then(|v| v.clone().map(|v| v.parse::<i32>().unwrap()))
                .unwrap_or(crate::table::config::DEFAULT_NUM_INDEX_COLS),
            configuration
                .get("delta.dataSkippingStatsColumns")
                .and_then(|v| v.as_ref().map(|v| v.split(',').collect::<Vec<&str>>())),
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
pub(crate) fn get_target_file_size(
    config: &Option<crate::table::config::TableConfig<'_>>,
    configuration: &HashMap<String, Option<String>>,
) -> i64 {
    match &config {
        Some(conf) => conf.target_file_size(),
        _ => configuration
            .get("delta.targetFileSize")
            .and_then(|v| v.clone().map(|v| v.parse::<i64>().unwrap()))
            .unwrap_or(crate::table::config::DEFAULT_TARGET_FILE_SIZE),
    }
}

#[cfg(feature = "datafusion")]
mod datafusion_utils {
    use datafusion::execution::context::SessionState;
    use datafusion_common::DFSchema;
    use datafusion_expr::Expr;

    use crate::{delta_datafusion::expr::parse_predicate_expression, DeltaResult};

    /// Used to represent user input of either a Datafusion expression or string expression
    #[derive(Debug)]
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
