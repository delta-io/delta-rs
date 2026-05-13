use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchemaRef, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use datafusion::{catalog::Session, common::HashSet, prelude::Expr};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::delta_datafusion::table_provider::next::SnapshotWrapper;
use crate::delta_datafusion::{DataFusionMixins as _, FindFilesExprProperties};
use crate::kernel::{EagerSnapshot, Snapshot, Version};
use crate::kernel::transaction::PROTOCOL;
use crate::logstore::{LogStore, LogStoreExt as _};
use crate::table::normalize_table_url;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

mod data_sink;
pub(crate) mod next;

const PATH_COLUMN: &str = "__delta_rs_path";

pub(crate) fn resolve_file_column_name(
    input_schema: &Schema,
    file_column_name: Option<&str>,
) -> DeltaResult<String> {
    let column_names: HashSet<&str> = input_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();

    match file_column_name {
        Some(name) => {
            if column_names.contains(name) {
                return Err(DeltaTableError::Generic(format!(
                    "Unable to add file path column since column with name {name} exists"
                )));
            }

            Ok(name.to_owned())
        }
        None => {
            let prefix = PATH_COLUMN;
            let mut idx = 0;
            let mut name = prefix.to_owned();

            while column_names.contains(name.as_str()) {
                idx += 1;
                name = format!("{prefix}_{idx}");
            }

            Ok(name)
        }
    }
}

#[derive(Debug, Clone)]
/// Used to specify if additional metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Include the source path for each record. The name of this column is determined by `file_column_name`
    pub(super) include_file_column: bool,
    /// Column name that contains the source path.
    ///
    /// If include_file_column is true and the name is None then it will be auto-generated
    /// Otherwise the user provided name will be used
    pub(super) file_column_name: Option<String>,
    /// Whether to wrap partition values in a dictionary encoding to potentially save space
    pub(super) wrap_partition_values: Option<bool>,
    /// Whether to push down filter in end result or just prune the files
    pub(super) enable_parquet_pushdown: bool,
    /// Schema to scan table with
    pub(super) schema: Option<SchemaRef>,
}

impl Default for DeltaScanConfigBuilder {
    fn default() -> Self {
        DeltaScanConfigBuilder {
            include_file_column: false,
            file_column_name: None,
            wrap_partition_values: None,
            enable_parquet_pushdown: true,
            schema: None,
        }
    }
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Indicate that a column containing a records file path is included and column name is user defined.
    pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
        self.file_column_name = Some(name.to_string());
        self.include_file_column = true;
        self
    }

    /// Whether to wrap partition values in a dictionary encoding
    pub fn wrap_partition_values(mut self, wrap: bool) -> Self {
        self.wrap_partition_values = Some(wrap);
        self
    }

    /// Allow pushdown of the scan filter
    /// When disabled the filter will only be used for pruning files
    pub fn with_parquet_pushdown(mut self, pushdown: bool) -> Self {
        self.enable_parquet_pushdown = pushdown;
        self
    }

    /// Use the provided [SchemaRef] for the [`crate::delta_datafusion::DeltaScanNext`]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &EagerSnapshot) -> DeltaResult<DeltaScanConfig> {
        let file_column_name = if self.include_file_column {
            Some(resolve_file_column_name(
                snapshot.input_schema().as_ref(),
                self.file_column_name.as_deref(),
            )?)
        } else {
            None
        };

        Ok(DeltaScanConfig {
            file_column_name,
            wrap_partition_values: self.wrap_partition_values.unwrap_or(true),
            enable_parquet_pushdown: self.enable_parquet_pushdown,
            schema: self.schema.clone(),
            schema_force_view_types: true,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Include additional metadata columns during a [`crate::delta_datafusion::DeltaScanNext`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
    /// Wrap partition values in a dictionary encoding, defaults to true
    pub wrap_partition_values: bool,
    /// Allow pushdown of the scan filter, defaults to true
    pub enable_parquet_pushdown: bool,
    /// If true, parquet reader will read columns of `Utf8`/`Utf8Large`
    /// with Utf8View, and `Binary`/`BinaryLarge` with `BinaryView`
    pub schema_force_view_types: bool,
    /// Schema to read as
    pub schema: Option<SchemaRef>,
}

impl Default for DeltaScanConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaScanConfig {
    /// Create a new default [`DeltaScanConfig`]
    pub fn new() -> Self {
        Self {
            file_column_name: None,
            wrap_partition_values: true,
            enable_parquet_pushdown: true,
            schema_force_view_types: true,
            schema: None,
        }
    }

    pub fn new_from_session(session: &dyn Session) -> Self {
        let config_options = session.config().options();
        Self {
            file_column_name: None,
            wrap_partition_values: true,
            enable_parquet_pushdown: config_options.execution.parquet.pushdown_filters,
            schema_force_view_types: config_options.execution.parquet.schema_force_view_types,
            schema: None,
        }
    }

    pub fn with_file_column_name<S: ToString>(mut self, name: S) -> Self {
        self.file_column_name = Some(name.to_string());
        self
    }

    /// Whether to wrap partition values in a dictionary encoding
    pub fn with_wrap_partition_values(mut self, wrap: bool) -> Self {
        self.wrap_partition_values = wrap;
        self
    }

    /// Allow pushdown of the scan filter
    pub fn with_parquet_pushdown(mut self, pushdown: bool) -> Self {
        self.enable_parquet_pushdown = pushdown;
        self
    }

    /// Use the provided [SchemaRef] for the [`crate::delta_datafusion::DeltaScanNext`]
    ///
    /// This schema will be used when reading data from the underlying files.
    /// The column names must match those in the table schema, but can have
    /// different (yet compatible) types - e.g. string view types can be used
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// Builder for a datafusion [TableProvider] for a Delta table
///
/// A table provider can be built by providing either a log store, a Snapshot,
/// or an eager snapshot. If some Snapshot is provided, that will be used directly,
/// and no IO will be performed when building the provider.
pub struct TableProviderBuilder {
    log_store: Option<Arc<dyn LogStore>>,
    snapshot: Option<SnapshotWrapper>,
    session: Option<Arc<dyn Session>>,
    file_column: Option<String>,
    row_index_column: Option<String>,
    table_version: Option<Version>,
    /// Predicates used only for file skipping in kernel log replay
    file_skipping_predicates: Option<Vec<Expr>>,
}

impl fmt::Debug for TableProviderBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableProviderBuilder")
            .field("log_store", &self.log_store)
            .field("snapshot", &self.snapshot)
            .field("has_session", &self.session.is_some())
            .field("file_column", &self.file_column)
            .field("row_index_column", &self.row_index_column)
            .field("table_version", &self.table_version)
            .field("file_skipping_predicates", &self.file_skipping_predicates)
            .finish()
    }
}

impl Default for TableProviderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TableProviderBuilder {
    fn new() -> Self {
        Self {
            log_store: None,
            snapshot: None,
            session: None,
            file_column: None,
            row_index_column: None,
            table_version: None,
            file_skipping_predicates: None,
        }
    }

    /// Provide the log store to use for the table provider
    pub fn with_log_store(mut self, log_store: impl Into<Arc<dyn LogStore>>) -> Self {
        self.log_store = Some(log_store.into());
        self
    }

    /// Provide an eager snapshot to use for the table provider
    pub fn with_eager_snapshot(mut self, snapshot: impl Into<Arc<EagerSnapshot>>) -> Self {
        self.snapshot = Some(SnapshotWrapper::EagerSnapshot(snapshot.into()));
        self
    }

    /// Provide a snapshot to use for the table provider
    pub fn with_snapshot(mut self, snapshot: impl Into<Arc<Snapshot>>) -> Self {
        self.snapshot = Some(SnapshotWrapper::Snapshot(snapshot.into()));
        self
    }

    /// Provide a DataFusion session for scan config defaults.
    pub fn with_session<S>(mut self, session: Arc<S>) -> Self
    where
        S: Session + 'static,
    {
        self.session = Some(session);
        self
    }

    /// Specify the version of the table to provide
    pub fn with_table_version(mut self, version: impl Into<Option<Version>>) -> Self {
        self.table_version = version.into();
        self
    }

    /// Specify the name of the file column to include in the scan
    ///
    /// If specified, this will append a column to the table,
    /// containing the source file path for each record.
    pub fn with_file_column(mut self, file_column: impl ToString) -> Self {
        self.file_column = Some(file_column.to_string());
        self
    }

    /// Add a row index column to scan output.
    pub(crate) fn with_row_index_column(mut self, row_index_column: impl ToString) -> Self {
        self.row_index_column = Some(row_index_column.to_string());
        self
    }

    /// Add predicates applied only during file skipping.
    ///
    /// There are cases where we may want to skip files that definitely do
    /// not contain any data that matches a predicate, but read all data
    /// if any records may match, to rewrite the file with updated records.
    ///
    /// The file skipping predicates will thus not be pushed into the parquet
    /// scan. However any predicate that gets pushed into the scan during execution
    /// planning will be applied.
    pub(crate) fn with_file_skipping_predicates(
        mut self,
        file_skipping_predicates: impl IntoIterator<Item = Expr>,
    ) -> Self {
        self.file_skipping_predicates = Some(file_skipping_predicates.into_iter().collect());
        self
    }

    pub async fn build(self) -> Result<next::DeltaScan> {
        let TableProviderBuilder {
            log_store,
            snapshot,
            session,
            file_column,
            row_index_column,
            table_version,
            file_skipping_predicates,
        } = self;

        let mut config = session
            .as_ref()
            .map_or_else(DeltaScanConfig::new, |session| {
                DeltaScanConfig::new_from_session(session.as_ref())
            });
        if let Some(file_column) = file_column {
            config = config.with_file_column_name(file_column);
        }

        let snapshot = match snapshot {
            Some(wrapper) => wrapper,
            None => {
                if let Some(log_store) = log_store.as_ref() {
                    SnapshotWrapper::Snapshot(
                        Snapshot::try_new(log_store, Default::default(), table_version)
                            .await?
                            .into(),
                    )
                } else {
                    return Err(DataFusionError::Plan(
                        "Either a log store or a snapshot must be provided to build a Delta TableProvider".to_string(),
                    ));
                }
            }
        };

        PROTOCOL
            .can_read_from_protocol(snapshot.snapshot().protocol())
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        if let Some(log_store) = log_store.as_ref() {
            let snapshot_root_identity = next::canonical_table_root_identity(
                snapshot.snapshot().scan_builder().build()?.table_root(),
            );
            let log_store_root = log_store.table_root_url();
            let log_store_root_identity = next::canonical_table_root_identity(&log_store_root);

            let snapshot_root_redacted = next::redact_url_for_error(&snapshot_root_identity);
            let log_store_root_redacted = next::redact_url_for_error(&log_store_root_identity);
            if snapshot_root_identity != log_store_root_identity {
                return Err(DataFusionError::Plan(format!(
                    "Provided snapshot root ({snapshot_root_redacted}) does not match provided log store root ({log_store_root_redacted})"
                )));
            }
        }

        let mut provider = next::DeltaScan::new(snapshot, config)?;
        if let Some(row_index_column) = row_index_column {
            provider = provider.with_row_index_column(row_index_column)?;
        }
        if let Some(log_store) = log_store {
            provider = provider.with_log_store(log_store);
        }

        if let Some(skipping) = file_skipping_predicates {
            // validate that the expressions contain no illegal variants
            // that are not eligible for file skipping, e.g. volatile functions.
            for term in &skipping {
                let mut visitor = FindFilesExprProperties::default();
                term.visit(&mut visitor)?;
                visitor.result?;
            }
            provider = provider.with_file_skipping_predicate(skipping);
        }

        Ok(provider)
    }
}

impl std::future::IntoFuture for TableProviderBuilder {
    type Output = Result<Arc<dyn TableProvider>>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;
        Box::pin(async move { Ok(Arc::new(this.build().await?) as _) })
    }
}

impl DeltaTable {
    /// Get a table provider for the table referenced by this DeltaTable.
    ///
    /// See [`TableProviderBuilder`] for options when building the provider.
    pub fn table_provider(&self) -> TableProviderBuilder {
        let mut builder = TableProviderBuilder::new().with_log_store(self.log_store());
        if let Ok(state) = self.snapshot() {
            builder = builder.with_snapshot(state.snapshot().snapshot().clone());
        }
        builder
    }

    /// Ensure the provided DataFusion session is prepared to read this table.
    ///
    /// This registers the table's root object store with the session's `RuntimeEnv` if missing.
    /// Registration is idempotent and will not overwrite an existing mapping.
    ///
    /// If the session already has an object store registered for the table's URL but it is stale or
    /// incorrect, this method will not replace it. To override an existing mapping, call
    /// `RuntimeEnv::register_object_store` directly.
    ///
    /// ```rust,no_run
    /// use datafusion::prelude::SessionContext;
    /// use deltalake_core::{DeltaResult, DeltaTable};
    ///
    /// # fn main() -> DeltaResult<()> {
    /// let table = DeltaTable::new_in_memory();
    /// let ctx = SessionContext::new();
    /// let state = ctx.state();
    /// table.update_datafusion_session(&state)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn update_datafusion_session(&self, session: &dyn Session) -> DeltaResult<()> {
        crate::delta_datafusion::DeltaSessionExt::ensure_object_store_registered(
            session,
            self.log_store().as_ref(),
            None,
        )
    }
}

pub(crate) fn update_datafusion_session(
    session: &dyn Session,
    log_store: &dyn LogStore,
    operation_id: Option<Uuid>,
) -> DeltaResult<()> {
    crate::delta_datafusion::DeltaSessionExt::ensure_object_store_registered(
        session,
        log_store,
        operation_id,
    )
}

/// Physical scan wrapper used by DataFusion plan serialization.
///
/// Kept for physical extension codec compatibility. Construct Delta table providers with
/// [`DeltaTable::table_provider`], [`TableProviderBuilder`], or
/// [`crate::delta_datafusion::DeltaScanNext`].
#[derive(Debug)]
pub(super) struct DeltaScan {
    /// The normalized [Url] of the ObjectStore root
    table_url: Url,
    /// Column that contains an index that maps to the original metadata Add
    config: DeltaScanConfig,
    /// The parquet scan to wrap
    parquet_scan: Arc<dyn ExecutionPlan>,
    /// The schema of the table to be used when evaluating expressions
    logical_schema: Arc<Schema>,
    /// Metrics for scan reported via DataFusion
    metrics: ExecutionPlanMetricsSet,
}

impl DeltaScan {
    pub(crate) fn new(
        table_url: &Url,
        config: DeltaScanConfig,
        parquet_scan: Arc<dyn ExecutionPlan>,
        logical_schema: Arc<Schema>,
    ) -> Self {
        Self {
            table_url: normalize_table_url(table_url),
            metrics: ExecutionPlanMetricsSet::new(),
            config,
            parquet_scan,
            logical_schema,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct DeltaScanWire {
    /// This [Url] should have already been passed through [normalize_table_url]
    table_url: Url,
    config: DeltaScanConfig,
    logical_schema: Arc<Schema>,
}

impl From<&DeltaScan> for DeltaScanWire {
    fn from(scan: &DeltaScan) -> Self {
        Self {
            table_url: scan.table_url.clone(),
            config: scan.config.clone(),
            logical_schema: scan.logical_schema.clone(),
        }
    }
}

impl DeltaScanWire {
    pub(super) fn into_delta_scan(self, parquet_scan: Arc<dyn ExecutionPlan>) -> DeltaScan {
        DeltaScan::new(
            &self.table_url,
            self.config,
            parquet_scan,
            self.logical_schema,
        )
    }
}

impl DisplayAs for DeltaScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaScan")
    }
}

impl ExecutionPlan for DeltaScan {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parquet_scan.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.parquet_scan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.parquet_scan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "DeltaScan wrong number of children {}",
                children.len()
            )));
        }
        Ok(Arc::new(DeltaScan {
            table_url: self.table_url.clone(),
            config: self.config.clone(),
            parquet_scan: children[0].clone(),
            logical_schema: self.logical_schema.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(parquet_scan) = self.parquet_scan.repartitioned(target_partitions, config)? {
            Ok(Some(Arc::new(DeltaScan {
                table_url: self.table_url.clone(),
                config: self.config.clone(),
                parquet_scan,
                logical_schema: self.logical_schema.clone(),
                metrics: self.metrics.clone(),
            })))
        } else {
            Ok(None)
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.parquet_scan.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.parquet_scan.partition_statistics(partition)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }
}

pub(crate) fn simplify_expr(
    session: &dyn Session,
    df_schema: DFSchemaRef,
    expr: Expr,
) -> Result<Arc<dyn PhysicalExpr>> {
    let execution_props = session.execution_props();
    let context = SimplifyContext::default()
        .with_schema(df_schema.clone())
        .with_query_execution_start_time(execution_props.query_execution_start_time)
        .with_config_options(
            execution_props
                .config_options()
                .cloned()
                .unwrap_or_else(|| session.config().options().clone()),
        );
    let simplifier = ExprSimplifier::new(context).with_max_cycles(10);
    session.create_physical_expr(simplifier.simplify(expr)?, df_schema.as_ref())
}

#[cfg(test)]
mod tests {
    use crate::kernel::{DataType, PrimitiveType, StructField, StructType};
    use crate::operations::create::CreateBuilder;
    use crate::test_utils::object_store::{
        drain_recorded_object_store_operations as drain_recorded_ops, recording_log_store,
    };
    use crate::{DeltaTable, DeltaTableConfig, DeltaTableError};
    use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionState;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect_partitioned;
    use std::sync::Arc;
    use url::Url;

    use super::*;

    async fn create_in_memory_id_table() -> Result<DeltaTable, DeltaTableError> {
        let schema = StructType::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        )])?;
        DeltaTable::new_in_memory()
            .create()
            .with_columns(schema.fields().cloned())
            .await
    }

    async fn create_test_table() -> Result<DeltaTable, DeltaTableError> {
        use tempfile::TempDir;

        let tmp_dir = TempDir::new().unwrap();
        let table_path = tmp_dir.path().to_str().unwrap();

        let schema = StructType::try_new(vec![
            StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Long),
                false,
            ),
            StructField::new(
                "value".to_string(),
                DataType::Primitive(PrimitiveType::String),
                false,
            ),
        ])?;

        CreateBuilder::new()
            .with_location(table_path)
            .with_columns(schema.fields().cloned())
            .await
    }

    fn create_test_session_state() -> Arc<SessionState> {
        use datafusion::execution::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;
        let config = SessionConfig::new();
        Arc::new(SessionStateBuilder::new().with_config(config).build())
    }

    #[test]
    fn test_resolve_file_column_name_avoids_collisions() {
        let schema = Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new(PATH_COLUMN, ArrowDataType::Utf8, true),
            Field::new(format!("{PATH_COLUMN}_1"), ArrowDataType::Utf8, true),
        ]);

        let resolved = resolve_file_column_name(&schema, None).unwrap();
        assert_eq!(resolved, format!("{PATH_COLUMN}_2"));
    }

    #[test]
    fn test_resolve_file_column_name_rejects_explicit_collision() {
        let schema = Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("file_col", ArrowDataType::Utf8, true),
        ]);

        let err = resolve_file_column_name(&schema, Some("file_col")).unwrap_err();
        assert!(
            err.to_string()
                .contains("Unable to add file path column since column with name file_col exists")
        );
    }

    #[tokio::test]
    async fn test_insert_into_simple() {
        let table = create_test_table().await.unwrap();
        let session_state = create_test_session_state();

        let table_provider = table.table_provider().await.unwrap();
        let schema = table_provider.schema();
        let value_array: ArrayRef = match schema.field(1).data_type() {
            ArrowDataType::Utf8 => Arc::new(StringArray::from(vec!["test1", "test2"])),
            ArrowDataType::Utf8View => Arc::new(StringViewArray::from(vec!["test1", "test2"])),
            data_type => panic!("unexpected value field data type: {data_type:?}"),
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])), value_array],
        )
        .unwrap();

        let mem_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let logical_plan = mem_table
            .scan(&*session_state, None, &[], None)
            .await
            .unwrap();

        let result_plan = table_provider
            .insert_into(&*session_state, logical_plan, InsertOp::Append)
            .await
            .unwrap();

        assert!(!result_plan.schema().fields().is_empty());

        let result_schema = result_plan.schema();
        assert_eq!(result_schema.fields().len(), 1);
        assert_eq!(result_schema.field(0).name(), "count");
        assert_eq!(
            result_schema.field(0).data_type(),
            &arrow::datatypes::DataType::UInt64
        );
        assert_eq!(result_plan.children().len(), 1);
        assert!(result_plan.metrics().is_some());
    }

    #[tokio::test]
    async fn test_table_provider_from_loaded_table_supports_insert_into() {
        let table = create_in_memory_id_table().await.unwrap();
        let log_store = table.log_store();
        let provider = table.table_provider().await.unwrap();

        let session = Arc::new(crate::delta_datafusion::create_session().into_inner());
        let state = session.state_ref().read().clone();

        let schema = provider.schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![11, 13]))],
        )
        .unwrap();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let input = mem_table.scan(&state, None, &[], None).await.unwrap();

        let write_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await
            .unwrap();
        let _write_batches: Vec<_> = collect_partitioned(write_plan, session.task_ctx())
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        let read_provider = next::DeltaScan::builder()
            .with_log_store(log_store)
            .await
            .unwrap();
        session
            .register_table("delta_table", read_provider)
            .unwrap();
        let batches = session
            .sql("SELECT id FROM delta_table ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected = vec!["+----+", "| id |", "+----+", "| 11 |", "| 13 |", "+----+"];
        datafusion::assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_table_provider_from_loaded_table_reuses_seeded_snapshot_state() {
        let base = crate::test_utils::TestTables::Simple
            .table_builder()
            .unwrap()
            .build_storage()
            .unwrap();
        let (log_store, mut operations) = recording_log_store(base);

        let mut table = DeltaTable::new(log_store.clone(), DeltaTableConfig::default());
        table.load().await.unwrap();

        drain_recorded_ops(&mut operations).await;

        let provider = table.table_provider().await.unwrap();

        let session = Arc::new(crate::delta_datafusion::create_session().into_inner());
        let state = session.state_ref().read().clone();
        let read_plan = provider.scan(&state, None, &[], None).await.unwrap();
        let _read_batches: Vec<_> = collect_partitioned(read_plan, session.task_ctx())
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        let replay_ops = drain_recorded_ops(&mut operations).await;
        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "expected loaded table provider scan to reuse seeded snapshot state, got {replay_ops:?}",
        );
    }

    #[tokio::test]
    async fn test_builder_allows_matching_snapshot_and_log_store_root() {
        let table = create_in_memory_id_table().await.unwrap();
        let snapshot = table.snapshot().unwrap().snapshot().snapshot().clone();
        let log_store = table.log_store();

        let provider = next::DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(log_store)
            .build()
            .await;

        assert!(
            provider.is_ok(),
            "unexpected error: {}",
            provider.unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_builder_allows_same_root_with_different_query_on_log_store() {
        let one_url = Url::parse("memory:///same-root?snap-token").unwrap();
        let one_store =
            crate::logstore::logstore_for(&one_url, crate::logstore::StorageConfig::default())
                .unwrap();
        let schema = StructType::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        )])
        .unwrap();
        let table_one = CreateBuilder::new()
            .with_log_store(one_store)
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        let snapshot = table_one.snapshot().unwrap().snapshot().snapshot().clone();

        let two_url = Url::parse("memory:///same-root?log-token").unwrap();
        let two_store =
            crate::logstore::logstore_for(&two_url, crate::logstore::StorageConfig::default())
                .unwrap();

        let provider = next::DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(two_store)
            .build()
            .await;

        assert!(
            provider.is_ok(),
            "unexpected error: {}",
            provider.unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_builder_rejects_mismatched_snapshot_and_log_store() {
        let one_url = Url::parse("memory:///same-root?snap-token").unwrap();
        let one_store =
            crate::logstore::logstore_for(&one_url, crate::logstore::StorageConfig::default())
                .unwrap();
        let schema = StructType::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        )])
        .unwrap();
        let table_one = CreateBuilder::new()
            .with_log_store(one_store)
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        let snapshot = table_one.snapshot().unwrap().snapshot().snapshot().clone();

        let two_url = Url::parse("memory:///different-root?log-token").unwrap();
        let two_store =
            crate::logstore::logstore_for(&two_url, crate::logstore::StorageConfig::default())
                .unwrap();

        let err = next::DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(two_store)
            .build()
            .await
            .unwrap_err();

        let err_str = err.to_string();
        assert!(
            err_str.contains("snapshot") || err_str.contains("Snapshot"),
            "unexpected error: {err_str}"
        );
        assert!(
            err_str.contains("log store") || err_str.contains("log_store"),
            "unexpected error: {err_str}"
        );
        assert!(
            !err_str.contains("snap-token"),
            "unexpected error: {err_str}"
        );
        assert!(
            !err_str.contains("log-token"),
            "unexpected error: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_builder_with_file_selection_propagates_to_scan() {
        let log_store = crate::test_utils::TestTables::Simple
            .table_builder()
            .unwrap()
            .build_storage()
            .unwrap();
        let snapshot = Arc::new(
            crate::kernel::Snapshot::try_new(&log_store, Default::default(), None)
                .await
                .unwrap(),
        );
        let table_root = snapshot
            .scan_builder()
            .build()
            .unwrap()
            .table_root()
            .clone();
        let missing_file_id = table_root
            .join("__does_not_exist__.parquet")
            .unwrap()
            .to_string();

        let provider = next::DeltaScan::builder()
            .with_snapshot(snapshot)
            .build()
            .await
            .unwrap()
            .with_file_selection(next::FileSelection::new([missing_file_id]));

        let session = Arc::new(crate::delta_datafusion::create_session().into_inner());
        let state = session.state_ref().read().clone();
        let err = provider.scan(&state, None, &[], None).await.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("File selection contains"),
            "unexpected error: {err_str}"
        );
    }
}
