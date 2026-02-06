use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;
use arrow_schema::Schema;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::{Result, Statistics};
use datafusion::datasource::TableType;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_plan::ExecutionPlan;
use url::Url;
use crate::delta_datafusion::{DataFusionMixins, DeltaScanBuilder, DeltaScanConfigBuilder};
use crate::delta_datafusion::table_provider::get_pushdown_filters;
use crate::{DeltaResult, DeltaTable, DeltaTableConfig, DeltaTableError};
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;

impl DeltaTable {
    pub fn table_provider_old(&self) -> DeltaTableOldProvider {
        self.clone().into()
    }
}

// each delta table must register a specific object store, since paths are internally
// handled relative to the table root.
pub(crate) fn register_store(store: LogStoreRef, env: &RuntimeEnv) {
    let object_store_url = store.object_store_url();
    let url: &Url = object_store_url.as_ref();
    env.register_object_store(url, store.object_store(None));
}

#[derive(Debug, Clone)]
pub struct DeltaTableOldProvider {
    /// The state of the table as of the most recent loaded Delta log entry.
    pub state: Option<DeltaTableState>,
    /// the load options used during load
    pub config: DeltaTableConfig,
    /// log store
    pub(crate) log_store: LogStoreRef,
}

impl DeltaTableOldProvider {
    pub fn snapshot(&self) -> DeltaResult<&DeltaTableState> {
        self.state.as_ref().ok_or(DeltaTableError::NotInitialized)
    }
    pub fn log_store(&self) -> LogStoreRef {
        self.log_store.clone()
    }
}

impl From<DeltaTable> for DeltaTableOldProvider {
    fn from(value: DeltaTable) -> Self {
        Self {
            state: value.state.clone(),
            config: value.config.clone(),
            log_store: value.log_store.clone()
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaTableOldProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.snapshot().unwrap().snapshot().read_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        _session: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("scan is not available for this table provider; use scan_with_args")
    }

    async fn scan_with_args<'a>(&self, state: &dyn Session, args: ScanArgs<'a>) -> Result<ScanResult> {
        register_store(self.log_store(), state.runtime_env().as_ref());
        let filters = args.filters().unwrap_or(&[]);
        let filter_expr = conjunction(filters.iter().cloned());

        let config = DeltaScanConfigBuilder {
            include_file_column: false,
            file_column_name: None,
            wrap_partition_values: None,
            enable_parquet_pushdown: true,
            schema: None,
        };

        let config = config
            .build(self.snapshot()?.snapshot())?;

        let projection = args.projection().map(|p| p.to_vec());
        let scan = DeltaScanBuilder::new(self.snapshot()?.snapshot(), self.log_store(), state)
            .with_projection(projection.as_ref())
            .with_projection_deep(args.projection_deep())
            .with_limit(args.limit())
            .with_filter(filter_expr)
            .with_scan_config(config)
            .build()
            .await?;

        Ok(ScanResult::new(Arc::new(scan)))
    }
    
    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot()?.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot().ok()?.datafusion_table_statistics()
    }
}
