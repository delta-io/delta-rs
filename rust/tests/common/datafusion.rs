use datafusion::datasource::datasource::TableProviderFactory;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionConfig;
use deltalake::delta_datafusion::DeltaTableFactory;
use std::collections::HashMap;
use std::sync::Arc;

pub fn context_with_delta_table_factory() -> SessionContext {
    let mut table_factories: HashMap<String, Arc<dyn TableProviderFactory>> = HashMap::new();
    table_factories.insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
    let cfg = RuntimeConfig::new().with_table_factories(table_factories);
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    SessionContext::with_config_rt(ses, Arc::new(env))
}
