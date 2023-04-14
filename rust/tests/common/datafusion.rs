use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionConfig;
use deltalake::delta_datafusion::DeltaTableFactory;
use std::sync::Arc;

pub fn context_with_delta_table_factory() -> SessionContext {
    let cfg = RuntimeConfig::new();
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let mut state = SessionState::with_config_rt(ses, Arc::new(env));
    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
    SessionContext::with_state(state)
}
