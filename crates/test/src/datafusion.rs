use deltalake_core::datafusion::execution::context::{SessionContext, SessionState};
use deltalake_core::datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use deltalake_core::datafusion::prelude::SessionConfig;
use deltalake_core::delta_datafusion::DeltaTableFactory;
use std::sync::Arc;

pub fn context_with_delta_table_factory() -> SessionContext {
    let cfg = RuntimeConfig::new();
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let mut state = SessionState::new_with_config_rt(ses, Arc::new(env));
    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
    SessionContext::new_with_state(state)
}
