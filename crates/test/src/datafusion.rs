use deltalake_core::datafusion::execution::context::SessionContext;
use deltalake_core::datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use deltalake_core::datafusion::execution::session_state::SessionStateBuilder;
use deltalake_core::datafusion::prelude::SessionConfig;
use deltalake_core::delta_datafusion::DeltaTableFactory;
use std::sync::Arc;

pub fn context_with_delta_table_factory() -> SessionContext {
    let cfg = RuntimeConfig::new();
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let mut state = SessionStateBuilder::new()
        .with_config(ses)
        .with_runtime_env(Arc::new(env))
        .build();
    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
    SessionContext::new_with_state(state)
}
