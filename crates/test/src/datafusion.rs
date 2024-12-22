use deltalake_core::datafusion::execution::context::SessionContext;
use deltalake_core::datafusion::execution::session_state::SessionStateBuilder;
use deltalake_core::delta_datafusion::DeltaTableFactory;
use std::sync::Arc;

pub fn context_with_delta_table_factory() -> SessionContext {
    let mut state = SessionStateBuilder::new().build();
    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
    SessionContext::new_with_state(state)
}
