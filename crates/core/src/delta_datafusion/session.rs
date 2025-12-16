use std::sync::Arc;

use datafusion::{
    catalog::Session,
    common::{Result as DataFusionResult, exec_datafusion_err},
    execution::{
        SessionState, SessionStateBuilder,
        disk_manager::DiskManagerBuilder,
        memory_pool::FairSpillPool,
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    prelude::{SessionConfig, SessionContext},
    sql::planner::ParserOptions,
};

use crate::delta_datafusion::planner::DeltaPlanner;

pub fn create_session() -> DeltaSessionContext {
    DeltaSessionContext::default()
}

#[cfg(test)]
pub fn create_test_session() -> DeltaSessionContext {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    let session = DeltaSessionContext::default();
    session.inner.runtime_env().register_object_store(
        &url::Url::parse("memory:///").unwrap(),
        Arc::new(InMemory::new()),
    );
    session
}

// Given a `Session` reference, get the concrete `SessionState` reference
// Note: this may stop working in future versions,
#[deprecated(
    since = "0.29.1",
    note = "Stop gap to get rid of all explicit session state references"
)]
pub(crate) fn session_state_from_session(session: &dyn Session) -> DataFusionResult<&SessionState> {
    session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| exec_datafusion_err!("Failed to downcast Session to SessionState"))
}

/// A wrapper for sql_parser's ParserOptions to capture sane default table defaults
pub struct DeltaParserOptions {
    inner: ParserOptions,
}

impl Default for DeltaParserOptions {
    fn default() -> Self {
        DeltaParserOptions {
            inner: ParserOptions {
                enable_ident_normalization: false,
                ..ParserOptions::default()
            },
        }
    }
}

impl From<DeltaParserOptions> for ParserOptions {
    fn from(value: DeltaParserOptions) -> Self {
        value.inner
    }
}

/// A wrapper for Deltafusion's SessionConfig to capture sane default table defaults
pub struct DeltaSessionConfig {
    inner: SessionConfig,
}

impl Default for DeltaSessionConfig {
    fn default() -> Self {
        DeltaSessionConfig {
            inner: SessionConfig::default()
                .set_bool("datafusion.sql_parser.enable_ident_normalization", false),
        }
    }
}

impl From<DeltaSessionConfig> for SessionConfig {
    fn from(value: DeltaSessionConfig) -> Self {
        value.inner
    }
}

/// A builder for configuring DataFusion RuntimeEnv with Delta-specific defaults
#[derive(Default)]
pub struct DeltaRuntimeEnvBuilder {
    inner: RuntimeEnvBuilder,
}

impl DeltaRuntimeEnvBuilder {
    pub fn new() -> Self {
        Self {
            inner: RuntimeEnvBuilder::new(),
        }
    }

    pub fn with_max_spill_size(mut self, size: usize) -> Self {
        let memory_pool = FairSpillPool::new(size);
        self.inner = self.inner.with_memory_pool(Arc::new(memory_pool));
        self
    }

    pub fn with_max_temp_directory_size(mut self, size: u64) -> Self {
        let disk_manager = DiskManagerBuilder::default().with_max_temp_directory_size(size);
        self.inner = self.inner.with_disk_manager_builder(disk_manager);
        self
    }

    pub fn build(self) -> Arc<RuntimeEnv> {
        self.inner.build_arc().unwrap()
    }
}

/// A wrapper for DataFusion's SessionContext with Delta-specific defaults
///
/// This provides a way of creating DataFusion sessions with consistent
/// Delta Lake configuration (case-sensitive identifiers, Delta planner, etc.)
pub struct DeltaSessionContext {
    inner: SessionContext,
}

impl DeltaSessionContext {
    /// Create a new DeltaSessionContext with default configuration
    pub fn new() -> Self {
        let config = DeltaSessionConfig::default().into();
        let runtime_env = RuntimeEnvBuilder::new().build_arc().unwrap();
        Self::new_with_config_and_runtime(config, runtime_env)
    }

    /// Create a DeltaSessionContext with a custom RuntimeEnv
    pub fn with_runtime_env(runtime_env: Arc<RuntimeEnv>) -> Self {
        let config = DeltaSessionConfig::default().into();
        Self::new_with_config_and_runtime(config, runtime_env)
    }

    fn new_with_config_and_runtime(config: SessionConfig, runtime_env: Arc<RuntimeEnv>) -> Self {
        let planner = DeltaPlanner::new();
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_query_planner(planner)
            .build();

        let inner = SessionContext::new_with_state(state);
        Self { inner }
    }

    pub fn into_inner(self) -> SessionContext {
        self.inner
    }

    pub fn state(&self) -> SessionState {
        self.inner.state()
    }
}

impl Default for DeltaSessionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl From<DeltaSessionContext> for SessionContext {
    fn from(value: DeltaSessionContext) -> Self {
        value.inner
    }
}
