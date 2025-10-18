use datafusion::{
    catalog::Session,
    common::{exec_datafusion_err, Result as DataFusionResult},
    execution::{SessionState, SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
    sql::planner::ParserOptions,
};

use crate::delta_datafusion::planner::DeltaPlanner;

pub fn create_session() -> DeltaSessionContext {
    DeltaSessionContext::default()
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

/// A wrapper for Deltafusion's SessionContext to capture sane default table defaults
pub struct DeltaSessionContext {
    inner: SessionContext,
}

impl DeltaSessionContext {
    pub fn new() -> Self {
        let ctx = SessionContext::new_with_config(DeltaSessionConfig::default().into());
        let planner = DeltaPlanner::new();
        let state = SessionStateBuilder::new_from_existing(ctx.state())
            .with_query_planner(planner)
            .build();
        let inner = SessionContext::new_with_state(state);
        Self { inner }
    }

    pub fn into_inner(self) -> SessionContext {
        self.inner
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
