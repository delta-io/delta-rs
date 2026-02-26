use std::sync::Arc;

use datafusion::{
    catalog::Session as DataFusionSession,
    execution::{
        SessionState, SessionStateBuilder,
        disk_manager::DiskManagerBuilder,
        memory_pool::FairSpillPool,
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    prelude::{SessionConfig, SessionContext},
    sql::planner::ParserOptions,
};
use url::Url;
use uuid::Uuid;

use crate::delta_datafusion::engine::AsObjectStoreUrl;
use crate::delta_datafusion::planner::DeltaPlanner;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::logstore::LogStore;

pub fn create_session() -> DeltaSessionContext {
    DeltaSessionContext::default()
}

/// Create a [`SessionState`] with optional spill-to-disk configuration.
///
/// When either parameter is `Some`, a [`FairSpillPool`] memory pool and a sized
/// [`DiskManagerBuilder`] are wired into the runtime environment so that
/// DataFusion can spill intermediate results to disk instead of running out of
/// memory.
///
/// # Arguments
/// * `max_spill_size` – Maximum bytes kept in memory before spilling. `None` uses DataFusion's default (unbounded) pool.
/// * `max_temp_directory_size` – Maximum disk space for temporary spill files. `None` uses DataFusion's default disk manager.
pub fn create_session_state_with_spill_config(
    max_spill_size: Option<usize>,
    max_temp_directory_size: Option<u64>,
) -> SessionState {
    if max_spill_size.is_none() && max_temp_directory_size.is_none() {
        return DeltaSessionContext::new().state();
    }

    let mut builder = DeltaRuntimeEnvBuilder::new();
    if let Some(spill_size) = max_spill_size {
        builder = builder.with_max_spill_size(spill_size);
    }
    if let Some(directory_size) = max_temp_directory_size {
        builder = builder.with_max_temp_directory_size(directory_size);
    }

    DeltaSessionContext::with_runtime_env(builder.build()).state()
}

pub(crate) trait DeltaSessionExt: DataFusionSession {
    /// Ensure the session's `RuntimeEnv` has the object store registered for the log store's
    /// root URL.
    ///
    /// This method is idempotent and will not overwrite an existing object store mapping for the
    /// URL.
    ///
    /// Note: this is a best-effort "check then register" helper and is not atomic. Concurrent
    /// callers may race and register the same mapping multiple times (typically benign).
    ///
    /// If the session already has a (stale/incorrect) object store registered for the URL, this
    /// method will not replace it; callers must explicitly override the mapping via
    /// `RuntimeEnv::register_object_store`.
    fn ensure_object_store_registered(
        &self,
        log_store: &dyn LogStore,
        operation_id: Option<Uuid>,
    ) -> DeltaResult<()>;

    /// Ensure the session's `RuntimeEnv` has a per-table, prefixed object store registered.
    ///
    /// Delta-rs historically registered a synthetic per-table object store URL (using the
    /// `delta-rs://...` scheme) that maps to `LogStore::object_store` (scoped to the table root).
    /// This is primarily a migration helper for legacy DataFusion integrations that expect
    /// table-relative paths.
    ///
    /// Only non-migrated/legacy operations should rely on this behavior; the newer table provider
    /// path does not rely on these internal/special `delta-rs://...` URLs.
    ///
    /// This does not support fully-qualified file URLs (e.g. shallow clones). Prefer
    /// `ensure_object_store_registered` in new code.
    ///
    /// This method is idempotent and will not overwrite an existing object store mapping for the
    /// table's delta-rs object store URL.
    fn ensure_log_store_registered(&self, log_store: &dyn LogStore) -> DeltaResult<()> {
        let object_store_url = log_store.object_store_url();
        if self.runtime_env().object_store(&object_store_url).is_err() {
            self.runtime_env()
                .register_object_store(object_store_url.as_ref(), log_store.object_store(None));
        }
        Ok(())
    }
}

impl<T> DeltaSessionExt for T
where
    T: DataFusionSession + ?Sized,
{
    fn ensure_object_store_registered(
        &self,
        log_store: &dyn LogStore,
        operation_id: Option<Uuid>,
    ) -> DeltaResult<()> {
        let url = log_store.root_url().as_object_store_url();
        if self.runtime_env().object_store(&url).is_err() {
            self.runtime_env()
                .register_object_store(url.as_ref(), log_store.root_object_store(operation_id));
        }
        Ok(())
    }
}

/// Controls how delta-rs resolves a caller-provided DataFusion `Session` into a `SessionState`.
///
/// This is an opt-in knob on operations that accept `with_session_state(...)`. Defaults to
/// `InternalDefaults` to preserve existing behavior.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionFallbackPolicy {
    /// If the provided session is not a `SessionState`, log a warning and use internal defaults.
    InternalDefaults,
    /// Derive a `SessionState` from the `Session` trait (runtime/config/UDF registries).
    DeriveFromTrait,
    /// Return an error if the provided session is not a `SessionState`.
    RequireSessionState,
}

impl Default for SessionFallbackPolicy {
    fn default() -> Self {
        Self::InternalDefaults
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolvedSessionStateKind {
    NoSessionProvided,
    CallerWasSessionState,
    DerivedFromTrait,
    InternalDefaultsIncompatibleSession,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SessionResolveContext<'a> {
    pub operation: &'static str,
    pub table_uri: Option<&'a Url>,
    pub cdc: bool,
}

pub(crate) fn resolve_session_state(
    provided: Option<&dyn DataFusionSession>,
    policy: SessionFallbackPolicy,
    default_state: impl FnOnce() -> SessionState,
    ctx: SessionResolveContext<'_>,
) -> DeltaResult<(SessionState, ResolvedSessionStateKind)> {
    let Some(provided) = provided else {
        return Ok((default_state(), ResolvedSessionStateKind::NoSessionProvided));
    };

    if let Some(state) = provided.as_any().downcast_ref::<SessionState>() {
        return Ok((
            state.clone(),
            ResolvedSessionStateKind::CallerWasSessionState,
        ));
    }

    match policy {
        SessionFallbackPolicy::InternalDefaults => {
            warn_incompatible_session(ctx, provided);
            Ok((
                default_state(),
                ResolvedSessionStateKind::InternalDefaultsIncompatibleSession,
            ))
        }
        SessionFallbackPolicy::DeriveFromTrait => {
            let derived = derive_session_state_from_trait(ctx, provided);
            Ok((derived, ResolvedSessionStateKind::DerivedFromTrait))
        }
        SessionFallbackPolicy::RequireSessionState => Err(DeltaTableError::generic(format!(
            "{operation}: provided DataFusion Session (session_id={session_id}) is not a SessionState. \
To fix: pass a concrete SessionState (e.g. Arc::new(create_session().state()) or Arc::new(session_ctx.state())). \
Catalogs are not transferable via the Session trait. See delta-io/delta-rs#4081.",
            operation = ctx.operation,
            session_id = provided.session_id(),
        ))),
    }
}

fn warn_incompatible_session(ctx: SessionResolveContext<'_>, session: &dyn DataFusionSession) {
    let table_uri = ctx.table_uri.map(|u| u.as_str());
    tracing::warn!(
        operation = ctx.operation,
        table_uri = ?table_uri,
        session_id = %session.session_id(),
        cdc = ctx.cdc,
        "Provided DataFusion Session is not a SessionState; falling back to internal defaults. \
    This may ignore the caller's runtime/config/execution props/UDF registries/caches. \
    Catalogs are not transferable via the Session trait. See delta-io/delta-rs#4081."
    );
}

fn derive_session_state_from_trait(
    ctx: SessionResolveContext<'_>,
    session: &dyn DataFusionSession,
) -> SessionState {
    let scalar_fns = session
        .scalar_functions()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let aggregate_fns = session
        .aggregate_functions()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let window_fns = session
        .window_functions()
        .values()
        .cloned()
        .collect::<Vec<_>>();

    tracing::debug!(
        operation = ctx.operation,
        session_id = %session.session_id(),
        preserved_runtime_env = true,
        preserved_config = true,
        preserved_execution_props = true,
        preserved_table_options = true,
        preserved_scalar_functions = scalar_fns.len(),
        preserved_aggregate_functions = aggregate_fns.len(),
        preserved_window_functions = window_fns.len(),
        dropped_catalogs = true,
        dropped_custom_planners = true,
        dropped_optimizer_rules = true,
        "Derived SessionState from Session trait (catalogs/planners/optimizers not transferable)"
    );

    SessionStateBuilder::new()
        .with_default_features()
        .with_config(session.config().clone())
        .with_runtime_env(Arc::clone(session.runtime_env()))
        .with_execution_props(session.execution_props().clone())
        .with_table_options(session.table_options().clone())
        .with_scalar_functions(scalar_fns)
        .with_aggregate_functions(aggregate_fns)
        .with_window_functions(window_fns)
        .with_query_planner(DeltaPlanner::new())
        .build()
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
                .set_bool("datafusion.sql_parser.enable_ident_normalization", false)
                .set_bool("datafusion.execution.parquet.schema_force_view_types", true)
                // Workaround: hash-join dynamic filtering (IN-list pushdown) can panic when join
                // keys include dictionary arrays (still reproducible with DF 52.1.x crates).
                // Disable IN-list pushdown and fall back to hash lookups.
                .set_usize("datafusion.optimizer.hash_join_inlist_pushdown_max_size", 0)
                .set_usize(
                    "datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values",
                    0,
                ),
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::test_utils::datafusion::{WrapperSession, make_test_scalar_udf};

    const TEST_UDF_NAME: &str = "delta_rs_test_udf";

    fn make_incompatible_session() -> WrapperSession {
        let runtime_env = DeltaRuntimeEnvBuilder::new()
            .with_max_spill_size(123)
            .build();
        let config = SessionConfig::new().with_batch_size(8192);

        let udf = make_test_scalar_udf(TEST_UDF_NAME);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_query_planner(DeltaPlanner::new())
            .with_scalar_functions(vec![udf])
            .build();

        WrapperSession::new(state)
    }

    fn ctx(operation: &'static str) -> SessionResolveContext<'static> {
        SessionResolveContext {
            operation,
            table_uri: None,
            cdc: false,
        }
    }

    #[test]
    fn ensure_object_store_registered_is_idempotent() {
        let table = crate::DeltaTable::new_in_memory();
        let session = create_session().state();

        session
            .ensure_object_store_registered(table.log_store().as_ref(), None)
            .unwrap();
        session
            .ensure_object_store_registered(table.log_store().as_ref(), None)
            .unwrap();

        let url = table.log_store().root_url().as_object_store_url();
        assert!(session.runtime_env().object_store(&url).is_ok());
    }

    #[test]
    fn derive_from_trait_preserves_runtime_env_config_and_udfs() {
        let wrapper = make_incompatible_session();

        let (derived, kind) = resolve_session_state(
            Some(&wrapper),
            SessionFallbackPolicy::DeriveFromTrait,
            || create_session().state(),
            ctx("test"),
        )
        .unwrap();

        assert_eq!(kind, ResolvedSessionStateKind::DerivedFromTrait);
        assert!(Arc::ptr_eq(derived.runtime_env(), wrapper.runtime_env()));
        assert_eq!(
            derived.config().options().execution.batch_size,
            wrapper.config().options().execution.batch_size
        );

        let original_udf = wrapper
            .scalar_functions()
            .get(TEST_UDF_NAME)
            .cloned()
            .unwrap();
        let derived_udf = derived
            .scalar_functions()
            .get(TEST_UDF_NAME)
            .cloned()
            .unwrap();
        assert!(Arc::ptr_eq(&original_udf, &derived_udf));
    }

    #[test]
    fn internal_defaults_uses_default_state_when_incompatible() {
        let wrapper = make_incompatible_session();

        let invoked = AtomicUsize::new(0);
        let default_runtime_env = RuntimeEnvBuilder::new().build_arc().unwrap();
        let default_state = || {
            invoked.fetch_add(1, Ordering::SeqCst);
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_batch_size(1024))
                .with_runtime_env(Arc::clone(&default_runtime_env))
                .with_query_planner(DeltaPlanner::new())
                .build()
        };

        let (resolved, kind) = resolve_session_state(
            Some(&wrapper),
            SessionFallbackPolicy::InternalDefaults,
            default_state,
            ctx("test"),
        )
        .unwrap();

        assert_eq!(invoked.load(Ordering::SeqCst), 1);
        assert_eq!(
            kind,
            ResolvedSessionStateKind::InternalDefaultsIncompatibleSession
        );
        assert!(Arc::ptr_eq(resolved.runtime_env(), &default_runtime_env));
        assert!(!Arc::ptr_eq(resolved.runtime_env(), wrapper.runtime_env()));
    }

    #[test]
    fn require_session_state_errors_for_incompatible_session() {
        let wrapper = make_incompatible_session();

        let err = resolve_session_state(
            Some(&wrapper),
            SessionFallbackPolicy::RequireSessionState,
            || create_session().state(),
            ctx("test"),
        )
        .unwrap_err();

        let msg = err.to_string();
        assert!(msg.contains("not a SessionState"));
        assert!(msg.contains("Arc::new"));
    }

    #[test]
    fn compatible_session_state_does_not_invoke_default() {
        let state = create_session().state();
        let invoked = AtomicUsize::new(0);

        let default_state = || {
            invoked.fetch_add(1, Ordering::SeqCst);
            create_session().state()
        };

        let (_resolved, kind) = resolve_session_state(
            Some(&state),
            SessionFallbackPolicy::InternalDefaults,
            default_state,
            ctx("test"),
        )
        .unwrap();

        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert_eq!(kind, ResolvedSessionStateKind::CallerWasSessionState);
    }
}
