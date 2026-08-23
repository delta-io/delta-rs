use std::sync::Arc;

use datafusion::catalog::Session as DataFusionSession;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;

use crate::DeltaTable;
use crate::delta_datafusion::{SessionFallbackPolicy, create_session};
use crate::protocol::SaveMode;
use crate::test_utils::datafusion::WrapperSession;
use crate::writer::test_utils::{get_delta_schema, get_record_batch};

const PLANNING_ERROR_MESSAGE: &str = "intentional error for test";

fn wrapper_session(fail_planning: bool) -> Arc<dyn DataFusionSession> {
    let state = create_session().state();
    let wrapper = if fail_planning {
        WrapperSession::new_with_planning_error(state, PLANNING_ERROR_MESSAGE)
    } else {
        WrapperSession::new(state)
    };
    Arc::new(wrapper)
}

fn incompatible_session() -> Arc<dyn DataFusionSession> {
    wrapper_session(false)
}

fn incompatible_error_session() -> Arc<dyn DataFusionSession> {
    wrapper_session(true)
}

async fn empty_table() -> DeltaTable {
    let schema = get_delta_schema();
    DeltaTable::new_in_memory()
        .create()
        .with_columns(schema.fields().cloned())
        .await
        .unwrap()
}

async fn table_with_data() -> DeltaTable {
    empty_table()
        .await
        .write(vec![get_record_batch(None, false)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

fn assert_require_session_state_error(err: crate::DeltaTableError, operation: &str) {
    let msg = err.to_string();
    assert!(msg.contains(&format!("{operation}:")));
    assert!(msg.contains("not a SessionState"));
}

#[tokio::test]
async fn optimize_require_session_state_errors_for_incompatible_session() {
    let table = empty_table().await;

    let err = table
        .optimize()
        .with_session_state(incompatible_session())
        .with_session_fallback_policy(SessionFallbackPolicy::RequireSessionState)
        .await
        .unwrap_err();
    assert_require_session_state_error(err, "optimize");
}

#[tokio::test]
async fn merge_require_session_state_errors_for_incompatible_session() {
    let table = empty_table().await;
    let ctx = SessionContext::new();
    let source = ctx.read_batch(get_record_batch(None, false)).unwrap();

    let err = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_session_state(incompatible_session())
        .with_session_fallback_policy(SessionFallbackPolicy::RequireSessionState)
        .await
        .unwrap_err();
    assert_require_session_state_error(err, "merge");
}

#[tokio::test]
async fn update_require_session_state_errors_for_incompatible_session() {
    let table = empty_table().await;

    let err = table
        .update()
        .with_session_state(incompatible_session())
        .with_session_fallback_policy(SessionFallbackPolicy::RequireSessionState)
        .await
        .unwrap_err();
    assert_require_session_state_error(err, "update");
}

#[tokio::test]
async fn write_require_session_state_errors_for_incompatible_session() {
    let table = empty_table().await;

    let err = table
        .write(vec![get_record_batch(None, false)])
        .with_save_mode(SaveMode::Append)
        .with_session_state(incompatible_session())
        .with_session_fallback_policy(SessionFallbackPolicy::RequireSessionState)
        .await
        .unwrap_err();
    assert_require_session_state_error(err, "write");
}

#[tokio::test]
async fn delete_internal_defaults_falls_back_for_incompatible_session() {
    let table = table_with_data().await;

    let result = table
        .delete()
        .with_predicate(col("value").eq(lit(1)))
        .with_session_state(incompatible_error_session())
        .await;

    assert!(
        result.is_ok(),
        "expected internal defaults fallback to avoid incompatible session errors, got: {result:?}"
    );
}

#[tokio::test]
async fn delete_require_session_state_errors_for_incompatible_session() {
    let table = table_with_data().await;

    let err = table
        .delete()
        .with_predicate(col("value").eq(lit(1)))
        .with_session_state(incompatible_session())
        .with_session_fallback_policy(SessionFallbackPolicy::RequireSessionState)
        .await
        .unwrap_err();
    assert_require_session_state_error(err, "delete");
}
