//! Add a check constraint to a table

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use serde_json::json;

use crate::delta_datafusion::{find_files, register_store, DeltaDataChecker, DeltaScanBuilder};
use crate::kernel::{Action, CommitInfo, IsolationLevel, Metadata, Protocol};
use crate::logstore::LogStoreRef;
use crate::operations::datafusion_utils::Expression;
use crate::operations::transaction::commit;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::table::Constraint;
use crate::DeltaTable;
use crate::{DeltaResult, DeltaTableError};
use crate::operations::collect_sendable_stream;

/// Build a constraint to add to a table
pub struct ConstraintBuilder {
    snapshot: DeltaTableState,
    name: Option<String>,
    expr: Option<Expression>,
    log_store: LogStoreRef,
    state: Option<SessionState>,
}

impl ConstraintBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            name: None,
            expr: None,
            snapshot,
            log_store,
            state: None,
        }
    }

    /// Specify the constraint to be added
    pub fn with_constraint<S: Into<String>, E: Into<Expression>>(
        mut self,
        column: S,
        expression: E,
    ) -> Self {
        self.name = Some(column.into());
        self.expr = Some(expression.into());
        self
    }

    /// Specify the datafusion session context
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }
}

impl std::future::IntoFuture for ConstraintBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            if this.name.is_none() {
                return Err(DeltaTableError::Generic("No name provided".to_string()));
            } else if this.expr.is_none() {
                return Err(DeltaTableError::Generic(
                    "No expression provided".to_string(),
                ));
            }

            let name = this.name.unwrap();
            let expr = match this.expr.unwrap() {
                Expression::String(s) => s,
                Expression::DataFusion(e) => e.to_string(),
            };

            let state = this.state.unwrap_or_else(|| {
                let session = SessionContext::new();
                register_store(this.log_store.clone(), session.runtime_env());
                session.state()
            });
            dbg!(&state);

            let checker = DeltaDataChecker::with_constraints(vec![Constraint::new("*", &expr)]);

            let files_to_check =
                find_files(&this.snapshot, this.log_store.clone(), &state, None).await?;
            dbg!(&files_to_check.candidates);
            let scan = DeltaScanBuilder::new(&this.snapshot, this.log_store.clone(), &state)
                .with_files(&files_to_check.candidates)
                .build()
                .await?;
            let scan = Arc::new(scan);

            let task_ctx = Arc::new(TaskContext::from(&state));

            let record_stream: SendableRecordBatchStream = scan.execute(0, task_ctx)?;
            let records = collect_sendable_stream(record_stream).await?;
            for batch in records {
                checker.check_batch(&batch).await?;
            }

            dbg!(&name, &expr);
            let mut metadata = this
                .snapshot
                .current_metadata()
                .ok_or(DeltaTableError::NoMetadata)?
                .clone();
            let configuration_key = format!("delta.constraints.{}", name);

            if metadata.configuration.contains_key(&configuration_key) {
                return Err(DeltaTableError::Generic(format!(
                    "Constraint with name: {} already exists, expr: {}",
                    name, expr
                )));
            }

            metadata
                .configuration
                .insert(format!("delta.constraints.{}", name), Some(expr.clone()));

            let protocol = Protocol {
                min_reader_version: if this.snapshot.min_reader_version() > 1 {
                    this.snapshot.min_reader_version()
                } else {
                    1
                },
                min_writer_version: if this.snapshot.min_reader_version() > 3 {
                    this.snapshot.min_reader_version()
                } else {
                    3
                },
                reader_features: this.snapshot.reader_features().cloned(),
                writer_features: this.snapshot.writer_features().cloned(),
            };

            let operational_parameters = HashMap::from_iter([
                ("name".to_string(), json!(&name)),
                ("expr".to_string(), json!(&expr)),
            ]);
            let commit_info = CommitInfo {
                timestamp: Some(Utc::now().timestamp_millis()),
                operation: Some("ADD CONSTRAINT".to_string()),
                operation_parameters: Some(operational_parameters),
                read_version: Some(this.snapshot.version()),
                isolation_level: Some(IsolationLevel::Serializable),
                is_blind_append: Some(false),
                ..Default::default()
            };

            let actions = vec![
                Action::CommitInfo(commit_info),
                Action::Metadata(Metadata::try_from(metadata)?),
                Action::Protocol(protocol),
            ];

            let operations = DeltaOperation::AddConstraint {
                name: name.clone(),
                expr: expr.clone(),
            };

            dbg!(&actions);

            let _version = commit(
                this.log_store.as_ref(),
                &actions,
                operations,
                &this.snapshot,
                None,
            )
            .await?;
            dbg!(_version);

            Ok(DeltaTable::new_with_state(this.log_store, this.snapshot))
        })
    }
}
