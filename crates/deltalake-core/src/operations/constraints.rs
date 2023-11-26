use std::collections::HashMap;

use chrono::Utc;
use datafusion::execution::context::SessionState;
use futures::future::BoxFuture;

use crate::kernel::{Action, CommitInfo, Metadata, Protocol};
use crate::logstore::LogStoreRef;
use crate::operations::datafusion_utils::Expression;
use crate::operations::transaction::commit;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;
use crate::{DeltaResult, DeltaTableError};

pub struct ConstraintBuilder {
    snapshot: DeltaTableState,
    name: Option<String>,
    expr: Option<Expression>,
    log_store: LogStoreRef,
    state: Option<SessionState>,
}

impl ConstraintBuilder {
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            name: None,
            expr: None,
            snapshot,
            log_store,
            state: None,
        }
    }

    pub fn with_constraint<S: Into<String>, E: Into<Expression>>(
        mut self,
        column: S,
        expression: E,
    ) -> Self {
        self.name = Some(column.into());
        self.expr = Some(expression.into());
        self
    }

    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }
}

impl std::future::IntoFuture for ConstraintBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        let fut = async move {
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
            let mut metadata = this
                .snapshot
                .current_metadata()
                .ok_or(DeltaTableError::NoMetadata)?
                .clone();

            metadata
                .configuration
                .insert(format!("delta.constraints.{}", name), Some(expr.into()));

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
                reader_features: None,
                writer_features: None,
            };

            let operational_parameters = HashMap::from_iter(&[(name, expr)]);
            let commit_info = CommitInfo {
                timestamp: Some(Utc::now().timestamp_millis()),
                operation: Some("ADD CONSTRAINT".to_string()),
                operation_parameters: Some(operational_parameters),
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

            let version = commit(
                this.log_store.as_ref(),
                &actions,
                operations,
                &this.snapshot,
                None,
            )
            .await?;

            Ok(DeltaTable::new_with_state(this.log_store, this.snapshot))
        };
        Box::pin(fut)
    }
}
