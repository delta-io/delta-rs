//! Add a check constraint to a table

use std::sync::Arc;

use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::ToDFSchema;
use futures::future::BoxFuture;
use futures::StreamExt;
use itertools::Itertools;

use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::{
    register_store, DeltaDataChecker, DeltaScanBuilder, DeltaSessionContext,
};
use crate::kernel::{Protocol, StructField, StructType, WriterFeatures};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::table::Constraint;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

use super::cast::merge_struct;
use super::datafusion_utils::into_expr;
use super::transaction::{CommitBuilder, CommitProperties};

/// Build a constraint to add to a table
pub struct AlterColumnBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Fields to add/merge into schema
    fields: Option<Vec<StructField>>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl super::Operation<()> for AlterColumnBuilder {}

impl AlterColumnBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            fields: None,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the constraint to be added
    pub fn with_fields(mut self, fields: Vec<StructField>) -> Self {
        self.fields = Some(fields);
        self
    }
    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }
}

impl std::future::IntoFuture for AlterColumnBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let mut metadata = this.snapshot.metadata().clone();
            let fields: StructType = match this.fields {
                Some(v) => v.into_iter().collect(),
                None => return Err(DeltaTableError::Generic("No fields provided".to_string())),
            };

            let table_schema = this.snapshot.schema();

            let new_table_schema = merge_struct(table_schema, &fields)?;

            dbg!(&new_table_schema);

            let operation = DeltaOperation::AlterColumn {
                fields: fields.into_iter().cloned().collect_vec(),
            };

            metadata.schema_string = serde_json::to_string(&new_table_schema)?;

            let actions = vec![metadata.into()];

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions)
                .build(Some(&this.snapshot), this.log_store.clone(), operation)
                .await?;

            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
        })
    }
}

#[cfg(feature = "datafusion")]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion_expr::{col, lit};

    use crate::writer::test_utils::{create_bare_table, get_arrow_schema, get_record_batch};
    use crate::{DeltaOps, DeltaResult, DeltaTable};
}
