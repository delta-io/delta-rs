//! Add a check constraint to a table

use futures::future::BoxFuture;
use itertools::Itertools;
use maplit::hashset;

use super::cast::merge_struct;
use super::transaction::{CommitBuilder, CommitProperties, PROTOCOL};

use crate::kernel::{Protocol, ReaderFeatures, StructField, WriterFeatures};
use crate::logstore::LogStoreRef;
use crate::operations::set_tbl_properties::convert_properties_to_features;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Build a constraint to add to a table
pub struct AddColumnBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Fields to add/merge into schema
    fields: Option<Vec<StructField>>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl super::Operation<()> for AddColumnBuilder {}

impl AddColumnBuilder {
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

impl std::future::IntoFuture for AddColumnBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let mut metadata = this.snapshot.metadata().clone();
            let fields = match this.fields {
                Some(v) => v,
                None => return Err(DeltaTableError::Generic("No fields provided".to_string())),
            };

            let table_schema = this.snapshot.schema();
            let new_table_schema =
                merge_struct(table_schema, &fields.clone().into_iter().collect())?;

            // TODO(ion): Think of a way how we can simply this checking through the API or centralize some checks.
            let contains_timestampntz = PROTOCOL.contains_timestampntz(&fields);
            let protocol = this.snapshot.protocol();

            let maybe_new_protocol = if contains_timestampntz {
                if protocol.min_reader_version == 3 && protocol.min_writer_version == 7 {
                    let mut new_protocol = protocol.clone();
                    new_protocol = new_protocol
                        .with_reader_features(vec![ReaderFeatures::TimestampWithoutTimezone]);
                    new_protocol = new_protocol
                        .with_writer_features(vec![WriterFeatures::TimestampWithoutTimezone]);
                    Some(new_protocol)
                } else {
                    let new_protocol = Protocol {
                        min_reader_version: 3,
                        min_writer_version: 7,
                        writer_features: Some(hashset! {WriterFeatures::TimestampWithoutTimezone}),
                        reader_features: Some(hashset! {ReaderFeatures::TimestampWithoutTimezone}),
                    };
                    // Convert existing properties to features since we advance the protocol to v3,7
                    Some(convert_properties_to_features(
                        new_protocol,
                        &metadata.configuration,
                    ))
                }
            } else {
                None
            };

            let operation = DeltaOperation::AddColumn {
                fields: fields.into_iter().collect_vec(),
            };

            metadata.schema_string = serde_json::to_string(&new_table_schema)?;

            let mut actions = vec![metadata.into()];

            if let Some(new_protocol) = maybe_new_protocol {
                actions.push(new_protocol.into())
            }

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
