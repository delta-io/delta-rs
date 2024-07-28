//! Enable table features

use futures::future::BoxFuture;

use super::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::TableFeatures;
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;
use crate::{DeltaResult, DeltaTableError};

/// Enable table features for a table
pub struct AddTableFeatureBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Name of the feature
    name: Option<TableFeatures>,
    /// Allow protocol versions to be increased by setting features
    allow_protocol_versions_increase: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl super::Operation<()> for AddTableFeatureBuilder {}

impl AddTableFeatureBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            name: None,
            allow_protocol_versions_increase: false,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the feature to be added
    pub fn with_feature<S: Into<TableFeatures>>(mut self, name: S) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Specify if you want to allow protocol version to be increased
    pub fn with_allow_protocol_versions_increase(mut self, allow: bool) -> Self {
        self.allow_protocol_versions_increase = allow;
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }
}

impl std::future::IntoFuture for AddTableFeatureBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let name = this
                .name
                .ok_or(DeltaTableError::Generic("No features provided".to_string()))?;

            let (reader_feature, writer_feature) = name.to_reader_writer_features();

            let mut protocol = this.snapshot.protocol().clone();

            if !this.allow_protocol_versions_increase {
                if reader_feature.is_some()
                    && writer_feature.is_some()
                    && protocol.min_reader_version == 3
                    && protocol.min_writer_version == 7
                {
                    return Err(DeltaTableError::Generic("Table feature enables reader and writer feature, but reader is not v3, and writer not v7. Set allow_protocol_versions_increase or increase versions explicitly through set_tbl_properties".to_string()));
                } else if reader_feature.is_some() && protocol.min_reader_version < 3 {
                    return Err(DeltaTableError::Generic("Table feature enables reader feature, but min_reader is not v3. Set allow_protocol_versions_increase or increase version explicitly through set_tbl_properties".to_string()));
                } else if writer_feature.is_some() && protocol.min_writer_version < 7 {
                    return Err(DeltaTableError::Generic("Table feature enables writer feature, but min_writer is not v7. Set allow_protocol_versions_increase or increase version explicitly through set_tbl_properties".to_string()));
                }
            }

            if let Some(reader_feature) = reader_feature {
                protocol = protocol.with_reader_features(vec![reader_feature]);
            }

            if let Some(writer_feature) = writer_feature {
                protocol = protocol.with_writer_features(vec![writer_feature]);
            }

            let operation = DeltaOperation::AddFeature { name };

            let actions = vec![protocol.into()];

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
