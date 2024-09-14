//! Enable table features

use futures::future::BoxFuture;
use itertools::Itertools;

use super::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{ReaderFeatures, TableFeatures, WriterFeatures};
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
    name: Vec<TableFeatures>,
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
            name: vec![],
            allow_protocol_versions_increase: false,
            snapshot,
            log_store,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the features to be added
    pub fn with_feature<S: Into<TableFeatures>>(mut self, name: S) -> Self {
        self.name.push(name.into());
        self
    }

    /// Specify the features to be added
    pub fn with_features<S: Into<TableFeatures>>(mut self, name: Vec<S>) -> Self {
        self.name
            .extend(name.into_iter().map(Into::into).collect_vec());
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
            let name = if this.name.is_empty() {
                return Err(DeltaTableError::Generic("No features provided".to_string()));
            } else {
                this.name
            };
            let (reader_features, writer_features): (
                Vec<Option<ReaderFeatures>>,
                Vec<Option<WriterFeatures>>,
            ) = name.iter().map(|v| v.to_reader_writer_features()).unzip();
            let reader_features = reader_features.into_iter().flatten().collect_vec();
            let writer_features = writer_features.into_iter().flatten().collect_vec();

            let mut protocol = this.snapshot.protocol().clone();

            if !this.allow_protocol_versions_increase {
                if !reader_features.is_empty()
                    && !writer_features.is_empty()
                    && !(protocol.min_reader_version == 3 && protocol.min_writer_version == 7)
                {
                    return Err(DeltaTableError::Generic("Table feature enables reader and writer feature, but reader is not v3, and writer not v7. Set allow_protocol_versions_increase or increase versions explicitly through set_tbl_properties".to_string()));
                } else if !reader_features.is_empty() && protocol.min_reader_version < 3 {
                    return Err(DeltaTableError::Generic("Table feature enables reader feature, but min_reader is not v3. Set allow_protocol_versions_increase or increase version explicitly through set_tbl_properties".to_string()));
                } else if !writer_features.is_empty() && protocol.min_writer_version < 7 {
                    return Err(DeltaTableError::Generic("Table feature enables writer feature, but min_writer is not v7. Set allow_protocol_versions_increase or increase version explicitly through set_tbl_properties".to_string()));
                }
            }

            protocol = protocol.with_reader_features(reader_features);
            protocol = protocol.with_writer_features(writer_features);

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

#[cfg(feature = "datafusion")]
#[cfg(test)]
mod tests {
    use delta_kernel::DeltaResult;

    use crate::{
        kernel::TableFeatures,
        writer::test_utils::{create_bare_table, get_record_batch},
        DeltaOps,
    };

    #[tokio::test]
    async fn add_feature() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await
            .unwrap();
        let table = DeltaOps(write);
        let result = table
            .add_feature()
            .with_feature(TableFeatures::ChangeDataFeed)
            .with_allow_protocol_versions_increase(true)
            .await
            .unwrap();

        assert!(&result
            .protocol()
            .cloned()
            .unwrap()
            .writer_features
            .unwrap_or_default()
            .contains(&crate::kernel::WriterFeatures::ChangeDataFeed));

        let result = DeltaOps(result)
            .add_feature()
            .with_feature(TableFeatures::DeletionVectors)
            .with_allow_protocol_versions_increase(true)
            .await
            .unwrap();

        let current_protocol = &result.protocol().cloned().unwrap();
        assert!(&current_protocol
            .writer_features
            .clone()
            .unwrap_or_default()
            .contains(&crate::kernel::WriterFeatures::DeletionVectors));
        assert!(&current_protocol
            .reader_features
            .clone()
            .unwrap_or_default()
            .contains(&crate::kernel::ReaderFeatures::DeletionVectors));
        assert_eq!(result.version(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn add_feature_disallowed_increase() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await
            .unwrap();
        let table = DeltaOps(write);
        let result = table
            .add_feature()
            .with_feature(TableFeatures::ChangeDataFeed)
            .await;

        assert!(result.is_err());
        Ok(())
    }
}
