use std::convert::identity;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{actions::Metadata, schema::SchemaRef, PredicateRef, Version};
use futures::{FutureExt, TryStreamExt as _};
use url::Url;

use crate::errors::DeltaResult;
use crate::kernel::snapshot::{EagerSnapshot, Snapshot as LegacySnapshot};
use crate::kernel::CommitInfo;
use crate::logstore::LogStore;

use super::stream::SendableRBStream;
use super::{LogStoreHandler, Snapshot};

#[async_trait::async_trait]
impl Snapshot for LegacySnapshot {
    fn version(&self) -> Version {
        self.version() as u64
    }

    fn schema(&self) -> ArrowSchemaRef {
        todo!()
    }

    fn table_properties(&self) -> &TableProperties {
        todo!()
    }

    fn logical_files(
        &self,
        log_store: &LogStoreHandler,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        todo!()
    }

    async fn application_transaction_version(
        &self,
        log_store: &LogStoreHandler,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        todo!()
    }

    async fn commit_infos(
        &self,
        log_store: &LogStoreHandler,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Vec<(Version, CommitInfo)>> {
        Ok(self
            .commit_infos(log_store.log_store(), limit)
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flat_map(identity)
            .collect())
    }

    async fn update(
        &mut self,
        log_store: &LogStoreHandler,
        target_version: Option<Version>,
    ) -> DeltaResult<bool> {
        let current_version = self.version();
        self.update(log_store.log_store(), target_version.map(|v| v as i64))
            .await?;
        Ok(current_version != self.version())
    }
}

#[async_trait::async_trait]
impl Snapshot for EagerSnapshot {
    fn version(&self) -> Version {
        self.version() as u64
    }

    fn schema(&self) -> ArrowSchemaRef {
        todo!()
    }

    fn table_properties(&self) -> &TableProperties {
        todo!()
    }

    fn logical_files(
        &self,
        log_store: &LogStoreHandler,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        todo!()
    }

    async fn application_transaction_version(
        &self,
        log_store: &LogStoreHandler,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        self.transaction_version(app_id).await
    }

    async fn commit_infos(
        &self,
        log_store: &LogStoreHandler,
        start_version: Option<Version>,
        limit: Option<usize>,
    ) -> DeltaResult<Vec<(Version, CommitInfo)>> {
        Ok(self
            .snapshot
            .commit_infos(log_store.log_store(), limit)
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flat_map(identity)
            .collect())
    }

    async fn update(
        &mut self,
        log_store: &LogStoreHandler,
        target_version: Option<Version>,
    ) -> DeltaResult<bool> {
        let current_version = self.version();
        self.update(log_store.log_store(), target_version.map(|v| v as i64))
            .await?;
        Ok(current_version != self.version())
    }
}
