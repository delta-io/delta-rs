use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::ObjectStore;

use self::log_segment::LogSegment;
use self::replay::ReplayStream;
use crate::kernel::{actions::ActionType, StructType};
use crate::table::config::TableConfig;
use crate::{DeltaResult, DeltaTableConfig};

use super::{Metadata, Protocol};

mod extract;
mod log_segment;
mod replay;

/// A snapshot of a Delta table
pub struct Snapshot {
    log_segment: LogSegment,
    store: Arc<dyn ObjectStore>,
    config: DeltaTableConfig,
    protocol: Protocol,
    metadata: Metadata,
    schema: StructType,
}

impl Snapshot {
    /// Create a new snapshot from a log segment
    pub async fn try_new(
        table_root: &Path,
        store: Arc<dyn ObjectStore>,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let log_segment = LogSegment::try_new(table_root, version, store.as_ref()).await?;
        let (protocol, metadata) = log_segment.read_metadata(store.clone(), &config).await?;
        let schema = serde_json::from_str(&metadata.schema_string)?;
        Ok(Self {
            log_segment,
            store,
            config,
            protocol,
            metadata,
            schema,
        })
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.log_segment.version
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> &StructType {
        &self.schema
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    /// Get the table root of the snapshot
    pub fn table_root(&self) -> &Path {
        &self.log_segment.table_root
    }

    /// Well known table configuration
    pub fn table_config(&self) -> TableConfig<'_> {
        TableConfig(&self.metadata.configuration)
    }

    /// Get the files in the snapshot
    pub fn files(&self) -> DeltaResult<ReplayStream<BoxStream<'_, DeltaResult<RecordBatch>>>> {
        lazy_static::lazy_static! {
            static ref COMMIT_SCHEMA: StructType = StructType::new(vec![
                ActionType::Add.schema_field().clone(),
                ActionType::Remove.schema_field().clone(),
            ]);
            static ref CHECKPOINT_SCHEMA: StructType = StructType::new(vec![
                ActionType::Add.schema_field().clone(),
            ]);
        }

        let log_stream =
            self.log_segment
                .commit_stream(self.store.clone(), &COMMIT_SCHEMA, &self.config)?;

        let checkpoint_stream = self.log_segment.checkpoint_stream(
            self.store.clone(),
            &CHECKPOINT_SCHEMA,
            &self.config,
        );

        Ok(ReplayStream::new(log_stream, checkpoint_stream))
    }
}

#[cfg(test)]
mod tests {
    use deltalake_test::utils::*;
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn test_snapshot_files() -> TestResult {
        let context = IntegrationContext::new(Box::new(LocalStorageIntegration::default()))?;
        context.load_table(TestTables::Simple).await?;
        context.load_table(TestTables::Checkpoints).await?;

        let store = context
            .table_builder(TestTables::Simple)
            .build_storage()?
            .object_store();

        let snapshot =
            Snapshot::try_new(&Path::default(), store.clone(), Default::default(), None).await?;

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema(), &expected);

        let batches = snapshot.files()?.try_collect::<Vec<_>>().await?;
        let expected = [
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| add                                                                                                                                                                                                                                                             |",
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {path: part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet, partitionValues: {}, size: 262, modificationTime: 1587968626000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet, partitionValues: {}, size: 262, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "| {path: part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet, partitionValues: {}, size: 429, modificationTime: 1587968602000, dataChange: true, stats: , tags: , deletionVector: , baseRowId: , defaultRowCommitVersion: , clusteringProvider: } |",
            "+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        let store = context
            .table_builder(TestTables::Checkpoints)
            .build_storage()?
            .object_store();

        for version in 0..=12 {
            let snapshot = Snapshot::try_new(
                &Path::default(),
                store.clone(),
                Default::default(),
                Some(version),
            )
            .await?;
            let batches = snapshot.files()?.try_collect::<Vec<_>>().await?;
            let num_files = batches.iter().map(|b| b.num_rows() as i64).sum::<i64>();
            assert_eq!(num_files, version);
        }

        Ok(())
    }
}
