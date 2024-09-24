#![allow(dead_code, unused_variables)]
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::kernel::{Action, Add, Remove, StructType};
use deltalake_core::logstore::LogStore;
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::operations::transaction::CommitBuilder;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::DeltaTable;
use deltalake_core::DeltaTableBuilder;
use deltalake_core::{ObjectStore, Path};
use tempfile::TempDir;

pub mod clock;
pub mod concurrent;
#[cfg(feature = "datafusion")]
pub mod datafusion;
pub mod read;
pub mod utils;

pub use concurrent::test_concurrent_writes;
pub use read::*;
pub use utils::{IntegrationContext, TestResult};

#[derive(Default)]
pub struct TestContext {
    /// The main table under test
    pub table: Option<DeltaTable>,
    pub backend: Option<Arc<dyn LogStore>>,
    /// The configuration used to create the backend.
    pub config: HashMap<String, String>,
    /// An object when it is dropped will clean up any temporary resources created for the test
    pub storage_context: Option<Box<dyn Any>>,
}

pub struct LocalFS {
    pub tmp_dir: TempDir,
}

impl TestContext {
    pub async fn from_env() -> Self {
        let backend = std::env::var("DELTA_RS_TEST_BACKEND");
        let backend_ref = backend.as_ref().map(|s| s.as_str());
        match backend_ref {
            Ok("LOCALFS") | Err(std::env::VarError::NotPresent) => setup_local_context().await,
            _ => panic!("Invalid backend for delta-rs tests"),
        }
    }

    pub fn get_storage(&mut self) -> Arc<dyn LogStore> {
        if self.backend.is_none() {
            self.backend = Some(self.new_storage())
        }

        self.backend.as_ref().unwrap().clone()
    }

    fn new_storage(&self) -> Arc<dyn LogStore> {
        let config = self.config.clone();
        let uri = config.get("URI").unwrap().to_string();
        DeltaTableBuilder::from_uri(uri)
            .with_storage_options(config)
            .build_storage()
            .unwrap()
    }

    //Create and set a new table from the provided schema
    pub async fn create_table_from_schema(
        &mut self,
        schema: StructType,
        partitions: &[&str],
    ) -> DeltaTable {
        let p = partitions
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let log_store = self.new_storage();
        CreateBuilder::new()
            .with_log_store(log_store)
            .with_table_name("delta-rs_test_table")
            .with_comment("Table created by delta-rs tests")
            .with_columns(schema.fields().cloned())
            .with_partition_columns(p)
            .await
            .unwrap()
    }
}

pub async fn setup_local_context() -> TestContext {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut config = HashMap::new();
    config.insert(
        "URI".to_owned(),
        tmp_dir.path().to_str().to_owned().unwrap().to_string(),
    );

    let localfs = LocalFS { tmp_dir };

    TestContext {
        storage_context: Some(Box::new(localfs)),
        config,
        ..TestContext::default()
    }
}

pub async fn add_file(
    table: &mut DeltaTable,
    path: &Path,
    data: Bytes,
    partition_values: &[(&str, Option<&str>)],
    create_time: i64,
    commit_to_log: bool,
) {
    let backend = table.object_store();
    backend.put(path, data.clone().into()).await.unwrap();

    if commit_to_log {
        let mut part_values = HashMap::new();
        for v in partition_values {
            part_values.insert(v.0.to_string(), v.1.map(|v| v.to_string()));
        }

        let add = Add {
            path: path.as_ref().into(),
            size: data.len() as i64,
            modification_time: create_time,
            partition_values: part_values,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
            default_row_commit_version: None,
            base_row_id: None,
            deletion_vector: None,
            clustering_provider: None,
        };
        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };
        let actions = vec![Action::Add(add)];
        let snapshot = table.snapshot().unwrap().snapshot();

        CommitBuilder::default()
            .with_actions(actions)
            .build(Some(snapshot), table.log_store(), operation)
            .await
            .unwrap();
        table.update().await.unwrap();
    }
}

pub async fn remove_file(
    table: &mut DeltaTable,
    path: &str,
    partition_values: &[(&str, Option<&str>)],
    deletion_timestamp: i64,
) {
    let mut part_values = HashMap::new();
    for v in partition_values {
        part_values.insert(v.0.to_string(), v.1.map(|v| v.to_string()));
    }

    let remove = Remove {
        path: path.into(),
        deletion_timestamp: Some(deletion_timestamp),
        partition_values: Some(part_values),
        data_change: true,
        extended_file_metadata: None,
        size: None,
        deletion_vector: None,
        default_row_commit_version: None,
        base_row_id: None,
        tags: None,
    };
    let operation = DeltaOperation::Delete { predicate: None };
    let actions = vec![Action::Remove(remove)];
    let snapshot = table.snapshot().unwrap().snapshot();

    CommitBuilder::default()
        .with_actions(actions)
        .build(Some(snapshot), table.log_store(), operation)
        .await
        .unwrap();
    table.update().await.unwrap();
}
