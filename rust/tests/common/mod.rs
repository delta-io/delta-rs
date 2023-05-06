#![allow(dead_code, unused_variables, deprecated)]

use bytes::Bytes;
use deltalake::action::{self, Add, Remove};
use deltalake::builder::DeltaTableBuilder;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaTable, DeltaTableConfig, DeltaTableMetaData, Schema};
use object_store::{path::Path, ObjectStore};
use serde_json::{Map, Value};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tempdir::TempDir;

#[cfg(feature = "azure")]
pub mod adls;
pub mod clock;
#[cfg(feature = "datafusion")]
pub mod datafusion;
#[cfg(feature = "hdfs")]
pub mod hdfs;
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
pub mod s3;
pub mod schemas;

#[derive(Default)]
pub struct TestContext {
    /// The main table under test
    pub table: Option<DeltaTable>,
    pub backend: Option<Arc<DeltaObjectStore>>,
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
            #[cfg(feature = "azure")]
            Ok("AZURE_GEN2") => adls::setup_azure_gen2_context().await,
            #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
            Ok("S3_LOCAL_STACK") => s3::setup_s3_context().await,
            #[cfg(feature = "hdfs")]
            Ok("HDFS") => hdfs::setup_hdfs_context(),
            _ => panic!("Invalid backend for delta-rs tests"),
        }
    }

    pub fn get_storage(&mut self) -> Arc<DeltaObjectStore> {
        if self.backend.is_none() {
            self.backend = Some(self.new_storage())
        }

        self.backend.as_ref().unwrap().clone()
    }

    fn new_storage(&self) -> Arc<DeltaObjectStore> {
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
        schema: Schema,
        partitions: &[&str],
    ) -> DeltaTable {
        let p = partitions
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let table_meta = DeltaTableMetaData::new(
            Some("delta-rs_test_table".to_owned()),
            Some("Table created by delta-rs tests".to_owned()),
            None,
            schema.clone(),
            p,
            HashMap::new(),
        );

        let backend = self.new_storage();
        let mut dt = DeltaTable::new(backend, DeltaTableConfig::default());
        let mut commit_info = Map::<String, Value>::new();

        let protocol = action::Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        commit_info.insert(
            "operation".to_string(),
            serde_json::Value::String("CREATE TABLE".to_string()),
        );
        dt.create(
            table_meta.clone(),
            protocol.clone(),
            Some(commit_info),
            None,
        )
        .await
        .unwrap();

        dt
    }
}

pub async fn setup_local_context() -> TestContext {
    let tmp_dir = tempdir::TempDir::new("delta-rs_tests").unwrap();
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
    backend.put(path, data.clone()).await.unwrap();

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
            ..Default::default()
        };
        let mut transaction = table.create_transaction(None);
        transaction.add_action(action::Action::add(add));
        transaction.commit(None, None).await.unwrap();
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
        ..Default::default()
    };
    let mut transaction = table.create_transaction(None);
    transaction.add_action(action::Action::remove(remove));
    transaction.commit(None, None).await.unwrap();
}
