use chrono::Utc;
use deltalake_core::kernel::{
    Action, Add, DataType, PrimitiveType, Remove, StructField, StructType,
};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::operations::transaction::CommitBuilder;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::storage::{GetResult, ObjectStoreResult};
use deltalake_core::DeltaTable;
use object_store::path::Path as StorePath;
use object_store::{
    MultipartUpload, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

pub fn cleanup_dir_except<P: AsRef<Path>>(path: P, ignore_files: Vec<String>) {
    for d in fs::read_dir(path).unwrap().flatten() {
        let path = d.path();
        let name = d.path().file_name().unwrap().to_str().unwrap().to_string();

        if !ignore_files.contains(&name) && !name.starts_with('.') {
            fs::remove_file(path).unwrap();
        }
    }
}

// TODO: should we drop this
#[allow(dead_code)]
pub async fn create_table_from_json(
    path: &str,
    schema: Value,
    partition_columns: Vec<&str>,
    config: Value,
) -> DeltaTable {
    assert!(path.starts_with("../test/tests/data"));
    std::fs::create_dir_all(path).unwrap();
    std::fs::remove_dir_all(path).unwrap();
    std::fs::create_dir_all(path).unwrap();
    let schema: StructType = serde_json::from_value(schema).unwrap();
    let config: HashMap<String, Option<String>> = serde_json::from_value(config).unwrap();
    create_test_table(path, schema, partition_columns, config).await
}

pub async fn create_test_table(
    path: &str,
    schema: StructType,
    partition_columns: Vec<&str>,
    config: HashMap<String, Option<String>>,
) -> DeltaTable {
    CreateBuilder::new()
        .with_location(path)
        .with_table_name("test-table")
        .with_comment("A table for running tests")
        .with_columns(schema.fields().cloned())
        .with_partition_columns(partition_columns)
        .with_configuration(config)
        .await
        .unwrap()
}

pub async fn create_table(
    path: &str,
    config: Option<HashMap<String, Option<String>>>,
) -> DeltaTable {
    let log_dir = Path::new(path).join("_delta_log");
    fs::create_dir_all(&log_dir).unwrap();
    cleanup_dir_except(log_dir, vec![]);

    let schema = StructType::new(vec![StructField::new(
        "id".to_string(),
        DataType::Primitive(PrimitiveType::Integer),
        true,
    )]);

    create_test_table(path, schema, Vec::new(), config.unwrap_or_default()).await
}

pub fn add(offset_millis: i64) -> Add {
    Add {
        path: Uuid::new_v4().to_string(),
        size: 100,
        partition_values: Default::default(),
        modification_time: Utc::now().timestamp_millis() - offset_millis,
        data_change: true,
        stats: None,
        stats_parsed: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    }
}

pub async fn commit_add(table: &mut DeltaTable, add: &Add) -> i64 {
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };
    commit_actions(table, vec![Action::Add(add.clone())], operation).await
}

pub async fn commit_removes(table: &mut DeltaTable, removes: Vec<&Remove>) -> i64 {
    let vec = removes
        .iter()
        .map(|r| Action::Remove((*r).clone()))
        .collect();
    let operation = DeltaOperation::Delete { predicate: None };
    commit_actions(table, vec, operation).await
}

pub async fn commit_actions(
    table: &mut DeltaTable,
    actions: Vec<Action>,
    operation: DeltaOperation,
) -> i64 {
    let version = CommitBuilder::default()
        .with_actions(actions)
        .build(
            Some(table.snapshot().unwrap()),
            table.log_store().clone(),
            operation,
        )
        .await
        .unwrap()
        .version();
    table.update().await.unwrap();
    version
}

#[derive(Debug)]
pub struct SlowStore {
    inner: Arc<dyn ObjectStore>,
}
impl std::fmt::Display for SlowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl SlowStore {
    #[allow(dead_code)]
    pub fn new(
        location: Url,
        _options: impl Into<deltalake_core::storage::StorageOptions> + Clone,
    ) -> deltalake_core::DeltaResult<Self> {
        Ok(Self {
            inner: deltalake_core::storage::store_for(&location)?,
        })
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowStore {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &StorePath, bytes: PutPayload) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &StorePath,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &StorePath) -> ObjectStoreResult<GetResult> {
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(0.01)).await;
        self.inner.get(location).await
    }

    /// Perform a get request with options
    ///
    /// Note: options.range will be ignored if [`GetResult::File`]
    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(
        &self,
        location: &StorePath,
        range: std::ops::Range<usize>,
    ) -> ObjectStoreResult<bytes::Bytes> {
        self.inner.get_range(location, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &StorePath) -> ObjectStoreResult<object_store::ObjectMeta> {
        self.inner.head(location).await
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &StorePath) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures::stream::BoxStream<'_, ObjectStoreResult<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    /// List all the objects with the given prefix and a location greater than `offset`
    ///
    /// Some stores, such as S3 and GCS, may be able to push `offset` down to reduce
    /// the number of network requests required
    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> futures::stream::BoxStream<'_, ObjectStoreResult<object_store::ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> ObjectStoreResult<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &StorePath, to: &StorePath) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &StorePath, to: &StorePath) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(
        &self,
        from: &StorePath,
        to: &StorePath,
    ) -> ObjectStoreResult<()> {
        self.inner.rename_if_not_exists(from, to).await
    }

    async fn put_multipart(
        &self,
        location: &StorePath,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
}
