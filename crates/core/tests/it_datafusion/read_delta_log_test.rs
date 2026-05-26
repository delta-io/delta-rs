use deltalake_core::logstore::object_store::{GetResult, Result as ObjectStoreResult};
use deltalake_core::{DeltaResult, DeltaTableBuilder, DeltaTableError};
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, MultipartUpload, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions,
};
use pretty_assertions::assert_eq;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use url::Url;

use crate::fs_common;

fn find_git_root() -> PathBuf {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .output()
        .unwrap();
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
}

#[derive(Debug)]
struct InstrumentedStore {
    inner: Arc<dyn ObjectStore>,
    recorded_gets: Option<Mutex<Vec<String>>>,
    delay_gets: bool,
}

impl std::fmt::Display for InstrumentedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl InstrumentedStore {
    fn new_slow(location: Url) -> DeltaResult<Self> {
        Ok(Self {
            inner: deltalake_core::logstore::store_for(&location, None::<(&str, &str)>)?,
            recorded_gets: None,
            delay_gets: true,
        })
    }

    fn new_recording(location: Url) -> DeltaResult<Self> {
        Ok(Self {
            inner: deltalake_core::logstore::store_for(&location, None::<(&str, &str)>)?,
            recorded_gets: Some(Mutex::new(Vec::new())),
            delay_gets: false,
        })
    }

    fn recorded_gets(&self) -> Vec<String> {
        self.recorded_gets
            .as_ref()
            .expect("recorded_gets not enabled for this store")
            .lock()
            .expect("recorded_gets mutex poisoned")
            .clone()
    }

    fn clear_recorded_gets(&self) {
        self.recorded_gets
            .as_ref()
            .expect("recorded_gets not enabled for this store")
            .lock()
            .expect("recorded_gets mutex poisoned")
            .clear();
    }

    fn record_get(&self, location: &StorePath) {
        if let Some(recorded_gets) = &self.recorded_gets {
            recorded_gets
                .lock()
                .expect("recorded_gets mutex poisoned")
                .push(location.to_string());
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for InstrumentedStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.record_get(location);
        if self.delay_gets {
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(0.01)).await;
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[std::ops::Range<u64>],
    ) -> ObjectStoreResult<Vec<bytes::Bytes>> {
        self.record_get(location);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, ObjectStoreResult<StorePath>>,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<object_store::ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> ObjectStoreResult<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
}

#[tokio::test]
#[ignore]
async fn test_log_buffering() {
    let n_commits = 10;
    let path = "../test/tests/data/simple_table_with_no_checkpoint";
    let mut table = fs_common::create_table(path, None).await;
    for _ in 0..n_commits {
        let a = fs_common::add(3 * 60 * 1000);
        fs_common::commit_add(&mut table, &a).await;
    }

    let max_iter = 10;
    let buf_size = 10;

    let location = Url::from_directory_path(path).unwrap();

    // use storage that sleeps 10ms on every `get`
    let store = Arc::new(InstrumentedStore::new_slow(location.clone()).unwrap());

    let mut seq_version = 0;
    let t = SystemTime::now();
    for _x in 0..max_iter {
        let mut table_seq = DeltaTableBuilder::from_url(Url::from_directory_path(path).unwrap())
            .unwrap()
            .with_storage_backend(store.clone(), location.clone())
            .with_version(0)
            .with_log_buffer_size(1)
            .unwrap()
            .load()
            .await
            .expect("Failed to load table");
        table_seq.update_incremental(None).await.unwrap();
        seq_version = table_seq.version().unwrap();
    }
    let time_seq = t.elapsed().unwrap();

    let mut buf_version = 0;
    let t2 = SystemTime::now();
    for _x in 0..max_iter {
        let mut table_buf = DeltaTableBuilder::from_url(Url::from_directory_path(path).unwrap())
            .unwrap()
            .with_storage_backend(store.clone(), location.clone())
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table_buf.update_incremental(None).await.unwrap();
        buf_version = table_buf.version().unwrap();
    }
    let time_buf = t2.elapsed().unwrap();

    // buffered was at least 2x faster
    assert!(time_buf.as_secs_f64() < 0.5 * time_seq.as_secs_f64());

    // updates match
    assert_eq!(seq_version, 10);
    assert_eq!(buf_version, 10);
}

#[tokio::test]
async fn test_log_buffering_success_explicit_version() {
    let n_commits = 10;
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().to_path_buf();
    let mut table = fs_common::create_table(&path.to_string_lossy(), None).await;
    for _ in 0..n_commits {
        let a = fs_common::add(3 * 60 * 1000);
        fs_common::commit_add(&mut table, &a).await;
    }
    let buf_sizes = [1, 2, 10, 50];
    for buf_size in buf_sizes {
        let table_uri = Url::from_directory_path(std::fs::canonicalize(&path).unwrap()).unwrap();
        let mut table = DeltaTableBuilder::from_url(table_uri)
            .unwrap()
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(None).await.unwrap();
        assert_eq!(table.version(), Some(10));

        let mut table = DeltaTableBuilder::from_url(Url::from_directory_path(&path).unwrap())
            .unwrap()
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(Some(0)).await.unwrap();
        assert_eq!(table.version(), Some(0));

        let mut table = DeltaTableBuilder::from_url(Url::from_directory_path(&path).unwrap())
            .unwrap()
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(Some(1)).await.unwrap();
        assert_eq!(table.version(), Some(1));

        let mut table = DeltaTableBuilder::from_url(Url::from_directory_path(&path).unwrap())
            .unwrap()
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(Some(10)).await.unwrap();
        assert_eq!(table.version(), Some(10));

        let mut table = DeltaTableBuilder::from_url(Url::from_directory_path(&path).unwrap())
            .unwrap()
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(None).await.unwrap();
        assert_eq!(table.version(), Some(10));
    }
}

#[tokio::test]
async fn test_update_incremental_rejects_downgrade_and_load_version_allows_it() {
    let n_commits = 10;
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().to_path_buf();
    let mut table = fs_common::create_table(&path.to_string_lossy(), None).await;
    for _ in 0..n_commits {
        let add = fs_common::add(3 * 60 * 1000);
        fs_common::commit_add(&mut table, &add).await;
    }

    let table_uri = Url::from_directory_path(std::fs::canonicalize(&path).unwrap()).unwrap();
    let mut table = DeltaTableBuilder::from_url(table_uri)
        .unwrap()
        .with_version(0)
        .with_log_buffer_size(10)
        .unwrap()
        .load()
        .await
        .unwrap();
    table.update_incremental(None).await.unwrap();
    assert_eq!(table.version(), Some(10));

    let err = table.update_incremental(Some(0)).await.unwrap_err();
    assert!(
        matches!(
            err,
            DeltaTableError::VersionDowngrade {
                current_version: 10,
                requested_version: 0,
            }
        ),
        "expected typed downgrade error from update_incremental: {err}"
    );

    assert_eq!(table.version(), Some(10));
    table.load_version(0).await.unwrap();
    assert_eq!(table.version(), Some(0));
}

#[tokio::test]
async fn test_update_incremental_does_not_reread_initial_commit() {
    let n_commits = 10;
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().to_path_buf();
    let mut table = fs_common::create_table(&path.to_string_lossy(), None).await;
    for _ in 0..n_commits {
        let add = fs_common::add(3 * 60 * 1000);
        fs_common::commit_add(&mut table, &add).await;
    }

    let location = Url::from_directory_path(&path).unwrap();
    let store_root = Url::from_directory_path(path.ancestors().last().unwrap()).unwrap();
    let store = Arc::new(InstrumentedStore::new_recording(store_root).unwrap());
    let mut table = DeltaTableBuilder::from_url(location.clone())
        .unwrap()
        .with_storage_backend(store.clone(), location)
        .with_version(0)
        .load()
        .await
        .unwrap();

    store.clear_recorded_gets();
    table.update_incremental(None).await.unwrap();

    let recorded_gets = store.recorded_gets();
    assert_eq!(table.version(), Some(n_commits));
    assert!(
        recorded_gets
            .iter()
            .any(|path| path.ends_with("_delta_log/00000000000000000001.json")),
        "expected incremental update to read newer commits: {recorded_gets:?}"
    );
    assert!(
        !recorded_gets
            .iter()
            .any(|path| path.ends_with("_delta_log/00000000000000000000.json")),
        "update_incremental reread the initial commit instead of reusing loaded state: {recorded_gets:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_incremental_same_version_checkpoint_refresh_skips_redundant_hint_lookup() {
    let n_commits = 2;
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().to_path_buf();
    let mut table = fs_common::create_table(&path.to_string_lossy(), None).await;
    for _ in 0..n_commits {
        let add = fs_common::add(3 * 60 * 1000);
        fs_common::commit_add(&mut table, &add).await;
    }

    let location = Url::from_directory_path(&path).unwrap();
    let store_root = Url::from_directory_path(path.ancestors().last().unwrap()).unwrap();
    let store = Arc::new(InstrumentedStore::new_recording(store_root).unwrap());

    deltalake_core::checkpoints::create_checkpoint_from_table_url_and_cleanup(
        location.clone(),
        table.version().unwrap(),
        Some(false),
        None,
    )
    .await
    .unwrap();

    let mut table = DeltaTableBuilder::from_url(location.clone())
        .unwrap()
        .with_storage_backend(store.clone(), location)
        .load()
        .await
        .unwrap();
    assert_eq!(table.version(), Some(n_commits));

    store.clear_recorded_gets();
    table.update_incremental(None).await.unwrap();

    let recorded_gets = store.recorded_gets();
    assert!(
        !recorded_gets
            .iter()
            .any(|path| path.ends_with("_delta_log/_last_checkpoint")),
        "same-version update reread _last_checkpoint even though the current snapshot was already checkpoint-backed: {recorded_gets:?}"
    );
}

#[tokio::test]
async fn test_log_buffering_fail() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().to_path_buf();
    let _table = fs_common::create_table(&path.to_string_lossy(), None).await;
    let table_uri = Url::from_directory_path(std::fs::canonicalize(&path).unwrap()).unwrap();
    let table_result = DeltaTableBuilder::from_url(table_uri)
        .and_then(|builder| builder.with_version(0).with_log_buffer_size(0));
    assert!(table_result.is_err());
}

#[tokio::test]
#[ignore = "not implemented"]
async fn test_read_liquid_table() -> DeltaResult<()> {
    let path = "../test/tests/data/table_with_liquid_clustering";
    let _table =
        deltalake_core::open_table(Url::from_directory_path(Path::new(&path)).unwrap()).await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_read_table_features() -> DeltaResult<()> {
    let path = "../test/tests/data/simple_table_features";
    let table_uri =
        Url::from_directory_path(std::fs::canonicalize(path)?).expect("Failed to create Url path");
    let table = deltalake_core::open_table(table_uri).await?;
    let rf = table.snapshot()?.protocol().reader_features();
    let wf = table.snapshot()?.protocol().writer_features();

    assert!(rf.is_some());
    assert!(wf.is_some());
    assert_eq!(rf.unwrap().len(), 4);
    assert_eq!(wf.unwrap().len(), 13);
    Ok(())
}

// test for: https://github.com/delta-io/delta-rs/issues/1302
#[tokio::test]
async fn read_delta_table_from_dlt() {
    let table = deltalake_core::open_table(
        Url::from_directory_path(
            std::fs::canonicalize("../test/tests/data/delta-live-table")
                .expect("Failed to canonicalize"),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(1));
    assert!(table.snapshot().is_ok());
}

#[tokio::test]
async fn read_delta_table_with_null_stats_in_notnull_struct() {
    let table_path =
        find_git_root().join("crates/test/tests/data/table_with_null_stats_in_notnull_struct");
    let table_uri = Url::from_directory_path(std::fs::canonicalize(&table_path).unwrap()).unwrap();
    let table = deltalake_core::open_table(table_uri).await.unwrap();
    assert_eq!(table.version(), Some(1));
    assert!(table.snapshot().is_ok());
}

#[tokio::test]
#[ignore = "not implemented"]
async fn read_delta_table_with_renamed_partitioning_column() {
    let table = deltalake_core::open_table(
        Url::from_directory_path(Path::new(
            "../test/tests/data/table_with_partitioning_mapping",
        ))
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(4));
    assert!(table.snapshot().is_ok());
}
