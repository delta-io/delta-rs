use deltalake_core::{DeltaResult, DeltaTableBuilder};
use pretty_assertions::assert_eq;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;
use url::Url;

#[allow(dead_code)]
mod fs_common;

fn find_git_root() -> PathBuf {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .output()
        .unwrap();
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
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
    let store = std::sync::Arc::new(fs_common::SlowStore::new(location.clone()).unwrap());

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
#[ignore = "not implemented"]
async fn test_read_table_features() -> DeltaResult<()> {
    let path = "../test/tests/data/simple_table_features";
    let table_uri = Url::from_directory_path(path).unwrap();
    let mut _table = deltalake_core::open_table(table_uri).await?;
    let rf = _table.snapshot()?.protocol().reader_features();
    let wf = _table.snapshot()?.protocol().writer_features();

    assert!(rf.is_some());
    assert!(wf.is_some());
    assert_eq!(rf.unwrap().len(), 4);
    assert_eq!(wf.unwrap().len(), 13);
    Ok(())
}

// test for: https://github.com/delta-io/delta-rs/issues/1302
#[tokio::test]
#[ignore = "not implemented"]
async fn read_delta_table_from_dlt() {
    let table = deltalake_core::open_table(
        Url::from_directory_path(Path::new("../test/tests/data/delta-live-table")).unwrap(),
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
