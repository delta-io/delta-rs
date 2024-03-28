use deltalake_core::{DeltaResult, DeltaTableBuilder};
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::time::SystemTime;

#[allow(dead_code)]
mod fs_common;

#[tokio::test]
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

    let location = deltalake_core::table::builder::ensure_table_uri(path).unwrap();

    // use storage that sleeps 10ms on every `get`
    let store = std::sync::Arc::new(
        fs_common::SlowStore::new(
            location.clone(),
            deltalake_core::storage::StorageOptions::from(HashMap::new()),
        )
        .unwrap(),
    );

    let mut seq_version = 0;
    let t = SystemTime::now();
    for _x in 0..max_iter {
        let mut table_seq = DeltaTableBuilder::from_uri(path)
            .with_storage_backend(store.clone(), location.clone())
            .with_version(0)
            .with_log_buffer_size(1)
            .unwrap()
            .load()
            .await
            .expect("Failed to load table");
        table_seq.update_incremental(None).await.unwrap();
        seq_version = table_seq.version();
    }
    let time_seq = t.elapsed().unwrap();

    let mut buf_version = 0;
    let t2 = SystemTime::now();
    for _x in 0..max_iter {
        let mut table_buf = DeltaTableBuilder::from_uri(path)
            .with_storage_backend(store.clone(), location.clone())
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table_buf.update_incremental(None).await.unwrap();
        buf_version = table_buf.version();
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
    let path = "../test/tests/data/simple_table_with_no_checkpoint_2";
    let mut table = fs_common::create_table(path, None).await;
    for _ in 0..n_commits {
        let a = fs_common::add(3 * 60 * 1000);
        fs_common::commit_add(&mut table, &a).await;
    }
    let buf_sizes = [1, 2, 10, 50];
    for buf_size in buf_sizes {
        let mut table = DeltaTableBuilder::from_uri(path)
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(None).await.unwrap();
        assert_eq!(table.version(), 10);

        let mut table = DeltaTableBuilder::from_uri(path)
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(Some(0)).await.unwrap();
        assert_eq!(table.version(), 0);

        let mut table = DeltaTableBuilder::from_uri(path)
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(Some(1)).await.unwrap();
        assert_eq!(table.version(), 1);

        let mut table = DeltaTableBuilder::from_uri(path)
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(Some(10)).await.unwrap();
        assert_eq!(table.version(), 10);

        let mut table = DeltaTableBuilder::from_uri(path)
            .with_version(0)
            .with_log_buffer_size(buf_size)
            .unwrap()
            .load()
            .await
            .unwrap();
        table.update_incremental(Some(20)).await.unwrap();
        assert_eq!(table.version(), 10);
    }
}

#[tokio::test]
async fn test_log_buffering_fail() {
    let path = "../test/tests/data/simple_table_with_no_checkpoint";
    let table_err = DeltaTableBuilder::from_uri(path)
        .with_version(0)
        .with_log_buffer_size(0)
        .is_err();
    assert!(table_err);
}

#[tokio::test]
async fn test_read_liquid_table() -> DeltaResult<()> {
    let path = "../test/tests/data/table_with_liquid_clustering";
    let _table = deltalake_core::open_table(&path).await?;
    Ok(())
}

#[tokio::test]
async fn test_read_table_features() -> DeltaResult<()> {
    let mut _table = deltalake_core::open_table("../test/tests/data/simple_table_features").await?;
    let rf = _table.protocol()?.reader_features.clone();
    let wf = _table.protocol()?.writer_features.clone();

    assert!(rf.is_some());
    assert!(wf.is_some());
    assert_eq!(rf.unwrap().len(), 5);
    assert_eq!(wf.unwrap().len(), 13);
    Ok(())
}

// test for: https://github.com/delta-io/delta-rs/issues/1302
#[tokio::test]
async fn read_delta_table_from_dlt() {
    let table = deltalake_core::open_table("../test/tests/data/delta-live-table")
        .await
        .unwrap();
    assert_eq!(table.version(), 1);
    assert!(table.get_schema().is_ok());
}

#[tokio::test]
async fn read_delta_table_with_null_stats_in_notnull_struct() {
    let table =
        deltalake_core::open_table("../test/tests/data/table_with_null_stats_in_notnull_struct")
            .await
            .unwrap();
    assert_eq!(table.version(), 1);
    assert!(table.get_schema().is_ok());
}
