use chrono::Duration;
use deltalake_core::kernel::StructType;
use deltalake_core::operations::vacuum::Clock;
use deltalake_core::operations::DeltaOps;
use deltalake_test::clock::TestClock;
use deltalake_test::*;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};
use serde_json::json;
use std::sync::Arc;

/// Basic schema
pub fn get_xy_date_schema() -> StructType {
    serde_json::from_value(json!({
      "type": "struct",
      "fields": [
        {"name": "x", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "y", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "date", "type": "string", "nullable": false, "metadata": {}},
      ]
    }))
    .unwrap()
}

/// Schema that contains a column prefiexed with _
pub fn get_vacuum_underscore_schema() -> StructType {
    serde_json::from_value::<StructType>(json!({
      "type": "struct",
      "fields": [
        {"name": "x", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "y", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "_date", "type": "string", "nullable": false, "metadata": {}},
      ]
    }))
    .unwrap()
}

#[tokio::test]
// Validate vacuum works on a non-partitioned table
async fn test_non_partitioned_table() {
    let mut context = TestContext::from_env().await;
    let mut table = context
        .create_table_from_schema(get_xy_date_schema(), &[])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        Path::from("delete_me.parquet"),
        Path::from("dont_delete_me.parquet"),
    ];

    for path in paths {
        add_file(
            &mut table,
            &path,
            "random junk".as_bytes().into(),
            &[],
            clock.current_timestamp_millis(),
            true,
        )
        .await;
    }

    clock.tick(Duration::seconds(10));

    remove_file(
        &mut table,
        "delete_me.parquet",
        &[],
        clock.current_timestamp_millis(),
    )
    .await;

    let res = {
        clock.tick(Duration::days(8));
        let (_, metrics) = DeltaOps(table)
            .vacuum()
            .with_clock(Arc::new(clock.clone()))
            .await
            .unwrap();
        metrics
    };

    assert_eq!(res.files_deleted.len(), 1);
    assert!(is_deleted(&mut context, &Path::from("delete_me.parquet")).await);
    assert!(!is_deleted(&mut context, &Path::from("dont_delete_me.parquet")).await);
}

#[tokio::test]
// Validate vacuum works on a table with multiple partitions
async fn test_partitioned_table() {
    let mut context = TestContext::from_env().await;
    let mut table = context
        .create_table_from_schema(get_xy_date_schema(), &["date", "x"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        Path::from("date=2022-07-03/x=2/delete_me.parquet"),
        Path::from("date=2022-07-03/x=2/dont_delete_me.parquet"),
    ];
    let partition_values = [("date", Some("2022-07-03")), ("x", Some("2"))];

    for path in paths {
        add_file(
            &mut table,
            &path,
            "random junk".as_bytes().into(),
            &partition_values,
            clock.current_timestamp_millis(),
            true,
        )
        .await;
    }

    clock.tick(Duration::seconds(10));

    remove_file(
        &mut table,
        "date=2022-07-03/x=2/delete_me.parquet",
        &partition_values,
        clock.current_timestamp_millis(),
    )
    .await;

    let res = {
        clock.tick(Duration::days(8));
        let (_, metrics) = DeltaOps(table)
            .vacuum()
            .with_clock(Arc::new(clock.clone()))
            .await
            .unwrap();
        metrics
    };

    assert_eq!(res.files_deleted.len(), 1);
    assert!(
        is_deleted(
            &mut context,
            &Path::from("date=2022-07-03/x=2/delete_me.parquet")
        )
        .await
    );
    assert!(
        !is_deleted(
            &mut context,
            &Path::from("date=2022-07-03/x=2/dont_delete_me.parquet")
        )
        .await
    );
}

#[tokio::test]
// Partitions that start with _ are not ignored
async fn test_partitions_included() {
    let mut context = TestContext::from_env().await;
    let mut table = context
        .create_table_from_schema(get_vacuum_underscore_schema(), &["_date"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        Path::from("_date=2022-07-03/delete_me.parquet"),
        Path::from("_date=2022-07-03/dont_delete_me.parquet"),
    ];

    let partition_values = &[("_date", Some("2022-07-03"))];

    for path in paths {
        add_file(
            &mut table,
            &path,
            "random junk".as_bytes().into(),
            partition_values,
            clock.current_timestamp_millis(),
            true,
        )
        .await;
    }

    clock.tick(Duration::seconds(10));

    remove_file(
        &mut table,
        "_date=2022-07-03/delete_me.parquet",
        partition_values,
        clock.current_timestamp_millis(),
    )
    .await;

    let res = {
        clock.tick(Duration::days(8));
        let (_, metrics) = DeltaOps(table)
            .vacuum()
            .with_clock(Arc::new(clock.clone()))
            .await
            .unwrap();
        metrics
    };

    assert_eq!(res.files_deleted.len(), 1);
    assert!(
        is_deleted(
            &mut context,
            &Path::from("_date=2022-07-03/delete_me.parquet")
        )
        .await
    );
    assert!(
        !is_deleted(
            &mut context,
            &Path::from("_date=2022-07-03/dont_delete_me.parquet")
        )
        .await
    );
}

#[ignore]
#[tokio::test]
// files that are not managed by the delta log and have a last_modified greater
// than the retention period should be deleted. Unmanaged files and directories
// that start with _ or . are ignored
async fn test_non_managed_files() {
    let mut context = TestContext::from_env().await;
    let mut table = context
        .create_table_from_schema(get_xy_date_schema(), &["date"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths_delete = vec![
        Path::from("garbage_file"),
        Path::from("nested/garbage_file"),
        Path::from("nested2/really/deep/garbage_file"),
    ];

    let paths_ignore = vec![
        Path::from(".dotfile"),
        Path::from("_underscore"),
        Path::from("nested/.dotfile"),
        Path::from("nested2/really/deep/_underscore"),
        // Directories
        Path::from("_underscoredir/dont_delete_me"),
        Path::from("_dotdir/dont_delete_me"),
        Path::from("nested3/_underscoredir/dont_delete_me"),
        Path::from("nested4/really/deep/.dotdir/dont_delete_me"),
    ];

    for path in paths_delete.iter().chain(paths_ignore.iter()) {
        add_file(
            &mut table,
            path,
            "random junk".as_bytes().into(),
            &[],
            clock.current_timestamp_millis(),
            false,
        )
        .await;
    }

    // Validate unmanaged files are not deleted within the retention period

    let (table, res) = {
        clock.tick(Duration::hours(1));
        DeltaOps(table)
            .vacuum()
            .with_clock(Arc::new(clock.clone()))
            .await
            .unwrap()
    };

    assert_eq!(res.files_deleted.len(), 0);
    for path in paths_delete.iter().chain(paths_ignore.iter()) {
        assert!(!is_deleted(&mut context, path).await);
    }

    // Validate unmanaged files are deleted after the retention period
    let res = {
        clock.tick(Duration::hours(1));
        let (_, metrics) = DeltaOps(table)
            .vacuum()
            .with_clock(Arc::new(clock.clone()))
            .await
            .unwrap();
        metrics
    };

    assert_eq!(res.files_deleted.len(), paths_delete.len());
    for path in paths_delete {
        assert!(is_deleted(&mut context, &path).await);
    }

    for path in paths_ignore {
        assert!(!is_deleted(&mut context, &path).await);
    }
}

async fn is_deleted(context: &mut TestContext, path: &Path) -> bool {
    let backend = context.get_storage();
    let res = backend.object_store().head(path).await;
    matches!(res, Err(ObjectStoreError::NotFound { .. }))
}
