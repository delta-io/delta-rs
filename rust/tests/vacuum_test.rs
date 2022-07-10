use chrono::Duration;
use deltalake::storage::file::FileStorageBackend;
use deltalake::storage::StorageError;
use deltalake::vacuum::Clock;
use deltalake::vacuum::Vacuum;
use deltalake::StorageBackend;
use std::collections::HashMap;
use std::sync::Arc;

use common::clock::TestClock;
use common::schemas::{get_vacuum_underscore_schema, get_xy_date_schema};
use common::TestContext;
use std::time::SystemTime;

mod common;

#[tokio::test]
async fn vacuum_delta_8_0_table() {
    let backend = FileStorageBackend::new("");
    let mut table = deltalake::open_table(&backend.join_paths(&["tests", "data", "delta-0.8.0"]))
        .await
        .unwrap();

    let result = Vacuum::default()
        .with_retention_period(Duration::hours(1))
        .dry_run(true)
        .execute(&mut table)
        .await;

    assert!(matches!(result.unwrap_err(),
        deltalake::vacuum::VacuumError::InvalidVacuumRetentionPeriod {
            provided,
            min,
        } if provided == 1
            && min == 168,
    ));

    let result = Vacuum::default()
        .with_retention_period(Duration::hours(0))
        .dry_run(true)
        .enforce_retention_duration(false)
        .execute(&mut table)
        .await
        .unwrap();
    // do not enforce retention duration check with 0 hour will purge all files
    assert_eq!(
        result.files_deleted,
        vec![backend.join_paths(&[
            "tests",
            "data",
            "delta-0.8.0",
            "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet",
        ])]
    );

    let result = Vacuum::default()
        .with_retention_period(Duration::hours(169))
        .dry_run(true)
        .execute(&mut table)
        .await
        .unwrap();

    assert_eq!(
        result.files_deleted,
        vec![backend.join_paths(&[
            "tests",
            "data",
            "delta-0.8.0",
            "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet",
        ])]
    );

    let retention_hours = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 3600;
    let empty: Vec<String> = Vec::new();
    let result = Vacuum::default()
        .with_retention_period(Duration::hours(retention_hours as i64))
        .dry_run(true)
        .execute(&mut table)
        .await
        .unwrap();

    assert_eq!(result.files_deleted, empty);
}

#[tokio::test]
// Validate vacuum works on a non-partitioned table
async fn test_non_partitioned_table() {
    let mut context = TestContext::from_env().await;
    context
        .create_table_from_schema(get_xy_date_schema(), &[])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = ["delete_me.parquet", "dont_delete_me.parquet"];

    for path in paths {
        context
            .add_file(
                path,
                "random junk".as_ref(),
                &[],
                clock.current_timestamp_millis(),
                true,
            )
            .await;
    }

    clock.tick(Duration::seconds(10));

    context
        .remove_file("delete_me.parquet", &[], clock.current_timestamp_millis())
        .await;

    let res = {
        clock.tick(Duration::days(8));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), 1);
    assert!(is_deleted(&mut context, "delete_me.parquet").await);
    assert!(!is_deleted(&mut context, "dont_delete_me.parquet").await);
}

#[tokio::test]
// Validate vacuum works on a table with multiple partitions
async fn test_partitioned_table() {
    let mut context = TestContext::from_env().await;
    context
        .create_table_from_schema(get_xy_date_schema(), &["date", "x"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        "date=2022-07-03/x=2/delete_me.parquet",
        "date=2022-07-03/x=2/dont_delete_me.parquet",
    ];
    let partition_values = [("date", Some("2022-07-03")), ("x", Some("2"))];

    for path in paths {
        context
            .add_file(
                path,
                "random junk".as_ref(),
                &partition_values,
                clock.current_timestamp_millis(),
                true,
            )
            .await;
    }

    clock.tick(Duration::seconds(10));

    context
        .remove_file(
            "date=2022-07-03/x=2/delete_me.parquet",
            &partition_values,
            clock.current_timestamp_millis(),
        )
        .await;

    let res = {
        clock.tick(Duration::days(8));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), 1);
    assert!(is_deleted(&mut context, "date=2022-07-03/x=2/delete_me.parquet").await);
    assert!(!is_deleted(&mut context, "date=2022-07-03/x=2/dont_delete_me.parquet").await);
}

#[tokio::test]
// Validate that files and directories that start with '.' or '_' are ignored
async fn test_ignored_files() {
    let mut context = TestContext::from_env().await;
    context
        .create_table_from_schema(get_xy_date_schema(), &["date"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        ".dotfile",
        "_underscore",
        "nested/.dotfile",
        "nested2/really/deep/_underscore",
        // Directories
        "_underscoredir/dont_delete_me",
        "_dotdir/dont_delete_me",
        "nested3/_underscoredir/dont_delete_me",
        "nested4/really/deep/.dotdir/dont_delete_me",
    ];

    for path in paths {
        context
            .add_file(
                path,
                "random junk".as_ref(),
                &[],
                clock.current_timestamp_millis(),
                false,
            )
            .await;
    }

    let res = {
        clock.tick(Duration::days(8));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), 0);
    for path in paths {
        assert!(!is_deleted(&mut context, path).await);
    }
}

#[tokio::test]
//Partitions that start with _ are not ignored
async fn test_partitions_included() {
    let mut context = TestContext::from_env().await;
    context
        .create_table_from_schema(get_vacuum_underscore_schema(), &["_date"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        "_date=2022-07-03/delete_me.parquet",
        "_date=2022-07-03/dont_delete_me.parquet",
    ];

    let partition_values = &[("_date", Some("2022-07-03"))];

    for path in paths {
        context
            .add_file(
                path,
                "random junk".as_ref(),
                partition_values,
                clock.current_timestamp_millis(),
                true,
            )
            .await;
    }

    clock.tick(Duration::seconds(10));

    context
        .remove_file(
            "_date=2022-07-03/delete_me.parquet",
            partition_values,
            clock.current_timestamp_millis(),
        )
        .await;

    let res = {
        clock.tick(Duration::days(8));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), 1);
    assert!(is_deleted(&mut context, "_date=2022-07-03/delete_me.parquet").await);
    assert!(!is_deleted(&mut context, "_date=2022-07-03/dont_delete_me.parquet").await);
}

#[tokio::test]
// files that are not managed by the delta log and have a last_modified greater than the retention period should be deleted
async fn test_non_managed_files() {
    let mut context = TestContext::from_env().await;
    context
        .create_table_from_schema(get_xy_date_schema(), &["date"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        "garbage_file",
        "nested/garbage_file",
        "nested2/really/deep/garbage_file",
    ];

    for path in paths {
        context
            .add_file(
                path,
                "random junk".as_ref(),
                &[],
                clock.current_timestamp_millis(),
                false,
            )
            .await;
    }

    // Validate unmanaged files are not deleted within the retention period

    let res = {
        clock.tick(Duration::hours(1));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), 0);
    for path in paths {
        assert!(!is_deleted(&mut context, path).await);
    }

    // Validate unmanaged files are deleted after the retention period
    let res = {
        clock.tick(Duration::days(8));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), paths.len());
    for path in paths {
        assert!(is_deleted(&mut context, path).await);
    }
}

async fn is_deleted(context: &mut TestContext, path: &str) -> bool {
    let uri = context.table.as_ref().unwrap().table_uri.to_string();
    let backend = context.get_storage();
    let path = uri + "/" + path;
    let res = backend.head_obj(&path).await;
    match res {
        Err(StorageError::NotFound) => true,
        _ => false,
    }
}
