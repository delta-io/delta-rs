use chrono::Duration;
use deltalake::vacuum::Clock;
use deltalake::vacuum::Vacuum;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore};
use std::sync::Arc;

use common::clock::TestClock;
use common::schemas::{get_vacuum_underscore_schema, get_xy_date_schema};
use common::TestContext;
use std::time::SystemTime;

mod common;

#[tokio::test]
async fn vacuum_delta_8_0_table() {
    let mut table = deltalake::open_table("./tests/data/delta-0.8.0")
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
        vec!["part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"]
    );

    let result = Vacuum::default()
        .with_retention_period(Duration::hours(169))
        .dry_run(true)
        .execute(&mut table)
        .await
        .unwrap();

    assert_eq!(
        result.files_deleted,
        vec!["part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"]
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

    let paths = [
        Path::from("delete_me.parquet"),
        Path::from("dont_delete_me.parquet"),
    ];

    for path in paths {
        context
            .add_file(
                &path,
                "random junk".as_bytes().into(),
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
    assert!(is_deleted(&mut context, &Path::from("delete_me.parquet")).await);
    assert!(!is_deleted(&mut context, &Path::from("dont_delete_me.parquet")).await);
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
        Path::from("date=2022-07-03/x=2/delete_me.parquet"),
        Path::from("date=2022-07-03/x=2/dont_delete_me.parquet"),
    ];
    let partition_values = [("date", Some("2022-07-03")), ("x", Some("2"))];

    for path in paths {
        context
            .add_file(
                &path,
                "random junk".as_bytes().into(),
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
    context
        .create_table_from_schema(get_vacuum_underscore_schema(), &["_date"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        Path::from("_date=2022-07-03/delete_me.parquet"),
        Path::from("_date=2022-07-03/dont_delete_me.parquet"),
    ];

    let partition_values = &[("_date", Some("2022-07-03"))];

    for path in paths {
        context
            .add_file(
                &path,
                "random junk".as_bytes().into(),
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
    context
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
        context
            .add_file(
                path,
                "random junk".as_bytes().into(),
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
    for path in paths_delete.iter().chain(paths_ignore.iter()) {
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
    let res = backend.head(path).await;
    match res {
        Err(ObjectStoreError::NotFound { .. }) => true,
        _ => false,
    }
}
