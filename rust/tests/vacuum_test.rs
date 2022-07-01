use deltalake::DeltaTable;
use serial_test::serial;
use std::error::Error;
use chrono::Duration;
use deltalake::vacuum::Vacuum;
use std::time::SystemTime;
use deltalake::storage::file::FileStorageBackend;
use deltalake::StorageBackend;


/*
TODO new Tests:
* Do a actual run and check acutally deleted files matches what was expected
* Create some tables that contain files that should be ignored (e.g dotfiles, cdc, etc)
*/

#[tokio::test]
async fn vacuum_delta_8_0_table() {
    let backend = FileStorageBackend::new("");
    let mut table = deltalake::open_table(&backend.join_paths(&["tests", "data", "delta-0.8.0"]))
        .await
        .unwrap();

    let retention_hours = 1;
    let dry_run = true;

    let result = Vacuum::default()
        .with_retention_period(Duration::hours(1))
        .dry_run(true)
        .execute(&mut table)
        .await;

    assert!(matches!(result.unwrap_err(),
        deltalake::vacuum::VacuumError::InvalidVacuumRetentionPeriod {
            provided,
            min,
        } if provided == Duration::hours(1).num_milliseconds() 
            && min == table.get_state().tombstone_retention_millis(),
    ));


    let result = Vacuum::default()
        .with_retention_period(Duration::hours(0))
        .dry_run(true)
        .enforce_retention_duration(false)
        .execute(&mut table)
        .await.unwrap();
    // do not enforce retention duration check with 0 hour will purge all files
    assert_eq!(
        result.files_to_delete,
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
        .await.unwrap();

    assert_eq!(
        result.files_to_delete,
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
        .execute(&mut table).await.unwrap();

    assert_eq!(
        result.files_to_delete,
        empty
    );
}
