use chrono::Duration;
use deltalake::storage::file::FileStorageBackend;
use deltalake::vacuum::Vacuum;
use deltalake::StorageBackend;

use std::time::SystemTime;

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
