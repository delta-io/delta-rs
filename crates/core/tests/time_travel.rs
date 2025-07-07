use chrono::{DateTime, FixedOffset, Utc};
use std::fs::{FileTimes, OpenOptions};
use std::path::Path;
use std::time::SystemTime;

#[tokio::test]
async fn time_travel_by_ds() {
    // test time travel on a table with an uncommitted delta in a .tmp subfolder

    // git does not preserve mtime, so we need to manually set it in the test
    let log_dir = "../test/tests/data/simple_table/_delta_log";
    let log_mtime_pair = vec![
        ("00000000000000000000.json", "2020-05-01T22:47:31-07:00"),
        ("00000000000000000001.json", "2020-05-02T22:47:31-07:00"),
        ("00000000000000000002.json", "2020-05-03T22:47:31-07:00"),
        ("00000000000000000003.json", "2020-05-04T22:47:31-07:00"),
        ("00000000000000000004.json", "2020-05-05T22:47:31-07:00"),
        // Final file is uncommitted by Spark and is in a .tmp subdir
        (
            ".tmp/00000000000000000005.json",
            "2020-05-06T22:47:31-07:00",
        ),
    ];
    for (fname, ds) in log_mtime_pair {
        let ts: SystemTime = ds_to_ts(ds).into();
        let full_path = Path::new(log_dir).join(fname);
        let file = OpenOptions::new().write(true).open(full_path).unwrap();
        let times = FileTimes::new().set_accessed(ts).set_modified(ts);
        file.set_times(times).unwrap()
    }

    let mut table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-01T00:47:31-07:00",
    )
    .await
    .unwrap();

    assert_eq!(table.version(), Some(0));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-02T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(1));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-02T23:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(1));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-03T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(2));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-04T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(3));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-05T21:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(3));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-05T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(4));

    // Final append in .tmp subdir is uncommitted and should be ignored
    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-25T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(4));
}

#[tokio::test]
async fn time_travel_by_ds_checkpoint_vacuumed() {
    // git does not preserve mtime, so we need to manually set it in the test
    let log_dir = "../test/tests/data/checkpoints_vacuumed/_delta_log";
    let log_mtime_pair = vec![
        (
            "00000000000000000005.checkpoint.parquet",
            "2020-05-01T22:47:31-07:00",
        ),
        ("00000000000000000005.json", "2020-05-02T22:47:31-07:00"),
        ("00000000000000000006.json", "2020-05-03T22:47:31-07:00"),
        ("00000000000000000007.json", "2020-05-04T22:47:31-07:00"),
        ("00000000000000000008.json", "2020-05-05T22:47:31-07:00"),
        ("00000000000000000009.json", "2020-05-06T22:47:31-07:00"),
        (
            "00000000000000000010.checkpoint.parquet",
            "2020-05-06T22:47:31-07:00",
        ),
        ("00000000000000000010.json", "2020-05-08T22:47:31-07:00"),
        ("00000000000000000011.json", "2020-05-09T22:47:31-07:00"),
        ("00000000000000000012.json", "2020-05-10T22:47:31-07:00"),
    ];
    for (fname, ds) in log_mtime_pair {
        let ts: SystemTime = ds_to_ts(ds).into();
        let full_path = Path::new(log_dir).join(fname);
        let file = OpenOptions::new().write(true).open(full_path).unwrap();
        let times = FileTimes::new().set_accessed(ts).set_modified(ts);
        file.set_times(times).unwrap()
    }

    let table = deltalake_core::open_table_with_ds(
        "../test/tests/data/checkpoints_vacuumed",
        "2020-05-02T00:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(5));

    let table = deltalake_core::open_table_with_ds(
        "../test/tests/data/checkpoints_vacuumed",
        "2020-05-09T00:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(10));

    let latest_version = table.get_latest_version().await.unwrap();
    assert_eq!(latest_version, 12);

    // Test that a temporary checkpoint file breaks binary search
    let existing_dir = "../test/tests/data/checkpoints_vacuumed/_delta_log";
    let temp_checkpoint = tempfile::NamedTempFile::with_prefix_in(
        "00000000000000000000.checkpoint.parquet.tmp",
        existing_dir,
    )
    .unwrap();
    assert!(temp_checkpoint.path().exists());

    let table = deltalake_core::open_table_with_ds(
        "../test/tests/data/checkpoints_vacuumed",
        "2020-05-09T00:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(10));
}

fn ds_to_ts(ds: &str) -> DateTime<Utc> {
    let fixed_dt = DateTime::<FixedOffset>::parse_from_rfc3339(ds).unwrap();
    DateTime::<Utc>::from(fixed_dt)
}
