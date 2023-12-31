use chrono::{DateTime, FixedOffset, Utc};
use std::path::Path;

#[tokio::test]
async fn time_travel_by_ds() {
    // git does not preserve mtime, so we need to manually set it in the test
    let log_dir = "./tests/data/simple_table/_delta_log";
    let log_mtime_pair = vec![
        ("00000000000000000000.json", "2020-05-01T22:47:31-07:00"),
        ("00000000000000000001.json", "2020-05-02T22:47:31-07:00"),
        ("00000000000000000002.json", "2020-05-03T22:47:31-07:00"),
        ("00000000000000000003.json", "2020-05-04T22:47:31-07:00"),
        ("00000000000000000004.json", "2020-05-05T22:47:31-07:00"),
    ];

    // Need to set both the commit timestamp in each log file and, as a secondary, the modifed time of the file in case the timestamp is not defined
    let log_dir_path = Path::new(log_dir);
    for (fname, ds) in log_mtime_pair {
        let ts = ds_to_ts(ds);
        let file = log_dir_path.join(fname);
        let contents = std::fs::read_to_string(&file).unwrap();
        let my_reg = regex::Regex::new(r#"("timestamp"):\s*\d+([\s,}])"#).unwrap();
        let new_contents = my_reg
            .replace(&contents, format!("$1:{}$2", ts))
            .to_string();
        std::fs::write(&file, new_contents).unwrap();
        utime::set_file_times(file, ts, ts).unwrap();
    }

    let mut table = deltalake_core::open_table_with_ds(
        "./tests/data/simple_table",
        "2020-05-01T00:47:31-07:00",
    )
    .await
    .unwrap();

    assert_eq!(table.version(), 0);

    table = deltalake_core::open_table_with_ds(
        "./tests/data/simple_table",
        "2020-05-02T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), 1);

    table = deltalake_core::open_table_with_ds(
        "./tests/data/simple_table",
        "2020-05-02T23:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), 1);

    table = deltalake_core::open_table_with_ds(
        "./tests/data/simple_table",
        "2020-05-03T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), 2);

    table = deltalake_core::open_table_with_ds(
        "./tests/data/simple_table",
        "2020-05-04T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), 3);

    table = deltalake_core::open_table_with_ds(
        "./tests/data/simple_table",
        "2020-05-05T21:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), 3);

    table = deltalake_core::open_table_with_ds(
        "./tests/data/simple_table",
        "2020-05-05T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), 4);

    table = deltalake_core::open_table_with_ds(
        "./tests/data/simple_table",
        "2020-05-25T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), 4);
}

fn ds_to_ts(ds: &str) -> i64 {
    let fixed_dt = DateTime::<FixedOffset>::parse_from_rfc3339(ds).unwrap();
    DateTime::<Utc>::from(fixed_dt).timestamp_millis()
}
