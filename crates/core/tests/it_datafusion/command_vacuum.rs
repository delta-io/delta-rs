use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
use chrono::Duration;
use deltalake_core::DeltaTable;
use deltalake_core::kernel::StructType;
use deltalake_core::operations::vacuum::Clock;
use deltalake_core::protocol::SaveMode;
use deltalake_test::clock::TestClock;
use deltalake_test::*;
use object_store::{Error as ObjectStoreError, ObjectStoreExt as _, path::Path};
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
        let (_, metrics) = table
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
        let (_, metrics) = table
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
        let (_, metrics) = table
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
        table
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
        let (_, metrics) = table
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

/// `payload` has no leaf that is eligible for min/max statistics, so it must not reach the parsed
/// stats schema as an empty struct. Such a struct used to shrink to zero rows the first time log
/// replay filtered a batch partially, and the next commit then panicked in `StructArray::slice`.
/// The partial filter needs one commit that adds several files and a later commit that removes
/// only some of them. See delta-io/delta-rs#3237.
#[tokio::test]
async fn test_vacuum_struct_column_without_min_max_eligible_leaves() -> TestResult {
    fn batch(ids: Vec<i64>, parts: Vec<&str>) -> RecordBatch {
        let payload: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(ArrowField::new("data", ArrowDataType::Binary, true)),
            Arc::new(BinaryArray::from_vec(vec![b"x"; ids.len()])) as ArrayRef,
        )]));
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ("part", Arc::new(StringArray::from(parts)) as ArrayRef),
            ("payload", payload),
        ])
        .unwrap()
    }

    let table = DeltaTable::new_in_memory()
        .write(vec![batch(vec![1, 2, 3], vec!["a", "b", "c"])])
        .with_partition_columns(["part"])
        .await?;
    // Tombstone one of the three files, so the next replay filters that batch partially.
    let (table, _metrics) = table.delete().with_predicate("part = 'a'").await?;
    let table = table.write(vec![batch(vec![4], vec!["d"])]).await?;

    // Parsed statistics must carry `id` only, with no empty `payload` struct.
    let add_actions = table.snapshot()?.add_actions_table(true)?;
    let add_actions_schema = add_actions.schema();
    let columns = add_actions_schema.fields();
    assert!(columns.iter().any(|field| field.name() == "min.id"));
    assert!(
        !columns
            .iter()
            .any(|field| field.name().starts_with("min.payload"))
    );

    let (_table, metrics) = table
        .vacuum()
        .with_retention_period(Duration::hours(0))
        .with_enforce_retention_duration(false)
        .with_dry_run(false)
        .await?;
    assert_eq!(metrics.files_deleted.len(), 1);

    Ok(())
}

async fn is_deleted(context: &mut TestContext, path: &Path) -> bool {
    let backend = context.get_storage();
    let res = backend.object_store(None).head(path).await;
    matches!(res, Err(ObjectStoreError::NotFound { .. }))
}
