//! Spark logs `add.path` with non-ASCII partition values unescaped (`city=Zürich/...`), while
//! delta-rs escapes them (`city=Z%C3%BCrich/...`). Readers pair a remove with its add by the
//! exact path string, so every remove delta-rs writes for an existing file must reuse the path
//! from the log, or the file stays active.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::assert_batches_sorted_eq;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::DeltaSessionContext;
use deltalake_core::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake_core::operations::optimize::OptimizeType;
use deltalake_core::protocol::SaveMode;
use deltalake_core::test_utils::TestTables;
use deltalake_core::{DeltaTable, open_table};
use serde_json::Value;
use tempfile::TempDir;
use url::Url;

const ZURICH: &str = "Zürich";

const FIXTURE_ROWS: [&str; 10] = [
    "+----+--------+---+",
    "| id | city   | v |",
    "+----+--------+---+",
    "| 1  | Zürich | a |",
    "| 2  | Paris  | a |",
    "| 3  | 東京   | a |",
    "| 4  | Zürich | b |",
    "| 5  | Paris  | b |",
    "| 6  | 東京   | b |",
    "+----+--------+---+",
];

/// A private copy of the Spark fixture table, kept alive by `_dir` for the whole test.
struct Fixture {
    _dir: TempDir,
    root: std::path::PathBuf,
    table: DeltaTable,
}

/// Copies the Spark-written fixture to a temp dir and opens it, so a test can commit freely.
async fn spark_fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    fs_extra::dir::copy(
        TestTables::SparkNonAsciiPartitions.as_path(),
        dir.path(),
        &Default::default(),
    )
    .unwrap();
    let root = dir
        .path()
        .join(TestTables::SparkNonAsciiPartitions.as_name());
    let table = open_table(Url::from_directory_path(&root).unwrap())
        .await
        .unwrap();
    Fixture {
        _dir: dir,
        root,
        table,
    }
}

/// Returns the raw JSON `kind` actions (`add`, `remove`, ...) of one commit, as written to the log.
fn commit_actions(root: &Path, version: u64, kind: &str) -> Vec<Value> {
    let commit = root.join(format!("_delta_log/{version:020}.json"));
    std::fs::read_to_string(commit)
        .unwrap()
        .lines()
        .filter_map(|line| {
            serde_json::from_str::<Value>(line)
                .unwrap()
                .get(kind)
                .cloned()
        })
        .collect()
}

/// Returns the `add.path` strings exactly as logged in `versions`, without decoding.
fn logged_add_paths(root: &Path, versions: std::ops::RangeInclusive<u64>) -> BTreeSet<String> {
    versions
        .flat_map(|version| commit_actions(root, version, "add"))
        .map(|add| add["path"].as_str().unwrap().to_string())
        .collect()
}

/// Returns the logged paths of the fixture's own adds, optionally only those in partition `city`.
fn fixture_add_paths(root: &Path, city: Option<&str>) -> BTreeSet<String> {
    logged_add_paths(root, 1..=2)
        .into_iter()
        .filter(|path| city.is_none_or(|city| path.starts_with(&format!("city={city}/"))))
        .collect()
}

/// Returns the `remove.path` strings logged by the table's latest commit, as written.
fn removed_paths(table: &DeltaTable, root: &Path) -> BTreeSet<String> {
    let version = table.version().unwrap();
    commit_actions(root, version, "remove")
        .into_iter()
        .map(|remove| remove["path"].as_str().unwrap().to_string())
        .collect()
}

/// Returns every row of `table` in no particular order, for use with `assert_batches_sorted_eq!`.
async fn all_rows(table: &DeltaTable) -> Vec<RecordBatch> {
    let ctx: SessionContext = DeltaSessionContext::default().into();
    table.update_datafusion_session(&ctx.state()).unwrap();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    ctx.sql("SELECT id, city, v FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

/// Opens the table at `root` from scratch, so assertions read the log instead of in-memory state.
async fn reloaded(root: &Path) -> DeltaTable {
    open_table(Url::from_directory_path(root).unwrap())
        .await
        .unwrap()
}

/// Builds an `(id, city, v)` batch matching the fixture schema, for writes and merge sources.
fn batch(rows: &[(i32, &str, &str)]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_delete_removes_spark_non_ascii_partition() {
    let Fixture { _dir, root, table } = spark_fixture().await;

    let (table, metrics) = table
        .delete()
        .with_predicate(col("city").eq(lit(ZURICH)))
        .await
        .unwrap();

    assert_eq!(metrics.num_removed_files, 2);
    assert_eq!(
        removed_paths(&table, &root),
        fixture_add_paths(&root, Some(ZURICH))
    );
    let expected = [
        "+----+-------+---+",
        "| id | city  | v |",
        "+----+-------+---+",
        "| 2  | Paris | a |",
        "| 3  | 東京  | a |",
        "| 5  | Paris | b |",
        "| 6  | 東京  | b |",
        "+----+-------+---+",
    ];
    assert_batches_sorted_eq!(expected, &all_rows(&reloaded(&root).await).await);
}

#[tokio::test]
async fn test_update_rewrites_spark_non_ascii_partition_without_duplicates() {
    let Fixture { _dir, root, table } = spark_fixture().await;

    let (table, _) = table
        .update()
        .with_predicate(col("city").eq(lit(ZURICH)))
        .with_update("v", lit("U"))
        .await
        .unwrap();

    assert_eq!(
        removed_paths(&table, &root),
        fixture_add_paths(&root, Some(ZURICH))
    );
    let expected = [
        "+----+--------+---+",
        "| id | city   | v |",
        "+----+--------+---+",
        "| 1  | Zürich | U |",
        "| 2  | Paris  | a |",
        "| 3  | 東京   | a |",
        "| 4  | Zürich | U |",
        "| 5  | Paris  | b |",
        "| 6  | 東京   | b |",
        "+----+--------+---+",
    ];
    assert_batches_sorted_eq!(expected, &all_rows(&reloaded(&root).await).await);
}

#[tokio::test]
async fn test_merge_rewrites_spark_non_ascii_partition_without_duplicates() {
    let Fixture { _dir, root, table } = spark_fixture().await;
    let source = SessionContext::new()
        .read_batch(batch(&[(1, ZURICH, "M")]))
        .unwrap();

    let (table, _) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| update.update("v", col("source.v")))
        .unwrap()
        .await
        .unwrap();

    let removed = removed_paths(&table, &root);
    assert_eq!(removed.len(), 1);
    assert!(removed.is_subset(&fixture_add_paths(&root, Some(ZURICH))));
    let expected = [
        "+----+--------+---+",
        "| id | city   | v |",
        "+----+--------+---+",
        "| 1  | Zürich | M |",
        "| 2  | Paris  | a |",
        "| 3  | 東京   | a |",
        "| 4  | Zürich | b |",
        "| 5  | Paris  | b |",
        "| 6  | 東京   | b |",
        "+----+--------+---+",
    ];
    assert_batches_sorted_eq!(expected, &all_rows(&reloaded(&root).await).await);
}

#[tokio::test]
async fn test_optimize_compacts_spark_non_ascii_partitions_without_duplicates() {
    let Fixture { _dir, root, table } = spark_fixture().await;

    let (table, metrics) = table
        .optimize()
        .with_type(OptimizeType::Compact)
        .await
        .unwrap();

    assert_eq!(metrics.num_files_removed, 6);
    assert_eq!(removed_paths(&table, &root), fixture_add_paths(&root, None));
    assert_batches_sorted_eq!(FIXTURE_ROWS, &all_rows(&reloaded(&root).await).await);
}

#[tokio::test]
async fn test_zorder_spark_non_ascii_partitions_without_duplicates() {
    let Fixture { _dir, root, table } = spark_fixture().await;

    let (table, _) = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["id".to_string()]))
        .await
        .unwrap();

    assert_eq!(removed_paths(&table, &root), fixture_add_paths(&root, None));
    assert_batches_sorted_eq!(FIXTURE_ROWS, &all_rows(&reloaded(&root).await).await);
}

#[tokio::test]
async fn test_overwrite_replaces_spark_non_ascii_partitions() {
    let Fixture { _dir, root, table } = spark_fixture().await;

    let table = table
        .write(vec![batch(&[(100, "Paris", "OW")])])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(removed_paths(&table, &root), fixture_add_paths(&root, None));
    let expected = [
        "+-----+-------+----+",
        "| id  | city  | v  |",
        "+-----+-------+----+",
        "| 100 | Paris | OW |",
        "+-----+-------+----+",
    ];
    assert_batches_sorted_eq!(expected, &all_rows(&reloaded(&root).await).await);
}

#[tokio::test]
async fn test_replace_where_replaces_spark_non_ascii_partition() {
    let Fixture { _dir, root, table } = spark_fixture().await;

    let table = table
        .write(vec![batch(&[(100, ZURICH, "RW")])])
        .with_save_mode(SaveMode::Overwrite)
        .with_replace_where(col("city").eq(lit(ZURICH)))
        .await
        .unwrap();

    assert_eq!(
        removed_paths(&table, &root),
        fixture_add_paths(&root, Some(ZURICH))
    );
    let expected = [
        "+-----+--------+----+",
        "| id  | city   | v  |",
        "+-----+--------+----+",
        "| 2   | Paris  | a  |",
        "| 3   | 東京   | a  |",
        "| 5   | Paris  | b  |",
        "| 6   | 東京   | b  |",
        "| 100 | Zürich | RW |",
        "+-----+--------+----+",
    ];
    assert_batches_sorted_eq!(expected, &all_rows(&reloaded(&root).await).await);
}

#[tokio::test]
async fn test_restore_removes_spark_non_ascii_files() {
    let Fixture { _dir, root, table } = spark_fixture().await;

    let (table, _) = table.restore().with_version_to_restore(1).await.unwrap();

    assert_eq!(removed_paths(&table, &root), logged_add_paths(&root, 2..=2));
    let expected = [
        "+----+--------+---+",
        "| id | city   | v |",
        "+----+--------+---+",
        "| 1  | Zürich | a |",
        "| 2  | Paris  | a |",
        "| 3  | 東京   | a |",
        "+----+--------+---+",
    ];
    assert_batches_sorted_eq!(expected, &all_rows(&reloaded(&root).await).await);
}

#[tokio::test]
async fn test_filesystem_check_removes_missing_spark_non_ascii_file() {
    let Fixture { _dir, root, table } = spark_fixture().await;
    let missing = logged_add_paths(&root, 1..=1)
        .into_iter()
        .find(|path| path.starts_with(&format!("city={ZURICH}/")))
        .unwrap();
    std::fs::remove_file(root.join(&missing)).unwrap();

    let (table, metrics) = table.filesystem_check().await.unwrap();

    assert_eq!(metrics.files_removed.len(), 1);
    assert_eq!(removed_paths(&table, &root), BTreeSet::from([missing]));
    let table = reloaded(&root).await;
    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 5);
}

#[tokio::test]
async fn test_delete_keeps_escaped_remove_paths_for_delta_rs_tables() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let table = DeltaTable::try_from_url(Url::from_directory_path(&root).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns([
            StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Integer), true),
            StructField::new(
                "city",
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new("v", DeltaDataType::Primitive(PrimitiveType::String), true),
        ])
        .with_partition_columns(["city"])
        .await
        .unwrap();
    let table = table
        .write(vec![batch(&[
            (1, ZURICH, "a"),
            (2, "Paris", "a"),
            (3, "a b", "a"),
        ])])
        .await
        .unwrap();
    let zurich_adds = logged_add_paths(&root, 1..=1)
        .into_iter()
        .filter(|path| path.starts_with("city=Z%25C3%25BCrich/"))
        .collect::<BTreeSet<_>>();
    assert_eq!(zurich_adds.len(), 1);

    let (table, _) = table
        .delete()
        .with_predicate(col("city").eq(lit(ZURICH)).or(col("city").eq(lit("a b"))))
        .await
        .unwrap();

    let removed = removed_paths(&table, &root);
    assert_eq!(removed.len(), 2);
    assert!(removed.is_subset(&logged_add_paths(&root, 1..=1)));
    assert!(removed.is_superset(&zurich_adds));
    let expected = [
        "+----+-------+---+",
        "| id | city  | v |",
        "+----+-------+---+",
        "| 2  | Paris | a |",
        "+----+-------+---+",
    ];
    assert_batches_sorted_eq!(expected, &all_rows(&reloaded(&root).await).await);
}
