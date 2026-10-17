use chrono::Utc;
use deltalake_core::DeltaTable;
use deltalake_core::checkpoints::{cleanup_expired_logs_for, create_checkpoint};
use deltalake_core::kernel::{DataType, PrimitiveType};
use deltalake_core::logstore::object_store::ObjectStoreExt as _;
use deltalake_core::writer::{DeltaWriter, JsonWriter};
use deltalake_core::{DeltaTableBuilder, ensure_table_uri, errors::DeltaResult};
use deltalake_test::utils::*;
use object_store::path::Path;
use serde_json::json;
use serial_test::serial;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;
use url::Url;

/// Clone an existing test table from the tests crate into a [TempDir] for use in an integration
/// test
pub fn clone_table(table_name: impl AsRef<str> + std::fmt::Display) -> TempDir {
    // Create a temporary directory
    let tmp_dir = TempDir::new().expect("Failed to make temp dir");

    // Copy recursively from the test data directory to the temporary directory
    let source_path = format!("../test/tests/data/{table_name}");
    let options = fs_extra::dir::CopyOptions {
        content_only: true,
        ..Default::default()
    };
    println!("copying from {source_path}");
    fs_extra::dir::copy(source_path, tmp_dir.path(), &options).unwrap();
    tmp_dir
}

#[tokio::test]
#[serial]
// This test requires refactoring and a revisit
#[ignore]
async fn cleanup_metadata_fs_test() -> TestResult {
    let storage = Box::new(LocalStorageIntegration::default());
    let context = IntegrationContext::new(storage)?;
    cleanup_metadata_test(&context).await?;
    Ok(())
}

// Last-Modified for S3 could not be altered by user, hence using system pauses which makes
// test to run longer but reliable
async fn cleanup_metadata_test(context: &IntegrationContext) -> TestResult {
    let table_uri = context.root_uri();
    let table_url = deltalake_core::table::builder::parse_table_uri(table_uri).unwrap();
    let log_store = DeltaTableBuilder::from_url(table_url)?
        .with_allow_http(true)
        .build_storage()?;
    let object_store = log_store.object_store();

    let log_path = |version| {
        log_store
            .log_path()
            .clone()
            .join(format!("{version:020}.json"))
    };

    // we don't need to actually populate files with content as cleanup works only with file's metadata
    object_store
        .put(&log_path(0), bytes::Bytes::from("foo").into())
        .await?;

    // since we cannot alter s3 object metadata, we mimic it with pauses
    // also we forced to use 2 seconds since Last-Modified is stored in seconds
    std::thread::sleep(Duration::from_secs(2));
    object_store
        .put(&log_path(1), bytes::Bytes::from("foo").into())
        .await?;

    std::thread::sleep(Duration::from_secs(3));
    object_store
        .put(&log_path(2), bytes::Bytes::from("foo").into())
        .await?;

    let v0time = object_store.head(&log_path(0)).await?.last_modified;
    let v1time = object_store.head(&log_path(1)).await?.last_modified;
    let v2time = object_store.head(&log_path(2)).await?.last_modified;

    // we choose the retention timestamp to be between v1 and v2 so v2 will be kept but other removed.
    let retention_timestamp =
        v1time.timestamp_millis() + (v2time.timestamp_millis() - v1time.timestamp_millis()) / 2;

    assert!(retention_timestamp > v0time.timestamp_millis());
    assert!(retention_timestamp > v1time.timestamp_millis());
    assert!(retention_timestamp < v2time.timestamp_millis());

    let removed = cleanup_expired_logs_for(3, log_store.as_ref(), retention_timestamp).await?;

    assert_eq!(removed, 2);
    assert!(object_store.head(&log_path(0)).await.is_err());
    assert!(object_store.head(&log_path(1)).await.is_err());
    assert!(object_store.head(&log_path(2)).await.is_ok());

    // after test cleanup
    object_store.delete(&log_path(2)).await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_issue_1420_cleanup_expired_logs_for() -> DeltaResult<()> {
    let _ = std::fs::remove_dir_all("./tests/data/issue_1420");

    // Create the directory and get absolute path
    std::fs::create_dir_all("./tests/data/issue_1420").unwrap();
    let path = std::path::Path::new("./tests/data/issue_1420")
        .canonicalize()
        .unwrap();
    let mut table = DeltaTable::try_from_url(url::Url::from_directory_path(path).unwrap())
        .await?
        .create()
        .with_column(
            "id",
            DataType::Primitive(PrimitiveType::Integer),
            false,
            None,
        )
        .await?;

    let mut writer = JsonWriter::for_table(&table)?;
    writer.write(vec![json!({"id": 1})]).await?;
    writer.flush_and_commit(&mut table).await?; // v1

    writer.write(vec![json!({"id": 2})]).await?;
    writer.flush_and_commit(&mut table).await?; // v2
    assert_eq!(table.version(), Some(2));

    create_checkpoint(&table).await.unwrap(); // v2.checkpoint.parquet

    sleep(Duration::from_secs(1)).await;
    let ts = Utc::now(); // use this ts for log retention expiry

    // Should delete v1 but not v2 or v2.checkpoint.parquet
    cleanup_expired_logs_for(
        table.version().unwrap(),
        table.log_store().as_ref(),
        ts.timestamp_millis(),
    )
    .await?;

    assert!(
        table
            .log_store()
            .object_store()
            .head(&Path::from(format!("_delta_log/{:020}.json", 1)))
            .await
            .is_err(),
        "commit should not exist"
    );

    assert!(
        table
            .log_store()
            .object_store()
            .head(&Path::from(format!("_delta_log/{:020}.json", 2)))
            .await
            .is_ok(),
        "commit should exist"
    );

    assert!(
        table
            .log_store()
            .object_store()
            .head(&Path::from(format!(
                "_delta_log/{:020}.checkpoint.parquet",
                2
            )))
            .await
            .is_ok(),
        "checkpoint should exist"
    );

    // pretend time advanced but there is no new versions after v2
    // v2 and v2.checkpoint.parquet should still be there
    let ts = Utc::now();
    sleep(Duration::from_secs(1)).await;

    cleanup_expired_logs_for(
        table.version().unwrap(),
        table.log_store().as_ref(),
        ts.timestamp_millis(),
    )
    .await?;

    assert!(
        table
            .log_store()
            .object_store()
            .head(&Path::from(format!("_delta_log/{:020}.json", 2)))
            .await
            .is_ok(),
        "commit should exist"
    );

    assert!(
        table
            .log_store()
            .object_store()
            .head(&Path::from(format!(
                "_delta_log/{:020}.checkpoint.parquet",
                2
            )))
            .await
            .is_ok(),
        "checkpoint should exist"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
/// This test validates a checkpoint can be updated on a pre deltalake (python) 1.x table
/// see also: <https://github.com/delta-io/delta-rs/issues/3527>
async fn test_older_checkpoint_reads() -> DeltaResult<()> {
    let temp_table = clone_table("python-0.25.5-checkpoint");
    let table_path = temp_table.path().to_str().unwrap();
    let table_url = ensure_table_uri(table_path).unwrap();
    let table = deltalake_core::open_table(table_url).await?;
    assert_eq!(table.version(), Some(1));
    create_checkpoint(&table).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
/// This test validates that we can read a table with v2 checkpoints
async fn test_v2_checkpoint_json() -> DeltaResult<()> {
    let temp_table = clone_table("v2-classic-checkpoint-json");
    let table_path = temp_table.path().to_str().unwrap();
    let table_url = ensure_table_uri(table_path).unwrap();
    let table = deltalake_core::open_table(table_url).await?;
    assert_eq!(table.version(), Some(1));
    create_checkpoint(&table).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
/// Regression test for <https://github.com/delta-io/delta-rs/issues/4462>
///
/// `checkpointProtection` is a writer-only table feature (see the protocol RFC
/// <https://github.com/delta-io/delta/blob/master/protocol_rfcs/checkpoint-protection.md>)
/// which constrains checkpoint creation and log cleanup around `DROP FEATURE` boundaries.
/// Plain reads and appends are unaffected by the feature, but tables with it enabled used to
/// fail writes with `Unsupported table features required: [Unknown("checkpointProtection")]`.
///
/// The `spark-checkpoint-protection` fixture was generated with PySpark 4.0.1 / delta-spark
/// 4.0.1 by writing a small table and then running:
/// `ALTER TABLE delta.`<path>` SET TBLPROPERTIES ('delta.feature.checkpointProtection' = 'supported')`
/// which produced a protocol of minReaderVersion=1 / minWriterVersion=7 with
/// `checkpointProtection` in `writerFeatures`.
async fn test_checkpoint_protection_read_write() -> TestResult {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use deltalake_core::operations::collect_sendable_stream;
    use deltalake_core::protocol::SaveMode;

    let temp_table = clone_table("spark-checkpoint-protection");
    let table_path = temp_table.path().to_str().unwrap();
    let table_url = ensure_table_uri(table_path).unwrap();

    let table = deltalake_core::open_table(table_url.clone()).await?;
    assert_eq!(table.version(), Some(2));
    let protocol = table.snapshot()?.protocol();
    assert_eq!(protocol.min_reader_version(), 1);
    assert_eq!(protocol.min_writer_version(), 7);
    // The kernel does not have a named variant for this feature yet, so it is
    // surfaced as `TableFeature::Unknown("checkpointProtection")`. `as_ref()` on
    // such a variant returns the variant name, not the payload, so compare via Debug.
    assert!(
        protocol.writer_features().is_some_and(|features| features
            .iter()
            .any(|f| format!("{:?}", f) == "Unknown(\"checkpointProtection\")")),
        "fixture should carry checkpointProtection in its writer features: {:?}",
        protocol.writer_features()
    );

    // Reading the PySpark-generated table must work: the feature is writer-only
    // and readers do not need to understand it
    let (_table, stream) = table.scan_table().await?;
    let batches = collect_sendable_stream(stream).await?;
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(row_count, 11);

    // Appending to the table must work as well; the feature only constrains
    // checkpoint creation and metadata cleanup
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int64, true),
        Field::new("name", ArrowDataType::Utf8, true),
        Field::new("flag", ArrowDataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![100])) as ArrayRef,
            Arc::new(StringArray::from(vec!["delta_rs"])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        ],
    )?;

    let table = table
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await?;
    assert_eq!(table.version(), Some(3));

    // The appended data is readable and the feature remains in the protocol
    let table = deltalake_core::open_table(table_url).await?;
    let protocol = table.snapshot()?.protocol();
    assert!(
        protocol.writer_features().is_some_and(|features| features
            .iter()
            .any(|f| format!("{:?}", f) == "Unknown(\"checkpointProtection\")")),
        "checkpointProtection should still be in the writer features after appending: {:?}",
        protocol.writer_features()
    );
    let (_table, stream) = table.scan_table().await?;
    let batches = collect_sendable_stream(stream).await?;
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(row_count, 12);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
/// Regression test for the safety concern raised in
/// <https://github.com/delta-io/delta-rs/pull/4817#issuecomment-5927159262>.
///
/// Ion observed that `open_table_with_version(old_version) + create_checkpoint()` might
/// bypass the `checkpointProtection` constraint because the old snapshot does not carry
/// the feature.  This test proves the invariant holds:
///
/// 1. Loading at a version *before* `checkpointProtection` was added yields a protocol
///    that has no writer features — so the constraint simply does not apply at that
///    historical protocol level.
///
/// 2. `create_checkpoint` on that old-version handle writes a checkpoint file that is
///    explicitly anchored to the old version (version 0 in the fixture), not the latest.
///    The delta-kernel snapshot built inside `create_checkpoint_for` is constructed
///    `at_version(0)`, so it reads the v0 protocol only.
///
/// 3. The latest table version (v2) — which *does* carry `checkpointProtection` — remains
///    fully readable and its protocol is unchanged after the old-version checkpoint lands.
///
/// 4. A normal `create_checkpoint` against the current (v2) handle also succeeds, proving
///    that claiming support for `checkpointProtection` (as the PR adds) does not introduce
///    any regression in the common, non-time-travel checkpoint path.
///
/// The `spark-checkpoint-protection` fixture has:
///   v0 — initial write, 10 rows, minReader=1/minWriter=2 (no writerFeatures)
///   v1 — SET TBLPROPERTIES adds checkpointProtection, bumps to minWriter=7
///   v2 — one-row append (checkpointProtection still present in protocol)
async fn test_checkpoint_protection_old_version_does_not_bypass_constraint() -> DeltaResult<()> {
    let temp_table = clone_table("spark-checkpoint-protection");
    let table_path = temp_table.path().to_str().unwrap();
    let table_url = ensure_table_uri(table_path).unwrap();

    // ── Step 1: open at version 0 (pre-checkpointProtection) ──────────────────────────
    let table_v0 = deltalake_core::open_table_with_version(table_url.clone(), 0).await?;
    assert_eq!(
        table_v0.version(),
        Some(0),
        "should be at version 0 after time-travel open"
    );

    // At version 0 the protocol is minReader=1/minWriter=2 with *no* writerFeatures.
    // checkpointProtection must not appear — it was only introduced in v1.
    let proto_v0 = table_v0.snapshot()?.protocol();
    assert_eq!(proto_v0.min_reader_version(), 1);
    assert_eq!(proto_v0.min_writer_version(), 2);
    assert!(
        proto_v0
            .writer_features()
            .map(|f| f.is_empty())
            .unwrap_or(true),
        "v0 protocol must carry no writer features; got: {:?}",
        proto_v0.writer_features()
    );

    // ── Step 2: create_checkpoint via the old-version handle ──────────────────────────
    // This is the exact API path Ion flagged.  create_checkpoint reads the version from
    // `table.snapshot()?.version()` — which is 0 here — and calls
    // `create_checkpoint_for(0, ...)`.  The kernel Snapshot is built at_version(0), so it
    // sees only the v0 protocol and has no checkpointProtection restriction to satisfy.
    create_checkpoint(&table_v0).await?;

    // The checkpoint file must be written at version 0, *not* at the latest version.
    let v0_ckpt_exists = table_v0
        .log_store()
        .object_store()
        .head(&Path::from(
            "_delta_log/00000000000000000000.checkpoint.parquet",
        ))
        .await
        .is_ok();
    assert!(
        v0_ckpt_exists,
        "checkpoint.parquet for version 0 must exist after create_checkpoint on the v0 handle"
    );

    // A checkpoint for v2 must NOT have been created by the v0 handle.
    let v2_ckpt_absent = table_v0
        .log_store()
        .object_store()
        .head(&Path::from(
            "_delta_log/00000000000000000002.checkpoint.parquet",
        ))
        .await
        .is_err();
    assert!(
        v2_ckpt_absent,
        "create_checkpoint on the v0 handle must not create a checkpoint at version 2"
    );

    // ── Step 3: latest table state must be undisturbed ────────────────────────────────
    let table_latest = deltalake_core::open_table(table_url.clone()).await?;
    assert_eq!(
        table_latest.version(),
        Some(2),
        "latest version must still be 2 after old-version checkpoint was created"
    );
    let proto_latest = table_latest.snapshot()?.protocol();
    assert!(
        proto_latest
            .writer_features()
            .is_some_and(|features| features
                .iter()
                .any(|f| format!("{:?}", f) == "Unknown(\"checkpointProtection\")")),
        "checkpointProtection must still be in the latest-version writer features: {:?}",
        proto_latest.writer_features()
    );

    // All 11 rows (10 from v0 + 1 appended in v2) must be readable.
    use deltalake_core::operations::collect_sendable_stream;
    let (_tbl, stream) = table_latest.scan_table().await?;
    let batches = collect_sendable_stream(stream).await?;
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        row_count, 11,
        "all 11 rows must be readable from the latest version"
    );

    // ── Step 4: current-version checkpoint path must also work ────────────────────────
    // Proves the PR's `checkpointProtection` registration does not block normal
    // (non-time-travel) checkpoint creation.
    create_checkpoint(&table_latest).await?;
    let v2_ckpt_exists = table_latest
        .log_store()
        .object_store()
        .head(&Path::from(
            "_delta_log/00000000000000000002.checkpoint.parquet",
        ))
        .await
        .is_ok();
    assert!(
        v2_ckpt_exists,
        "checkpoint.parquet for version 2 must exist after create_checkpoint on the latest handle"
    );

    Ok(())
}

#[tokio::test]
/// This test that we can read a table with domain metadata. Since we cannot
/// write domain metadata atm, we can at least test, that accessing restricted
/// domain metadata in the table fails with a proper error.
async fn test_checkpoint_with_domain_meta() -> DeltaResult<()> {
    let temp_table = clone_table("table-with-domain-metadata");
    let table_path = temp_table.path().to_str().unwrap();
    let table =
        deltalake_core::open_table(Url::parse(&format!("file://{table_path}")).unwrap()).await?;
    assert_eq!(table.version(), Some(108));
    let metadata = table
        .snapshot()
        .unwrap()
        .snapshot()
        .domain_metadata(&table.log_store(), "delta.clustering")
        .await;
    assert!(
        metadata.unwrap_err().to_string().contains(
            "User DomainMetadata are not allowed to use system-controlled 'delta.*' domain"
        )
    );
    Ok(())
}
