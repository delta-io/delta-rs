//! Integration test to verify correct behavior of S3 DynamoDb locking.
//! It inspects the state of the locking table after each operation.
#![cfg(all(
    feature = "integration_test",
    any(feature = "s3", feature = "s3-native-tls")
))]

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use deltalake_core::kernel::{Action, Add, DataType, PrimitiveType, StructField, StructType};
use deltalake_core::logstore::s3::{
    lock_client::DynamoDbLockClient, CommitEntry, RepairLogEntryResult, S3DynamoDbLogStore,
};
use deltalake_core::logstore::LogStore;
use deltalake_core::operations::transaction::{commit, prepare_commit};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::storage::commit_uri_from_version;
use deltalake_core::storage::config::StorageOptions;
use deltalake_core::storage::s3::S3StorageOptions;
use deltalake_core::table::builder::ensure_table_uri;
use deltalake_core::test_utils::{IntegrationContext, StorageIntegration, TestTables};
use deltalake_core::{DeltaOps, DeltaTable, DeltaTableBuilder};
use lazy_static::lazy_static;
use object_store::path::Path;
use serde_json::Value;
use serial_test::serial;

#[allow(dead_code)]
mod fs_common;

pub type TestResult<T> = Result<T, Box<dyn std::error::Error + 'static>>;

lazy_static! {
    static ref LOCK_TABLE_NAME: String = format!("delta_log_tests_constant");
    static ref OPTIONS: HashMap<String, String> = maplit::hashmap! {
        "allow_http".to_owned() => "true".to_owned(),
        "table_name".to_owned() => LOCK_TABLE_NAME.to_owned(),
    };
    static ref S3_OPTIONS: S3StorageOptions = S3StorageOptions::from_map(&OPTIONS);
    static ref CLIENT: DynamoDbLockClient =
        DynamoDbLockClient::try_new(&S3StorageOptions::from_map(&OPTIONS))
            .expect("failure initializing dynamodb lock client");
    static ref TABLE_PATH: String = format!("s3://my_delta_table_{}", uuid::Uuid::new_v4());
}

#[ctor::ctor]
fn prepare_dynamodb() {
    let _context = IntegrationContext::new(StorageIntegration::Amazon).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let _create_table_result = CLIENT.try_create_lock_table().await.unwrap();
        });
}

#[test]
fn client_config_picks_up_lock_table_name() {
    assert_eq!(CLIENT.get_lock_table_name(), LOCK_TABLE_NAME.clone());
}

#[tokio::test]
async fn get_missing_item() -> TestResult<()> {
    let client = &CLIENT;
    let version = i64::MAX;
    let result = client.get_commit_entry(&TABLE_PATH, version).await;
    assert_eq!(result.unwrap(), None);
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_append() -> TestResult<()> {
    let context = IntegrationContext::new(StorageIntegration::Amazon)?;
    let table = prepare_table(&context, "delta01").await?;
    validate_lock_table_state(&table, 0).await?;
    append_to_table("datav01.parquet", &table, None).await?;
    validate_lock_table_state(&table, 1).await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_repair() -> TestResult<()> {
    let context = IntegrationContext::new(StorageIntegration::Amazon)?;
    let table = prepare_table(&context, "repair_needed").await?;
    let options: StorageOptions = OPTIONS.clone().into();
    let log_store: S3DynamoDbLogStore = S3DynamoDbLogStore::try_new(
        ensure_table_uri(table.table_uri())?,
        options.clone(),
        &S3_OPTIONS,
        std::sync::Arc::new(table.object_store()),
    )?;

    // create an incomplete log entry, commit file not yet moved from its temporary location
    let entry = create_incomplete_commit_entry(&table, 1, "unfinished_commit").await?;
    let read_entry = CLIENT
        .get_latest_entry(&table.table_uri())
        .await?
        .expect("no latest entry!");
    assert_eq!(entry, read_entry);
    assert_eq!(
        RepairLogEntryResult::MovedFileAndFixedEntry,
        log_store.repair_entry(&read_entry).await?
    );
    // create another incomplete log entry, this time move the temporary file already
    let entry = create_incomplete_commit_entry(&table, 2, "unfinished_commit").await?;
    log_store
        .object_store()
        .rename_if_not_exists(&entry.temp_path, &commit_uri_from_version(entry.version))
        .await?;

    let read_entry = CLIENT
        .get_latest_entry(&table.table_uri())
        .await?
        .expect("no latest entry!");
    assert_eq!(entry, read_entry);
    assert_eq!(
        RepairLogEntryResult::FixedEntry,
        log_store.repair_entry(&read_entry).await?
    );
    validate_lock_table_state(&table, 2).await?;
    assert_eq!(
        RepairLogEntryResult::AlreadyCompleted,
        log_store.repair_entry(&read_entry).await?
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_repair_on_update() -> TestResult<()> {
    let context = IntegrationContext::new(StorageIntegration::Amazon)?;
    let mut table = prepare_table(&context, "repair_on_update").await?;
    let _entry = create_incomplete_commit_entry(&table, 1, "unfinished_commit").await?;
    table.update().await?;
    // table update should find and update to newest, incomplete commit entry
    assert_eq!(table.version(), 1);
    validate_lock_table_state(&table, 1).await?;
    Ok(())
}

const WORKERS: i64 = 3;
const COMMITS: i64 = 5;

#[tokio::test]
#[serial]
async fn test_concurrent_writers() -> TestResult<()> {
    // Goal: a test with multiple writers, very similar to `integration_concurrent_writes`
    let context = IntegrationContext::new(StorageIntegration::Amazon)?;
    let table = prepare_table(&context, "concurrent_writes").await?;
    let table_uri = table.table_uri();

    let mut workers = Vec::new();
    for w in 0..WORKERS {
        workers.push(Worker::new(&table_uri, format!("w{:02}", w)).await);
    }
    let mut futures = Vec::new();
    for mut w in workers {
        let run = tokio::spawn(async move { w.commit_sequence(COMMITS).await });
        futures.push(run)
    }

    let mut map = HashMap::new();
    for f in futures {
        map.extend(f.await?);
    }
    validate_lock_table_state(&table, WORKERS * COMMITS).await?;
    Ok(())
}

pub struct Worker {
    pub table: DeltaTable,
    pub name: String,
}

impl Worker {
    pub async fn new(path: &str, name: String) -> Self {
        let table = DeltaTableBuilder::from_uri(path)
            .with_allow_http(true)
            .with_storage_options(OPTIONS.clone())
            .load()
            .await
            .unwrap();
        Self { table, name }
    }

    async fn commit_sequence(&mut self, n: i64) -> HashMap<i64, String> {
        let mut result = HashMap::new();
        for seq_no in 0..n {
            let (v, name) = self.commit_file(seq_no).await;
            result.insert(v, name);
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        result
    }

    async fn commit_file(&mut self, seq_no: i64) -> (i64, String) {
        let name = format!("{}-{}", self.name, seq_no);
        let metadata = Some(maplit::hashmap! {
            "worker".to_owned() => Value::String(self.name.clone()),
            "current_version".to_owned() => Value::Number( seq_no.into() ),
        });
        let committed_as = append_to_table(&name, &self.table, metadata).await.unwrap();

        self.table.update().await.unwrap();
        (committed_as, name)
    }
}

async fn create_incomplete_commit_entry(
    table: &DeltaTable,
    version: i64,
    tag: &str,
) -> TestResult<CommitEntry> {
    let actions = vec![add_action(tag)];
    let temp_path = prepare_commit(
        table.object_store().as_ref(),
        &DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        },
        &actions,
        None,
    )
    .await?;
    let commit_entry = CommitEntry::new(version, temp_path);
    CLIENT
        .put_commit_entry(&table.table_uri(), &commit_entry)
        .await?;
    Ok(commit_entry)
}

fn add_action(name: &str) -> Action {
    let ts = (SystemTime::now() - Duration::from_secs(1800))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    Action::Add(Add {
        path: format!("{}.parquet", name),
        size: 396,
        partition_values: HashMap::new(),
        partition_values_parsed: None,
        modification_time: ts as i64,
        data_change: true,
        stats: None,
        stats_parsed: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
    })
}

async fn prepare_table(context: &IntegrationContext, table_name: &str) -> TestResult<DeltaTable> {
    CLIENT.try_create_lock_table().await?;
    let table_name = format!("{}_{}", table_name, uuid::Uuid::new_v4());
    let table_uri = context.uri_for_table(TestTables::Custom(table_name.to_owned()));
    let schema = StructType::new(vec![StructField::new(
        "Id".to_string(),
        DataType::Primitive(PrimitiveType::Integer),
        true,
    )]);
    let table = DeltaTableBuilder::from_uri(&table_uri)
        .with_allow_http(true)
        .with_storage_options(OPTIONS.clone())
        .build()?;
    // create delta table
    let table = DeltaOps(table)
        .create()
        .with_columns(schema.fields().clone())
        .await?;
    Ok(table)
}

async fn append_to_table(
    name: &str,
    table: &DeltaTable,
    metadata: Option<HashMap<String, Value>>,
) -> TestResult<i64> {
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };
    let actions = vec![add_action(name)];
    let version = commit(
        table.log_store().as_ref(),
        &actions,
        operation,
        &table.state,
        metadata,
    )
    .await
    .unwrap();
    Ok(version)
}

/// Check dynamodb lock state for consistency. The latest entry must be complete, match the expected
/// version and expiration time is around 24h in the future. Commits should cover a consecutive range
/// of versions, with monotonically non-decreasing expiration timestamps.
async fn validate_lock_table_state(table: &DeltaTable, expected_version: i64) -> TestResult<()> {
    let lock_entry = CLIENT.get_latest_entry(&table.table_uri()).await?.unwrap();
    assert!(lock_entry.complete);
    assert_eq!(lock_entry.version, expected_version);
    assert_eq!(lock_entry.version, table.get_latest_version().await?);

    validate_commit_entry(&lock_entry)?;

    let latest = CLIENT
        .get_latest_entries(&table.table_uri(), WORKERS * COMMITS)
        .await?;
    let max_version = latest.get(0).unwrap().version;
    assert_eq!(max_version, expected_version);

    // Pull out pairs of consecutive commit entries and verify invariants.
    // Sort order in `latest` is reverse chronological, newest to oldest.
    for (prev, next) in latest.iter().skip(1).zip(latest.iter()) {
        assert_eq!(prev.version + 1, next.version);
        assert!(prev.complete);
        assert!(prev.expire_time.is_some());
        assert!(prev.expire_time <= next.expire_time);
        validate_commit_entry(prev)?;
    }
    Ok(())
}

/// Check commit entry for consistency.
/// - `temp_path` must start with `_delta_log`
/// - `expire_time != None` <==> `complete == true`
/// - `expire_time` roughly 24h in the future
fn validate_commit_entry(entry: &CommitEntry) -> TestResult<()> {
    if entry.complete {
        assert!(entry.expire_time.is_some());
        let expire_time = entry.expire_time.unwrap();
        assert!(expire_time <= SystemTime::now() + Duration::from_secs(86400));
        assert!(expire_time >= SystemTime::now() + Duration::from_secs(86300));
    } else {
        assert_eq!(entry.expire_time, None);
    }
    assert!(entry.temp_path.prefix_matches(&Path::from("_delta_log")));
    Ok(())
}
