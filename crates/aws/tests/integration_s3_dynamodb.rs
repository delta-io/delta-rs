//! Integration test to verify correct behavior of S3 DynamoDb locking.
//! It inspects the state of the locking table after each operation.
#![cfg(feature = "integration_test")]

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::types::BillingMode;
use deltalake_aws::logstore::{RepairLogEntryResult, S3DynamoDbLogStore};
use deltalake_aws::storage::S3StorageOptions;
use deltalake_aws::{CommitEntry, DynamoDbConfig, DynamoDbLockClient};
use deltalake_core::kernel::{Action, Add, DataType, PrimitiveType, StructField, StructType};
use deltalake_core::logstore::{CommitOrBytes, LogStore};
use deltalake_core::operations::transaction::CommitBuilder;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::storage::commit_uri_from_version;
use deltalake_core::storage::StorageOptions;
use deltalake_core::table::builder::ensure_table_uri;
use deltalake_core::{DeltaOps, DeltaTable, DeltaTableBuilder, ObjectStoreError};
use deltalake_test::utils::*;
use lazy_static::lazy_static;
use object_store::path::Path;
use serde_json::Value;
use serial_test::serial;

mod common;
use common::*;

pub type TestResult<T> = Result<T, Box<dyn std::error::Error + 'static>>;

lazy_static! {
    static ref OPTIONS: HashMap<String, String> = maplit::hashmap! {
        "allow_http".to_owned() => "true".to_owned(),
    };
    static ref S3_OPTIONS: S3StorageOptions = S3StorageOptions::from_map(&OPTIONS).unwrap();
}

fn make_client() -> TestResult<DynamoDbLockClient> {
    let options: S3StorageOptions = S3StorageOptions::try_default().unwrap();
    Ok(DynamoDbLockClient::try_new(
        &options.sdk_config,
        None,
        None,
        None,
        None,
    )?)
}

#[test]
#[serial]
fn client_configs_via_env_variables() -> TestResult<()> {
    std::env::set_var(
        deltalake_aws::constants::MAX_ELAPSED_REQUEST_TIME_KEY_NAME,
        "64",
    );
    std::env::set_var(
        deltalake_aws::constants::LOCK_TABLE_KEY_NAME,
        "some_table".to_owned(),
    );
    std::env::set_var(
        deltalake_aws::constants::BILLING_MODE_KEY_NAME,
        "PAY_PER_REQUEST".to_owned(),
    );
    let client = make_client()?;
    let config = client.get_dynamodb_config();
    let options: S3StorageOptions = S3StorageOptions::try_default().unwrap();
    assert_eq!(
        DynamoDbConfig {
            billing_mode: BillingMode::PayPerRequest,
            lock_table_name: "some_table".to_owned(),
            max_elapsed_request_time: Duration::from_secs(64),
            sdk_config: options.sdk_config,
        },
        *config,
    );
    std::env::remove_var(deltalake_aws::constants::LOCK_TABLE_KEY_NAME);
    std::env::remove_var(deltalake_aws::constants::MAX_ELAPSED_REQUEST_TIME_KEY_NAME);
    std::env::remove_var(deltalake_aws::constants::BILLING_MODE_KEY_NAME);
    Ok(())
}

#[tokio::test]
#[serial]
async fn get_missing_item() -> TestResult<()> {
    let _context = IntegrationContext::new(Box::new(S3Integration::default()))?;
    let client = make_client()?;
    let version = i64::MAX;
    let result = client
        .get_commit_entry(
            &format!("s3a://my_delta_table_{}", uuid::Uuid::new_v4()),
            version,
        )
        .await;
    assert_eq!(result.unwrap(), None);
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_append() -> TestResult<()> {
    let context = IntegrationContext::new(Box::new(S3Integration::default()))?;
    let table = prepare_table(&context, "delta01").await?;
    validate_lock_table_state(&table, 0).await?;
    append_to_table("datav01.parquet", &table, None).await?;
    validate_lock_table_state(&table, 1).await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_repair_commit_entry() -> TestResult<()> {
    let context = IntegrationContext::new(Box::new(S3Integration::default()))?;
    let client = make_client()?;
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
    let read_entry = client
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

    let read_entry = client
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
    let context = IntegrationContext::new(Box::new(S3Integration::default()))?;
    let mut table = prepare_table(&context, "repair_on_update").await?;
    let _entry = create_incomplete_commit_entry(&table, 1, "unfinished_commit").await?;
    table.update().await?;
    // table update should find and update to newest, incomplete commit entry
    assert_eq!(table.version(), 1);
    validate_lock_table_state(&table, 1).await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_repair_on_load() -> TestResult<()> {
    let context = IntegrationContext::new(Box::new(S3Integration::default()))?;
    let mut table = prepare_table(&context, "repair_on_update").await?;
    let _entry = create_incomplete_commit_entry(&table, 1, "unfinished_commit").await?;
    table.load_version(1).await?;
    // table should fix the broken entry while loading a specific version
    assert_eq!(table.version(), 1);
    validate_lock_table_state(&table, 1).await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_abort_commit_entry() -> TestResult<()> {
    let context = IntegrationContext::new(Box::new(S3Integration::default()))?;
    let client = make_client()?;
    let table = prepare_table(&context, "abort_entry").await?;
    let options: StorageOptions = OPTIONS.clone().into();
    let log_store: S3DynamoDbLogStore = S3DynamoDbLogStore::try_new(
        ensure_table_uri(table.table_uri())?,
        options.clone(),
        &S3_OPTIONS,
        std::sync::Arc::new(table.object_store()),
    )?;

    let entry = create_incomplete_commit_entry(&table, 1, "unfinished_commit").await?;

    log_store
        .abort_commit_entry(
            entry.version,
            CommitOrBytes::TmpCommit(entry.temp_path.clone()),
        )
        .await?;

    // The entry should have been aborted - the latest entry should be one version lower
    if let Some(new_entry) = client.get_latest_entry(&table.table_uri()).await? {
        assert_eq!(entry.version - 1, new_entry.version);
    }
    // Temp commit file should have been deleted
    assert!(matches!(
        log_store.object_store().get(&entry.temp_path).await,
        Err(ObjectStoreError::NotFound { .. })
    ));

    // Test abort commit is idempotent - still works if already aborted
    log_store
        .abort_commit_entry(entry.version, CommitOrBytes::TmpCommit(entry.temp_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_abort_commit_entry_fail_to_delete_entry() -> TestResult<()> {
    // Test abort commit does not delete the temp commit if the DynamoDB entry is not deleted
    let context = IntegrationContext::new(Box::new(S3Integration::default()))?;
    let client = make_client()?;
    let table = prepare_table(&context, "abort_entry_fail").await?;
    let options: StorageOptions = OPTIONS.clone().into();
    let log_store: S3DynamoDbLogStore = S3DynamoDbLogStore::try_new(
        ensure_table_uri(table.table_uri())?,
        options.clone(),
        &S3_OPTIONS,
        std::sync::Arc::new(table.object_store()),
    )?;

    let entry = create_incomplete_commit_entry(&table, 1, "finished_commit").await?;

    // Mark entry as complete
    client
        .update_commit_entry(entry.version, &table.table_uri())
        .await?;

    // Abort will fail since we marked the entry as complete
    assert!(matches!(
        log_store
            .abort_commit_entry(
                entry.version,
                CommitOrBytes::TmpCommit(entry.temp_path.clone())
            )
            .await,
        Err(_),
    ));

    // Check temp commit file still exists
    assert!(log_store.object_store().get(&entry.temp_path).await.is_ok());

    Ok(())
}

const WORKERS: i64 = 3;
const COMMITS: i64 = 15;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_concurrent_writers() -> TestResult<()> {
    // Goal: a test with multiple writers, very similar to `integration_concurrent_writes`
    let context = IntegrationContext::new(Box::new(S3Integration::default()))?;
    println!(">>> preparing table");
    let table = prepare_table(&context, "concurrent_writes").await?;
    println!(">>> table prepared");
    let table_uri = table.table_uri();
    println!("Starting workers on {table_uri}");

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
        println!("Loaded table in worker: {table:?}");
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
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };
    let prepared = CommitBuilder::default()
        .with_actions(actions)
        .build(Some(table.snapshot()?), table.log_store(), operation)
        .into_prepared_commit_future()
        .await?;

    let tmp_commit = match prepared.commit_or_bytes() {
        CommitOrBytes::TmpCommit(tmp_commit) => tmp_commit,
        _ => unreachable!(),
    };

    let commit_entry = CommitEntry::new(version, tmp_commit.to_owned());
    make_client()?
        .put_commit_entry(&table.table_uri(), &commit_entry)
        .await?;
    Ok(commit_entry)
}

fn add_action(name: &str) -> Action {
    let ts = (SystemTime::now() - Duration::from_secs(1800))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    Add {
        path: format!("{}.parquet", name),
        size: 396,
        partition_values: HashMap::new(),
        modification_time: ts as i64,
        data_change: true,
        stats: None,
        stats_parsed: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    }
    .into()
}

async fn prepare_table(context: &IntegrationContext, table_name: &str) -> TestResult<DeltaTable> {
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
    println!("table built: {table:?}");
    // create delta table
    let table = DeltaOps(table)
        .create()
        .with_columns(schema.fields().cloned())
        .await?;
    println!("table created: {table:?}");
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
    let version = CommitBuilder::default()
        .with_actions(actions)
        .with_app_metadata(metadata.unwrap_or_default())
        .build(Some(table.snapshot()?), table.log_store(), operation)
        .await?
        .version();
    Ok(version)
}

/// Check dynamodb lock state for consistency. The latest entry must be complete, match the expected
/// version and expiration time is around 24h in the future. Commits should cover a consecutive range
/// of versions, with monotonically non-decreasing expiration timestamps.
async fn validate_lock_table_state(table: &DeltaTable, expected_version: i64) -> TestResult<()> {
    let client = make_client()?;
    let lock_entry = client.get_latest_entry(&table.table_uri()).await?.unwrap();
    assert!(lock_entry.complete);
    assert_eq!(lock_entry.version, expected_version);
    assert_eq!(lock_entry.version, table.get_latest_version().await?);

    validate_commit_entry(&lock_entry)?;

    let latest = client
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
