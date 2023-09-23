#![cfg(feature = "integration_test")]

use deltalake::operations::transaction::commit;
use deltalake::operations::DeltaOps;
use deltalake::protocol::{Action, Add, DeltaOperation, SaveMode};
use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use deltalake::{DeltaTable, DeltaTableBuilder, Schema, SchemaDataType, SchemaField};
use std::collections::HashMap;
use std::future::Future;
use std::iter::FromIterator;
use std::time::Duration;

#[tokio::test]
async fn test_concurrent_writes_local() -> TestResult {
    test_concurrent_writes(StorageIntegration::Local).await?;
    Ok(())
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn concurrent_writes_s3() -> TestResult {
    test_concurrent_writes(StorageIntegration::Amazon).await?;
    Ok(())
}

#[cfg(feature = "azure")]
#[tokio::test]
async fn test_concurrent_writes_azure() -> TestResult {
    test_concurrent_writes(StorageIntegration::Microsoft).await?;
    Ok(())
}

// tracked via https://github.com/datafusion-contrib/datafusion-objectstore-hdfs/issues/13
#[ignore]
#[cfg(feature = "hdfs")]
#[tokio::test]
async fn test_concurrent_writes_hdfs() -> TestResult {
    test_concurrent_writes(StorageIntegration::Hdfs).await?;
    Ok(())
}

async fn test_concurrent_writes(integration: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new(integration)?;
    let (_table, table_uri) = prepare_table(&context).await?;
    run_test(|name| Worker::new(&table_uri, name)).await;
    Ok(())
}

async fn prepare_table(
    context: &IntegrationContext,
) -> Result<(DeltaTable, String), Box<dyn std::error::Error + 'static>> {
    let schema = Schema::new(vec![SchemaField::new(
        "Id".to_string(),
        SchemaDataType::primitive("integer".to_string()),
        true,
        HashMap::new(),
    )]);

    let table_uri = context.uri_for_table(TestTables::Custom("concurrent_workers".into()));

    let table = DeltaTableBuilder::from_uri(&table_uri)
        .with_allow_http(true)
        .build()?;

    let table = DeltaOps(table)
        .create()
        .with_columns(schema.get_fields().clone())
        .await?;

    assert_eq!(0, table.version());
    assert_eq!(1, table.get_min_reader_version());
    assert_eq!(1, table.get_min_writer_version());
    assert_eq!(0, table.get_files().len());

    Ok((table, table_uri))
}

const WORKERS: i64 = 5;
const COMMITS: i64 = 3;

async fn run_test<F, Fut>(create_worker: F)
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Worker>,
{
    let mut workers = Vec::new();
    for w in 0..WORKERS {
        workers.push(create_worker(format!("w{}", w)).await);
    }

    let mut futures = Vec::new();
    for mut w in workers {
        let run = tokio::spawn(async move { w.commit_sequence(COMMITS).await });
        futures.push(run)
    }

    let mut map = HashMap::new();
    for f in futures {
        map.extend(f.await.unwrap());
    }

    // to ensure that there's been no collisions between workers of acquiring the same version
    assert_eq!(map.len() as i64, WORKERS * COMMITS);

    // check that we have unique and ascending versions committed
    let mut versions = Vec::from_iter(map.keys().copied());
    versions.sort();
    assert_eq!(versions, Vec::from_iter(1i64..=WORKERS * COMMITS));

    // check that each file for each worker is committed as expected
    let mut files = Vec::from_iter(map.values().cloned());
    files.sort();
    let mut expected = Vec::new();
    for w in 0..WORKERS {
        for c in 0..COMMITS {
            expected.push(format!("w{}-{}", w, c))
        }
    }
    assert_eq!(files, expected);
}

pub struct Worker {
    pub table: DeltaTable,
    pub name: String,
}

impl Worker {
    pub async fn new(path: &str, name: String) -> Self {
        std::env::set_var("DYNAMO_LOCK_OWNER_NAME", &name);
        let table = DeltaTableBuilder::from_uri(path)
            .with_allow_http(true)
            .load()
            .await
            .unwrap();
        Self { table, name }
    }

    async fn commit_sequence(&mut self, n: i64) -> HashMap<i64, String> {
        let mut result = HashMap::new();
        for i in 0..n {
            let name = format!("{}-{}", self.name, i);
            let v = self.commit_file(&name).await;
            result.insert(v, name);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        result
    }

    async fn commit_file(&mut self, name: &str) -> i64 {
        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };
        let actions = vec![Action::add(Add {
            path: format!("{}.parquet", name),
            size: 396,
            partition_values: HashMap::new(),
            partition_values_parsed: None,
            modification_time: 1564524294000,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
            deletion_vector: None,
        })];
        let version = commit(
            self.table.object_store().as_ref(),
            &actions,
            operation,
            &self.table.state,
            None,
        )
        .await
        .unwrap();
        self.table.update().await.unwrap();
        version
    }
}
