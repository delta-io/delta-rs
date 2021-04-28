#[cfg(feature = "s3")]
mod s3_common;

use deltalake::action;
use deltalake::DeltaTable;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

#[tokio::test]
#[cfg(all(feature = "s3", feature = "dynamodb"))]
async fn concurrent_writes_s3() {
    s3_helpers::cleanup().await;
    run_test(|name| s3_helpers::create_worker("s3://deltars/concurrent_workers", name)).await;
}

#[tokio::test]
async fn concurrent_writes_fs() {
    fs_helpers::cleanup();
    run_test(|name| s3_helpers::create_worker("./tests/data/concurrent_workers", name)).await;
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
        let p = f.await.unwrap();
        map.extend(p);
    }

    // to ensure that there's been no collisions between workers of acquiring the same version
    assert_eq!(map.len() as i64, WORKERS * COMMITS);

    println!("{:?}", map);
}

pub struct Worker {
    pub table: DeltaTable,
    pub name: String,
}

impl Worker {
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
        let actions = [action::Action::add(action::Add {
            path: format!("{}.parquet", name),
            size: 396,
            partitionValues: HashMap::new(),
            partitionValues_parsed: None,
            modificationTime: 1564524294000,
            dataChange: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        })];
        let mut tx = self.table.create_transaction(None);
        tx.commit_with(&actions, None).await.unwrap()
    }
}

#[cfg(all(feature = "s3", feature = "dynamodb"))]
mod s3_helpers {
    use crate::Worker;

    pub async fn create_worker(path: &str, name: String) -> Worker {
        std::env::set_var("DYNAMO_LOCK_OWNER_NAME", &name);
        let table = deltalake::open_table(path).await.unwrap();
        Worker { table, name }
    }

    pub async fn cleanup() {
        crate::s3_common::setup();
        std::env::set_var("AWS_S3_LOCKING_PROVIDER", "dynamodb");
        std::env::set_var("DYNAMO_LOCK_TABLE_NAME", "test_table");
        std::env::set_var("DYNAMO_LOCK_PARTITION_KEY_VALUE", "s3_multi_test");
        //crate::s3_common::delete_objects(Vec::new()).await;
    }
}

mod fs_helpers {
    pub fn cleanup() {}
}
