#![cfg(all(feature = "s3", feature = "integration_test"))]
use crate::s3_common;
use bytes::Bytes;
use deltalake::storage::s3::{S3StorageBackend, S3StorageOptions};
use deltalake::test_utils::{IntegrationContext, StorageIntegration, TestTables};
use deltalake::{DeltaTableBuilder, ObjectStore};
use object_store::path::Path;
use object_store::ObjectStore;
use object_store::{
    DynObjectStore, Error as ObjectStoreError, MultipartId, Result as ObjectStoreResult,
};
use serial_test::serial;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Duration;

#[allow(dead_code)]
mod s3_common;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn repair_when_worker_pauses_before_rename_test() {
    let err = run_repair_test_case("test_1", true).await.unwrap_err();
    // here worker is paused before copy,
    // so when it wakes up the source file is already copied and deleted
    // leading into NotFound error
    assert_eq!(format!("{:?}", err), "NotFound");
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn repair_when_worker_pauses_after_rename_test() {
    let err = run_repair_test_case("test_2", false).await.unwrap_err();
    // here worker is paused after copy but before delete,
    // so when it wakes up the delete operation will succeed since the file is already deleted,
    // but it'll fail on releasing a lock, since it's expired
    assert_eq!(format!("{:?}", err), "S3Generic(\"Lock is not released\")");
}

async fn run_repair_test_case(path: &str, pause_copy: bool) -> Result<(), ObjectStoreError> {
    let context = IntegrationContext::new(StorageIntegration::Amazon).unwrap();

    std::env::set_var("DYNAMO_LOCK_LEASE_DURATION", "2");

    let root_path = Path::from(path);
    let src1 = root_path.child("src1");
    let dst1 = root_path.child("dst1");

    let src2 = root_path.child("src2");
    let dst2 = root_path.child("dst2");

    let (s3_1, w1_pause) = {
        let copy = if pause_copy { Some(dst1.clone()) } else { None };
        let del = if pause_copy { None } else { Some(src1.clone()) };
        create_s3_backend(&context, "w1", copy, del)
    };
    let (s3_2, _) = create_s3_backend(&context, "w2", None, None);

    s3_1.put(&src1, Bytes::from("test1")).await.unwrap();
    s3_2.put(&src2, Bytes::from("test2")).await.unwrap();

    let rename1 = rename(s3_1, &src1, &dst1);
    // to ensure that first one is started actually first
    std::thread::sleep(Duration::from_secs(1));
    let rename2 = rename(s3_2, &src2, &dst2);

    rename2.await.unwrap().unwrap(); // ensure that worker 2 is ok
    resume(&w1_pause); // resume worker 1
    let result = rename1.await.unwrap(); // return the result of worker 1

    let s3 = context.object_store();
    // but first we check that the rename is successful and not overwritten
    async fn get_text(s3: &S3StorageBackend, path: &Path) -> String {
        std::str::from_utf8(&s3.get(path).await.unwrap().bytes().await.unwrap())
            .unwrap()
            .to_string()
    }

    assert_eq!(get_text(&s3, &dst1).await, "test1");
    assert_eq!(get_text(&s3, &dst2).await, "test2");

    async fn not_exists(s3: &S3StorageBackend, path: &Path) -> bool {
        if let Err(ObjectStoreError::NotFound { .. }) = s3.head(path).await {
            true
        } else {
            false
        }
    }

    assert!(not_exists(&s3, &src1).await);
    assert!(not_exists(&s3, &src2).await);

    result
}

fn rename(
    s3: S3StorageBackend,
    src: &Path,
    dst: &Path,
) -> JoinHandle<Result<(), ObjectStoreError>> {
    tokio::spawn(async move {
        println!("rename({}, {}) started", src, dst);
        let result = s3.rename_if_not_exists(src, dst).await;
        println!("rename({}, {}) finished", src, dst);
        result
    })
}

fn create_s3_backend(
    context: &IntegrationContext,
    name: &str,
    pause_copy: Option<String>,
    pause_del: Option<String>,
) -> (DelayedObjectStore, Arc<Mutex<bool>>) {
    let pause_until_true = Arc::new(Mutex::new(false));
    let store = DeltaTableBuilder::from_uri(&context.root_uri())
        .build_storage()
        .unwrap()
        .storage_backend();

    // let lock_client = dynamodb_lock::DynamoDbLockClient::new(
    //     rusoto_dynamodb::DynamoDbClient::new(s3_common::region()),
    //     dynamodb_lock::DynamoDbOptions::default(),
    // );

    let delayed_store = DelayedObjectStore {
        inner: store,
        name: name.to_string(),
        pause_before_copy_path: pause_copy,
        pause_before_delete_path: pause_del,
        pause_until_true,
    };

    (delayed_store, pause_until_true)
}

struct DelayedObjectStore {
    inner: Arc<DynObjectStore>,
    name: String,
    pause_before_copy_path: Option<Path>,
    pause_before_delete_path: Option<Path>,
    pause_until_true: Arc<Mutex<bool>>,
}

impl std::fmt::Display for DelayedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DelayedObjectStore({})", self.name)
    }
}

#[async_trait::async_trait]
impl ObjectStore for DelayedObjectStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        if let Some(ref path) = self.pause_before_copy_path {
            if location == path {
                pause(&self.pause_until_true);
            }
        }
        self.inner.put(location, bytes).await
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        if let Some(ref path) = self.pause_before_delete_path {
            if location == path {
                pause(&self.pause_until_true);
            }
        }
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let lock_client = match self.s3_lock_client {
            Some(ref lock_client) => lock_client,
            None => return Err(S3LockError::LockClientRequired.into()),
        };
        lock_client.rename_with_lock(self, from, to).await?;
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }
}

fn pause(pause_until_true: &Mutex<bool>) {
    println!("Simulating client unexpected pause.");
    let mut retries = 0;
    loop {
        retries += 1;
        let resume = {
            let value = pause_until_true.lock().unwrap();
            *value
        };
        if !resume {
            std::thread::sleep(Duration::from_millis(200));
        } else if !resume && retries > 60 {
            println!("Paused for more than 1 min, most likely an error");
            return;
        } else {
            println!("Waking up and continue to work");
            return;
        }
    }
}

fn resume(pause_until_true: &Mutex<bool>) {
    let mut value = pause_until_true.lock().unwrap();
    *value = true;
}
