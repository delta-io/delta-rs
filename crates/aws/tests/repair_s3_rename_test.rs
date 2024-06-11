#![cfg(feature = "integration_test")]

use bytes::Bytes;
use deltalake_aws::storage::S3StorageBackend;
use deltalake_core::storage::object_store::{
    DynObjectStore, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartId,
    ObjectMeta, PutOptions, PutResult, Result as ObjectStoreResult,
};
use deltalake_core::{DeltaTableBuilder, ObjectStore, Path};
use deltalake_test::utils::IntegrationContext;
use futures::stream::BoxStream;
use object_store::{MultipartUpload, PutMultipartOpts, PutPayload};
use serial_test::serial;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWrite;
use tokio::task::JoinHandle;
use tokio::time::Duration;

mod common;
use common::*;

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "currently tests are hanging"]
async fn repair_when_worker_pauses_before_rename_test() {
    let err = run_repair_test_case("test_1", true).await.unwrap_err();
    // here worker is paused before copy,
    // so when it wakes up the source file is already copied and deleted
    // leading into NotFound error
    assert!(matches!(err, ObjectStoreError::NotFound { .. }));
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "currently tests are hanging"]
async fn repair_when_worker_pauses_after_rename_test() {
    let err = run_repair_test_case("test_2", false).await.unwrap_err();
    // here worker is paused after copy but before delete,
    // so when it wakes up the delete operation will succeed since the file is already deleted,
    // but it'll fail on releasing a lock, since it's expired
    assert!(format!("{:?}", err).contains("ReleaseLock"));
}

async fn run_repair_test_case(path: &str, pause_copy: bool) -> Result<(), ObjectStoreError> {
    std::env::set_var("AWS_S3_LOCKING_PROVIDER", "dynamodb");
    std::env::set_var("DYNAMO_LOCK_LEASE_DURATION", "2");
    let context = IntegrationContext::new(Box::new(S3Integration::default())).unwrap();

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

    s3_1.put(&src1, Bytes::from("test1").into()).await.unwrap();
    s3_2.put(&src2, Bytes::from("test2").into()).await.unwrap();

    let rename1 = rename(s3_1, &src1, &dst1);
    // to ensure that first one is started actually first
    std::thread::sleep(Duration::from_secs(1));
    let rename2 = rename(s3_2, &src2, &dst2);

    rename2.await.unwrap().unwrap(); // ensure that worker 2 is ok
    resume(&w1_pause); // resume worker 1
    let result = rename1.await.unwrap(); // return the result of worker 1

    let s3 = context.object_store();
    // but first we check that the rename is successful and not overwritten
    async fn get_text(s3: &Arc<DynObjectStore>, path: &Path) -> String {
        std::str::from_utf8(&s3.get(path).await.unwrap().bytes().await.unwrap())
            .unwrap()
            .to_string()
    }

    assert_eq!(get_text(&s3, &dst1).await, "test1");
    assert_eq!(get_text(&s3, &dst2).await, "test2");

    async fn not_exists(s3: &Arc<DynObjectStore>, path: &Path) -> bool {
        matches!(s3.head(path).await, Err(ObjectStoreError::NotFound { .. }))
    }

    assert!(not_exists(&s3, &src1).await);
    assert!(not_exists(&s3, &src2).await);

    result
}

fn rename(
    s3: Arc<S3StorageBackend>,
    src: &Path,
    dst: &Path,
) -> JoinHandle<Result<(), ObjectStoreError>> {
    let lsrc = src.clone();
    let ldst = dst.clone();
    tokio::spawn(async move {
        println!("rename({}, {}) started", &lsrc, &ldst);
        let result = s3.rename_if_not_exists(&lsrc, &ldst).await;
        println!("rename({}, {}) finished", &lsrc, &ldst);
        result
    })
}

fn create_s3_backend(
    context: &IntegrationContext,
    name: &str,
    pause_copy: Option<Path>,
    pause_del: Option<Path>,
) -> (Arc<S3StorageBackend>, Arc<Mutex<bool>>) {
    let pause_until_true = Arc::new(Mutex::new(false));
    let store = DeltaTableBuilder::from_uri(context.root_uri())
        .with_allow_http(true)
        .build_storage()
        .unwrap()
        .object_store();

    let delayed_store = DelayedObjectStore {
        inner: store,
        name: name.to_string(),
        pause_before_copy_path: pause_copy,
        pause_before_delete_path: pause_del,
        pause_until_true: pause_until_true.clone(),
    };

    let backend = S3StorageBackend::try_new(Arc::new(delayed_store), false).unwrap();

    (Arc::new(backend), pause_until_true)
}

#[derive(Debug)]
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
    async fn rename(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        if let Some(ref path) = self.pause_before_copy_path {
            if path == to {
                pause(&self.pause_until_true);
            }
        }
        self.copy(from, to).await?;
        if let Some(ref path) = self.pause_before_delete_path {
            if path == from {
                pause(&self.pause_until_true);
            }
        }
        self.delete(from).await
    }

    async fn put(&self, location: &Path, bytes: PutPayload) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, options).await
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.rename_if_not_exists(from, to).await
    }

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
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
