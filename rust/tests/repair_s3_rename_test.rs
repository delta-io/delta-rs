#[cfg(feature = "s3")]
#[allow(dead_code)]
mod s3_common;

#[cfg(feature = "s3")]
mod s3 {

    use crate::s3_common;
    use deltalake::storage::s3::{dynamodb_lock, S3StorageBackend};
    use deltalake::{StorageBackend, StorageError};
    use rusoto_core::credential::ChainProvider;
    use rusoto_core::request::DispatchSignedRequestFuture;
    use rusoto_core::signature::SignedRequest;
    use rusoto_core::{DispatchSignedRequest, HttpClient};
    use rusoto_s3::S3Client;
    use serial_test::serial;
    use std::sync::{Arc, Mutex};
    use tokio::task::JoinHandle;
    use tokio::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn repair_when_worker_pauses_before_rename_test() {
        let err = run_repair_test_case("s3://deltars/repair_test_1", true)
            .await
            .unwrap_err();
        // here worker is paused before copy,
        // so when it wakes up the source file is already copied and deleted
        // leading into NotFound error
        assert_eq!(format!("{:?}", err), "NotFound");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn repair_when_worker_pauses_after_rename_test() {
        let err = run_repair_test_case("s3://deltars/repair_test_2", false)
            .await
            .unwrap_err();
        // here worker is paused after copy but before delete,
        // so when it wakes up the delete operation will succeed since the file is already deleted,
        // but it'll fail on releasing a lock, since it's expired
        assert_eq!(format!("{:?}", err), "S3Generic(\"Lock is not released\")");
    }

    async fn run_repair_test_case(path: &str, pause_copy: bool) -> Result<(), StorageError> {
        std::env::set_var("DYNAMO_LOCK_LEASE_DURATION", "2");
        s3_common::setup_dynamodb(path);
        s3_common::cleanup_dir_except(path, Vec::new()).await;

        let src1 = format!("{}/src1", path);
        let dst1 = format!("{}/dst1", path);

        let src2 = format!("{}/src2", path);
        let dst2 = format!("{}/dst2", path);

        let (s3_1, w1_pause) = {
            let copy = if pause_copy { Some(dst1.clone()) } else { None };
            let del = if pause_copy { None } else { Some(src1.clone()) };
            create_s3_backend("w1", copy, del)
        };
        let (s3_2, _) = create_s3_backend("w2", None, None);

        s3_1.put_obj(&src1, b"test1").await.unwrap();
        s3_2.put_obj(&src2, b"test2").await.unwrap();

        let rename1 = rename(s3_1, src1.clone(), dst1.clone());
        // to ensure that first one is started actually first
        std::thread::sleep(Duration::from_secs(1));
        let rename2 = rename(s3_2, src2.clone(), dst2.clone());

        rename2.await.unwrap().unwrap(); // ensure that worker 2 is ok
        resume(&w1_pause); // resume worker 1
        let result = rename1.await.unwrap(); // return the result of worker 1

        let s3 = S3StorageBackend::new().unwrap();
        // but first we check that the rename is successful and not overwritten
        async fn get_text(s3: &S3StorageBackend, path: &str) -> String {
            std::str::from_utf8(&s3.get_obj(path).await.unwrap())
                .unwrap()
                .to_string()
        }

        assert_eq!(get_text(&s3, &dst1).await, "test1");
        assert_eq!(get_text(&s3, &dst2).await, "test2");

        async fn not_exists(s3: &S3StorageBackend, path: &str) -> bool {
            if let Err(StorageError::NotFound) = s3.head_obj(path).await {
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
        src: String,
        dst: String,
    ) -> JoinHandle<Result<(), StorageError>> {
        tokio::spawn(async move {
            println!("rename({}, {}) started", &src, &dst);
            let result = s3.rename_obj(&src, &dst).await;
            println!("rename({}, {}) finished", &src, &dst);
            result
        })
    }

    fn create_s3_backend(
        name: &str,
        pause_copy: Option<String>,
        pause_del: Option<String>,
    ) -> (S3StorageBackend, Arc<Mutex<bool>>) {
        let pause_until_true = Arc::new(Mutex::new(false));
        let dispatcher = InterceptingDispatcher {
            client: HttpClient::new().unwrap(),
            name: name.to_string(),
            // lazy way to remove "s3:/" part
            pause_before_copy_path: pause_copy.map(|x| x[4..].to_string()),
            pause_before_delete_path: pause_del.map(|x| x[4..].to_string()),
            pause_until_true: pause_until_true.clone(),
        };

        let client = S3Client::new_with(dispatcher, ChainProvider::new(), s3_common::region());
        let lock_client = dynamodb_lock::DynamoDbLockClient::new(
            rusoto_dynamodb::DynamoDbClient::new(s3_common::region()),
            dynamodb_lock::Options::default(),
        );

        (
            S3StorageBackend::new_with(client, Some(Box::new(lock_client))),
            pause_until_true,
        )
    }

    struct InterceptingDispatcher {
        client: HttpClient,
        name: String,
        pause_before_copy_path: Option<String>,
        pause_before_delete_path: Option<String>,
        pause_until_true: Arc<Mutex<bool>>,
    }

    impl DispatchSignedRequest for InterceptingDispatcher {
        fn dispatch(
            &self,
            request: SignedRequest,
            timeout: Option<Duration>,
        ) -> DispatchSignedRequestFuture {
            if let Some(ref path) = self.pause_before_copy_path {
                if request.method == "PUT" && &request.path == path {
                    pause(&self.pause_until_true);
                }
            }

            if let Some(ref path) = self.pause_before_delete_path {
                if request.method == "DELETE" && &request.path == path {
                    pause(&self.pause_until_true);
                }
            }

            println!(
                "REQUEST[{}]: {} {}",
                &self.name, &request.method, &request.path
            );

            self.client.dispatch(request, timeout)
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
}
