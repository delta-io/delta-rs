#[cfg(feature = "s3")]
#[allow(dead_code)]
mod s3_common;

#[cfg(feature = "s3")]
mod dynamodb {
    use deltalake::storage::s3::dynamodb_lock::*;
    use maplit::hashmap;
    use rusoto_dynamodb::*;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    const TABLE: &str = "test_table";

    async fn create_dynamo_lock(key: &str, owner: &str) -> DynamoDbLockClient {
        let opts = Options {
            partition_key_value: key.to_string(),
            table_name: TABLE.to_string(),
            owner_name: owner.to_string(),
            lease_duration: 3,
            refresh_period: Duration::from_millis(500),
            additional_time_to_wait_for_lock: Duration::from_millis(500),
        };
        create_dynamo_lock_with(key, opts).await
    }

    async fn create_dynamo_lock_with(key: &str, opts: Options) -> DynamoDbLockClient {
        crate::s3_common::setup();
        let client = DynamoDbClient::new(crate::s3_common::region());
        let _ = client
            .delete_item(DeleteItemInput {
                key: hashmap! {
                    PARTITION_KEY_NAME.to_string() => AttributeValue {
                        s: Some(key.to_string()),
                        ..Default::default()
                    }
                },
                table_name: TABLE.to_string(),
                ..Default::default()
            })
            .await;
        DynamoDbLockClient::new(client, opts)
    }

    fn attr_val<T: ToString>(s: T) -> AttributeValue {
        AttributeValue {
            s: Some(s.to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_acquire_get_release_flow() {
        let lock = create_dynamo_lock("test_acquire_get_release_flow", "worker").await;

        let data = "data".to_string();
        let item = lock.acquire_lock(Some(&data)).await.unwrap();
        assert_eq!(item.owner_name, "worker");
        assert_eq!(item.lease_duration, Some(3));
        assert_eq!(item.data.as_ref(), Some(&data));
        assert_eq!(item.is_released, false);
        assert_eq!(
            item.lookup_time / 1000,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u128
        );

        let existing = lock.get_lock().await.unwrap().unwrap();
        //verify that in dynamodb it is the same lock
        assert_eq!(item.record_version_number, existing.record_version_number);
        // although it is the same lock, but lookup time is different
        assert_ne!(item.lookup_time, existing.lookup_time);

        // check data data given in acquire function is stored in dynamo
        assert_eq!(existing.data.as_ref(), Some(&data));
        assert!(lock.release_lock(&existing).await.unwrap());

        let released = lock.get_lock().await.unwrap();

        // check that lock is deleted after release
        assert!(released.is_none());
    }

    #[tokio::test]
    async fn test_acquire_expired_lock() {
        let key = "test_acquire_expired_lock";
        let c1 = create_dynamo_lock(key, "w1").await;
        let c2 = create_dynamo_lock(key, "w2").await;

        let l1 = c1.acquire_lock(None).await.unwrap();

        let now = Instant::now();
        let l2 = c2.acquire_lock(None).await.unwrap();

        // ensure that we've waiter for more than lease_duration
        assert!(now.elapsed().as_millis() > 3000);

        // cannot release the lock since it's been expired by other client
        assert!(!c1.release_lock(&l1).await.unwrap());

        // the owner successfully expires a lock
        assert!(c2.release_lock(&l2).await.unwrap());
    }

    #[tokio::test]
    async fn test_acquire_expired_lock_multiple_workers() {
        let key = "test_acquire_expired_lock_multiple_workers";
        let origin = create_dynamo_lock(key, "o").await;
        let c1 = create_dynamo_lock(key, "w1").await;
        let c2 = create_dynamo_lock(key, "w2").await;

        let _ = origin.acquire_lock(None).await.unwrap();
        let f1 = tokio::spawn(async move { c1.try_acquire_lock(None).await });
        let f2 = tokio::spawn(async move { c2.try_acquire_lock(None).await });

        let acquired = match (f1.await.unwrap(), f2.await.unwrap()) {
            (Ok(Some(lock)), Ok(None)) => {
                // first got the lock
                assert_eq!(lock.owner_name, "w1");
                lock
            }
            (Ok(None), Ok(Some(lock))) => {
                // second got the lock
                assert_eq!(lock.owner_name, "w2");
                lock
            }
            (r1, r2) => {
                panic!("{:?}, {:?}", r1, r2)
            }
        };

        let current = origin.get_lock().await.unwrap().unwrap();

        assert_eq!(
            acquired.record_version_number,
            current.record_version_number
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_non_expirable_lock() {
        let key = "test_non_expirable_lock";

        let opts = |owner: &str| Options {
            partition_key_value: key.to_string(),
            table_name: TABLE.to_string(),
            owner_name: owner.to_string(),
            lease_duration: 1,
            refresh_period: Duration::from_millis(100),
            additional_time_to_wait_for_lock: Duration::from_millis(100),
        };

        let w1 = create_dynamo_lock_with(key, opts("w1")).await;
        let w2 = create_dynamo_lock_with(key, opts("w2")).await;

        let dynamodb_client = DynamoDbClient::new(crate::s3_common::region());

        // manual acquire without setting lease_duration so it's considered as non-expirable
        let w1_rnv = "rvn_w1";
        dynamodb_client
            .put_item(PutItemInput {
                table_name: TABLE.to_string(),
                item: hashmap! {
                    PARTITION_KEY_NAME.to_string() => attr_val(key),
                    OWNER_NAME.to_string() => attr_val("w1"),
                    RECORD_VERSION_NUMBER.to_string() => attr_val(w1_rnv),
                },
                ..Default::default()
            })
            .await
            .unwrap();

        let current = w1.get_lock().await.unwrap().unwrap();
        assert_eq!(current.lease_duration, None);

        // start w2 to try to acquire the lock
        let acquire_lock = tokio::spawn(async move { w2.acquire_lock(None).await });

        tokio::time::sleep(Duration::from_secs(2)).await;

        let current = w1.get_lock().await.unwrap().unwrap();
        // verify that event after 2 sec (2x than lease_duration) that the non expirable lock
        // is still in dynamo and not expired
        assert_eq!(current.owner_name, "w1");
        assert_eq!(current.record_version_number, w1_rnv);

        // release w1, so w2 will acquire it
        assert!(w1.release_lock(&current).await.unwrap());
        let lock_w2 = acquire_lock.await.unwrap().unwrap();

        // check that it's actually another/new lock in dynamo
        let current = w1.get_lock().await.unwrap().unwrap();
        assert_ne!(w1_rnv, lock_w2.record_version_number);
        assert_eq!(current.record_version_number, lock_w2.record_version_number);
    }
}
