#[cfg(feature = "s3")]
mod s3_common;

#[cfg(feature = "dynamodb")]
mod dynamodb {
    use deltalake::storage::s3::dynamodb_lock::{
        attr, DynamoDbLockClient, Options, PARTITION_KEY_NAME,
    };
    use maplit::hashmap;
    use rusoto_dynamodb::*;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    async fn create_dynamo_lock(key: &str, owner: &str) -> DynamoDbLockClient {
        crate::s3_common::setup();
        let table_name = "test_table";
        let client = DynamoDbClient::new(crate::s3_common::region());
        let opts = Options {
            partition_key_value: key.to_string(),
            table_name: table_name.to_string(),
            owner_name: owner.to_string(),
            lease_duration: 3,
            refresh_period: Duration::from_millis(500),
            additional_time_to_wait_for_lock: Duration::from_millis(500),
        };
        let _ = client
            .delete_item(DeleteItemInput {
                key: hashmap! {
                    PARTITION_KEY_NAME.to_string() => attr(key)
                },
                table_name: table_name.to_string(),
                ..Default::default()
            })
            .await;
        DynamoDbLockClient::new(client, opts)
    }

    #[tokio::test]
    async fn test_acquire_get_release_flow() {
        let lock = create_dynamo_lock("test_acquire_get_release_flow", "worker").await;

        let item = lock.acquire_lock().await.unwrap();
        assert_eq!(item.owner_name, "worker");
        assert_eq!(item.lease_duration, 3);
        assert!(item.data.is_none());
        assert_eq!(item.is_released, false);
        assert_eq!(
            item.lookup_time / 1000,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u128
        );

        let mut existing = lock.get_lock().await.unwrap().unwrap();
        //verify that in dynamodb it is the same lock
        assert_eq!(item.record_version_number, existing.record_version_number);
        // although it is the same lock, but lookup time is different
        assert_ne!(item.lookup_time, existing.lookup_time);

        // save some arbitrary data into the lock item
        existing.data = Some("test".to_string());
        assert!(lock.release_lock(&existing).await.unwrap());

        let released = lock.get_lock().await.unwrap().unwrap();
        // the lock from dynamodb should be the same, but with is_released=true field
        assert_eq!(released.is_released, true);
        assert_eq!(
            released.record_version_number,
            existing.record_version_number
        );
        // check that the data above is stored successfully
        assert_eq!(released.data, existing.data);
    }

    #[tokio::test]
    async fn test_acquire_expired_lock() {
        let c1 = create_dynamo_lock("test_acquire_expired_lock", "w1").await;
        let c2 = create_dynamo_lock("test_acquire_expired_lock", "w2").await;

        let l1 = c1.acquire_lock().await.unwrap();

        let now = Instant::now();
        let l2 = c2.acquire_lock().await.unwrap();

        // ensure that we've waiter for more than lease_duration
        assert!(now.elapsed().as_millis() > 3000);

        // cannot release the lock since it's been expired by other client
        assert!(!c1.release_lock(&l1).await.unwrap());

        // the owner successfully expires a lock
        assert!(c2.release_lock(&l2).await.unwrap());

        // check what it is true
        let current = c1.get_lock().await.unwrap().unwrap();
        assert!(current.is_released);
        assert_eq!(current.record_version_number, l2.record_version_number);
    }

    #[tokio::test]
    async fn test_acquire_expired_lock_multiple_workers() {
        let origin = create_dynamo_lock("test_acquire_expired_lock_multiple_workers", "o").await;
        let c1 = create_dynamo_lock("test_acquire_expired_lock_multiple_workers", "w1").await;
        let c2 = create_dynamo_lock("test_acquire_expired_lock_multiple_workers", "w2").await;

        let _ = origin.acquire_lock().await.unwrap();
        let f1 = tokio::spawn(async move { c1.try_acquire_lock().await });
        let f2 = tokio::spawn(async move { c2.try_acquire_lock().await });

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
}
