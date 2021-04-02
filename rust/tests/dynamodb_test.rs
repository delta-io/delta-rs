#[cfg(feature = "dynamodb")]
#[macro_use]
extern crate maplit;

#[cfg(feature = "dynamodb")]
mod dynamodb {
    use deltalake::s3::dynamodb_lock::{attr, DynamoDbLockClient, Options, PARTITION_KEY_NAME};
    use rusoto_core::Region;
    use rusoto_dynamodb::*;
    use serial_test::serial;
    use std::time::{SystemTime, UNIX_EPOCH};

    async fn create_dynamo_lock(key: &str, owner: &str) -> DynamoDbLockClient {
        let table_name = "test_table";
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        let client = DynamoDbClient::new(Region::Custom {
            name: "custom".to_string(),
            endpoint: "http://localhost:4566".to_string(),
        });
        let opts = Options {
            partition_key_value: key.to_string(),
            table_name: table_name.to_string(),
            owner_name: owner.to_string(),
            lease_duration: 3,
            sleep_millis: 200,
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
    #[serial]
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
        lock.release_lock(&existing).await.unwrap();

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
}
