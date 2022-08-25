use rusoto_core::Region;

pub fn region() -> Region {
    Region::Custom {
        name: "custom".to_string(),
        endpoint: ENDPOINT.to_string(),
    }
}

pub fn setup_dynamodb(key: &str) {
    std::env::set_var("AWS_S3_LOCKING_PROVIDER", "dynamodb");
    std::env::set_var("DYNAMO_LOCK_TABLE_NAME", "test_table");
    std::env::set_var("DYNAMO_LOCK_PARTITION_KEY_VALUE", key);
    std::env::set_var("DYNAMO_LOCK_REFRESH_PERIOD_MILLIS", "100");
    std::env::set_var("DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS", "100");
}
