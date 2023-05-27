#[cfg(test)]
mod tests {
    use std::time::Duration;
    use super::*;

    use maplit::hashmap;
    use distributed_lock::dynamodb::{dynamo_lock_options, DynamoDbOptions};

    #[test]
    fn lock_options_default_test() {
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_TABLE_NAME, "some_table");
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME, "some_owner");
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE,
            "some_pk",
        );
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_LEASE_DURATION, "40");
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_REFRESH_PERIOD_MILLIS,
            "2000",
        );
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS,
            "3000",
        );

        let options = DynamoDbOptions::default();

        assert_eq!(
            DynamoDbOptions {
                partition_key_value: "some_pk".to_string(),
                table_name: "some_table".to_string(),
                owner_name: "some_owner".to_string(),
                lease_duration: 40,
                refresh_period: Duration::from_millis(2000),
                additional_time_to_wait_for_lock: Duration::from_millis(3000),
            },
            options
        );
    }

    #[test]
    fn lock_options_from_map_test() {
        let options = DynamoDbOptions::from_map(hashmap! {
            dynamo_lock_options::DYNAMO_LOCK_TABLE_NAME.to_string() => "a_table".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME.to_string() => "an_owner".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE.to_string() => "a_pk".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_LEASE_DURATION.to_string() => "60".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_REFRESH_PERIOD_MILLIS.to_string() => "4000".to_string(),
            dynamo_lock_options::DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS.to_string() => "5000".to_string(),
        });

        assert_eq!(
            DynamoDbOptions {
                partition_key_value: "a_pk".to_string(),
                table_name: "a_table".to_string(),
                owner_name: "an_owner".to_string(),
                lease_duration: 60,
                refresh_period: Duration::from_millis(4000),
                additional_time_to_wait_for_lock: Duration::from_millis(5000),
            },
            options
        );
    }

    #[test]
    fn lock_options_mixed_test() {
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_TABLE_NAME, "some_table");
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME, "some_owner");
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE,
            "some_pk",
        );
        std::env::set_var(dynamo_lock_options::DYNAMO_LOCK_LEASE_DURATION, "40");
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_REFRESH_PERIOD_MILLIS,
            "2000",
        );
        std::env::set_var(
            dynamo_lock_options::DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_MILLIS,
            "3000",
        );

        let options = DynamoDbOptions::from_map(hashmap! {
            dynamo_lock_options::DYNAMO_LOCK_PARTITION_KEY_VALUE.to_string() => "overridden_key".to_string()
        });

        assert_eq!(
            DynamoDbOptions {
                partition_key_value: "overridden_key".to_string(),
                table_name: "some_table".to_string(),
                owner_name: "some_owner".to_string(),
                lease_duration: 40,
                refresh_period: Duration::from_millis(2000),
                additional_time_to_wait_for_lock: Duration::from_millis(3000),
            },
            options
        );
    }
}
