dynamodb lock
=============

Dynamodb based distributed lock implemented in pure Rust. The design is largely
inspired by
[amazon-dynamodb-lock-client](https://github.com/awslabs/amazon-dynamodb-lock-client).

It is used by the [delta-rs](https://github.com/delta-io/delta-rs) project to
implement PUT if absence semantic for concurrent S3 writes. It is considered
production ready and battle tested through the
[kafka-delta-ingest](https://github.com/delta-io/kafka-delta-ingest) project.

Usage
-----

```rust
let dynamodb_client = rusoto_dynamodb::DynamoDbClient::new(rusoto_core::Region::default());
let lock_client = dynamodb_lock::DynamoDbLockClient::new(
    dynamodb_client,
    dynamodb_lock::DynamoDbOptions::default(),
);

let lock_data = "Moe";
let lock = lock_client.try_acquire_lock(lock_data).await?.unwrap();

if lock.acquired_expired_lock {
    // error handling when acquired an expired lock
}

// do stuff in the critical region

lock_client.release_lock(&lock).await?;
```

For real world example, see
https://github.com/delta-io/delta-rs/blob/main/rust/src/storage/s3/mod.rs.
