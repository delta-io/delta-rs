use rusoto_core::Region;
use rusoto_s3::{DeleteObjectRequest, ListObjectsV2Request, S3Client, S3};

pub const ENDPOINT: &str = "http://localhost:4566";

pub fn setup() {
    std::env::set_var("AWS_REGION", "us-east-2");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_ENDPOINT_URL", ENDPOINT);
}

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

pub async fn cleanup_dir_except(path: &str, ignore_files: Vec<String>) {
    setup();
    let client = S3Client::new(region());
    let dir = deltalake::parse_uri(path).unwrap().into_s3object().unwrap();

    for obj in list_objects(&client, dir.bucket, dir.key).await {
        let name = obj.split("/").last().unwrap().to_string();
        if !ignore_files.contains(&name) && !name.starts_with(".") {
            let req = DeleteObjectRequest {
                bucket: dir.bucket.to_string(),
                key: obj,
                ..Default::default()
            };
            client.delete_object(req).await.unwrap();
        }
    }
}

async fn list_objects(client: &S3Client, bucket: &str, prefix: &str) -> Vec<String> {
    let mut list = Vec::new();
    let result = client
        .list_objects_v2(ListObjectsV2Request {
            bucket: bucket.to_string(),
            prefix: Some(prefix.to_string()),
            ..Default::default()
        })
        .await
        .unwrap();

    if let Some(contents) = result.contents {
        for obj in contents {
            list.push(obj.key.unwrap());
        }
    }

    list
}
