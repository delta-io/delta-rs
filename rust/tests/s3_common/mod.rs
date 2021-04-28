use rusoto_core::Region;
use rusoto_s3::{Delete, DeleteObjectsRequest, ObjectIdentifier, S3Client, S3};

pub const ENDPOINT: &str = "http://localhost:4566";

#[allow(dead_code)]
pub fn setup() {
    std::env::set_var("AWS_REGION", "us-east-2");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_ENDPOINT_URL", ENDPOINT);
}

#[allow(dead_code)]
pub fn region() -> Region {
    Region::Custom {
        name: "custom".to_string(),
        endpoint: ENDPOINT.to_string(),
    }
}

#[allow(dead_code)]
pub async fn delete_objects<S: Into<String>>(paths: Vec<S>) {
    crate::s3_common::setup();
    let client = S3Client::new(crate::s3_common::region());

    let mut objects = Vec::new();
    for key in paths {
        objects.push(ObjectIdentifier {
            key: key.into(),
            ..Default::default()
        })
    }
    let req = DeleteObjectsRequest {
        bucket: "deltars".to_string(),
        delete: Delete {
            objects,
            ..Default::default()
        },
        ..Default::default()
    };

    client.delete_objects(req).await.unwrap();
}
