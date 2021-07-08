use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};

#[tokio::test]
async fn lambda_checkpoint_smoke_test() {
    std::env::set_var("AWS_REGION", "us-east-2");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");

    // CI sets the endpoint URL differently.
    // Set to localhost if not present.
    if let Err(_) = std::env::var("AWS_ENDPOINT_URL") {
        std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
    }
    let region = Region::Custom {
        name: "custom".to_string(),
        endpoint: std::env::var("AWS_ENDPOINT_URL").unwrap(),
    };
    let client = S3Client::new(region);

    // copy local json files to S3
    for n in 0i32..11i32 {
        let padded = format!("{:0>20}", n);
        let local = format!(
            "../../rust/tests/data/checkpoints/_delta_log/{}.json",
            padded
        );
        let remote = format!("checkpoint-test/_delta_log/{}.json", padded);
        let content = std::fs::read_to_string(local).unwrap();
        let request = PutObjectRequest {
            bucket: "delta-checkpoint".to_string(),
            key: remote,
            body: Some(content.into_bytes().into()),
            ..Default::default()
        };
        let _ = client.put_object(request).await.unwrap();
    }

    // wait a bit for the lambda to trigger and do its thing
    // (GitHub action takes a while)
    std::thread::sleep(std::time::Duration::from_secs(40));

    // verify the checkpoint was created
    let request = GetObjectRequest {
        bucket: "delta-checkpoint".to_string(),
        key: "checkpoint-test/_delta_log/00000000000000000010.checkpoint.parquet".to_string(),
        ..Default::default()
    };
    let result = client.get_object(request).await;

    assert!(result.is_ok());

    // verify the _last_checkpoint file was created
    let request = GetObjectRequest {
        bucket: "delta-checkpoint".to_string(),
        key: "checkpoint-test/_delta_log/_last_checkpoint".to_string(),
        ..Default::default()
    };
    let result = client.get_object(request).await;

    assert!(result.is_ok());
}
