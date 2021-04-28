use rusoto_core::Region;

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
