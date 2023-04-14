use super::TestContext;
use std::collections::HashMap;

pub struct Hdfs {
    name_node: String,
}

pub fn setup_hdfs_context() -> TestContext {
    let mut config = HashMap::new();

    let name_node = "hdfs://localhost:9000".to_owned();

    config.insert("URI".to_owned(), name_node.clone());

    TestContext {
        storage_context: Some(Box::new(Hdfs { name_node })),
        config,
        ..TestContext::default()
    }
}
