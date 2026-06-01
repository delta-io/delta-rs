use std::collections::HashMap;

pub const HF_TOKEN: &str = "hf.token";
pub const HF_ENDPOINT: &str = "hf.endpoint";
pub const HF_REVISION: &str = "hf.revision";

#[derive(Debug, Default)]
pub struct HfStorageConfig {
    pub token: Option<String>,
    pub endpoint: Option<String>,
    pub revision: Option<String>,
}

impl From<&HashMap<String, String>> for HfStorageConfig {
    fn from(map: &HashMap<String, String>) -> Self {
        Self {
            token: map.get(HF_TOKEN).cloned(),
            endpoint: map.get(HF_ENDPOINT).cloned(),
            revision: map.get(HF_REVISION).cloned(),
        }
    }
}
