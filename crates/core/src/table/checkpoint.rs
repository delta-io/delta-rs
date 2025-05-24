//! Checkpoint related items for {DeltaTable]
//!
//! Note: this code may disappear in the future once the full replay via kernel is realized

use serde::{Deserialize, Serialize};

/// Metadata for a checkpoint file
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct CheckPoint {
    /// Delta table version
    pub(crate) version: i64, // 20 digits decimals
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The number of fragments if the last checkpoint was written in multiple parts. This field is optional.
    pub(crate) parts: Option<u32>, // 10 digits decimals
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The number of bytes of the checkpoint. This field is optional.
    pub(crate) size_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The number of AddFile actions in the checkpoint. This field is optional.
    pub(crate) num_of_add_files: Option<i64>,
}

impl CheckPoint {
    /// Creates a new checkpoint from the given parameters.
    pub fn new(version: i64, size: i64, parts: Option<u32>) -> Self {
        Self {
            version,
            size,
            parts: parts.or(None),
            size_in_bytes: None,
            num_of_add_files: None,
        }
    }
}

impl PartialEq for CheckPoint {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
    }
}

impl Eq for CheckPoint {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_should_serialize_in_camel_case() {
        let checkpoint = CheckPoint {
            version: 1,
            size: 1,
            parts: None,
            size_in_bytes: Some(1),
            num_of_add_files: Some(1),
        };

        let checkpoint_json_serialized =
            serde_json::to_string(&checkpoint).expect("could not serialize to json");

        assert!(checkpoint_json_serialized.contains("sizeInBytes"));
        assert!(checkpoint_json_serialized.contains("numOfAddFiles"));
    }
}
