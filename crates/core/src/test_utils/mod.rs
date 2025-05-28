mod factories;

use std::{collections::HashMap, path::PathBuf, process::Command};

use url::Url;

pub use self::factories::*;
use crate::DeltaTableBuilder;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + 'static>>;

/// Reference tables from the test data folder
pub enum TestTables {
    Simple,
    SimpleWithCheckpoint,
    SimpleCommit,
    Golden,
    Delta0_8_0Partitioned,
    Delta0_8_0SpecialPartitioned,
    Checkpoints,
    LatestNotCheckpointed,
    WithDvSmall,
    Custom(String),
}

impl TestTables {
    pub fn as_path(&self) -> PathBuf {
        let data_path = find_git_root().join("crates/test/tests/data");
        match self {
            Self::Simple => data_path.join("simple_table"),
            Self::SimpleWithCheckpoint => data_path.join("simple_table_with_checkpoint"),
            Self::SimpleCommit => data_path.join("simple_commit"),
            Self::Golden => data_path.join("golden/data-reader-array-primitives"),
            Self::Delta0_8_0Partitioned => data_path.join("delta-0.8.0-partitioned"),
            Self::Delta0_8_0SpecialPartitioned => data_path.join("delta-0.8.0-special-partition"),
            Self::Checkpoints => data_path.join("checkpoints"),
            Self::LatestNotCheckpointed => data_path.join("latest_not_checkpointed"),
            Self::WithDvSmall => data_path.join("table-with-dv-small"),
            // the data path for upload does not apply to custom tables.
            Self::Custom(_) => todo!(),
        }
    }

    pub fn as_name(&self) -> String {
        match self {
            Self::Simple => "simple".into(),
            Self::SimpleWithCheckpoint => "simple_table_with_checkpoint".into(),
            Self::SimpleCommit => "simple_commit".into(),
            Self::Golden => "golden".into(),
            Self::Delta0_8_0Partitioned => "delta-0.8.0-partitioned".into(),
            Self::Delta0_8_0SpecialPartitioned => "delta-0.8.0-special-partition".into(),
            Self::Checkpoints => "checkpoints".into(),
            Self::LatestNotCheckpointed => "latest_not_checkpointed".into(),
            Self::WithDvSmall => "table-with-dv-small".into(),
            Self::Custom(name) => name.to_owned(),
        }
    }

    pub fn uri_for_table(&self, root_uri: impl AsRef<str>) -> String {
        let root_uri = root_uri.as_ref();
        if root_uri.ends_with('/') {
            format!("{}{}", root_uri, self.as_name())
        } else {
            format!("{}/{}", root_uri, self.as_name())
        }
    }

    pub fn table_builder(&self) -> DeltaTableBuilder {
        let url = Url::from_directory_path(self.as_path()).unwrap();
        DeltaTableBuilder::from_uri(url).with_allow_http(true)
    }
}

fn find_git_root() -> PathBuf {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .output()
        .unwrap();
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
}

/// Test fixture that allows setting environment variables for the duration of a test.
///
/// Existing environment variables that are overwritten will be restored when the fixture is dropped.
pub fn with_env(vars: Vec<(&str, &str)>) -> impl Drop {
    // Store the original values before modifying
    let original_values: HashMap<String, Option<String>> = vars
        .iter()
        .map(|(key, _)| (key.to_string(), std::env::var(key).ok()))
        .collect();

    // Set all the new environment variables
    for (key, value) in vars {
        std::env::set_var(key, value);
    }

    // Create a cleanup struct that will restore original values when dropped
    struct EnvCleanup(HashMap<String, Option<String>>);

    impl Drop for EnvCleanup {
        fn drop(&mut self) {
            for (key, maybe_value) in self.0.iter() {
                match maybe_value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    EnvCleanup(original_values)
}

#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let mut expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let formatted = arrow::util::pretty::pretty_format_batches($CHUNKS)
            .unwrap()
            .to_string();
        // fix for windows: \r\n -->

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{expected_lines:#?}\nactual:\n\n{actual_lines:#?}\n\n",
        );
    };
}

pub use assert_batches_sorted_eq;

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    fn test_api_with_env() {
        let _env = with_env(vec![
            ("API_KEY", "test_key"),
            ("API_URL", "http://test.example.com"),
        ]);

        // Test code using these environment variables
        assert_eq!(env::var("API_KEY").unwrap(), "test_key");
        assert_eq!(env::var("API_URL").unwrap(), "http://test.example.com");

        drop(_env);

        assert!(env::var("API_KEY").is_err());
        assert!(env::var("API_URL").is_err());
    }
}
