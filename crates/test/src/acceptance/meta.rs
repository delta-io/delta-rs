use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use delta_kernel::{Error, Version};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum AssertionError {
    #[error("Invalid test case data")]
    InvalidTestCase,

    #[error("Kernel error: {0}")]
    KernelError(#[from] Error),
}

pub type TestResult<T, E = AssertionError> = std::result::Result<T, E>;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct TestCaseInfoJson {
    name: String,
    description: String,
}

#[derive(PartialEq, Eq, Debug)]
pub struct TestCaseInfo {
    name: String,
    description: String,
    root_dir: PathBuf,
}

impl TestCaseInfo {
    /// Root path for this test cases Delta table.
    pub fn table_root(&self) -> TestResult<Url> {
        let table_root = self.root_dir.join("delta");
        Url::from_directory_path(table_root).map_err(|_| AssertionError::InvalidTestCase)
    }

    pub fn root_dir(&self) -> &PathBuf {
        &self.root_dir
    }

    pub fn table_summary(&self) -> TestResult<TableVersionMetaData> {
        let info_path = self
            .root_dir()
            .join("expected/latest/table_version_metadata.json");
        let file = File::open(info_path).map_err(|_| AssertionError::InvalidTestCase)?;
        let info: TableVersionMetaData =
            serde_json::from_reader(file).map_err(|_| AssertionError::InvalidTestCase)?;
        Ok(info)
    }

    pub fn all_table_versions(&self) -> TestResult<impl Iterator<Item = TestResult<TableVersion>>> {
        use std::fs;

        let expected_dir = self.root_dir().join("expected");
        let entries = fs::read_dir(expected_dir).map_err(|_| AssertionError::InvalidTestCase)?;

        let summaries = entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_dir() {
                    let metadata_file = path.join("table_version_metadata.json");
                    if metadata_file.exists() {
                        Some(metadata_file)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .map(|metadata_path| {
                let data_dir = metadata_path.parent().unwrap().join("table_content");
                let file =
                    File::open(metadata_path).map_err(|_| AssertionError::InvalidTestCase)?;
                let meta: TableVersionMetaData =
                    serde_json::from_reader(file).map_err(|_| AssertionError::InvalidTestCase)?;
                Ok(TableVersion { meta, data_dir })
            });

        Ok(summaries)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct TableVersion {
    pub meta: TableVersionMetaData,
    pub data_dir: PathBuf,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct TableVersionMetaData {
    pub version: Version,
    pub properties: HashMap<String, String>,
    pub min_reader_version: i32,
    pub min_writer_version: i32,
}

pub fn read_dat_case(case_root: impl AsRef<Path>) -> TestResult<TestCaseInfo> {
    let info_path = case_root.as_ref().join("test_case_info.json");
    let file = File::open(info_path).map_err(|_| AssertionError::InvalidTestCase)?;
    let info: TestCaseInfoJson =
        serde_json::from_reader(file).map_err(|_| AssertionError::InvalidTestCase)?;
    Ok(TestCaseInfo {
        root_dir: case_root.as_ref().into(),
        name: info.name,
        description: info.description,
    })
}
