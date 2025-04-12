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
