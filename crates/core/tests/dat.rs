//! DAT (Delta Acceptance Tests) integration tests
//!
//! These tests require the DAT test data to be downloaded first.
//! Run `make setup-dat` to download the test data, then run the tests.
//!
//! Tests are skipped if the dat directory doesn't exist.

use std::path::PathBuf;

use deltalake_core::DeltaTableBuilder;
use deltalake_test::{TestResult, acceptance::read_dat_case};
use pretty_assertions::assert_eq;

static SKIPPED_TESTS: &[&str; 1] = &["iceberg_compat_v1"];

#[tokio::test]
#[ignore = "requires DAT test data - run 'make setup-dat' first"]
async fn test_protocol_and_meta() -> TestResult<()> {
    let dat_path = PathBuf::from("../../dat/v0.0.3/reader_tests/generated");
    if !dat_path.exists() {
        println!("Skipping DAT tests: {dat_path:?} does not exist. Run 'make setup-dat' first.");
        return Ok(());
    }

    for entry in std::fs::read_dir(&dat_path)? {
        let entry = entry?;
        let case_dir = entry.path();
        if !case_dir.is_dir() {
            continue;
        }
        if SKIPPED_TESTS.iter().any(|c| case_dir.ends_with(c)) {
            println!("Skipping test: {case_dir:?}");
            continue;
        }

        let case = read_dat_case(&case_dir)?;
        let table_info = case.table_summary()?;

        let table = DeltaTableBuilder::from_url(case.table_root()?)?
            .load()
            .await?;

        let snapshot = table.snapshot()?;
        let protocol = snapshot.protocol();
        assert_eq!(snapshot.version() as u64, table_info.version);
        assert_eq!(
            (protocol.min_reader_version(), protocol.min_writer_version()),
            (table_info.min_reader_version, table_info.min_writer_version)
        );
    }

    Ok(())
}
