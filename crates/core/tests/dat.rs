use std::path::PathBuf;

use deltalake_core::DeltaTableBuilder;
use deltalake_test::{TestResult, acceptance::read_dat_case};
use pretty_assertions::assert_eq;
use rstest::rstest;

static SKIPPED_TESTS: &[&str; 1] = &["iceberg_compat_v1"];

#[rstest]
#[tokio::test]
async fn test_protocol_and_meta(
    #[files("../../dat/v0.0.3/reader_tests/generated/**/test_case_info.json")] path: PathBuf,
) -> TestResult<()> {
    let case_dir = path.parent().expect("parent dir");
    if SKIPPED_TESTS.iter().any(|c| case_dir.ends_with(c)) {
        println!("Skipping test: {case_dir:?}");
        return Ok(());
    }

    let case = read_dat_case(case_dir)?;
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

    Ok(())
}
