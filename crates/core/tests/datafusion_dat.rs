//! DAT (Delta Acceptance Tests) DataFusion integration tests
//!
//! These tests require the DAT test data to be downloaded first.
//! Run `make setup-dat` to download the test data, then run the tests.
//!
//! Tests are skipped if the dat directory doesn't exist.

#![cfg(feature = "datafusion")]
use std::{path::PathBuf, sync::Arc};

use datafusion::{
    catalog::{Session, TableProvider},
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
};
use deltalake_core::{DeltaTableBuilder, delta_datafusion::create_session};
use deltalake_test::acceptance::assert_data_matches;
use deltalake_test::{
    TestResult,
    acceptance::{TableVersion, read_dat_case},
};
use url::Url;

static SKIPPED_TESTS: &[&str; 1] = &["iceberg_compat_v1"];

async fn parquet_provider(
    table_path: &TableVersion,
    state: &dyn Session,
) -> TestResult<Arc<dyn TableProvider>> {
    let table_url = Url::from_directory_path(&table_path.data_dir).unwrap();
    let table_path = ListingTableUrl::parse(table_url)?;
    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");
    let config = ListingTableConfig::new(table_path).with_listing_options(listing_options);
    let config = config.infer_schema(state).await?;
    Ok(Arc::new(ListingTable::try_new(config)?))
}

#[tokio::test]
#[ignore = "requires DAT test data - run 'make setup-dat' first"]
async fn scan_dat() -> TestResult<()> {
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
        let ctx = create_session().into_inner();
        let table = DeltaTableBuilder::from_url(case.table_root()?)?.build()?;

        for version in case.all_table_versions()? {
            let version = version?;

            let pq = parquet_provider(&version, &ctx.state()).await?;
            let schema = pq.schema();
            let columns = schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>();
            let expected = ctx.read_table(pq)?.collect().await?;

            let delta = table
                .table_provider()
                .with_table_version(version.meta.version)
                .await?;
            let actual = ctx
                .read_table(delta)?
                .select_columns(&columns)?
                .collect()
                .await?;

            assert_data_matches(&actual, &expected)?;
        }
    }

    Ok(())
}
