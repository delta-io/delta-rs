use std::path::Path;

use deltalake_core::DeltaTableBuilder;
use deltalake_test::acceptance::read_dat_case;

static SKIPPED_TESTS: &[&str; 4] = &[
    "iceberg_compat_v1",
    "column_mapping",
    "check_constraints",
    "deletion_vectors",
];

fn reader_test_eager(path: &Path) -> datatest_stable::Result<()> {
    let root_dir = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        path.parent().unwrap().to_str().unwrap()
    );
    for skipped in SKIPPED_TESTS {
        if root_dir.ends_with(skipped) {
            println!("Skipping test: {skipped}");
            return Ok(());
        }
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let case = read_dat_case(root_dir).unwrap();

            let table = DeltaTableBuilder::from_uri(case.table_root().unwrap())
                .unwrap()
                .load()
                .await
                .expect("table");
            let table_info = case.table_summary().expect("load summary");
            let snapshot = table.snapshot().expect("Failed to load snapshot");
            let protocol = snapshot.protocol();
            assert_eq!(snapshot.version() as u64, table_info.version);
            assert_eq!(
                (protocol.min_reader_version(), protocol.min_writer_version()),
                (table_info.min_reader_version, table_info.min_writer_version)
            );
        });
    Ok(())
}

datatest_stable::harness! {
    { test = reader_test_eager, root = "../../dat/v0.0.3/reader_tests/generated/", pattern = r"test_case_info\.json" },
}
