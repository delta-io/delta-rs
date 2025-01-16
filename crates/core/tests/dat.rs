use std::path::Path;
use std::sync::Arc;

use delta_kernel::Table;
use deltalake_core::kernel::snapshot_next::{LazySnapshot, Snapshot};
use deltalake_test::acceptance::read_dat_case;

static SKIPPED_TESTS: &[&str; 1] = &["iceberg_compat_v1"];

fn reader_test_lazy(path: &Path) -> datatest_stable::Result<()> {
    let root_dir = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        path.parent().unwrap().to_str().unwrap()
    );
    for skipped in SKIPPED_TESTS {
        if root_dir.ends_with(skipped) {
            println!("Skipping test: {}", skipped);
            return Ok(());
        }
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let case = read_dat_case(root_dir).unwrap();

            let table = Table::try_from_uri(case.table_root().unwrap()).expect("table");
            let snapshot = LazySnapshot::try_new(
                table,
                Arc::new(object_store::local::LocalFileSystem::default()),
                None,
            )
            .await
            .unwrap();

            let table_info = case.table_summary().expect("load summary");
            assert_eq!(snapshot.version(), table_info.version);
            assert_eq!(
                (
                    snapshot.protocol().min_reader_version(),
                    snapshot.protocol().min_writer_version()
                ),
                (table_info.min_reader_version, table_info.min_writer_version)
            );
        });
    Ok(())
}

fn reader_test_eager(path: &Path) -> datatest_stable::Result<()> {
    let root_dir = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        path.parent().unwrap().to_str().unwrap()
    );
    for skipped in SKIPPED_TESTS {
        if root_dir.ends_with(skipped) {
            println!("Skipping test: {}", skipped);
            return Ok(());
        }
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let case = read_dat_case(root_dir).unwrap();

            let table = Table::try_from_uri(case.table_root().unwrap()).expect("table");
            let snapshot = LazySnapshot::try_new(
                table,
                Arc::new(object_store::local::LocalFileSystem::default()),
                None,
            )
            .await
            .unwrap();

            let table_info = case.table_summary().expect("load summary");
            assert_eq!(snapshot.version(), table_info.version);
            assert_eq!(
                (
                    snapshot.protocol().min_reader_version(),
                    snapshot.protocol().min_writer_version()
                ),
                (table_info.min_reader_version, table_info.min_writer_version)
            );
        });
    Ok(())
}

datatest_stable::harness!(
    reader_test_lazy,
    "../../dat/out/reader_tests/generated/",
    r"test_case_info\.json",
    reader_test_eager,
    "../../dat/out/reader_tests/generated/",
    r"test_case_info\.json"
);
