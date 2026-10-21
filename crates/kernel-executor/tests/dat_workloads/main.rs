//! Runs the DAT workloads (`make setup-dat`) on the kernel's default engine and on
//! [`DataFusionEngine`], and checks that both give the same result: the same rows, the same
//! snapshot, or both an error.
//!
//! The expected results in the corpus are not checked: where the kernel itself diverges from
//! Spark, both engines diverge the same way.

mod run;
mod spec;

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use delta_kernel::Engine;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use deltalake_kernel_executor::DataFusionEngine;
use rstest::rstest;
use tokio::runtime::Handle;

use crate::run::run;
use crate::spec::{Spec, Workload};

/// Workloads where the default engine is wrong and the executor is right. For these the executor
/// must succeed and the engines must still disagree, so an entry goes once the kernel is fixed.
const DEFAULT_ENGINE_BUGS: &[(&str, &[&str])] = &[
    (
        "reads INT96 timestamps as nanoseconds, which overflow outside 1677-2262",
        &[
            "write_boundary_date_and_timestamp_range_ends_latest",
            "write_boundary_date_and_timestamp_range_ends_read_all",
            "write_boundary_date_and_timestamp_range_ends_read_max_date",
            "write_boundary_extreme_values_with_checkpoint_latest",
            "write_boundary_extreme_values_with_checkpoint_read_all",
        ],
    ),
    (
        "cannot read widened list elements or map keys and values",
        &[
            "type_widening_array_element_read_all",
            "type_widening_map_key_value_widening_read_all",
        ],
    ),
];

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matches_the_default_engine(
    #[files("../../dat/v0.0.8/*/specs/*.json")] spec_path: PathBuf,
) {
    let workload = Workload::load(&spec_path);
    if let Spec::Write = workload.spec {
        println!("skipped: the workload writes");
        return;
    }
    let default_engine_bug = DEFAULT_ENGINE_BUGS
        .iter()
        .find(|(_, workloads)| workloads.contains(&workload.name.as_str()))
        .map(|(bug, _)| *bug);
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let default: Arc<dyn Engine> = Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(Handle::current())))
            .build(),
    );
    let datafusion: Arc<dyn Engine> =
        DataFusionEngine::new_from_context(SessionContext::new().task_ctx());

    // Both engines block on the runtime, so the workload runs off its worker threads.
    let (expected, actual) =
        tokio::task::spawn_blocking(move || (run(&workload, default), run(&workload, datafusion)))
            .await
            .unwrap();

    if let Some(bug) = default_engine_bug {
        let actual = actual.expect("the executor reads this workload");
        assert!(
            !matches!(expected, Ok(expected) if expected == actual),
            "the default engine no longer {bug}: remove the workload from DEFAULT_ENGINE_BUGS"
        );
        return;
    }
    match (expected, actual) {
        (Ok(expected), Ok(actual)) => assert_eq!(actual, expected),
        (Err(expected), Err(actual)) => {
            println!("both engines fail\n  default: {expected}\n  datafusion: {actual}")
        }
        (expected, actual) => {
            panic!("the engines disagree\n  default: {expected:?}\n  datafusion: {actual:?}")
        }
    }
}
