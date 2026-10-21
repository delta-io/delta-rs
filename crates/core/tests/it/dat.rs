use std::path::PathBuf;

use delta_kernel::table_features::TableFeature;
use deltalake_core::kernel::Protocol;
use deltalake_test::acceptance::{KnownFailures, SnapshotSpec, Spec, Workload, check_workload};
use rstest::rstest;
use url::Url;

/// Snapshot workloads where deltalake-core does not do what the spec expects, grouped by the
/// component at fault. A workload that expects an error is listed only when the load succeeds.
/// None is the executor's: on every workload, its `DataFusionEngine` gives the same result as the
/// kernel's default engine (the executor's DAT test checks this).
const KNOWN_FAILURES: KnownFailures = &[
    // delta-kernel: the kernel alone, on its default engine, gives the same result.
    (
        "loads tables that Spark rejects: invalid protocols and corrupt logs",
        &[
            "checkpoints_checkpoint_only_table_snapshot",
            "column_mapping_cm_err_003_invalid_mode_snapshot",
            "corruption_err_duplicate_add_same_version_snapshot",
            "corruption_err_missing_version_0_snapshot",
            "corruption_only_remove_file_snapshot",
            "deletion_vectors_checkpoint_only_read_snapshot",
            "evolvability_fc_empty_json_line_snapshot",
            "log_replay_log_err_missing_metadata_snapshot",
            "log_replay_log_err_missing_protocol_snapshot",
            "protocol_versions_empty_reader_features_snapshot",
            "protocol_versions_err_001_protocol_too_high_snapshot",
            "protocol_versions_err_002_unsupported_feature_snapshot",
            "protocol_versions_features_case_sensitivity_snapshot",
            "protocol_versions_reader_feature_not_in_writer_snapshot",
            "protocol_versions_reader_v3_writer_lt_7_snapshot",
            "protocol_versions_reader_v4_error_snapshot",
            "protocol_versions_unknown_reader_feature_snapshot",
            "protocol_versions_unknown_writer_feature_ok_snapshot",
        ],
    ),
    (
        "time travel to a deleted version does not fail",
        &[
            "time_travel_deleted_version_retention_error_snapshot",
            "time_travel_deleted_version_snapshot",
        ],
    ),
    (
        "no fallback to log replay when the checkpoint is missing or incomplete",
        &[
            "checkpoints_incomplete_multipart_snapshot",
            "checkpoints_missing_checkpoint_file_snapshot",
        ],
    ),
    (
        "the metadata comes from the version 0 CRC file, which disagrees with the commit",
        &["evolvability_fc_extra_metadata_keys_snapshot"],
    ),
    (
        "a protocol downgrade fails with unmasked nulls in metaData",
        &["protocol_versions_protocol_downgrade_snapshot"],
    ),
    // deltalake-core: the kernel alone does what the spec expects.
    // These are behavior differences and acceptable.
    (
        "time travel to a timestamp before the first commit does not fail",
        &["time_travel_timestamp_before_retention_error_snapshot_ts_2026-09-29_19-03-05.796"],
    ),
    (
        "a truncated last line in a commit fails the load; the kernel and Spark skip it",
        &["corruption_truncated_commit_snapshot"],
    ),
    (
        "the interval year-to-month type panics in crates/core/src/kernel/snapshot/iterators/\
         scan_row.rs",
        &["interval_types_intv_003_interval_partitioned_snapshot"],
    ),
];

/// A snapshot workload loads the table and checks its protocol and metadata, or expects the
/// load to fail.
#[rstest]
#[tokio::test]
async fn test_snapshot_workload(
    #[files("../../dat/v0.0.8/*/specs/*snapshot*.json")] path: PathBuf,
) {
    let Workload {
        name,
        spec,
        table_root,
    } = Workload::load(&path);
    let Spec::Snapshot(spec) = spec else {
        panic!("{name} is not a snapshot workload");
    };
    check_workload(&name, KNOWN_FAILURES, check(spec, table_root)).await;
}

async fn check(spec: SnapshotSpec, table_root: Url) -> Result<(), String> {
    let table = spec.at.load(&table_root).await;
    match (table, &spec.expected) {
        (Ok(table), Some(expected)) => {
            let snapshot = table.snapshot().map_err(|err| err.to_string())?;
            if sorted(snapshot.protocol()) != sorted(&expected.protocol) {
                return Err(format!(
                    "protocol {:?}, expected {:?}",
                    snapshot.protocol(),
                    expected.protocol
                ));
            }
            if snapshot.metadata() != &expected.metadata {
                return Err(format!(
                    "metadata {:?}, expected {:?}",
                    snapshot.metadata(),
                    expected.metadata
                ));
            }
            Ok(())
        }
        (Err(_), None) => Ok(()),
        (Ok(_), None) => Err("the load succeeds, but the workload expects an error".into()),
        (Err(err), Some(_)) => Err(err.to_string()),
    }
}

/// The versions and features of `protocol`, with the features sorted: their order has no meaning.
fn sorted(protocol: &Protocol) -> (i32, i32, Option<Vec<String>>, Option<Vec<String>>) {
    let sorted = |features: Option<&[TableFeature]>| {
        features.map(|features| {
            let mut names: Vec<String> = features.iter().map(ToString::to_string).collect();
            names.sort();
            names
        })
    };
    (
        protocol.min_reader_version(),
        protocol.min_writer_version(),
        sorted(protocol.reader_features()),
        sorted(protocol.writer_features()),
    )
}
