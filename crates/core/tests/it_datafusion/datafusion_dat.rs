use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::create_session;
use deltalake_test::acceptance::{KnownFailures, ReadSpec, Spec, Workload, check_workload};
use rstest::rstest;
use url::Url;

/// Read workloads where deltalake-core does not do what the spec expects, grouped by the component
/// at fault. A workload that expects an error is listed only when the read succeeds. None is the
/// executor's: on every workload, its `DataFusionEngine` gives the same result as the kernel's
/// default engine (the executor's DAT test checks this).
const KNOWN_FAILURES: KnownFailures = &[
    // delta-kernel: the kernel alone, on its default engine, gives the same result.
    (
        "reads tables that Spark rejects: invalid protocols and corrupt logs",
        &[
            "checkpoints_checkpoint_only_table_read_all",
            "column_mapping_cm_err_003_invalid_mode_read_all",
            "corruption_err_duplicate_add_same_version_read_all",
            "corruption_err_missing_version_0_read_all",
            "corruption_only_remove_file_read_all",
            "evolvability_fc_empty_json_line_read_all",
            "log_replay_log_err_missing_metadata_read_all",
            "log_replay_log_err_missing_protocol_read_all",
        ],
    ),
    (
        "time travel to a timestamp after the last commit does not fail",
        &[
            "time_travel_future_timestamp_error_read_ts_2099-12-31_23-59-59.999",
            "time_travel_timestamps_read_ts_future",
        ],
    ),
    (
        "no fallback to log replay when the checkpoint is missing or incomplete",
        &[
            "checkpoints_incomplete_multipart_read_all",
            "checkpoints_missing_checkpoint_file_read_all",
            "corruption_wrong_last_checkpoint_read_all",
            "corruption_wrong_last_checkpoint_read_id_lt_5",
        ],
    ),
    (
        "a truncated last line in a commit fails the read; Spark skips it",
        &["corruption_truncated_commit_read_all"],
    ),
    (
        "an add and a remove of the same path with a deletion vector in one commit: 2 rows, \
         Spark reads 0",
        &["corruption_err_add_and_remove_same_path_dv_read_all"],
    ),
    // deltalake-core: the kernel alone does what the spec expects.
    (
        "time travel to a timestamp before the first commit does not fail",
        &["time_travel_timestamps_read_ts_epoch"],
    ),
    (
        "the interval year-to-month type panics in crates/core/src/kernel/snapshot/iterators/\
         scan_row.rs",
        &["interval_types_intv_003_interval_partitioned_read_all"],
    ),
    (
        "a LIKE filter on a nested field is dropped: the provider reports it as exact, and the \
         parquet row filter skips nested fields",
        &[
            "data_skipping_starts_with_nested_read_ab_like_a",
            "data_skipping_starts_with_nested_read_ab_like_app",
            "data_skipping_starts_with_nested_read_ab_like_b",
            "data_skipping_starts_with_nested_read_miss_c",
            "data_skipping_starts_with_nested_read_miss_z",
        ],
    ),
    (
        "IS NOT NULL on a nested field added later keeps the rows of older files",
        &[
            "schema_evolution_add_nested_field_read_infoemail_is_not_null",
            "schema_evolution_nested_field_pred_read_infoemail_is_not_null",
        ],
    ),
    (
        "the typeWidening reader feature is rejected; the kernel reads these tables through the \
         executor",
        &[
            "checkpoints_v2_with_type_widening_read_all",
            "checkpoints_v2_with_type_widening_read_v0",
            "merge_mergeSchemaEvoWidenType_readAll",
            "merge_mergeSchemaEvoWidenType_readLargeAmount",
            "type_widening_array_element_read_all",
            "type_widening_byte_to_int_read_all",
            "type_widening_byte_to_int_read_gt_max_byte",
            "type_widening_byte_to_int_read_gt_max_short",
            "type_widening_change_tracking_across_widening_read_all",
            "type_widening_change_tracking_across_widening_read_original",
            "type_widening_colmap_rename_filter_wide",
            "type_widening_colmap_rename_project_renamed",
            "type_widening_colmap_rename_read_all",
            "type_widening_cross_physical_decimal_read_all",
            "type_widening_cross_physical_decimal_read_v1_int32_only",
            "type_widening_cross_physical_decimal_read_v3_through_int64",
            "type_widening_date_to_timestamp_ntz_read_all",
            "type_widening_date_to_timestamp_ntz_read_v1_before_widening",
            "type_widening_decimal_precision_read_all",
            "type_widening_decimal_precision_read_large_values",
            "type_widening_float_to_double_read_all",
            "type_widening_float_to_double_read_high_precision",
            "type_widening_full_numeric_chain_read_all",
            "type_widening_full_numeric_chain_read_v1_byte_only",
            "type_widening_full_numeric_chain_read_v3_through_short",
            "type_widening_full_numeric_chain_read_v5_through_int",
            "type_widening_int_to_long_read_all",
            "type_widening_int_to_long_read_gt_max_int",
            "type_widening_int_to_long_read_v1_before_widening",
            "type_widening_map_key_value_widening_read_all",
            "type_widening_map_key_value_widening_read_v1_before_widening",
            "type_widening_nested_field_read_all",
            "type_widening_nested_field_read_large_count",
            "type_widening_null_handling_read_all",
            "type_widening_null_handling_read_non_null",
            "type_widening_null_handling_read_nulls_only",
            "type_widening_project_non_widened_project_non_widened",
            "type_widening_project_non_widened_read_all",
            "type_widening_project_widened_project_widened_only",
            "type_widening_project_widened_project_widened_with_id",
            "type_widening_project_widened_read_all",
            "type_widening_row_tracking_combo_read_all",
            "type_widening_row_tracking_combo_read_wide_values",
            "type_widening_short_to_int_read_all",
            "type_widening_short_to_int_read_gt_max_short",
            "type_widening_short_to_long_read_all",
            "type_widening_short_to_long_read_gt_max_int",
            "type_widening_short_to_long_read_gt_max_short",
            "type_widening_stats_after_change_predicate_cross_range",
            "type_widening_stats_after_change_predicate_new_range",
            "type_widening_stats_after_change_predicate_old_range",
            "type_widening_stats_after_change_read_all",
            "type_widening_with_column_mapping_filter_on_widened",
            "type_widening_with_column_mapping_project_renamed_widened",
            "type_widening_with_column_mapping_read_all",
            "type_widening_with_data_skipping_read_all",
            "type_widening_with_data_skipping_read_large_values_only",
            "type_widening_with_data_skipping_read_with_predicate_on_widened",
            "type_widening_with_dv_read_all",
            "type_widening_with_dv_read_wide_values",
            "type_widening_with_partition_read_all",
            "type_widening_with_partition_read_partition_A",
            "type_widening_with_partition_read_wide_values",
            "write_features_type_widening_inserts_latest",
            "write_features_type_widening_inserts_read_all",
            "write_features_type_widening_inserts_read_ge_20",
        ],
    ),
    // This test: DataFusion's SQL cannot run the workload's Spark SQL predicate.
    // Some could be resolved using datafusion-spark extension planner
    (
        "the predicate uses Spark SQL that DataFusion lacks: size, year, month, date_add, \
         date_sub, datediff, trunc, or LIKE on a number",
        &[
            "check_constraints_array_constraint_read_sizetags_gt_2",
            "data_skipping_date_add_sub_read_d_gte_date_adddate20240101_1",
            "data_skipping_date_add_sub_read_miss_date_sub",
            "data_skipping_datediff_read_datediffd_date20240101_gt_100",
            "data_skipping_datediff_read_datediffd_date20240101_lte_10",
            "data_skipping_month_function_read_miss_month",
            "data_skipping_month_function_read_monthd_eq_1",
            "data_skipping_month_function_read_monthd_eq_6",
            "data_skipping_nulls_only_null_read_like_x",
            "data_skipping_trunc_date_read_miss_trunc",
            "data_skipping_trunc_date_read_truncd_month_eq_date20240301",
            "data_skipping_trunc_date_read_truncd_month_eq_date20240601",
            "data_skipping_year_function_read_miss_year",
            "data_skipping_year_function_read_yeard_eq_2024",
            "data_skipping_year_function_read_yeard_eq_2025",
            "variant_array_variant_filter_array_size",
        ],
    ),
];

/// A read workload scans the table through DataFusion and checks the row count, or expects the
/// read to fail.
#[rstest]
#[tokio::test]
async fn scan_dat(
    #[files("../../dat/v0.0.8/*/specs/*.json")]
    #[exclude("snapshot")]
    path: PathBuf,
) {
    let Workload {
        name,
        spec,
        table_root,
    } = Workload::load(&path);
    let Spec::Read(spec) = spec else {
        println!("skipped: not a read workload");
        return;
    };
    check_workload(&name, KNOWN_FAILURES, check(spec, table_root)).await;
}

async fn check(spec: ReadSpec, table_root: Url) -> Result<(), String> {
    let rows = read(&table_root, &spec).await;
    match (rows, &spec.expected) {
        (Ok(rows), Some(expected)) if rows == expected.row_count => Ok(()),
        (Ok(rows), Some(expected)) => Err(format!("{rows} rows, expected {}", expected.row_count)),
        (Err(_), None) => Ok(()),
        (Ok(_), None) => Err("the read succeeds, but the workload expects an error".into()),
        (Err(err), Some(_)) => Err(err.to_string()),
    }
}

/// The number of rows `spec` reads, with its predicate and projection as SQL.
async fn read(table_root: &Url, spec: &ReadSpec) -> datafusion::error::Result<usize> {
    let table = spec.at.load(table_root).await?;
    let ctx: SessionContext = create_session().into_inner();
    // Spark resolves column names case-insensitively; keep the names as written.
    ctx.sql("SET datafusion.sql_parser.enable_ident_normalization = false")
        .await?;
    ctx.register_table("t", table.table_provider().await?)?;

    let columns = match &spec.columns {
        Some(columns) => columns
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(", "),
        None => "*".to_string(),
    };
    let filter = match &spec.predicate {
        Some(predicate) => format!(" WHERE {}", spark_types_to_datafusion(predicate)),
        None => String::new(),
    };
    let batches = ctx
        .sql(&format!("SELECT {columns} FROM t{filter}"))
        .await?
        .collect()
        .await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

/// `predicate` with the Spark type names DataFusion's SQL does not know replaced.
fn spark_types_to_datafusion(predicate: &str) -> String {
    predicate
        .replace("TIMESTAMP_NTZ", "TIMESTAMP")
        .replace("AS BYTE", "AS TINYINT")
        .replace("AS SHORT", "AS SMALLINT")
}
