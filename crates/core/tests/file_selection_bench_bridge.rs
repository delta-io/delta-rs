#![cfg(feature = "datafusion")]

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionContext, col, lit};
use deltalake_core::DeltaTable;
use deltalake_core::delta_datafusion::bench_support;
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::protocol::SaveMode;
use deltalake_test::TestResult;

fn batch(ids: Vec<i32>, parts: Vec<&str>) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("part", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(parts)),
        ],
    )?)
}

#[tokio::test]
async fn test_out_of_crate_bridge_exposes_file_selection_paths() -> TestResult {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(vec![
            StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                false,
            ),
            StructField::new(
                "part".to_string(),
                DataType::Primitive(PrimitiveType::String),
                false,
            ),
        ])
        .with_partition_columns(["part"])
        .await?;
    let table = table
        .write(vec![batch(vec![1, 2], vec!["a", "a"])?])
        .with_save_mode(SaveMode::Append)
        .await?;
    let table = table
        .write(vec![batch(vec![100, 101], vec!["b", "b"])?])
        .with_save_mode(SaveMode::Append)
        .await?;

    let snapshot = table.snapshot()?.snapshot().clone();
    let log_store = table.log_store();
    let session = SessionContext::new().state();

    let partition_result = bench_support::find_files(
        &snapshot,
        log_store.clone(),
        &session,
        Some(col("part").eq(lit("a"))),
    )
    .await?;
    assert!(partition_result.partition_scan);
    assert_eq!(partition_result.candidates.len(), 1);
    assert!(
        partition_result.candidates[0].path.contains("part=a/"),
        "expected partition-only path to match partition file, got {}",
        partition_result.candidates[0].path
    );

    let mem_table = bench_support::add_actions_partition_mem_table(&snapshot)?
        .expect("partition mem table should exist");
    let field_names = mem_table
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(field_names, vec!["__delta_rs_path", "part"]);

    let data_predicate = col("id").gt(lit(50i32));
    let data_result = bench_support::find_files(
        &snapshot,
        log_store.clone(),
        &session,
        Some(data_predicate.clone()),
    )
    .await?;
    assert!(!data_result.partition_scan);
    assert_eq!(data_result.candidates.len(), 1);
    assert!(
        data_result.candidates[0].path.contains("part=b/"),
        "expected data predicate to match partition b file, got {}",
        data_result.candidates[0].path
    );

    let direct_scan = bench_support::find_files_scan(
        &snapshot,
        log_store.clone(),
        &session,
        data_predicate.clone(),
    )
    .await?;
    assert_eq!(direct_scan.len(), 1);
    assert_eq!(direct_scan[0].path, data_result.candidates[0].path);

    let matched_scan =
        bench_support::scan_files_where_matches(&session, &snapshot, log_store, data_predicate)
            .await?
            .expect("matched file scan should exist");
    assert_eq!(matched_scan.files_set().len(), 1);
    assert_eq!(matched_scan.predicate(), &col("id").gt(lit(50i32)));

    let plan = session.create_physical_plan(matched_scan.scan()).await?;
    let batches = collect(plan, session.task_ctx()).await?;
    let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(row_count, 2);

    Ok(())
}
