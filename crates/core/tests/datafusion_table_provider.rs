#![cfg(feature = "datafusion")]
use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::assert_batches_sorted_eq;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::{ExecutionPlan, collect_partitioned};
use datafusion::prelude::{SessionContext, col, lit};
use deltalake_core::delta_datafusion::create_session;
use deltalake_core::delta_datafusion::engine::DataFusionEngine;
use deltalake_core::kernel::Snapshot;
use deltalake_test::TestResult;
use deltalake_test::acceptance::read_dat_case;

async fn scan_dat(case: &str) -> TestResult<(Snapshot, SessionContext)> {
    let root_dir = format!(
        "{}/../../dat/v0.0.3/reader_tests/generated/{}/",
        env!["CARGO_MANIFEST_DIR"],
        case
    );
    let root_dir = std::fs::canonicalize(root_dir)?;
    let case = read_dat_case(root_dir)?;

    let session = create_session().into_inner();
    let engine = DataFusionEngine::new_from_session(&session.state());

    let snapshot =
        Snapshot::try_new_with_engine(engine.clone(), case.table_root()?, Default::default(), None)
            .await?;

    Ok((snapshot, session))
}

async fn collect_plan(
    plan: Arc<dyn ExecutionPlan>,
    session: &SessionContext,
) -> TestResult<Vec<RecordBatch>> {
    let batches: Vec<_> = collect_partitioned(plan, session.task_ctx())
        .await?
        .into_iter()
        .flatten()
        .collect();
    Ok(batches)
}

#[tokio::test]
async fn test_all_primitive_types() -> TestResult<()> {
    let (snapshot, session) = scan_dat("all_primitive_types").await?;

    let plan = snapshot.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+------+-------+-------+-------+------+---------+---------+-------+----------+---------+------------+----------------------+",
        "| utf8 | int64 | int32 | int16 | int8 | float32 | float64 | bool  | binary   | decimal | date32     | timestamp            |",
        "+------+-------+-------+-------+------+---------+---------+-------+----------+---------+------------+----------------------+",
        "| 0    | 0     | 0     | 0     | 0    | 0.0     | 0.0     | true  |          | 10.000  | 1970-01-01 | 1970-01-01T00:00:00Z |",
        "| 1    | 1     | 1     | 1     | 1    | 1.0     | 1.0     | false | 00       | 11.000  | 1970-01-02 | 1970-01-01T01:00:00Z |",
        "| 2    | 2     | 2     | 2     | 2    | 2.0     | 2.0     | true  | 0000     | 12.000  | 1970-01-03 | 1970-01-01T02:00:00Z |",
        "| 3    | 3     | 3     | 3     | 3    | 3.0     | 3.0     | false | 000000   | 13.000  | 1970-01-04 | 1970-01-01T03:00:00Z |",
        "| 4    | 4     | 4     | 4     | 4    | 4.0     | 4.0     | true  | 00000000 | 14.000  | 1970-01-05 | 1970-01-01T04:00:00Z |",
        "+------+-------+-------+-------+------+---------+---------+-------+----------+---------+------------+----------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    let plan = snapshot
        .scan(&session.state(), Some(&vec![1, 3]), &[], None)
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+-------+-------+",
        "| int64 | int16 |",
        "+-------+-------+",
        "| 0     | 0     |",
        "| 1     | 1     |",
        "| 2     | 2     |",
        "| 3     | 3     |",
        "| 4     | 4     |",
        "+-------+-------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    let plan = snapshot
        .scan(&session.state(), Some(&vec![1, 3]), &[], Some(2))
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+-------+-------+",
        "| int64 | int16 |",
        "+-------+-------+",
        "| 0     | 0     |",
        "| 1     | 1     |",
        "+-------+-------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // While we are passing a filter, the table provider does not yet push down
    // the filter to the parquet scan, so we expect the same result as above.
    let pred = col("float64").gt(lit(2.0_f64));
    let plan = snapshot
        .scan(&session.state(), Some(&vec![1, 3, 6]), &[pred], None)
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+-------+-------+---------+",
        "| int64 | int16 | float64 |",
        "+-------+-------+---------+",
        "| 0     | 0     | 0.0     |",
        "| 1     | 1     | 1.0     |",
        "| 2     | 2     | 2.0     |",
        "| 3     | 3     | 3.0     |",
        "| 4     | 4     | 4.0     |",
        "+-------+-------+---------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_multi_partitioned() -> TestResult<()> {
    let (snapshot, session) = scan_dat("multi_partitioned").await?;

    let plan = snapshot.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+--------+------------+------------+",
        "| number | letter | date       | data       |",
        "+--------+--------+------------+------------+",
        "| 6      | /%20%f | 1970-01-01 | 68656c6c6f |",
        "| 7      | b      | 1970-01-01 | f09f9888   |",
        "+--------+--------+------------+------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // While are not yet pushing down predicates to the parquet scan, the number values
    // are separated by files, so we expect only one file to be read here.
    let pred = col("number").gt(lit(6_i64));
    let plan = snapshot.scan(&session.state(), None, &[pred], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+--------+------------+----------+",
        "| number | letter | date       | data     |",
        "+--------+--------+------------+----------+",
        "| 7      | b      | 1970-01-01 | f09f9888 |",
        "+--------+--------+------------+----------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    let pred = col("letter").eq(lit("/%20%f"));
    let plan = snapshot.scan(&session.state(), None, &[pred], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+--------+------------+------------+",
        "| number | letter | date       | data       |",
        "+--------+--------+------------+------------+",
        "| 6      | /%20%f | 1970-01-01 | 68656c6c6f |",
        "+--------+--------+------------+------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // Since we are able to do an exact filter on a partition column,
    // Datafusion will not include partition columns that are only referenced
    // in a predicate but not part of the end result in the passed projection.
    let pred = col("letter").eq(lit("/%20%f"));
    let plan = snapshot
        .scan(&session.state(), Some(&vec![0]), &[pred], None)
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 6      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // COUNT(*) queries may not include any columns in the projection,
    // but we still need to process predicates properly within the scan.
    let pred = col("letter").eq(lit("/%20%f"));
    let plan = snapshot
        .scan(&session.state(), Some(&vec![]), &[pred], None)
        .await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec!["++", "++", "++"];
    assert_batches_sorted_eq!(&expected, &batches);
    let n_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(n_rows, 1);

    Ok(())
}

#[tokio::test]
async fn test_column_mapping() -> TestResult<()> {
    let (snapshot, session) = scan_dat("column_mapping").await?;

    let plan = snapshot.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+---------+------------+",
        "| letter | new_int | date       |",
        "+--------+---------+------------+",
        "| a      | 25      | 2017-05-01 |",
        "| a      | 25      | 2017-05-01 |",
        "| a      | 604     | 1997-01-01 |",
        "| a      | 604     | 1997-01-01 |",
        "| a      | 692     | 2017-09-01 |",
        "| a      | 692     | 2017-09-01 |",
        "| a      | 95      | 1983-04-01 |",
        "| a      | 95      | 1983-04-01 |",
        "| b      | 228     | 1978-12-01 |",
        "| b      | 228     | 1978-12-01 |",
        "+--------+---------+------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

#[tokio::test]
async fn test_deletion_vectors() -> TestResult<()> {
    let (snapshot, session) = scan_dat("deletion_vectors").await?;

    let plan = snapshot.scan(&session.state(), None, &[], None).await?;
    let batches: Vec<_> = collect_plan(plan, &session).await?;
    let expected = vec![
        "+--------+-----+------------+",
        "| letter | int | date       |",
        "+--------+-----+------------+",
        "| b      | 228 | 1978-12-01 |",
        "+--------+-----+------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}
