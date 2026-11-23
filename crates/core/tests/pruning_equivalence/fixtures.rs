//! Small SQLLogicTest subset for pruning fixtures. `actual` is a local Delta
//! table; other tables use ordinary DataFusion SQL. Records are separated by a
//! blank line. Query answers are tab-separated rows or a shared result label.

use std::{collections::HashMap, fs, path::Path, sync::Arc};

use arrow::util::display::array_value_to_string;
use datafusion::{
    execution::context::SQLOptions,
    logical_expr::{DdlStatement, LogicalPlan, WriteOp, expr_rewriter::unnormalize_col},
    prelude::SessionContext,
};
use deltalake_core::{DeltaTableBuilder, delta_datafusion::create_session, protocol::SaveMode};

use super::{LabResult, Url, open_table};

mod layout;

type Rows = Vec<Vec<Option<String>>>;

#[derive(Default)]
struct Comparisons {
    count: usize,
    labels: HashMap<String, LabeledRows>,
}

struct LabeledRows {
    width: usize,
    rows: Rows,
    unchecked_line: Option<usize>,
}

fn display_cell(value: &Option<String>) -> &str {
    match value.as_deref() {
        None => "NULL",
        Some("") => "(empty)",
        Some(value) => value,
    }
}

async fn execute_statement(
    ctx: &SessionContext,
    sql: &str,
    uri: &Url,
    layout: &layout::Layout,
) -> LabResult<()> {
    let plan = ctx.state().create_logical_plan(sql).await?;
    match &plan {
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create))
            if create.name.table() == "actual" =>
        {
            let table = DeltaTableBuilder::from_url(uri.clone())?.build()?;
            let writer = table
                .write([])
                .with_save_mode(SaveMode::ErrorIfExists)
                .with_input_plan(create.input.as_ref().clone())
                .with_session_state(Arc::new(ctx.state()));
            layout.apply(writer).await?;
        }
        LogicalPlan::Dml(dml)
            if dml.table_name.table() == "actual" && dml.op == WriteOp::Delete =>
        {
            let delete = open_table(uri.clone()).await?.delete();
            let delete = match dml.input.as_ref() {
                LogicalPlan::Filter(filter) => {
                    delete.with_predicate(unnormalize_col(filter.predicate.clone()))
                }
                LogicalPlan::TableScan(_) => delete,
                _ => return Err("only DELETE with an optional WHERE is supported on actual".into()),
            };
            delete.await?;
        }
        _ => {
            let refresh =
                matches!(&plan, LogicalPlan::Dml(dml) if dml.table_name.table() == "actual");
            ctx.execute_logical_plan(plan).await?.collect().await?;
            if !refresh {
                return Ok(());
            }
        }
    }
    // Delta providers read a snapshot. Reopen after each commit so subsequent
    // fixture queries observe exactly what a new reader would see on disk.
    let provider = open_table(uri.clone()).await?.table_provider().await?;
    ctx.deregister_table("actual")?;
    ctx.register_table("actual", provider)?;
    Ok(())
}

async fn read_rows(
    ctx: &SessionContext,
    sql: &str,
    width: usize,
    sort: bool,
    uri: &Url,
) -> LabResult<Rows> {
    let read_only = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let df = ctx.sql_with_options(sql, read_only).await?;
    if df.schema().fields().len() != width {
        return Err(format!(
            "expected {width} columns, got {}",
            df.schema().fields().len()
        )
        .into());
    }
    let mut actual = Rows::new();
    for batch in layout::collect(ctx, df, sql, uri).await? {
        for i in 0..batch.num_rows() {
            let row = batch
                .columns()
                .iter()
                .map(|col| {
                    col.is_valid(i)
                        .then(|| array_value_to_string(col.as_ref(), i))
                        .transpose()
                })
                .collect::<Result<_, _>>()?;
            actual.push(row);
        }
    }
    if sort {
        actual.sort_unstable_by(|a, b| {
            a.iter()
                .map(display_cell)
                .cmp(b.iter().map(display_cell))
                .then_with(|| a.cmp(b))
        });
    }
    Ok(actual)
}

async fn run_record(
    ctx: &SessionContext,
    uri: &Url,
    header: &str,
    lines: &[&str],
    comparisons: &mut Comparisons,
    line_number: usize,
    layout: &layout::Layout,
) -> LabResult<()> {
    let separator = lines.iter().position(|line| line.trim() == "----");
    let sql = lines[..separator.unwrap_or(lines.len())].join("\n");
    let expected = separator.map(|i| &lines[i + 1..]);
    let tokens: Vec<_> = header.split_whitespace().collect();
    let (width, sort, label) = match tokens.as_slice() {
        ["statement", "ok"] if expected.is_none() => {
            return execute_statement(ctx, &sql, uri, layout).await;
        }
        ["statement", "error"] => {
            let needle = expected.unwrap_or_default().join("\n");
            return match execute_statement(ctx, &sql, uri, layout).await {
                Ok(()) => Err("expected SQL to fail".into()),
                Err(error) if error.to_string().contains(&needle) => Ok(()),
                Err(error) => {
                    Err(format!("expected error containing {needle:?}, got {error}").into())
                }
            };
        }
        ["query", columns, options @ ..]
            if columns.bytes().all(|c| matches!(c, b'I' | b'R' | b'T')) =>
        {
            let (sort, label) = match options {
                [] => (false, None),
                [sort @ ("rowsort" | "nosort"), rest @ ..] if rest.len() <= 1 => {
                    (*sort == "rowsort", rest.first().copied())
                }
                _ => return Err(format!("unsupported query options in {header:?}").into()),
            };
            (columns.len(), sort, label)
        }
        _ => return Err(format!("unsupported directive {header:?}").into()),
    };
    let expected = expected.ok_or("query requires a ---- result separator")?;
    let actual = read_rows(ctx, &sql, width, sort, uri).await?;
    let has_literal = label.is_none() || !expected.is_empty();
    if has_literal {
        let literals: Rows = expected
            .iter()
            .map(|line| {
                line.split('\t')
                    .map(|v| match v {
                        "NULL" => None,
                        "(empty)" => Some(String::new()),
                        v => Some(v.to_owned()),
                    })
                    .collect()
            })
            .collect();
        if actual != literals {
            return Err(format!("rows differ: expected {literals:?}, got {actual:?}").into());
        }
        comparisons.count += 1;
    }
    if let Some(label) = label {
        if let Some(previous) = comparisons.labels.get_mut(label) {
            if previous.width != width {
                return Err(format!(
                    "label {label:?} originally had {} columns, got {width}",
                    previous.width
                )
                .into());
            }
            if previous.rows != actual {
                return Err(format!(
                    "rows differ for label {label:?}: expected {:?}, got {actual:?}",
                    previous.rows
                )
                .into());
            }
            previous.unchecked_line = None;
            comparisons.count += 1;
        } else {
            comparisons.labels.insert(
                label.to_owned(),
                LabeledRows {
                    width,
                    rows: actual,
                    unchecked_line: (!has_literal).then_some(line_number),
                },
            );
        }
    }
    Ok(())
}

async fn run_fixture(name: &str, script: &str, uri: &Url) -> LabResult<()> {
    let ctx = create_session().into_inner();
    let mut comparisons = Comparisons::default();
    let mut layout = layout::Layout::default();
    let mut started = false;
    let mut lines = script.lines().enumerate().peekable();
    while let Some((i, header)) = lines.next() {
        if let Some(directive) = header.trim().strip_prefix("# delta:") {
            if started {
                return Err(format!(
                    "{name}:{}: layout directive must precede the first record",
                    i + 1
                )
                .into());
            }
            layout
                .parse(directive.trim())
                .map_err(|e| format!("{name}:{}: {e}", i + 1))?;
            continue;
        }
        if header.trim().is_empty() || header.trim_start().starts_with('#') {
            continue;
        }
        started = true;
        let mut record = Vec::new();
        while let Some((_, line)) = lines.next_if(|(_, line)| !line.trim().is_empty()) {
            record.push(line);
        }
        run_record(&ctx, uri, header, &record, &mut comparisons, i + 1, &layout)
            .await
            .map_err(|e| format!("{name}:{} ({header}): {e}\n{}", i + 1, record.join("\n")))?;
    }
    if let Some((line, label)) = comparisons
        .labels
        .iter()
        .filter_map(|(name, value)| value.unchecked_line.map(|line| (line, name)))
        .min()
    {
        return Err(format!("{name}:{line}: unmatched result label {label:?}").into());
    }
    if comparisons.count == 0 {
        return Err(format!("{name}:1: no result comparisons").into());
    }
    Ok(())
}

#[path = "fixtures/suite.rs"]
mod suite;

#[rstest::rstest]
#[case::insert("INSERT INTO actual VALUES (2)", "DML not supported")]
#[case::ddl("CREATE TABLE extra AS SELECT 2 AS id", "DDL not supported")]
#[case::session("SET datafusion.execution.batch_size = 42", "Statement not supported")]
#[tokio::test]
async fn query_rejects_side_effects_before_execution(
    #[case] sql: &str,
    #[case] reason: &str,
) -> LabResult<()> {
    let dir = tempfile::tempdir()?;
    let uri = Url::from_directory_path(dir.path()).unwrap();
    let script =
        format!("statement ok\nCREATE TABLE actual AS SELECT 1 AS id\n\nquery I\n{sql}\n----");
    let result = run_fixture("mutation.test", &script, &uri).await;
    assert_eq!(
        open_table(uri).await?.version(),
        Some(0),
        "query changed the durable table"
    );
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("mutation.test:4") && error.contains(reason) && error.contains(sql),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn statement_insert_refreshes_snapshot() -> LabResult<()> {
    let dir = tempfile::tempdir()?;
    let uri = Url::from_directory_path(dir.path()).unwrap();
    let script = "statement ok\nCREATE TABLE actual AS SELECT 1 AS id\n\nstatement ok\nINSERT INTO actual VALUES (2)\n\nquery I rowsort\nSELECT id FROM actual\n----\n1\n2";
    run_fixture("insert.test", script, &uri).await?;
    assert_eq!(open_table(uri).await?.version(), Some(1));
    Ok(())
}

#[rstest::rstest]
#[case::empty_literal("query I\nSELECT 1 WHERE false\n----")]
#[case::paired_empty(
    "query I nosort empty\nSELECT 1 WHERE false\n----\n\nquery I nosort empty\nSELECT 2 WHERE false\n----"
)]
#[case::checked_label("query I nosort a\nSELECT 1\n----\n1")]
#[tokio::test]
async fn accepts_checked_results(#[case] script: &str) -> LabResult<()> {
    let dir = tempfile::tempdir()?;
    run_fixture(
        "valid.test",
        script,
        &Url::from_directory_path(dir.path()).unwrap(),
    )
    .await
}

#[rstest::rstest]
#[case::setup_only(
    "statement ok\nCREATE TABLE actual AS SELECT 1 AS id",
    1,
    "no result comparisons"
)]
#[case::orphan_even_with_literal(
    "query I\nSELECT 1\n----\n1\n\nquery I rowsort typo\nSELECT 999\n----",
    6,
    "typo"
)]
#[case::changed_value(
    "query II rowsort same\nSELECT 1, 7\n----\n\nquery II rowsort same\nSELECT 1, 8\n----",
    5,
    "rows differ"
)]
#[case::duplicate_missing(
    "query I rowsort same\nVALUES (1), (1)\n----\n\nquery I rowsort same\nVALUES (1)\n----",
    5,
    "rows differ"
)]
#[case::wrong_expected("query I\nSELECT 42\n----\n43", 1, "rows differ")]
#[case::wrong_width("query II\nSELECT 1 WHERE false\n----", 1, "column")]
#[case::empty_label_width(
    "query I nosort group1\nSELECT 1 WHERE false\n----\n\nquery II nosort group1\nSELECT 1, 2 WHERE false\n----",
    5,
    "column"
)]
#[case::unexpected_success("statement error\nCREATE TABLE t(a INT)", 1, "expected SQL to fail")]
#[case::unknown_directive("statment ok\nSELECT 1", 1, "unsupported directive")]
#[tokio::test]
async fn rejects_incorrect_fixture_results(
    #[case] script: &str,
    #[case] line: usize,
    #[case] reason: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let uri = Url::from_directory_path(dir.path()).unwrap();
    let error = run_fixture("bad.test", script, &uri)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(&format!("bad.test:{line}")) && error.contains(reason),
        "{error}"
    );
}
