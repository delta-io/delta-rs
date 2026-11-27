use arrow::record_batch::RecordBatch;
use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    dataframe::DataFrame,
    physical_plan,
};
use deltalake_core::{delta_datafusion::DeltaScanExec, operations::write::WriteBuilder};

use super::*;

#[derive(Default)]
pub(super) struct Layout {
    partition_columns: Vec<String>,
    statistics: Option<bool>,
}

impl Layout {
    pub(super) fn parse(&mut self, directive: &str) -> LabResult<()> {
        if let Some(columns) = directive.strip_prefix("partition-by ") {
            if !self.partition_columns.is_empty() {
                return Err("duplicate partition-by directive".into());
            }
            for column in columns.split(',').map(str::trim) {
                if column.is_empty() || self.partition_columns.iter().any(|c| c == column) {
                    return Err(format!("empty or duplicate partition column {column:?}").into());
                }
                self.partition_columns.push(column.to_owned());
            }
            return Ok(());
        }
        let enabled = match directive {
            "stats on" => true,
            "stats off" => false,
            _ => return Err(format!("unknown Delta layout directive {directive:?}").into()),
        };
        if self.statistics.replace(enabled).is_some() {
            return Err("duplicate stats directive".into());
        }
        Ok(())
    }

    pub(super) fn apply(&self, mut writer: WriteBuilder) -> WriteBuilder {
        if !self.partition_columns.is_empty() {
            writer = writer.with_partition_columns(self.partition_columns.clone());
        }
        if let Some(enabled) = self.statistics {
            let indexed = if enabled { "-1" } else { "0" };
            writer =
                writer.with_configuration([("delta.dataSkippingNumIndexedCols", Some(indexed))]);
        }
        writer
    }
}

pub(super) async fn collect(
    ctx: &SessionContext,
    df: DataFrame,
    sql: &str,
    uri: &Url,
) -> LabResult<Vec<RecordBatch>> {
    let mut directives = sql
        .lines()
        .filter_map(|line| line.trim().strip_prefix("-- delta-scan:"));
    let Some(expected) = directives.next() else {
        return Ok(df.collect().await?);
    };
    if directives.next().is_some() {
        return Err("duplicate delta-scan expectation".into());
    }
    let (scanned, total) = expected
        .trim()
        .split_once('/')
        .ok_or("expected -- delta-scan: scanned/total")?;
    let expected = (
        scanned.trim().parse::<usize>()?,
        total.trim().parse::<usize>()?,
    );
    let plan = df.create_physical_plan().await?;
    let batches = physical_plan::collect(plan.clone(), ctx.task_ctx()).await?;
    let mut scans = Vec::new();
    plan.apply(|node| {
        if node.downcast_ref::<DeltaScanExec>().is_some() {
            scans.push(
                node.metrics()
                    .and_then(|m| m.sum_by_name("count_files_scanned"))
                    .map(|m| m.as_usize()),
            );
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let [Some(scanned)] = scans.as_slice() else {
        return Err(format!(
            "delta-scan requires one Delta scan with count_files_scanned, found {scans:?}"
        )
        .into());
    };
    let total = open_table(uri.clone()).await?.get_file_uris()?.count();
    if (*scanned, total) != expected {
        return Err(format!(
            "expected delta-scan {}/{}, observed {scanned}/{total}",
            expected.0, expected.1
        )
        .into());
    }
    Ok(batches)
}

#[rstest::rstest]
#[case::partitioned(include_str!("layout_partition.test"), true, false)]
#[case::with_statistics(include_str!("layout_stats.test"), false, true)]
#[case::without_statistics(include_str!("layout_stats.test"), false, false)]
#[tokio::test]
async fn persists_physical_layout(
    #[case] script: &str,
    #[case] partitioned: bool,
    #[case] statistics: bool,
) -> LabResult<()> {
    use serde_json::Value;
    let dir = tempfile::tempdir()?;
    let uri = Url::from_directory_path(dir.path()).unwrap();
    // Repeat the exact same two writes and id predicate with skipping statistics disabled.
    let script = if !partitioned && !statistics {
        script
            .replace("stats on", "stats off")
            .replace("1/2", "2/2")
    } else {
        script.to_owned()
    };
    run_fixture("layout.test", &script, &uri).await?;
    let table = open_table(uri).await?;
    let metadata = table.snapshot()?.metadata();
    let partitions: &[&str] = if partitioned { &["p", "q"] } else { &[] };
    assert_eq!(metadata.partition_columns(), partitions);
    let mut found = Vec::new();
    for version in 0..=1 {
        let file = dir.path().join(format!("_delta_log/{version:020}.json"));
        for line in fs::read_to_string(file)?.lines() {
            let action: Value = serde_json::from_str(line)?;
            if let Some(add) = action.get("add") {
                let stats: Value = serde_json::from_str(add["stats"].as_str().unwrap_or("{}"))?;
                assert_eq!(
                    stats.pointer("/minValues/id").is_some(),
                    statistics,
                    "{add}"
                );
                assert_eq!(
                    stats.pointer("/maxValues/id").is_some(),
                    statistics,
                    "{add}"
                );
                found.push(add["partitionValues"].clone());
            }
        }
    }
    let expected = if partitioned {
        serde_json::json!([{"p": "1", "q": "3"}, {"p": "2", "q": "4"}])
    } else {
        serde_json::json!([{}, {}])
    };
    assert_eq!(serde_json::Value::Array(found), expected);
    Ok(())
}

#[rstest::rstest]
#[case("2/2")]
#[case("1/3")]
#[tokio::test]
async fn rejects_wrong_scan_count(#[case] wrong: &str) -> LabResult<()> {
    let dir = tempfile::tempdir()?;
    let uri = Url::from_directory_path(dir.path()).unwrap();
    let script = include_str!("layout_stats.test").replace("1/2", wrong);
    let err = run_fixture("wrong_scan.test", &script, &uri)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("wrong_scan.test:9") && err.contains("observed 1/2"),
        "{err}"
    );
    Ok(())
}
