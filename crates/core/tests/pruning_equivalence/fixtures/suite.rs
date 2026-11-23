use super::{LabResult, Path, Url, fs, open_table, run_fixture};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    Passing,
    Known,
}

use Group::{Known, Passing};

// Report and replay the same relative names on every platform. file_type()
// also keeps a symlink to an ancestor from turning discovery into a cycle.
fn discover(directory: &Path, prefix: &str, paths: &mut Vec<String>) -> LabResult<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        if !kind.is_dir() && !(kind.is_file() && path.extension().is_some_and(|ext| ext == "test"))
        {
            continue;
        }
        let filename = entry
            .file_name()
            .into_string()
            .map_err(|name| format!("fixture path is not UTF-8: {name:?}"))?;
        let relative = format!("{prefix}{filename}");
        if kind.is_dir() {
            discover(&path, &format!("{relative}/"), paths)?;
        } else {
            paths.push(relative);
        }
    }
    Ok(())
}

async fn run_one(directory: &Path, name: &str) -> LabResult<()> {
    let script = fs::read_to_string(directory.join(name)).map_err(|e| format!("{name}: {e}"))?;
    let dir = tempfile::tempdir().map_err(|e| format!("{name}: {e}"))?;
    let uri = Url::from_directory_path(dir.path()).unwrap();
    run_fixture(name, &script, &uri).await?;
    // A passing fixture must have exercised the persistent provider.
    open_table(uri).await.map_err(|e| format!("{name}: {e}"))?;
    Ok(())
}

async fn run_suite(directory: &Path, group: Group, replay: Option<&str>) -> LabResult<String> {
    let mut paths = Vec::new();
    discover(directory, "", &mut paths)?;
    paths.sort_unstable();
    let discovered = paths.len();
    paths.retain(|name| {
        name.starts_with("known/") == (group == Known) && replay.is_none_or(|want| want == name)
    });

    let selected = paths.len();
    let mut failures = Vec::new();
    let mut executed = 0;
    for name in paths {
        executed += 1;
        if let Err(error) = run_one(directory, &name).await {
            failures.push(error.to_string());
        }
    }
    let summary = format!(
        "discovered={discovered} selected={selected} executed={executed} failing={}",
        failures.len()
    );
    if selected == 0 {
        return Err(format!(
            "{summary}; no fixtures in {group:?} matched DELTA_FIXTURE={replay:?}"
        )
        .into());
    }
    if !failures.is_empty() {
        return Err(format!("{summary}\n{}", failures.join("\n\n")).into());
    }
    Ok(summary)
}

async fn run_group(group: Group) -> LabResult<()> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pruning_equivalence");
    let replay = std::env::var_os("DELTA_FIXTURE")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "DELTA_FIXTURE must be UTF-8")
        })
        .transpose()?;
    let summary = run_suite(&dir, group, replay.as_deref())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    println!("{summary}");
    Ok(())
}

#[tokio::test]
async fn sql_fixtures() -> LabResult<()> {
    run_group(Passing).await
}

#[tokio::test]
#[ignore = "known pruning discrepancies; run a single relative path with DELTA_FIXTURE"]
async fn known_sql_fixtures() -> LabResult<()> {
    run_group(Known).await
}

#[tokio::test]
async fn discovers_groups_and_replays_exact_paths() -> LabResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    fs::create_dir_all(root.join("passing"))?;
    fs::create_dir_all(root.join("known/nested"))?;
    let good = "statement ok\nCREATE TABLE actual AS SELECT 1 AS x\n\nquery I\nSELECT x FROM actual\n----\n1";
    for path in ["top.test", "passing/check.test", "known/nested/check.test"] {
        fs::write(root.join(path), good)?;
    }
    fs::write(root.join("passing/notes.txt"), "ignored")?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(root, root.join("loop.test"))?;

    assert_eq!(
        run_suite(root, Passing, None).await?,
        "discovered=3 selected=2 executed=2 failing=0"
    );
    assert_eq!(
        run_suite(root, Known, Some("known/nested/check.test")).await?,
        "discovered=3 selected=1 executed=1 failing=0"
    );
    assert_eq!(
        run_suite(root, Passing, Some("passing/check.test")).await?,
        "discovered=3 selected=1 executed=1 failing=0"
    );
    for unmatched in [
        "check.test",
        "./passing/check.test",
        "known/nested/check.test",
        "../top.test",
        "",
    ] {
        let error = run_suite(root, Passing, Some(unmatched))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("selected=0") && error.contains(&format!("{unmatched:?}")),
            "{error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn reports_all_fixture_failures_in_sorted_order() -> LabResult<()> {
    let dir = tempfile::tempdir()?;
    fs::create_dir(dir.path().join("passing"))?;
    let bad = "statement ok\nCREATE TABLE actual AS SELECT 1 AS x\n\nquery I\nSELECT x FROM actual\n----\n99";
    fs::write(dir.path().join("z.test"), bad)?;
    fs::write(dir.path().join("passing/a.test"), bad)?;
    let error = run_suite(dir.path(), Passing, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("discovered=2 selected=2 executed=2 failing=2"),
        "{error}"
    );
    let a = error.find("passing/a.test:4").unwrap();
    assert!(a < error.find("z.test:4").unwrap(), "{error}");
    Ok(())
}
