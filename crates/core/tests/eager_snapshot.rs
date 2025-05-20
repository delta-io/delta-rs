use std::collections::HashMap;

use deltalake_core::{
    kernel::{transaction::CommitData, EagerSnapshot},
    protocol::{DeltaOperation, SaveMode},
    test_utils::add_as_remove,
};
use deltalake_test::utils::*;
use itertools::Itertools;

#[tokio::test]
async fn test_eager_snapshot_advance() -> TestResult {
    let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default())?;
    context.load_table(TestTables::Simple).await?;

    let log_store = context.table_builder(TestTables::Simple).build_storage()?;

    let mut snapshot = EagerSnapshot::try_new(&log_store, Default::default(), None).await?;

    let version = snapshot.version();

    let files = snapshot.file_actions()?.enumerate().collect_vec();
    let num_files = files.len();

    let split = files.split(|(idx, _)| *idx == num_files / 2).collect_vec();
    assert!(split.len() == 2 && !split[0].is_empty() && !split[1].is_empty());
    let (first, second) = split.into_iter().next_tuple().unwrap();

    let removes = first
        .iter()
        .map(|(_, add)| add_as_remove(add, add.data_change).into())
        .collect_vec();

    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };

    let actions = vec![CommitData::new(
        removes,
        operation,
        HashMap::new(),
        Vec::new(),
    )];

    let new_version = snapshot.advance(&actions)?;
    assert_eq!(new_version, version + 1);

    let new_files = snapshot.file_actions()?.map(|f| f.path).collect::<Vec<_>>();
    assert_eq!(new_files.len(), num_files - first.len());
    assert!(first
        .iter()
        .all(|(_, add)| { !new_files.contains(&add.path) }));
    assert!(second
        .iter()
        .all(|(_, add)| { new_files.contains(&add.path) }));

    Ok(())
}
