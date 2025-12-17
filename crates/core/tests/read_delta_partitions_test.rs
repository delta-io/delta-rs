use url::Url;

#[allow(dead_code)]
mod fs_common;

#[tokio::test]
async fn read_null_partitions_from_checkpoint() {
    use deltalake_core::kernel::Add;
    use serde_json::json;
    use std::collections::HashMap;

    let mut table = fs_common::create_table_from_json(
        "../test/tests/data/read_null_partitions_from_checkpoint",
        json!({
            "type": "struct",
            "fields": [
                {"name":"id","type":"integer","metadata":{},"nullable":true},
                {"name":"color","type":"string","metadata":{},"nullable":true},
            ]
        }),
        vec!["color"],
        json!({}),
    )
    .await;

    let delta_log = table
        .table_url()
        .to_file_path()
        .expect("Failed to convert to file path")
        .join("_delta_log");

    let add = |partition: Option<String>| Add {
        partition_values: HashMap::from([("color".to_string(), partition)]),
        ..fs_common::add(0)
    };

    fs_common::commit_add(&mut table, &add(Some("red".to_string()))).await;
    fs_common::commit_add(&mut table, &add(None)).await;
    deltalake_core::checkpoints::create_checkpoint(&table, None)
        .await
        .unwrap();

    // remove 0 version log to explicitly show that metadata is read from cp
    std::fs::remove_file(delta_log.clone().join("00000000000000000000.json")).unwrap();

    let cp = delta_log
        .clone()
        .join("00000000000000000002.checkpoint.parquet");
    assert!(cp.exists());

    // verify that table loads from checkpoint and handles null partitions
    let table = deltalake_core::open_table(table.table_url().clone())
        .await
        .unwrap();
    assert_eq!(table.version(), Some(2));
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn load_from_delta_8_0_table_with_special_partition() {
    use datafusion::physical_plan::SendableRecordBatchStream;
    use deltalake_core::DeltaTable;
    use futures::{StreamExt, future};

    let path = "../test/tests/data/delta-0.8.0-special-partition";
    let table = deltalake_core::open_table(
        Url::from_directory_path(std::fs::canonicalize(path).unwrap()).unwrap(),
    )
    .await
    .unwrap();

    let (_, stream): (DeltaTable, SendableRecordBatchStream) = table
        .scan_table()
        .with_columns(vec!["x", "y"])
        .await
        .unwrap();
    stream
        .for_each(|batch| {
            assert!(batch.is_ok());
            future::ready(())
        })
        .await;
}
