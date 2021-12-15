extern crate deltalake;

use deltalake::action::Add;
use deltalake::schema::SchemaDataType;
use maplit::hashmap;
use serde_json::json;
use std::collections::HashMap;
use std::convert::TryFrom;

#[allow(dead_code)]
mod fs_common;

#[test]
fn test_create_delta_table_partition() {
    let year = "2021";
    let path = format!("year={}", year);
    assert_eq!(
        deltalake::DeltaTablePartition::try_from(path.as_ref()).unwrap(),
        deltalake::DeltaTablePartition {
            key: "year",
            value: year
        }
    );

    let _wrong_path = "year=2021/month=";
    assert!(matches!(
        deltalake::DeltaTablePartition::try_from(_wrong_path).unwrap_err(),
        deltalake::DeltaTableError::PartitionError {
            partition: _wrong_path
        },
    ))
}

#[test]
fn test_match_partition() {
    let partition_2021 = deltalake::DeltaTablePartition {
        key: "year",
        value: "2021",
    };
    let partition_2020 = deltalake::DeltaTablePartition {
        key: "year",
        value: "2020",
    };
    let partition_2019 = deltalake::DeltaTablePartition {
        key: "year",
        value: "2019",
    };

    let partition_year_2020_filter = deltalake::PartitionFilter {
        key: "year",
        value: deltalake::PartitionValue::Equal("2020"),
    };
    let partition_month_12_filter = deltalake::PartitionFilter {
        key: "month",
        value: deltalake::PartitionValue::Equal("12"),
    };
    let string_type = SchemaDataType::primitive(String::from("string"));

    assert_eq!(
        partition_year_2020_filter.match_partition(&partition_2021, &string_type),
        false
    );
    assert_eq!(
        partition_year_2020_filter.match_partition(&partition_2020, &string_type),
        true
    );
    assert_eq!(
        partition_year_2020_filter.match_partition(&partition_2019, &string_type),
        false
    );
    assert_eq!(
        partition_month_12_filter.match_partition(&partition_2019, &string_type),
        false
    );
}

#[test]
fn test_match_filters() {
    let partitions = vec![
        deltalake::DeltaTablePartition {
            key: "year",
            value: "2021",
        },
        deltalake::DeltaTablePartition {
            key: "month",
            value: "12",
        },
    ];

    let string_type = SchemaDataType::primitive(String::from("string"));
    let partition_data_types: HashMap<&str, &SchemaDataType> =
        vec![("year", &string_type), ("month", &string_type)]
            .into_iter()
            .collect();

    let valid_filters = deltalake::PartitionFilter {
        key: "year",
        value: deltalake::PartitionValue::Equal("2021"),
    };

    let valid_filter_month = deltalake::PartitionFilter {
        key: "month",
        value: deltalake::PartitionValue::Equal("12"),
    };

    let invalid_filter = deltalake::PartitionFilter {
        key: "year",
        value: deltalake::PartitionValue::Equal("2020"),
    };

    assert_eq!(
        valid_filters.match_partitions(&partitions, &partition_data_types),
        true
    );
    assert_eq!(
        valid_filter_month.match_partitions(&partitions, &partition_data_types),
        true
    );
    assert_eq!(
        invalid_filter.match_partitions(&partitions, &partition_data_types),
        false
    );
}

#[tokio::test]
async fn read_null_partitions_from_checkpoint() {
    let mut table = fs_common::create_table_from_json(
        "./tests/data/read_null_partitions_from_checkpoint",
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

    println!("{}", table.table_uri);

    let delta_log = std::path::Path::new(&table.table_uri).join("_delta_log");

    let add = |partition: Option<String>| Add {
        partition_values: hashmap! {
            "color".to_string() => partition
        },
        ..fs_common::add(0)
    };

    fs_common::commit_add(&mut table, &add(Some("red".to_string()))).await;
    fs_common::commit_add(&mut table, &add(None)).await;
    deltalake::checkpoints::create_checkpoint(&table)
        .await
        .unwrap();

    // remove 0 version log to explicitly show that metadata is read from cp
    std::fs::remove_file(delta_log.clone().join("00000000000000000000.json")).unwrap();

    let cp = delta_log
        .clone()
        .join("00000000000000000002.checkpoint.parquet");
    assert!(cp.exists());

    // verify that table loads from checkpoint and handles null partitions
    let table = deltalake::open_table(&table.table_uri).await.unwrap();
    assert_eq!(table.version, 2);
}
