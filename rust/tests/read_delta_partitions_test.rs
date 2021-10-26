extern crate deltalake;

use std::convert::TryFrom;

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

    assert_eq!(
        partition_year_2020_filter.match_partition(&partition_2021),
        false
    );
    assert_eq!(
        partition_year_2020_filter.match_partition(&partition_2020),
        true
    );
    assert_eq!(
        partition_year_2020_filter.match_partition(&partition_2019),
        false
    );
    assert_eq!(
        partition_month_12_filter.match_partition(&partition_2019),
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

    assert_eq!(valid_filters.match_partitions(&partitions), true);
    assert_eq!(valid_filter_month.match_partitions(&partitions), true);
    assert_eq!(invalid_filter.match_partitions(&partitions), false);
}
