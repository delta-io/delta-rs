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
fn test_match_filter() {
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
        partition_2021.match_filter(&partition_year_2020_filter),
        false
    );
    assert_eq!(
        partition_2020.match_filter(&partition_year_2020_filter),
        true
    );
    assert_eq!(
        partition_2019.match_filter(&partition_year_2020_filter),
        false
    );
    assert_eq!(
        partition_2019.match_filter(&partition_month_12_filter),
        true
    );
}

#[test]
fn test_match_filters() {
    let partition_2021 = deltalake::DeltaTablePartition {
        key: "year",
        value: "2021",
    };

    let valid_filters = vec![
        deltalake::PartitionFilter {
            key: "year",
            value: deltalake::PartitionValue::Equal("2021"),
        },
        deltalake::PartitionFilter {
            key: "month",
            value: deltalake::PartitionValue::Equal("12"),
        },
    ];

    let invalid_filters = vec![
        deltalake::PartitionFilter {
            key: "year",
            value: deltalake::PartitionValue::Equal("2020"),
        },
        deltalake::PartitionFilter {
            key: "month",
            value: deltalake::PartitionValue::Equal("12"),
        },
    ];

    assert_eq!(partition_2021.match_filters(&valid_filters), true);
    assert_eq!(partition_2021.match_filters(&invalid_filters), false);
}
