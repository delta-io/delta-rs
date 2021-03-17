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
}

#[test]
fn test_filter_accept_partition() {
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
    let year_key = "year";
    let month_key = "month";
    let year_2020 = "2020";
    let month_12 = "12";

    let partition_year_2020_filter = deltalake::PartitionFilter {
        partition_key: year_key,
        partition_value: deltalake::PartitionValue::Equal(year_2020),
    };
    let partition_month_12_filter = deltalake::PartitionFilter {
        partition_key: month_key,
        partition_value: deltalake::PartitionValue::Equal(month_12),
    };

    assert_eq!(
        partition_2021.filter_accept_partition(&partition_year_2020_filter),
        false
    );
    assert_eq!(
        partition_2020.filter_accept_partition(&partition_year_2020_filter),
        true
    );
    assert_eq!(
        partition_2019.filter_accept_partition(&partition_year_2020_filter),
        false
    );
    assert_eq!(
        partition_2019.filter_accept_partition(&partition_month_12_filter),
        true
    );
}

#[test]
fn test_filters_accept_partition() {
    let partition_2021 = deltalake::DeltaTablePartition {
        key: "year",
        value: "2021",
    };

    let year_key = "year";
    let month_key = "month";
    let month_12 = "12";
    let year_2021 = "2021";
    let year_2022 = "2022";

    let partition_year_2021_filter = deltalake::PartitionFilter {
        partition_key: year_key,
        partition_value: deltalake::PartitionValue::Equal(year_2021),
    };
    let partition_month_12_filter = deltalake::PartitionFilter {
        partition_key: month_key,
        partition_value: deltalake::PartitionValue::Equal(month_12),
    };
    let partition_year_22_filter = deltalake::PartitionFilter {
        partition_key: year_key,
        partition_value: deltalake::PartitionValue::Equal(year_2022),
    };

    assert_eq!(
        partition_2021.filter_accept_partition(&partition_year_2021_filter),
        true
    );
    assert_eq!(
        partition_2021.filter_accept_partition(&partition_month_12_filter),
        true
    );
    assert_eq!(
        partition_2021.filter_accept_partition(&partition_year_22_filter),
        false
    );
}
