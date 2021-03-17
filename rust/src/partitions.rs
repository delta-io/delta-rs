use std::convert::TryFrom;

use crate::DeltaTableError;

#[derive(Clone, PartialEq, Eq)]
pub enum PartitionValue<T> {
    Equal(T),
    NotEqual(T),
    In(Vec<T>),
    NotIn(Vec<T>),
}
#[derive(Clone, Debug, PartialEq)]
pub struct DeltaTablePartition<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> DeltaTablePartition<'a> {
    pub fn filter_accept_partition(&self, filter: &PartitionFilter<&'a str>) -> bool {
        if filter.partition_key != self.key {
            return true;
        }

        match &filter.partition_value {
            PartitionValue::Equal(value) => &self.value == value,
            PartitionValue::NotEqual(value) => &self.value != value,
            PartitionValue::In(value) => value.contains(&self.value),
            PartitionValue::NotIn(value) => !value.contains(&self.value),
        }
    }

    pub fn filters_accept_partition(&self, filters: &Vec<PartitionFilter<&'a str>>) -> bool {
        filters
            .iter()
            .all(|filter| self.filter_accept_partition(filter))
    }
}

impl<'a> TryFrom<&'a str> for DeltaTablePartition<'a> {
    type Error = DeltaTableError;

    fn try_from(partition: &'a str) -> Result<Self, DeltaTableError> {
        match partition.find("=") {
            Some(position) => Ok(DeltaTablePartition {
                key: &partition[0..position],
                value: &partition[position + 1..],
            }),
            _ => Err(DeltaTableError::PartitionError {
                partition: partition.to_string(),
            }),
        }
    }
}

impl<'a, T> TryFrom<(&'a str, &str, T)> for PartitionFilter<'a, T> {
    type Error = DeltaTableError;

    fn try_from(filter: (&'a str, &str, T)) -> Result<Self, DeltaTableError> {
        match filter {
            (key, "=", value) => Ok(PartitionFilter {
                partition_key: key,
                partition_value: PartitionValue::Equal(value),
            }),
            (key, "!=", value) => Ok(PartitionFilter {
                partition_key: key,
                partition_value: PartitionValue::NotEqual(value),
            }),
            (_, op, _) => {
                return Err(DeltaTableError::InvalidOperationFilter {
                    operation_filter: op.to_string(),
                })
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct PartitionFilter<'a, T> {
    pub partition_key: &'a str,
    pub partition_value: PartitionValue<T>,
}
