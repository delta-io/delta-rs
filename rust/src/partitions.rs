use std::convert::TryFrom;

use crate::DeltaTableError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionValue<T> {
    Equal(T),
    NotEqual(T),
    In(Vec<T>),
    NotIn(Vec<T>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionFilter<'a, T> {
    pub partition_key: &'a str,
    pub partition_value: PartitionValue<T>,
}

impl<'a, T: std::fmt::Debug> TryFrom<(&'a str, &str, T)> for PartitionFilter<'a, T> {
    type Error = DeltaTableError;

    fn try_from(filter: (&'a str, &str, T)) -> Result<Self, DeltaTableError> {
        match filter {
            (key, "=", value) if !key.is_empty() => Ok(PartitionFilter {
                partition_key: key,
                partition_value: PartitionValue::Equal(value),
            }),
            (key, "!=", value) if !key.is_empty() => Ok(PartitionFilter {
                partition_key: key,
                partition_value: PartitionValue::NotEqual(value),
            }),
            (_, _, _) => Err(DeltaTableError::InvalidPartitionFilter {
                partition_filter: format!("{:?}", filter),
            }),
        }
    }
}

impl<'a, T: std::fmt::Debug> TryFrom<(&'a str, &str, Vec<T>)> for PartitionFilter<'a, T> {
    type Error = DeltaTableError;

    fn try_from(filter: (&'a str, &str, Vec<T>)) -> Result<Self, DeltaTableError> {
        match filter {
            (key, "in", value) if !key.is_empty() => Ok(PartitionFilter {
                partition_key: key,
                partition_value: PartitionValue::In(value),
            }),
            (key, "not in", value) if !key.is_empty() => Ok(PartitionFilter {
                partition_key: key,
                partition_value: PartitionValue::NotIn(value),
            }),
            (_, _, _) => Err(DeltaTableError::InvalidPartitionFilter {
                partition_filter: format!("{:?}", filter),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

    pub fn filters_accept_partition(&self, filters: &[PartitionFilter<&'a str>]) -> bool {
        filters
            .iter()
            .all(|filter| self.filter_accept_partition(filter))
    }
}

impl<'a> TryFrom<&'a str> for DeltaTablePartition<'a> {
    type Error = DeltaTableError;

    fn try_from(partition: &'a str) -> Result<Self, DeltaTableError> {
        match partition.find('=') {
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
