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
    pub key: &'a str,
    pub value: PartitionValue<T>,
}

impl<'a> PartitionFilter<'a, &str> {
    pub fn match_partition(&self, partition: &DeltaTablePartition<'a>) -> bool {
        if self.key != partition.key {
            return false;
        }

        match &self.value {
            PartitionValue::Equal(value) => value == &partition.value,
            PartitionValue::NotEqual(value) => value != &partition.value,
            PartitionValue::In(value) => value.contains(&partition.value),
            PartitionValue::NotIn(value) => !value.contains(&partition.value),
        }
    }

    pub fn match_partitions(&self, partitions: &[DeltaTablePartition<'a>]) -> bool {
        partitions
            .iter()
            .any(|partition| self.match_partition(&partition))
    }
}

impl<'a, T: std::fmt::Debug> TryFrom<(&'a str, &str, T)> for PartitionFilter<'a, T> {
    type Error = DeltaTableError;

    fn try_from(filter: (&'a str, &str, T)) -> Result<Self, DeltaTableError> {
        match filter {
            (key, "=", value) if !key.is_empty() => Ok(PartitionFilter {
                key,
                value: PartitionValue::Equal(value),
            }),
            (key, "!=", value) if !key.is_empty() => Ok(PartitionFilter {
                key,
                value: PartitionValue::NotEqual(value),
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
                key,
                value: PartitionValue::In(value),
            }),
            (key, "not in", value) if !key.is_empty() => Ok(PartitionFilter {
                key,
                value: PartitionValue::NotIn(value),
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

impl<'a> TryFrom<&'a str> for DeltaTablePartition<'a> {
    type Error = DeltaTableError;

    fn try_from(partition: &'a str) -> Result<Self, DeltaTableError> {
        let partition_splitted: Vec<&str> = partition.split('=').collect();
        match partition_splitted {
            partition_splitted if partition_splitted.len() == 2 => Ok(DeltaTablePartition {
                key: &partition_splitted[0],
                value: &partition_splitted[1],
            }),
            _ => Err(DeltaTableError::PartitionError {
                partition: partition.to_string(),
            }),
        }
    }
}
