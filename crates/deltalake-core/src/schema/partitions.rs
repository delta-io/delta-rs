//! Delta Table partition handling logic.

use std::convert::TryFrom;

use chrono::{NaiveDateTime, ParseResult};

use crate::errors::DeltaTableError;
use crate::kernel::{DataType, PrimitiveType};
use std::cmp::Ordering;
use std::collections::HashMap;

/// A special value used in Hive to represent the null partition in partitioned tables
pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

/// A Enum used for selecting the partition value operation when filtering a DeltaTable partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionValue {
    /// The partition value with the equal operator
    Equal(String),
    /// The partition value with the not equal operator
    NotEqual(String),
    /// The partition value with the greater than operator
    GreaterThan(String),
    /// The partition value with the greater than or equal operator
    GreaterThanOrEqual(String),
    /// The partition value with the less than operator
    LessThan(String),
    /// The partition value with the less than or equal operator
    LessThanOrEqual(String),
    /// The partition values with the in operator
    In(Vec<String>),
    /// The partition values with the not in operator
    NotIn(Vec<String>),
}

/// A Struct used for filtering a DeltaTable partition by key and value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionFilter {
    /// The key of the PartitionFilter
    pub key: String,
    /// The value of the PartitionFilter
    pub value: PartitionValue,
}

fn parse_timestamp(timestamp_str: &str) -> ParseResult<NaiveDateTime> {
    // Timestamp format as per https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    let format = "%Y-%m-%d %H:%M:%S%.f";

    NaiveDateTime::parse_from_str(timestamp_str, format)
}

fn compare_typed_value(
    partition_value: &str,
    filter_value: &str,
    data_type: &DataType,
) -> Option<Ordering> {
    match data_type {
        DataType::Primitive(primitive_type) => match primitive_type {
            PrimitiveType::Long
            | PrimitiveType::Integer
            | PrimitiveType::Short
            | PrimitiveType::Byte => match filter_value.parse::<i64>() {
                Ok(parsed_filter_value) => {
                    let parsed_partition_value = partition_value.parse::<i64>().unwrap();
                    parsed_partition_value.partial_cmp(&parsed_filter_value)
                }
                _ => None,
            },
            PrimitiveType::Float | PrimitiveType::Double => match filter_value.parse::<f64>() {
                Ok(parsed_filter_value) => {
                    let parsed_partition_value = partition_value.parse::<f64>().unwrap();
                    parsed_partition_value.partial_cmp(&parsed_filter_value)
                }
                _ => None,
            },
            PrimitiveType::Timestamp => match parse_timestamp(filter_value) {
                Ok(parsed_filter_value) => {
                    let parsed_partition_value = parse_timestamp(partition_value).unwrap();
                    parsed_partition_value.partial_cmp(&parsed_filter_value)
                }
                _ => None,
            },
            _ => partition_value.partial_cmp(filter_value),
        },
        _ => partition_value.partial_cmp(filter_value),
    }
}

/// Partition filters methods for filtering the DeltaTable partitions.
impl PartitionFilter {
    /// Indicates if a DeltaTable partition matches with the partition filter by key and value.
    pub fn match_partition(&self, partition: &DeltaTablePartition, data_type: &DataType) -> bool {
        if self.key != partition.key {
            return false;
        }

        match &self.value {
            PartitionValue::Equal(value) => {
                if let DataType::Primitive(PrimitiveType::Timestamp) = data_type {
                    compare_typed_value(&partition.value, value, data_type)
                        .map(|x| x.is_eq())
                        .unwrap_or(false)
                } else {
                    value == &partition.value
                }
            }
            PartitionValue::NotEqual(value) => {
                if let DataType::Primitive(PrimitiveType::Timestamp) = data_type {
                    compare_typed_value(&partition.value, value, data_type)
                        .map(|x| !x.is_eq())
                        .unwrap_or(false)
                } else {
                    value != &partition.value
                }
            }
            PartitionValue::GreaterThan(value) => {
                compare_typed_value(&partition.value, value, data_type)
                    .map(|x| x.is_gt())
                    .unwrap_or(false)
            }
            PartitionValue::GreaterThanOrEqual(value) => {
                compare_typed_value(&partition.value, value, data_type)
                    .map(|x| x.is_ge())
                    .unwrap_or(false)
            }
            PartitionValue::LessThan(value) => {
                compare_typed_value(&partition.value, value, data_type)
                    .map(|x| x.is_lt())
                    .unwrap_or(false)
            }
            PartitionValue::LessThanOrEqual(value) => {
                compare_typed_value(&partition.value, value, data_type)
                    .map(|x| x.is_le())
                    .unwrap_or(false)
            }
            PartitionValue::In(value) => value.contains(&partition.value),
            PartitionValue::NotIn(value) => !value.contains(&partition.value),
        }
    }

    /// Indicates if one of the DeltaTable partition among the list
    /// matches with the partition filter.
    pub fn match_partitions(
        &self,
        partitions: &[DeltaTablePartition],
        partition_col_data_types: &HashMap<&String, &DataType>,
    ) -> bool {
        let data_type = partition_col_data_types.get(&self.key).unwrap().to_owned();
        partitions
            .iter()
            .any(|partition| self.match_partition(partition, data_type))
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, value).
impl TryFrom<(&str, &str, &str)> for PartitionFilter {
    type Error = DeltaTableError;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, value).
    /// Returns a DeltaTableError in case of a malformed filter.
    fn try_from(filter: (&str, &str, &str)) -> Result<Self, DeltaTableError> {
        match filter {
            (key, "=", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::Equal(value.to_owned()),
            }),
            (key, "!=", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::NotEqual(value.to_owned()),
            }),
            (key, ">", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::GreaterThan(value.to_owned()),
            }),
            (key, ">=", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::GreaterThanOrEqual(value.to_owned()),
            }),
            (key, "<", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::LessThan(value.to_owned()),
            }),
            (key, "<=", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::LessThanOrEqual(value.to_owned()),
            }),
            (_, _, _) => Err(DeltaTableError::InvalidPartitionFilter {
                partition_filter: format!("{filter:?}"),
            }),
        }
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, list(value)).
impl TryFrom<(&str, &str, &[&str])> for PartitionFilter {
    type Error = DeltaTableError;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, list(value)).
    /// Returns a DeltaTableError in case of a malformed filter.
    fn try_from(filter: (&str, &str, &[&str])) -> Result<Self, DeltaTableError> {
        match filter {
            (key, "in", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::In(value.iter().map(|x| x.to_string()).collect()),
            }),
            (key, "not in", value) if !key.is_empty() => Ok(PartitionFilter {
                key: key.to_owned(),
                value: PartitionValue::NotIn(value.iter().map(|x| x.to_string()).collect()),
            }),
            (_, _, _) => Err(DeltaTableError::InvalidPartitionFilter {
                partition_filter: format!("{filter:?}"),
            }),
        }
    }
}

/// A Struct DeltaTablePartition used to represent a partition of a DeltaTable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaTablePartition {
    /// The key of the DeltaTable partition.
    pub key: String,
    /// The value of the DeltaTable partition.
    pub value: String,
}

/// Create a DeltaTable partition from a HivePartition string.
///
/// A HivePartition string is represented by a "key=value" format.
///
/// ```rust
/// use deltalake_core::DeltaTablePartition;
///
/// let hive_part = "ds=2023-01-01";
/// let partition = DeltaTablePartition::try_from(hive_part).unwrap();
/// assert_eq!("ds", partition.key);
/// assert_eq!("2023-01-01", partition.value);
/// ```
impl TryFrom<&str> for DeltaTablePartition {
    type Error = DeltaTableError;

    /// Try to create a DeltaTable partition from a HivePartition string.
    /// Returns a DeltaTableError if the string is not in the form of a HivePartition.
    fn try_from(partition: &str) -> Result<Self, DeltaTableError> {
        let partition_splitted: Vec<&str> = partition.split('=').collect();
        match partition_splitted {
            partition_splitted if partition_splitted.len() == 2 => Ok(DeltaTablePartition {
                key: partition_splitted[0].to_owned(),
                value: partition_splitted[1].to_owned(),
            }),
            _ => Err(DeltaTableError::PartitionError {
                partition: partition.to_string(),
            }),
        }
    }
}

impl DeltaTablePartition {
    /// Try to create a DeltaTable partition from a partition value kv pair.
    ///
    /// ```rust
    /// use deltalake_core::DeltaTablePartition;
    ///
    /// let value = ("ds", &Some("2023-01-01".to_string()));
    /// let null_default = "1979-01-01";
    /// let partition = DeltaTablePartition::from_partition_value(value, null_default);
    ///
    /// assert_eq!("ds", partition.key);
    /// assert_eq!("2023-01-01", partition.value);
    /// ```
    pub fn from_partition_value(
        partition_value: (&str, &Option<String>),
        default_for_null: &str,
    ) -> Self {
        let (k, v) = partition_value;
        let v = match v {
            Some(s) => s,
            None => default_for_null,
        };
        DeltaTablePartition {
            key: k.to_owned(),
            value: v.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tryfrom_invalid() {
        let buf = "this-is-not-a-partition";
        let partition = DeltaTablePartition::try_from(buf);
        assert!(partition.is_err());
    }
}
