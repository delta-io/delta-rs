//! Delta Table partition handling logic.

use std::convert::TryFrom;

use super::schema::SchemaDataType;
use crate::errors::DeltaTableError;
use std::cmp::Ordering;
use std::collections::HashMap;

/// A Enum used for selecting the partition value operation when filtering a DeltaTable partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionValue<T> {
    /// The partition value with the equal operator
    Equal(T),
    /// The partition value with the not equal operator
    NotEqual(T),
    /// The partition value with the greater than operator
    GreaterThan(T),
    /// The partition value with the greater than or equal operator
    GreaterThanOrEqual(T),
    /// The partition value with the less than operator
    LessThan(T),
    /// The partition value with the less than or equal operator
    LessThanOrEqual(T),
    /// The partition values with the in operator
    In(Vec<T>),
    /// The partition values with the not in operator
    NotIn(Vec<T>),
}

/// A Struct used for filtering a DeltaTable partition by key and value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionFilter<'a, T> {
    /// The key of the PartitionFilter
    pub key: &'a str,
    /// The value of the PartitionFilter
    pub value: PartitionValue<T>,
}

fn compare_typed_value(
    partition_value: &str,
    filter_value: &str,
    data_type: &SchemaDataType,
) -> Option<Ordering> {
    match data_type {
        SchemaDataType::primitive(primitive_type) => match primitive_type.as_str() {
            "long" | "integer" | "short" | "byte" => match filter_value.parse::<i64>() {
                Ok(parsed_filter_value) => {
                    let parsed_partition_value = partition_value.parse::<i64>().unwrap();
                    parsed_partition_value.partial_cmp(&parsed_filter_value)
                }
                _ => None,
            },
            "float" | "double" => match filter_value.parse::<f64>() {
                Ok(parsed_filter_value) => {
                    let parsed_partition_value = partition_value.parse::<f64>().unwrap();
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
impl<'a> PartitionFilter<'a, &str> {
    /// Indicates if a DeltaTable partition matches with the partition filter by key and value.
    pub fn match_partition(
        &self,
        partition: &DeltaTablePartition<'a>,
        data_type: &SchemaDataType,
    ) -> bool {
        if self.key != partition.key {
            return false;
        }

        match &self.value {
            PartitionValue::Equal(value) => value == &partition.value,
            PartitionValue::NotEqual(value) => value != &partition.value,
            PartitionValue::GreaterThan(value) => {
                compare_typed_value(partition.value, value.to_owned(), data_type)
                    .map(|x| x.is_gt())
                    .unwrap_or(false)
            }
            PartitionValue::GreaterThanOrEqual(value) => {
                compare_typed_value(partition.value, value.to_owned(), data_type)
                    .map(|x| x.is_ge())
                    .unwrap_or(false)
            }
            PartitionValue::LessThan(value) => {
                compare_typed_value(partition.value, value.to_owned(), data_type)
                    .map(|x| x.is_lt())
                    .unwrap_or(false)
            }
            PartitionValue::LessThanOrEqual(value) => {
                compare_typed_value(partition.value, value.to_owned(), data_type)
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
        partitions: &[DeltaTablePartition<'a>],
        partition_col_data_types: &HashMap<&str, &SchemaDataType>,
    ) -> bool {
        let data_type = partition_col_data_types.get(self.key).unwrap().to_owned();
        partitions
            .iter()
            .any(|partition| self.match_partition(partition, data_type))
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, value).
impl<'a, T: std::fmt::Debug> TryFrom<(&'a str, &str, T)> for PartitionFilter<'a, T> {
    type Error = DeltaTableError;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, value).
    /// Returns a DeltaTableError in case of a malformed filter.
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
            (key, ">", value) if !key.is_empty() => Ok(PartitionFilter {
                key,
                value: PartitionValue::GreaterThan(value),
            }),
            (key, ">=", value) if !key.is_empty() => Ok(PartitionFilter {
                key,
                value: PartitionValue::GreaterThanOrEqual(value),
            }),
            (key, "<", value) if !key.is_empty() => Ok(PartitionFilter {
                key,
                value: PartitionValue::LessThan(value),
            }),
            (key, "<=", value) if !key.is_empty() => Ok(PartitionFilter {
                key,
                value: PartitionValue::LessThanOrEqual(value),
            }),
            (_, _, _) => Err(DeltaTableError::InvalidPartitionFilter {
                partition_filter: format!("{filter:?}"),
            }),
        }
    }
}

/// Create a PartitionFilter from a filter Tuple with the structure (key, operation, list(value)).
impl<'a, T: std::fmt::Debug> TryFrom<(&'a str, &str, Vec<T>)> for PartitionFilter<'a, T> {
    type Error = DeltaTableError;

    /// Try to create a PartitionFilter from a Tuple of (key, operation, list(value)).
    /// Returns a DeltaTableError in case of a malformed filter.
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
                partition_filter: format!("{filter:?}"),
            }),
        }
    }
}

/// A Struct DeltaTablePartition used to represent a partition of a DeltaTable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaTablePartition<'a> {
    /// The key of the DeltaTable partition.
    pub key: &'a str,
    /// The value of the DeltaTable partition.
    pub value: &'a str,
}

/**
 * Create a DeltaTable partition from a HivePartition string.
 *
 * A HivePartition string is represented by a "key=value" format.
 *
 * ```rust
 * use deltalake::DeltaTablePartition;
 *
 * let hive_part = "ds=2023-01-01";
 * let partition = DeltaTablePartition::try_from(hive_part).unwrap();
 * assert_eq!("ds", partition.key);
 * assert_eq!("2023-01-01", partition.value);
 * ```
 */
impl<'a> TryFrom<&'a str> for DeltaTablePartition<'a> {
    type Error = DeltaTableError;

    /// Try to create a DeltaTable partition from a HivePartition string.
    /// Returns a DeltaTableError if the string is not in the form of a HivePartition.
    fn try_from(partition: &'a str) -> Result<Self, DeltaTableError> {
        let partition_splitted: Vec<&str> = partition.split('=').collect();
        match partition_splitted {
            partition_splitted if partition_splitted.len() == 2 => Ok(DeltaTablePartition {
                key: partition_splitted[0],
                value: partition_splitted[1],
            }),
            _ => Err(DeltaTableError::PartitionError {
                partition: partition.to_string(),
            }),
        }
    }
}

impl<'a> DeltaTablePartition<'a> {
    /**
     * Try to create a DeltaTable partition from a partition value kv pair.
     *
     * ```rust
     * use deltalake::DeltaTablePartition;
     *
     * let value = (&"ds".to_string(), &Some("2023-01-01".to_string()));
     * let null_default = "1979-01-01";
     * let partition = DeltaTablePartition::from_partition_value(value, null_default);
     *
     * assert_eq!("ds", partition.key);
     * assert_eq!("2023-01-01", partition.value);
     * ```
     */
    pub fn from_partition_value(
        partition_value: (&'a String, &'a Option<String>),
        default_for_null: &'a str,
    ) -> Self {
        let (k, v) = partition_value;
        let v = match v {
            Some(s) => s,
            None => default_for_null,
        };
        DeltaTablePartition { key: k, value: v }
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
