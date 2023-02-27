//! Methods to get Delta Table state in Arrow structures
//!
//! See [crate::table_state::DeltaTableState].

use crate::action::{ColumnCountStat, ColumnValueStat, Stats};
use crate::table_state::DeltaTableState;
use crate::DeltaDataTypeLong;
use crate::DeltaTableError;
use crate::SchemaDataType;
use crate::SchemaTypeStruct;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow::compute::cast;
use arrow::compute::kernels::cast_utils::Parser;
use arrow::datatypes::{DataType, Date32Type, Field, TimeUnit, TimestampMicrosecondType};
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

impl DeltaTableState {
    /// Get an [arrow::record_batch::RecordBatch] containing add action data.
    ///
    /// # Arguments
    ///
    /// * `flatten` - whether to flatten the schema. Partition values columns are
    ///   given the prefix `partition.`, statistics (null_count, min, and max) are
    ///   given the prefix `null_count.`, `min.`, and `max.`, and tags the
    ///   prefix `tags.`. Nested field names are concatenated with `.`.
    ///
    /// # Data schema
    ///
    /// Each row represents a file that is a part of the selected tables state.
    ///
    /// * `path` (String): relative or absolute to a file.
    /// * `size_bytes` (Int64): size of file in bytes.
    /// * `modification_time` (Millisecond Timestamp): time the file was created.
    /// * `data_change` (Boolean): false if data represents data moved from other files
    ///   in the same transaction.
    /// * `partition.{partition column name}` (matches column type): value of
    ///   partition the file corresponds to.
    /// * `null_count.{col_name}` (Int64): number of null values for column in
    ///   this file.
    /// * `min.{col_name}` (matches column type): minimum value of column in file
    ///   (if available).
    /// * `max.{col_name}` (matches column type): maximum value of column in file
    ///   (if available).
    /// * `tag.{tag_key}` (String): value of a metadata tag for the file.
    pub fn add_actions_table(
        &self,
        flatten: bool,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let mut paths = arrow::array::StringBuilder::with_capacity(
            self.files().len(),
            self.files().iter().map(|add| add.path.len()).sum(),
        );
        for action in self.files() {
            paths.append_value(&action.path);
        }

        let size = self
            .files()
            .iter()
            .map(|file| file.size)
            .collect::<Int64Array>();
        let mod_time: TimestampMillisecondArray = self
            .files()
            .iter()
            .map(|file| file.modification_time)
            .collect::<Vec<i64>>()
            .into();
        let data_change = self
            .files()
            .iter()
            .map(|file| Some(file.data_change))
            .collect::<BooleanArray>();

        let mut arrays: Vec<(Cow<str>, ArrayRef)> = vec![
            (Cow::Borrowed("path"), Arc::new(paths.finish())),
            (Cow::Borrowed("size_bytes"), Arc::new(size)),
            (Cow::Borrowed("modification_time"), Arc::new(mod_time)),
            (Cow::Borrowed("data_change"), Arc::new(data_change)),
        ];

        let metadata = self.current_metadata().ok_or(DeltaTableError::NoMetadata)?;

        if !metadata.partition_columns.is_empty() {
            let partition_cols_batch = self.partition_columns_as_batch(flatten)?;
            arrays.extend(
                partition_cols_batch
                    .schema()
                    .fields
                    .iter()
                    .map(|field| Cow::Owned(field.name().clone()))
                    .zip(partition_cols_batch.columns().iter().map(Arc::clone)),
            )
        }

        if self.files().iter().any(|add| add.stats.is_some()) {
            let stats = self.stats_as_batch(flatten)?;
            arrays.extend(
                stats
                    .schema()
                    .fields
                    .iter()
                    .map(|field| Cow::Owned(field.name().clone()))
                    .zip(stats.columns().iter().map(Arc::clone)),
            );
        }

        if self.files().iter().any(|add| {
            add.tags
                .as_ref()
                .map(|tags| !tags.is_empty())
                .unwrap_or(false)
        }) {
            let tags = self.tags_as_batch(flatten)?;
            arrays.extend(
                tags.schema()
                    .fields
                    .iter()
                    .map(|field| Cow::Owned(field.name().clone()))
                    .zip(tags.columns().iter().map(Arc::clone)),
            );
        }

        Ok(arrow::record_batch::RecordBatch::try_from_iter(arrays)?)
    }

    fn partition_columns_as_batch(
        &self,
        flatten: bool,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let metadata = self.current_metadata().ok_or(DeltaTableError::NoMetadata)?;

        let partition_column_types: Vec<arrow::datatypes::DataType> = metadata
            .partition_columns
            .iter()
            .map(
                |name| -> Result<arrow::datatypes::DataType, DeltaTableError> {
                    let field = metadata.schema.get_field_with_name(name)?;
                    Ok(field.get_type().try_into()?)
                },
            )
            .collect::<Result<_, DeltaTableError>>()?;

        // Create builder for each
        let mut builders = metadata
            .partition_columns
            .iter()
            .map(|name| {
                let builder = arrow::array::StringBuilder::new();
                (name.as_str(), builder)
            })
            .collect::<HashMap<&str, _>>();

        // Append values
        for action in self.files() {
            for (name, maybe_value) in action.partition_values.iter() {
                if let Some(value) = maybe_value {
                    builders.get_mut(name.as_str()).unwrap().append_value(value);
                } else {
                    builders.get_mut(name.as_str()).unwrap().append_null();
                }
            }
        }

        // Cast them to their appropriate types
        let partition_columns: Vec<ArrayRef> = metadata
            .partition_columns
            .iter()
            // Get the builders in their original order
            .map(|name| builders.remove(name.as_str()).unwrap())
            .zip(partition_column_types.iter())
            .map(|(mut builder, datatype)| {
                let string_arr: ArrayRef = Arc::new(builder.finish());
                Ok(cast(&string_arr, datatype)?)
            })
            .collect::<Result<_, DeltaTableError>>()?;

        // if flatten, append columns, otherwise combine into a struct column
        let partition_columns: Vec<(Cow<str>, ArrayRef)> = if flatten {
            partition_columns
                .into_iter()
                .zip(metadata.partition_columns.iter())
                .map(|(array, name)| {
                    let name: Cow<str> = Cow::Owned(format!("partition.{name}"));
                    (name, array)
                })
                .collect()
        } else {
            let fields = partition_column_types
                .into_iter()
                .zip(metadata.partition_columns.iter())
                .map(|(datatype, name)| arrow::datatypes::Field::new(name, datatype, true));
            let field_arrays = fields
                .zip(partition_columns.into_iter())
                .collect::<Vec<_>>();
            if field_arrays.is_empty() {
                vec![]
            } else {
                let arr = Arc::new(arrow::array::StructArray::from(field_arrays));
                vec![(Cow::Borrowed("partition_values"), arr)]
            }
        };

        Ok(arrow::record_batch::RecordBatch::try_from_iter(
            partition_columns,
        )?)
    }

    fn tags_as_batch(
        &self,
        flatten: bool,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let tag_keys: HashSet<&str> = self
            .files()
            .iter()
            .flat_map(|add| add.tags.as_ref().map(|tags| tags.keys()))
            .flatten()
            .map(|key| key.as_str())
            .collect();
        let mut builder_map: HashMap<&str, arrow::array::StringBuilder> = tag_keys
            .iter()
            .map(|&key| {
                (
                    key,
                    arrow::array::StringBuilder::with_capacity(self.files().len(), 64),
                )
            })
            .collect();

        for add in self.files() {
            for &key in &tag_keys {
                if let Some(value) = add
                    .tags
                    .as_ref()
                    .and_then(|tags| tags.get(key))
                    .and_then(|val| val.as_deref())
                {
                    builder_map.get_mut(key).unwrap().append_value(value);
                } else {
                    builder_map.get_mut(key).unwrap().append_null();
                }
            }
        }

        let mut arrays: Vec<(&str, ArrayRef)> = builder_map
            .into_iter()
            .map(|(key, mut builder)| (key, Arc::new(builder.finish()) as ArrayRef))
            .collect();
        // Sorted for consistent order
        arrays.sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
        if flatten {
            Ok(arrow::record_batch::RecordBatch::try_from_iter(
                arrays
                    .into_iter()
                    .map(|(key, array)| (format!("tags.{key}"), array)),
            )?)
        } else {
            Ok(arrow::record_batch::RecordBatch::try_from_iter(vec![(
                "tags",
                Arc::new(StructArray::from(
                    arrays
                        .into_iter()
                        .map(|(key, array)| {
                            (Field::new(key, array.data_type().clone(), true), array)
                        })
                        .collect_vec(),
                )) as ArrayRef,
            )])?)
        }
    }

    fn stats_as_batch(
        &self,
        flatten: bool,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let stats: Vec<Option<Stats>> = self
            .files()
            .iter()
            .map(|f| {
                f.get_stats()
                    .map_err(|err| DeltaTableError::InvalidStatsJson { json_err: err })
            })
            .collect::<Result<_, DeltaTableError>>()?;

        let num_records = arrow::array::Int64Array::from(
            stats
                .iter()
                .map(|maybe_stat| maybe_stat.as_ref().map(|stat| stat.num_records))
                .collect::<Vec<Option<i64>>>(),
        );
        let metadata = self.current_metadata().ok_or(DeltaTableError::NoMetadata)?;
        let schema = &metadata.schema;

        #[derive(Debug)]
        struct ColStats<'a> {
            path: Vec<&'a str>,
            null_count: Option<ArrayRef>,
            min_values: Option<ArrayRef>,
            max_values: Option<ArrayRef>,
        }

        let mut columnar_stats: Vec<ColStats> = SchemaLeafIterator::new(schema)
            .filter(|(_path, datatype)| !matches!(datatype, SchemaDataType::r#struct(_)))
            .map(|(path, datatype)| -> Result<ColStats, DeltaTableError> {
                let null_count: Option<ArrayRef> = stats
                    .iter()
                    .flat_map(|maybe_stat| {
                        maybe_stat
                            .as_ref()
                            .map(|stat| resolve_column_count_stat(&stat.null_count, &path))
                    })
                    .collect::<Option<Vec<DeltaDataTypeLong>>>()
                    .map(arrow::array::Int64Array::from)
                    .map(|arr| -> ArrayRef { Arc::new(arr) });

                let arrow_type: arrow::datatypes::DataType = datatype.try_into()?;

                // Min and max are collected for primitive values, not list or maps
                let min_values = if matches!(datatype, SchemaDataType::primitive(_)) {
                    stats
                        .iter()
                        .flat_map(|maybe_stat| {
                            maybe_stat
                                .as_ref()
                                .map(|stat| resolve_column_value_stat(&stat.min_values, &path))
                        })
                        .collect::<Option<Vec<&serde_json::Value>>>()
                        .map(|min_values| {
                            json_value_to_array_general(&arrow_type, min_values.into_iter())
                        })
                        .transpose()?
                } else {
                    None
                };

                let max_values = if matches!(datatype, SchemaDataType::primitive(_)) {
                    stats
                        .iter()
                        .flat_map(|maybe_stat| {
                            maybe_stat
                                .as_ref()
                                .map(|stat| resolve_column_value_stat(&stat.max_values, &path))
                        })
                        .collect::<Option<Vec<&serde_json::Value>>>()
                        .map(|max_values| {
                            json_value_to_array_general(&arrow_type, max_values.into_iter())
                        })
                        .transpose()?
                } else {
                    None
                };

                Ok(ColStats {
                    path,
                    null_count,
                    min_values,
                    max_values,
                })
            })
            .collect::<Result<_, DeltaTableError>>()?;

        let mut out_columns: Vec<(Cow<str>, ArrayRef)> =
            vec![(Cow::Borrowed("num_records"), Arc::new(num_records))];
        if flatten {
            for col_stats in columnar_stats {
                if let Some(null_count) = col_stats.null_count {
                    out_columns.push((
                        Cow::Owned(format!("null_count.{}", col_stats.path.join("."))),
                        null_count,
                    ));
                }
                if let Some(min_values) = col_stats.min_values {
                    out_columns.push((
                        Cow::Owned(format!("min.{}", col_stats.path.join("."))),
                        min_values,
                    ));
                }
                if let Some(max_values) = col_stats.max_values {
                    out_columns.push((
                        Cow::Owned(format!("max.{}", col_stats.path.join("."))),
                        max_values,
                    ));
                }
            }
        } else {
            let mut level = columnar_stats
                .iter()
                .map(|col_stat| col_stat.path.len())
                .max()
                .unwrap_or(0);

            let combine_arrays = |sub_fields: &Vec<ColStats>,
                                  getter: for<'a> fn(&'a ColStats) -> &'a Option<ArrayRef>|
             -> Option<ArrayRef> {
                let fields = sub_fields
                    .iter()
                    .flat_map(|sub_field| {
                        if let Some(values) = getter(sub_field) {
                            let field = Field::new(
                                *sub_field
                                    .path
                                    .last()
                                    .expect("paths must have at least one element"),
                                values.data_type().clone(),
                                false,
                            );
                            Some((field, Arc::clone(values)))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                if fields.is_empty() {
                    None
                } else {
                    Some(Arc::new(StructArray::from(fields)))
                }
            };

            while level > 0 {
                // Starting with most nested level, iteratively group null_count, min_values, max_values
                // into StructArrays, until it is consolidated into a single array.
                columnar_stats = columnar_stats
                    .into_iter()
                    .group_by(|col_stat| {
                        if col_stat.path.len() < level {
                            col_stat.path.clone()
                        } else {
                            col_stat.path[0..(level - 1)].to_vec()
                        }
                    })
                    .into_iter()
                    .map(|(prefix, group)| {
                        let current_fields: Vec<ColStats> = group.into_iter().collect();
                        if current_fields[0].path.len() < level {
                            debug_assert_eq!(current_fields.len(), 1);
                            current_fields.into_iter().next().unwrap()
                        } else {
                            ColStats {
                                path: prefix.to_vec(),
                                null_count: combine_arrays(&current_fields, |sub_field| {
                                    &sub_field.null_count
                                }),
                                min_values: combine_arrays(&current_fields, |sub_field| {
                                    &sub_field.min_values
                                }),
                                max_values: combine_arrays(&current_fields, |sub_field| {
                                    &sub_field.max_values
                                }),
                            }
                        }
                    })
                    .collect();
                level -= 1;
            }
            debug_assert!(columnar_stats.len() == 1);
            debug_assert!(columnar_stats
                .iter()
                .all(|col_stat| col_stat.path.is_empty()));

            if let Some(null_count) = columnar_stats[0].null_count.take() {
                out_columns.push((Cow::Borrowed("null_count"), null_count));
            }
            if let Some(min_values) = columnar_stats[0].min_values.take() {
                out_columns.push((Cow::Borrowed("min"), min_values));
            }
            if let Some(max_values) = columnar_stats[0].max_values.take() {
                out_columns.push((Cow::Borrowed("max"), max_values));
            }
        }

        Ok(arrow::record_batch::RecordBatch::try_from_iter(
            out_columns,
        )?)
    }
}

fn resolve_column_value_stat<'a>(
    values: &'a HashMap<String, ColumnValueStat>,
    path: &[&'a str],
) -> Option<&'a serde_json::Value> {
    let mut current = values;
    let (&name, path) = path.split_last()?;
    for &segment in path {
        current = current.get(segment)?.as_column()?;
    }
    let current = current.get(name)?;
    current.as_value()
}

fn resolve_column_count_stat(
    values: &HashMap<String, ColumnCountStat>,
    path: &[&str],
) -> Option<DeltaDataTypeLong> {
    let mut current = values;
    let (&name, path) = path.split_last()?;
    for &segment in path {
        current = current.get(segment)?.as_column()?;
    }
    let current = current.get(name)?;
    current.as_value()
}

struct SchemaLeafIterator<'a> {
    fields_remaining: VecDeque<(Vec<&'a str>, &'a SchemaDataType)>,
}

impl<'a> SchemaLeafIterator<'a> {
    fn new(schema: &'a SchemaTypeStruct) -> Self {
        SchemaLeafIterator {
            fields_remaining: schema
                .get_fields()
                .iter()
                .map(|field| (vec![field.get_name()], field.get_type()))
                .collect(),
        }
    }
}

impl<'a> std::iter::Iterator for SchemaLeafIterator<'a> {
    type Item = (Vec<&'a str>, &'a SchemaDataType);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((path, datatype)) = self.fields_remaining.pop_front() {
            if let SchemaDataType::r#struct(struct_type) = datatype {
                // push child fields to front
                for field in struct_type.get_fields() {
                    let mut new_path = path.clone();
                    new_path.push(field.get_name());
                    self.fields_remaining
                        .push_front((new_path, field.get_type()));
                }
            };

            Some((path, datatype))
        } else {
            None
        }
    }
}

fn json_value_to_array_general<'a>(
    datatype: &arrow::datatypes::DataType,
    values: impl Iterator<Item = &'a serde_json::Value>,
) -> Result<ArrayRef, DeltaTableError> {
    match datatype {
        DataType::Boolean => Ok(Arc::new(
            values
                .map(|value| value.as_bool())
                .collect::<BooleanArray>(),
        )),
        DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
            let i64_arr: ArrayRef =
                Arc::new(values.map(|value| value.as_i64()).collect::<Int64Array>());
            Ok(arrow::compute::cast(&i64_arr, datatype)?)
        }
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            let f64_arr: ArrayRef =
                Arc::new(values.map(|value| value.as_f64()).collect::<Float64Array>());
            Ok(arrow::compute::cast(&f64_arr, datatype)?)
        }
        DataType::Utf8 => Ok(Arc::new(
            values.map(|value| value.as_str()).collect::<StringArray>(),
        )),
        DataType::Binary => Ok(Arc::new(
            values.map(|value| value.as_str()).collect::<BinaryArray>(),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(Arc::new(TimestampMicrosecondArray::from(
                values
                    .map(|value| value.as_str().and_then(TimestampMicrosecondType::parse))
                    .collect::<Vec<Option<i64>>>(),
            )))
        }
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            values
                .map(|value| value.as_str().and_then(Date32Type::parse))
                .collect::<Vec<Option<i32>>>(),
        ))),
        _ => Err(DeltaTableError::Generic("Invalid datatype".to_string())),
    }
}
