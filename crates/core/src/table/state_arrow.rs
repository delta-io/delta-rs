//! Methods to get Delta Table state in Arrow structures
//!
//! See [crate::table::DeltaTableState].

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arrow_array::types::{Date32Type, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, NullArray,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow_cast::cast;
use arrow_cast::parse::Parser;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use delta_kernel::features::ColumnMappingMode;
use itertools::Itertools;

use super::state::DeltaTableState;
use crate::errors::DeltaTableError;
use crate::kernel::{Add, DataType as DeltaDataType, StructType};
use crate::protocol::{ColumnCountStat, ColumnValueStat, Stats};

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
        let files = self.file_actions()?;
        let mut paths = arrow::array::StringBuilder::with_capacity(
            files.len(),
            files.iter().map(|add| add.path.len()).sum(),
        );
        for action in &files {
            paths.append_value(&action.path);
        }

        let size = files.iter().map(|file| file.size).collect::<Int64Array>();
        let mod_time: TimestampMillisecondArray = files
            .iter()
            .map(|file| file.modification_time)
            .collect::<Vec<i64>>()
            .into();
        let data_change = files
            .iter()
            .map(|file| Some(file.data_change))
            .collect::<BooleanArray>();

        let mut arrays: Vec<(Cow<str>, ArrayRef)> = vec![
            (Cow::Borrowed("path"), Arc::new(paths.finish())),
            (Cow::Borrowed("size_bytes"), Arc::new(size)),
            (Cow::Borrowed("modification_time"), Arc::new(mod_time)),
            (Cow::Borrowed("data_change"), Arc::new(data_change)),
        ];

        let metadata = self.metadata();

        if !metadata.partition_columns.is_empty() {
            let partition_cols_batch = self.partition_columns_as_batch(flatten, &files)?;
            arrays.extend(
                partition_cols_batch
                    .schema()
                    .fields
                    .iter()
                    .map(|field| Cow::Owned(field.name().clone()))
                    .zip(partition_cols_batch.columns().iter().cloned()),
            )
        }

        if files.iter().any(|add| add.stats.is_some()) {
            let stats = self.stats_as_batch(flatten)?;
            arrays.extend(
                stats
                    .schema()
                    .fields
                    .iter()
                    .map(|field| Cow::Owned(field.name().clone()))
                    .zip(stats.columns().iter().cloned()),
            );
        }
        if files.iter().any(|add| add.deletion_vector.is_some()) {
            let delvs = self.deletion_vectors_as_batch(flatten, &files)?;
            arrays.extend(
                delvs
                    .schema()
                    .fields
                    .iter()
                    .map(|field| Cow::Owned(field.name().clone()))
                    .zip(delvs.columns().iter().cloned()),
            );
        }
        if files.iter().any(|add| {
            add.tags
                .as_ref()
                .map(|tags| !tags.is_empty())
                .unwrap_or(false)
        }) {
            let tags = self.tags_as_batch(flatten, &files)?;
            arrays.extend(
                tags.schema()
                    .fields
                    .iter()
                    .map(|field| Cow::Owned(field.name().clone()))
                    .zip(tags.columns().iter().cloned()),
            );
        }

        Ok(arrow::record_batch::RecordBatch::try_from_iter(arrays)?)
    }

    fn partition_columns_as_batch(
        &self,
        flatten: bool,
        files: &Vec<Add>,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let metadata = self.metadata();
        let column_mapping_mode = self.table_config().column_mapping_mode();
        let partition_column_types: Vec<arrow::datatypes::DataType> = metadata
            .partition_columns
            .iter()
            .map(
                |name| -> Result<arrow::datatypes::DataType, DeltaTableError> {
                    let schema = metadata.schema()?;
                    let field =
                        schema
                            .field(name)
                            .ok_or(DeltaTableError::MetadataError(format!(
                                "Invalid partition column {0}",
                                name
                            )))?;
                    Ok(field.data_type().try_into()?)
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

        let physical_name_to_logical_name = match column_mapping_mode {
            ColumnMappingMode::None => HashMap::with_capacity(0), // No column mapping, no need for this HashMap
            ColumnMappingMode::Id | ColumnMappingMode::Name => metadata
                .partition_columns
                .iter()
                .map(|name| -> Result<_, DeltaTableError> {
                    let physical_name = self
                        .schema()
                        .field(name)
                        .ok_or(DeltaTableError::MetadataError(format!(
                            "Invalid partition column {0}",
                            name
                        )))?
                        .physical_name(column_mapping_mode)?
                        .to_string();
                    Ok((physical_name, name.as_str()))
                })
                .collect::<Result<HashMap<String, &str>, DeltaTableError>>()?,
        };
        // Append values
        for action in files {
            for (name, maybe_value) in action.partition_values.iter() {
                let logical_name = match column_mapping_mode {
                    ColumnMappingMode::None => name.as_str(),
                    ColumnMappingMode::Id | ColumnMappingMode::Name => {
                        physical_name_to_logical_name.get(name.as_str()).ok_or(
                            DeltaTableError::MetadataError(format!(
                                "Invalid partition column {0}",
                                name
                            )),
                        )?
                    }
                };
                if let Some(value) = maybe_value {
                    builders.get_mut(logical_name).unwrap().append_value(value);
                // Unwrap is safe here since the name exists in the mapping where we check validity already
                } else {
                    builders.get_mut(logical_name).unwrap().append_null();
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
                .map(|(datatype, name)| arrow::datatypes::Field::new(name, datatype, true))
                .collect::<Vec<_>>();

            if fields.is_empty() {
                vec![]
            } else {
                let arr = Arc::new(arrow::array::StructArray::try_new(
                    Fields::from(fields),
                    partition_columns,
                    None,
                )?);
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
        files: &Vec<Add>,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let tag_keys: HashSet<&str> = files
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
                    arrow::array::StringBuilder::with_capacity(files.len(), 64),
                )
            })
            .collect();

        for add in files {
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
            let (fields, arrays): (Vec<_>, Vec<_>) = arrays
                .into_iter()
                .map(|(key, array)| (Field::new(key, array.data_type().clone(), true), array))
                .unzip();
            Ok(arrow::record_batch::RecordBatch::try_from_iter(vec![(
                "tags",
                Arc::new(StructArray::new(Fields::from(fields), arrays, None)) as ArrayRef,
            )])?)
        }
    }

    fn deletion_vectors_as_batch(
        &self,
        flatten: bool,
        files: &Vec<Add>,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let capacity = files.len();
        let mut storage_type = arrow::array::StringBuilder::with_capacity(capacity, 1);
        let mut path_or_inline_div = arrow::array::StringBuilder::with_capacity(capacity, 64);
        let mut offset = arrow::array::Int32Builder::with_capacity(capacity);
        let mut size_in_bytes = arrow::array::Int32Builder::with_capacity(capacity);
        let mut cardinality = arrow::array::Int64Builder::with_capacity(capacity);

        for add in files {
            if let Some(value) = &add.deletion_vector {
                storage_type.append_value(value.storage_type);
                path_or_inline_div.append_value(value.path_or_inline_dv.clone());
                if let Some(ofs) = value.offset {
                    offset.append_value(ofs);
                } else {
                    offset.append_null();
                }
                size_in_bytes.append_value(value.size_in_bytes);
                cardinality.append_value(value.cardinality);
            } else {
                storage_type.append_null();
                path_or_inline_div.append_null();
                offset.append_null();
                size_in_bytes.append_null();
                cardinality.append_null();
            }
        }
        if flatten {
            Ok(arrow::record_batch::RecordBatch::try_from_iter(vec![
                (
                    "deletionVector.storageType",
                    Arc::new(storage_type.finish()) as ArrayRef,
                ),
                (
                    "deletionVector.pathOrInlineDiv",
                    Arc::new(path_or_inline_div.finish()) as ArrayRef,
                ),
                (
                    "deletionVector.offset",
                    Arc::new(offset.finish()) as ArrayRef,
                ),
                (
                    "deletionVector.sizeInBytes",
                    Arc::new(size_in_bytes.finish()) as ArrayRef,
                ),
                (
                    "deletionVector.cardinality",
                    Arc::new(cardinality.finish()) as ArrayRef,
                ),
            ])?)
        } else {
            Ok(arrow::record_batch::RecordBatch::try_from_iter(vec![(
                "deletionVector",
                Arc::new(StructArray::new(
                    Fields::from(vec![
                        Field::new("storageType", DataType::Utf8, false),
                        Field::new("pathOrInlineDiv", DataType::Utf8, false),
                        Field::new("offset", DataType::Int32, true),
                        Field::new("sizeInBytes", DataType::Int32, false),
                        Field::new("cardinality", DataType::Int64, false),
                    ]),
                    vec![
                        Arc::new(storage_type.finish()) as ArrayRef,
                        Arc::new(path_or_inline_div.finish()) as ArrayRef,
                        Arc::new(offset.finish()) as ArrayRef,
                        Arc::new(size_in_bytes.finish()) as ArrayRef,
                        Arc::new(cardinality.finish()) as ArrayRef,
                    ],
                    None,
                )) as ArrayRef,
            )])?)
        }
    }

    fn stats_as_batch(
        &self,
        flatten: bool,
    ) -> Result<arrow::record_batch::RecordBatch, DeltaTableError> {
        let stats: Vec<Option<Stats>> = self
            .file_actions_iter()?
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
        let schema = self.schema();

        #[derive(Debug)]
        struct ColStats<'a> {
            path: Vec<&'a str>,
            null_count: Option<ArrayRef>,
            min_values: Option<ArrayRef>,
            max_values: Option<ArrayRef>,
        }

        let filter_out_empty_stats = |stats: &Result<ColStats, DeltaTableError>| -> bool {
            let is_field_empty = |arr: &Option<ArrayRef>| -> bool {
                match arr {
                    Some(arr) => arr.len() == arr.null_count(),
                    None => true,
                }
            };

            let is_stats_empty = |stats: &ColStats| -> bool {
                is_field_empty(&stats.null_count)
                    && is_field_empty(&stats.min_values)
                    && is_field_empty(&stats.max_values)
            };

            if let Ok(stats) = stats {
                !is_stats_empty(stats)
            } else {
                true // keep errs
            }
        };

        let mut columnar_stats: Vec<ColStats> = SchemaLeafIterator::new(schema)
            .filter(|(_path, datatype)| !matches!(datatype, DeltaDataType::Struct(_)))
            .map(|(path, datatype)| -> Result<ColStats, DeltaTableError> {
                let null_count = stats
                    .iter()
                    .map(|maybe_stat| {
                        maybe_stat
                            .as_ref()
                            .map(|stat| resolve_column_count_stat(&stat.null_count, &path))
                    })
                    .map(|null_count| null_count.flatten())
                    .collect::<Vec<Option<i64>>>();
                let null_count = Some(value_vec_to_array(null_count, |values| {
                    Ok(Arc::new(arrow::array::Int64Array::from(values)))
                })?);

                let arrow_type: arrow::datatypes::DataType = datatype.try_into()?;

                // Min and max are collected for primitive values, not list or maps
                let min_values = if matches!(datatype, DeltaDataType::Primitive(_)) {
                    let min_values = stats
                        .iter()
                        .map(|maybe_stat| {
                            maybe_stat
                                .as_ref()
                                .map(|stat| resolve_column_value_stat(&stat.min_values, &path))
                        })
                        .map(|min_value| min_value.flatten())
                        .collect::<Vec<Option<&serde_json::Value>>>();

                    Some(value_vec_to_array(min_values, |values| {
                        json_value_to_array_general(&arrow_type, values.into_iter())
                    })?)
                } else {
                    None
                };

                let max_values = if matches!(datatype, DeltaDataType::Primitive(_)) {
                    let max_values = stats
                        .iter()
                        .map(|maybe_stat| {
                            maybe_stat
                                .as_ref()
                                .map(|stat| resolve_column_value_stat(&stat.max_values, &path))
                        })
                        .map(|max_value| max_value.flatten())
                        .collect::<Vec<Option<&serde_json::Value>>>();
                    Some(value_vec_to_array(max_values, |values| {
                        json_value_to_array_general(&arrow_type, values.into_iter())
                    })?)
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
            .filter(filter_out_empty_stats)
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
                let (fields, arrays): (Vec<_>, Vec<_>) = sub_fields
                    .iter()
                    .flat_map(|sub_field| {
                        if let Some(values) = getter(sub_field) {
                            let field = Field::new(
                                *sub_field
                                    .path
                                    .last()
                                    .expect("paths must have at least one element"),
                                values.data_type().clone(),
                                true,
                            );
                            Some((field, Arc::clone(values)))
                        } else {
                            None
                        }
                    })
                    .unzip();
                if fields.is_empty() {
                    None
                } else {
                    Some(Arc::new(StructArray::new(
                        Fields::from(fields),
                        arrays,
                        None,
                    )))
                }
            };

            while level > 0 {
                // Starting with most nested level, iteratively group null_count, min_values, max_values
                // into StructArrays, until it is consolidated into a single array.
                columnar_stats = columnar_stats
                    .into_iter()
                    .chunk_by(|col_stat| {
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

fn value_vec_to_array<T, F>(
    value_vec: Vec<Option<T>>,
    map_fn: F,
) -> Result<ArrayRef, DeltaTableError>
where
    F: FnOnce(Vec<Option<T>>) -> Result<ArrayRef, DeltaTableError>,
{
    if value_vec.iter().all(Option::is_none) {
        Ok(Arc::new(NullArray::new(value_vec.len())))
    } else {
        map_fn(value_vec)
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
) -> Option<i64> {
    let mut current = values;
    let (&name, path) = path.split_last()?;
    for &segment in path {
        current = current.get(segment)?.as_column()?;
    }
    let current = current.get(name)?;
    current.as_value()
}

struct SchemaLeafIterator<'a> {
    fields_remaining: VecDeque<(Vec<&'a str>, &'a DeltaDataType)>,
}

impl<'a> SchemaLeafIterator<'a> {
    fn new(schema: &'a StructType) -> Self {
        SchemaLeafIterator {
            fields_remaining: schema
                .fields()
                .map(|field| (vec![field.name().as_ref()], field.data_type()))
                .collect(),
        }
    }
}

impl<'a> std::iter::Iterator for SchemaLeafIterator<'a> {
    type Item = (Vec<&'a str>, &'a DeltaDataType);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((path, datatype)) = self.fields_remaining.pop_front() {
            if let DeltaDataType::Struct(struct_type) = datatype {
                // push child fields to front
                for field in struct_type.fields() {
                    let mut new_path = path.clone();
                    new_path.push(field.name());
                    self.fields_remaining
                        .push_front((new_path, field.data_type()));
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
    values: impl Iterator<Item = Option<&'a serde_json::Value>>,
) -> Result<ArrayRef, DeltaTableError> {
    match datatype {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            values
                .map(|value| value.and_then(serde_json::Value::as_bool))
                .collect_vec(),
        ))),
        DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
            let i64_arr: ArrayRef = Arc::new(Int64Array::from(
                values
                    .map(|value| value.and_then(serde_json::Value::as_i64))
                    .collect_vec(),
            ));
            Ok(arrow::compute::cast(&i64_arr, datatype)?)
        }
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            let f64_arr: ArrayRef = Arc::new(Float64Array::from(
                values
                    .map(|value| value.and_then(serde_json::Value::as_f64))
                    .collect_vec(),
            ));
            Ok(arrow::compute::cast(&f64_arr, datatype)?)
        }
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .map(|value| value.and_then(serde_json::Value::as_str))
                .collect_vec(),
        ))),
        DataType::Binary => Ok(Arc::new(BinaryArray::from(
            values
                .map(|value| value.and_then(|value| value.as_str().map(|value| value.as_bytes())))
                .collect_vec(),
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => match tz {
            None => Ok(Arc::new(TimestampMicrosecondArray::from(
                values
                    .map(|value| {
                        value.and_then(|value| {
                            value.as_str().and_then(TimestampMicrosecondType::parse)
                        })
                    })
                    .collect_vec(),
            ))),
            Some(tz_str) if tz_str.as_ref() == "UTC" => Ok(Arc::new(
                TimestampMicrosecondArray::from(
                    values
                        .map(|value| {
                            value.and_then(|value| {
                                value.as_str().and_then(TimestampMicrosecondType::parse)
                            })
                        })
                        .collect_vec(),
                )
                .with_timezone("UTC"),
            )),
            _ => Err(DeltaTableError::Generic(format!(
                "Invalid datatype {}",
                datatype
            ))),
        },
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            values
                .map(|value| value.and_then(|value| value.as_str().and_then(Date32Type::parse)))
                .collect_vec(),
        ))),
        _ => Err(DeltaTableError::Generic(format!(
            "Invalid datatype {}",
            datatype
        ))),
    }
}
