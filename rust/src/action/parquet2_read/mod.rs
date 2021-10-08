//! Parquet deserialization for Action enum

use parquet2::encoding::hybrid_rle;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::DataPage;
use parquet2::read::decompress;
use parquet2::read::get_page_iterator;
use parquet2::read::levels::get_bit_width;

mod boolean;
mod primitive;
mod string;
mod validity;

use super::{Action, Add, MetaData, Protocol, Remove, Txn};
use crate::schema::{
    DeltaDataTypeInt, DeltaDataTypeLong, DeltaDataTypeTimestamp, DeltaDataTypeVersion, Guid,
};
use boolean::for_each_boolean_field_value;
use primitive::for_each_primitive_field_value;
use string::for_each_string_field_value;

/// Parquet deserilization error
#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    /// Generic parsing error
    #[error("{0}")]
    Generic(String),
    /// Invalid action found during parsing
    #[error("Invalid action: {0}")]
    InvalidAction(String),
    /// Error returned when parsing checkpoint parquet using parquet2 crate.
    #[error("Failed to parse parquet: {}", .source)]
    ParquetError {
        /// Parquet error details returned when parsing the checkpoint parquet
        #[from]
        source: parquet2::error::ParquetError,
    },
}

fn split_page<'a>(
    page: &'a DataPage,
    descriptor: &'a ColumnDescriptor,
) -> (i16, hybrid_rle::HybridRleDecoder<'a>, &'a [u8]) {
    let (_rep_levels, def_levels, values_buffer) = parquet2::page::split_buffer(page, descriptor);

    let max_def_level = descriptor.max_def_level();
    let def_bit_width = get_bit_width(max_def_level);
    let validity_iter =
        hybrid_rle::HybridRleDecoder::new(def_levels, def_bit_width, page.num_values());

    (max_def_level, validity_iter, values_buffer)
}

/// Trait for conversion between concrete action struct and Action enum variant
pub trait ActionVariant {
    /// Conrete action struct type
    type Variant;

    /// Return action struct wrapped in corresponding Action enum variant
    fn default_action() -> Action;

    /// Extract action struct from Action enum
    fn try_mut_from_action(a: &mut Action) -> Result<&mut Self, ParseError>;
}

impl ActionVariant for Add {
    type Variant = Add;

    fn default_action() -> Action {
        Action::add(Self::default())
    }

    fn try_mut_from_action(a: &mut Action) -> Result<&mut Self, ParseError> {
        match a {
            Action::add(v) => Ok(v),
            _ => Err(ParseError::Generic(format!(
                "expect Add action, got: {:?}",
                a
            ))),
        }
    }
}

impl ActionVariant for Remove {
    type Variant = Remove;

    fn default_action() -> Action {
        Action::remove(Self::default())
    }

    fn try_mut_from_action(a: &mut Action) -> Result<&mut Self, ParseError> {
        match a {
            Action::remove(v) => Ok(v),
            _ => Err(ParseError::Generic(format!(
                "expect remove action, got: {:?}",
                a
            ))),
        }
    }
}

impl ActionVariant for MetaData {
    type Variant = MetaData;

    fn default_action() -> Action {
        Action::metaData(Self::default())
    }

    fn try_mut_from_action(a: &mut Action) -> Result<&mut Self, ParseError> {
        match a {
            Action::metaData(v) => Ok(v),
            _ => Err(ParseError::Generic(format!(
                "expect metadata action, got: {:?}",
                a
            ))),
        }
    }
}

impl ActionVariant for Txn {
    type Variant = Txn;

    fn default_action() -> Action {
        Action::txn(Self::default())
    }

    fn try_mut_from_action(a: &mut Action) -> Result<&mut Self, ParseError> {
        match a {
            Action::txn(v) => Ok(v),
            _ => Err(ParseError::Generic(format!(
                "expect txn action, got: {:?}",
                a
            ))),
        }
    }
}

impl ActionVariant for Protocol {
    type Variant = Protocol;

    fn default_action() -> Action {
        Action::protocol(Self::default())
    }

    fn try_mut_from_action(a: &mut Action) -> Result<&mut Self, ParseError> {
        match a {
            Action::protocol(v) => Ok(v),
            _ => Err(ParseError::Generic(format!(
                "expect protocol action, got: {:?}",
                a
            ))),
        }
    }
}

fn deserialize_txn_column_page(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "version" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Txn, DeltaDataTypeVersion)| -> () { action.version = v },
            )?;
        }
        "appId" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Txn, String)| -> () { action.app_id = v },
            )?;
        }
        "lastUpdated" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Txn, DeltaDataTypeTimestamp)| -> () {
                    action.last_updated = Some(v)
                },
            )?;
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "Unexpected field `{}` in txn",
                f
            )))
        }
    }
    Ok(())
}

fn deserialize_add_column_page(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "path" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Add, String)| -> () { action.path = v },
            )?;
        }
        "size" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Add, DeltaDataTypeLong)| -> () { action.size = v },
            )?;
        }
        "partitionValues" => {
            // FIXME: support map
        }
        "dataChange" => {
            for_each_boolean_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Add, bool)| -> () { action.data_change = v },
            )?;
        }
        "tags" => {
            // FIXME: support map
        }
        "stats" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Add, String)| -> () { action.stats = Some(v) },
            )?;
        }
        "modificationTime" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Add, DeltaDataTypeTimestamp)| -> () {
                    action.modification_time = v
                },
            )?;
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "Unexpected field `{}` in add",
                f
            )))
        }
    }
    Ok(())
}

fn deserialize_remove_column_page(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "path" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Remove, String)| -> () { action.path = v },
            )?;
        }
        "deletionTimestamp" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Remove, DeltaDataTypeTimestamp)| -> () {
                    action.deletion_timestamp = Some(v)
                },
            )?;
        }
        "size" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Remove, DeltaDataTypeLong)| -> () { action.size = Some(v) },
            )?;
        }
        "partitionValues" => {
            // FIXME: support map
        }
        "dataChange" => {
            for_each_boolean_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Remove, bool)| -> () { action.data_change = v },
            )?;
        }
        "extendedFileMetadata" => {
            for_each_boolean_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Remove, bool)| -> () {
                    action.extended_file_metadata = Some(v)
                },
            )?;
        }
        "tags" => {
            // FIXME: support map
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "Unexpected field `{}` in remove",
                f
            )))
        }
    }
    Ok(())
}

fn deserialize_metadata_column_page(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "id" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut MetaData, Guid)| -> () { action.id = v },
            )?;
        }
        "name" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut MetaData, String)| -> () { action.name = Some(v) },
            )?;
        }
        "description" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut MetaData, String)| -> () { action.description = Some(v) },
            )?;
        }
        "format" => {
            let sub_f = field[1].as_ref();
            match sub_f {
                "provider" => {
                    for_each_string_field_value(
                        actions,
                        page,
                        descriptor,
                        |(action, v): (&mut MetaData, String)| -> () { action.format.provider = v },
                    )?;
                }
                "options" => {
                    // FIXME: parse map
                }
                _ => {
                    return Err(ParseError::InvalidAction(format!(
                        "Unexpected field `{}` in metaData.format",
                        sub_f,
                    )))
                }
            }
        }
        "schemaString" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut MetaData, String)| -> () { action.schema_string = v },
            )?;
        }
        "partitionColumns" => {
            // FIXME: parse string list
        }
        "createdTime" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut MetaData, DeltaDataTypeTimestamp)| -> () {
                    action.created_time = Some(v)
                },
            )?;
        }
        "configuration" => {
            // FIXME: parse map
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "Unexpected field `{}` in metaData",
                f
            )))
        }
    }
    Ok(())
}

fn deserialize_protocol_column_page(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "minReaderVersion" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Protocol, DeltaDataTypeInt)| -> () {
                    action.min_reader_version = v
                },
            )?;
        }
        "minWriterVersion" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |(action, v): (&mut Protocol, DeltaDataTypeInt)| -> () {
                    action.min_writer_version = v
                },
            )?;
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "Unexpected field `{}` in protocol",
                f
            )))
        }
    }
    Ok(())
}

fn deserialize_commit_info_column_page(
    _field: &[String],
    _actions: &mut Vec<Option<Action>>,
    _page: &DataPage,
    _descriptor: &ColumnDescriptor,
) -> Result<(), ParseError> {
    // FIXME: parse map
    Ok(())
}

fn deserialize_cdc_column_page(
    _field: &[String],
    _actions: &mut Vec<Option<Action>>,
    _page: &DataPage,
    _descriptor: &ColumnDescriptor,
) -> Result<(), ParseError> {
    // FIXME: support cdc action
    Ok(())
}

/// Return a vector of action from a given parquet row group
pub fn actions_from_row_group<R: std::io::Read + std::io::Seek>(
    row_group: parquet2::metadata::RowGroupMetaData,
    reader: &mut R,
) -> Result<Vec<Action>, ParseError> {
    let row_count = row_group.num_rows();
    let mut actions: Vec<Option<Action>> = vec![None; row_count as usize];

    for column_metadata in row_group.columns() {
        let column_desc = column_metadata.descriptor();
        let schema_path = column_desc.path_in_schema();

        let deserialize_column_page = match schema_path[0].as_ref() {
            "txn" => deserialize_txn_column_page,
            "add" => deserialize_add_column_page,
            "remove" => deserialize_remove_column_page,
            "metaData" => deserialize_metadata_column_page,
            "protocol" => deserialize_protocol_column_page,
            "commitInfo" => deserialize_commit_info_column_page,
            "cdc" => deserialize_cdc_column_page,
            _ => {
                return Err(ParseError::InvalidAction(format!(
                    "unexpected action: {}",
                    &schema_path[0]
                )));
            }
        };
        let field = &schema_path[1..];

        // FIXME: reuse buffer?
        let buffer = Vec::new();
        let pages = get_page_iterator(column_metadata, reader, None, buffer)?;

        let mut decompress_buffer = vec![];
        for maybe_page in pages {
            // FIXME: leverage null count and skip page if possible
            let page = maybe_page?;
            let page = decompress(page, &mut decompress_buffer)?;
            deserialize_column_page(field, &mut actions, &page, column_desc)?;
        }
    }

    Ok(actions.into_iter().map(|a| a.unwrap()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_add_action_without_partition_values_and_stats() {
        use parquet2::read::read_metadata;

        let path = "./tests/data/delta-0.2.0/_delta_log/00000000000000000003.checkpoint.parquet";
        let mut reader = File::open(path).unwrap();
        let metadata = read_metadata(&mut reader).unwrap();

        for row_group in metadata.row_groups {
            let actions = actions_from_row_group(row_group, &mut reader).unwrap();
            match &actions[9] {
                Action::add(add_action) => {
                    assert_eq!(add_action.partition_values.len(), 0);
                    assert_eq!(add_action.stats, None);
                }
                _ => panic!("expect add action"),
            }
        }
    }
}
