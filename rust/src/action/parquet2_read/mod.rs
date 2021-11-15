//! Parquet deserialization for Action enum

use std::collections::HashMap;

use log::warn;
use parquet2::encoding::hybrid_rle;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::DataPage;
use parquet2::read::decompress;
use parquet2::read::get_page_iterator;
use parquet2::read::levels::get_bit_width;

mod boolean;
mod map;
mod primitive;
mod string;
mod validity;

use crate::action::{Action, Add, CommitInfo, MetaData, Protocol, Remove, Txn};
use crate::schema::{
    DeltaDataTypeInt, DeltaDataTypeLong, DeltaDataTypeTimestamp, DeltaDataTypeVersion, Guid,
};
use boolean::for_each_boolean_field_value;
use map::for_each_map_field_value;
use primitive::for_each_primitive_field_value;
use string::{for_each_repeated_string_field_value, for_each_string_field_value};

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

#[derive(Default)]
struct DeserState {
    add_partition_values: map::MapState,
    add_tags: map::MapState,
    remove_partition_values: map::MapState,
    remove_tags: map::MapState,
    metadata_fromat_options: map::MapState,
    metadata_configuration: map::MapState,
}

fn hashmap_from_kvpairs<Key, Val>(
    keys: impl IntoIterator<Item = Key>,
    values: impl IntoIterator<Item = Val>,
) -> HashMap<Key, Val>
where
    Key: std::hash::Hash + std::cmp::Eq,
{
    keys.into_iter().zip(values.into_iter()).collect()
}

fn split_page<'a>(
    page: &'a DataPage,
    descriptor: &'a ColumnDescriptor,
) -> (i16, hybrid_rle::HybridRleDecoder<'a>, &'a [u8]) {
    let (_rep_levels, def_levels_buf, values_buf) = parquet2::page::split_buffer(page, descriptor);

    let max_def_level = descriptor.max_def_level();
    let def_bit_width = get_bit_width(max_def_level);
    let validity_iter =
        hybrid_rle::HybridRleDecoder::new(def_levels_buf, def_bit_width, page.num_values());

    (max_def_level, validity_iter, values_buf)
}

fn split_page_nested<'a>(
    page: &'a DataPage,
    descriptor: &'a ColumnDescriptor,
) -> (
    i16,
    hybrid_rle::HybridRleDecoder<'a>,
    i16,
    hybrid_rle::HybridRleDecoder<'a>,
    &'a [u8],
) {
    let (rep_levels, def_levels_buf, values_buf) = parquet2::page::split_buffer(page, descriptor);

    let max_rep_level = descriptor.max_rep_level();
    let rep_bit_width = get_bit_width(max_rep_level);
    let rep_iter = hybrid_rle::HybridRleDecoder::new(rep_levels, rep_bit_width, page.num_values());

    let max_def_level = descriptor.max_def_level();
    let def_bit_width = get_bit_width(max_def_level);
    let validity_iter =
        hybrid_rle::HybridRleDecoder::new(def_levels_buf, def_bit_width, page.num_values());

    (
        max_rep_level,
        rep_iter,
        max_def_level,
        validity_iter,
        values_buf,
    )
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

impl ActionVariant for CommitInfo {
    type Variant = CommitInfo;

    fn default_action() -> Action {
        Action::commitInfo(CommitInfo::new())
    }

    fn try_mut_from_action(a: &mut Action) -> Result<&mut Self, ParseError> {
        match a {
            Action::commitInfo(v) => Ok(v),
            _ => Err(ParseError::Generic(format!(
                "expect commitInfo action, got: {:?}",
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
    _state: &mut DeserState,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "version" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Txn, v: DeltaDataTypeVersion| action.version = v,
            )?;
        }
        "appId" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Txn, v: String| action.app_id = v,
            )?;
        }
        "lastUpdated" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Txn, v: DeltaDataTypeTimestamp| action.last_updated = Some(v),
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
    state: &mut DeserState,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "path" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Add, v: String| action.path = v,
            )?;
        }
        "size" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Add, v: DeltaDataTypeLong| action.size = v,
            )?;
        }
        "partitionValues" => {
            for_each_map_field_value(
                &field[1..],
                actions,
                page,
                descriptor,
                &mut state.add_partition_values,
                |action: &mut Add, v: (Vec<String>, Vec<Option<String>>)| {
                    action.partition_values = hashmap_from_kvpairs(v.0, v.1);
                },
            )?;
        }
        // FIXME suport partitionValueParsed
        "dataChange" => {
            for_each_boolean_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Add, v: bool| action.data_change = v,
            )?;
        }
        "tags" => {
            for_each_map_field_value(
                &field[1..],
                actions,
                page,
                descriptor,
                &mut state.add_tags,
                |action: &mut Add, v: (Vec<String>, Vec<Option<String>>)| {
                    action.tags = Some(hashmap_from_kvpairs(v.0, v.1));
                },
            )?;
        }
        // FIXME: support statsParsed
        "stats" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Add, v: String| action.stats = Some(v),
            )?;
        }
        "modificationTime" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Add, v: DeltaDataTypeTimestamp| action.modification_time = v,
            )?;
        }
        _ => {
            warn!("Unexpected field `{}` in add", f);
        }
    }
    Ok(())
}

fn deserialize_remove_column_page(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    state: &mut DeserState,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "path" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Remove, v: String| action.path = v,
            )?;
        }
        "deletionTimestamp" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Remove, v: DeltaDataTypeTimestamp| {
                    action.deletion_timestamp = Some(v)
                },
            )?;
        }
        "size" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Remove, v: DeltaDataTypeLong| action.size = Some(v),
            )?;
        }
        // FIXME suport partitionValueParsed
        "partitionValues" => {
            for_each_map_field_value(
                &field[1..],
                actions,
                page,
                descriptor,
                &mut state.remove_partition_values,
                |action: &mut Remove, v: (Vec<String>, Vec<Option<String>>)| {
                    action.partition_values = Some(hashmap_from_kvpairs(v.0, v.1));
                },
            )?;
        }
        "dataChange" => {
            for_each_boolean_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Remove, v: bool| action.data_change = v,
            )?;
        }
        "extendedFileMetadata" => {
            for_each_boolean_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Remove, v: bool| action.extended_file_metadata = Some(v),
            )?;
        }
        "tags" => {
            for_each_map_field_value(
                &field[1..],
                actions,
                page,
                descriptor,
                &mut state.remove_tags,
                |action: &mut Remove, v: (Vec<String>, Vec<Option<String>>)| {
                    action.tags = Some(hashmap_from_kvpairs(v.0, v.1));
                },
            )?;
        }
        _ => {
            warn!("Unexpected field `{}` in remove", f);
        }
    }
    Ok(())
}

fn deserialize_metadata_column_page(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    state: &mut DeserState,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "id" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |action: &mut MetaData, v: Guid| action.id = v,
            )?;
        }
        "name" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |action: &mut MetaData, v: String| action.name = Some(v),
            )?;
        }
        "description" => {
            for_each_string_field_value(
                actions,
                page,
                descriptor,
                |action: &mut MetaData, v: String| action.description = Some(v),
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
                        |action: &mut MetaData, v: String| action.format.provider = v,
                    )?;
                }
                "options" => {
                    for_each_map_field_value(
                        &field[2..],
                        actions,
                        page,
                        descriptor,
                        &mut state.metadata_fromat_options,
                        |action: &mut MetaData, v: (Vec<String>, Vec<Option<String>>)| {
                            action.format.options = hashmap_from_kvpairs(v.0, v.1);
                        },
                    )?;
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
                |action: &mut MetaData, v: String| action.schema_string = v,
            )?;
        }
        "partitionColumns" => {
            for_each_repeated_string_field_value(
                actions,
                page,
                descriptor,
                |action: &mut MetaData, v: Vec<String>| action.partition_columns = v,
            )?;
        }
        "createdTime" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut MetaData, v: DeltaDataTypeTimestamp| action.created_time = Some(v),
            )?;
        }
        "configuration" => {
            for_each_map_field_value(
                &field[1..],
                actions,
                page,
                descriptor,
                &mut state.metadata_configuration,
                |action: &mut MetaData, v: (Vec<String>, Vec<Option<String>>)| {
                    action.configuration = hashmap_from_kvpairs(v.0, v.1);
                },
            )?;
        }
        _ => {
            warn!("Unexpected field `{}` in metaData", f);
        }
    }
    Ok(())
}

fn deserialize_protocol_column_page(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    _state: &mut DeserState,
) -> Result<(), ParseError> {
    let f = field[0].as_ref();
    match f {
        "minReaderVersion" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Protocol, v: DeltaDataTypeInt| action.min_reader_version = v,
            )?;
        }
        "minWriterVersion" => {
            for_each_primitive_field_value(
                actions,
                page,
                descriptor,
                |action: &mut Protocol, v: DeltaDataTypeInt| action.min_writer_version = v,
            )?;
        }
        _ => {
            warn!("Unexpected field `{}` in protocol", f);
        }
    }
    Ok(())
}

fn deserialize_commit_info_column_page(
    _obj_keys: &[String],
    _actions: &mut Vec<Option<Action>>,
    _page: &DataPage,
    _descriptor: &ColumnDescriptor,
    _state: &mut DeserState,
) -> Result<(), ParseError> {
    // parquet snapshots shouldn't contain commit info
    Ok(())
}

fn deserialize_cdc_column_page(
    _field: &[String],
    _actions: &mut Vec<Option<Action>>,
    _page: &DataPage,
    _descriptor: &ColumnDescriptor,
    _state: &mut DeserState,
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
    // TODO: reuse actions buffer
    let mut actions: Vec<Option<Action>> = vec![None; row_count as usize];
    let mut state = DeserState::default();

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

        // FIXME: reuse buffer between loops
        let buffer = Vec::new();
        let pages = get_page_iterator(column_metadata, &mut *reader, None, buffer)?;

        let mut decompress_buffer = vec![];
        for maybe_page in pages {
            // FIXME: leverage null count and skip page if possible
            let page = maybe_page?;
            let page = decompress(page, &mut decompress_buffer)?;
            deserialize_column_page(field, &mut actions, &page, column_desc, &mut state)?;
        }
    }

    Ok(actions.into_iter().map(|a| a.unwrap()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::File;

    #[test]
    fn test_add_action_without_partition_values_and_stats() {
        use parquet2::read::read_metadata;

        let path = "./tests/data/delta-0.2.0/_delta_log/00000000000000000003.checkpoint.parquet";
        let mut reader = File::open(path).unwrap();
        let metadata = read_metadata(&mut reader).unwrap();

        for row_group in metadata.row_groups {
            let actions = actions_from_row_group(row_group, &mut reader).unwrap();
            match &actions[0] {
                Action::protocol(protocol) => {
                    assert_eq!(protocol.min_reader_version, 1,);
                    assert_eq!(protocol.min_writer_version, 2,);
                }
                _ => panic!("expect protocol action"),
            }
            match &actions[1] {
                Action::metaData(metaData) => {
                    assert_eq!(metaData.id, "22ef18ba-191c-4c36-a606-3dad5cdf3830");
                    assert_eq!(metaData.name, None);
                    assert_eq!(metaData.description, None);
                    assert_eq!(
                        metaData.format,
                        crate::action::Format::new("parquet".to_string(), None),
                    );
                    assert_eq!(metaData.schema_string, "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}");
                    assert_eq!(metaData.partition_columns.len(), 0);
                    assert_eq!(metaData.created_time, Some(1564524294376));
                    assert_eq!(metaData.configuration, HashMap::new());
                }
                _ => panic!("expect txn action, got: {:?}", &actions[1]),
            }

            match &actions[2] {
                Action::txn(txn) => {
                    assert_eq!(txn.app_id, "e4a20b59-dd0e-4c50-b074-e8ae4786df30");
                    assert_eq!(txn.version, 0);
                    assert_eq!(txn.last_updated, Some(1564524299648));
                }
                _ => panic!("expect txn action, got: {:?}", &actions[1]),
            }
            match &actions[3] {
                Action::remove(remove) => {
                    assert_eq!(
                        remove.path,
                        "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet"
                    );
                    assert_eq!(remove.deletion_timestamp, Some(1564524298213));
                    assert_eq!(remove.data_change, false);
                    assert_eq!(remove.extended_file_metadata, Some(false));
                    assert_eq!(remove.partition_values, None);
                    assert_eq!(remove.size, None);
                    assert_eq!(remove.tags, None);
                }
                _ => panic!("expect remove action, got: {:?}", &actions[2]),
            }
            match &actions[9] {
                Action::add(add_action) => {
                    assert_eq!(
                        add_action.path,
                        "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"
                    );
                    assert_eq!(add_action.size, 400);
                    assert_eq!(add_action.modification_time, 1564524297000);
                    assert_eq!(add_action.partition_values.len(), 0);
                    assert_eq!(add_action.data_change, false);
                    assert_eq!(add_action.stats, None);
                    assert_eq!(add_action.tags, None);
                }
                _ => panic!("expect add action, got: {:?}", &actions[9]),
            }
        }
    }

    #[test]
    fn test_add_action_with_partition_values() {
        use parquet2::read::read_metadata;

        let path = "./tests/data/read_null_partitions_from_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet";
        let mut reader = File::open(path).unwrap();
        let metadata = read_metadata(&mut reader).unwrap();

        for row_group in metadata.row_groups {
            let actions = actions_from_row_group(row_group, &mut reader).unwrap();
            match &actions[0] {
                Action::protocol(protocol) => {
                    assert_eq!(protocol.min_reader_version, 1,);
                    assert_eq!(protocol.min_writer_version, 2,);
                }
                _ => panic!("expect protocol action"),
            }
            match &actions[1] {
                Action::metaData(metaData) => {
                    assert_eq!(metaData.id, "e3501f3e-ca63-4521-84f2-5901b5b66ac1");
                    assert_eq!(metaData.name, None);
                    assert_eq!(metaData.description, None);
                    assert_eq!(
                        metaData.format,
                        crate::action::Format::new("parquet".to_string(), None),
                    );
                    assert_eq!(
                        metaData.schema_string,
                        r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"color","type":"string","nullable":true,"metadata":{}}]}"#
                    );
                    assert_eq!(metaData.partition_columns, vec!["color"]);
                    assert_eq!(metaData.created_time, Some(1655607917641));
                    assert_eq!(metaData.configuration, HashMap::new());
                }
                _ => panic!("expect txn action, got: {:?}", &actions[1]),
            }

            match &actions[2] {
                Action::add(add_action) => {
                    assert_eq!(add_action.path, "9160473b-59b0-4d51-b442-e495f1d9965f");
                    assert_eq!(add_action.size, 100);
                    assert_eq!(add_action.modification_time, 1655607917660);
                    assert_eq!(add_action.partition_values.len(), 1);
                    assert_eq!(
                        add_action.partition_values.get("color").unwrap(),
                        &Some("red".to_string())
                    );
                    assert_eq!(add_action.data_change, false);
                    assert_eq!(add_action.stats, None);
                    assert_eq!(add_action.tags, None);
                }
                _ => panic!("expect add action, got: {:?}", &actions[9]),
            }
            match &actions[3] {
                Action::add(add_action) => {
                    assert_eq!(add_action.path, "4cf2c035-9fa7-4d9a-b09f-e13aa7aceb3d");
                    assert_eq!(add_action.size, 100);
                    assert_eq!(add_action.modification_time, 1655607917674);
                    assert_eq!(add_action.partition_values.len(), 1);
                    assert_eq!(add_action.partition_values.get("color").unwrap(), &None);
                    assert_eq!(add_action.data_change, false);
                    assert_eq!(add_action.stats, None);
                    assert_eq!(add_action.tags, None);
                }
                _ => panic!("expect add action, got: {:?}", &actions[9]),
            }
        }
    }
}
