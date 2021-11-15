// use std::collections::HashMap;

use parquet2::metadata::ColumnDescriptor;
use parquet2::page::DataPage;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveConvertedType;

use super::string::for_each_repeated_string_field_value_with_idx;
use super::{ActionVariant, ParseError};
use crate::action::Action;

#[derive(Default)]
pub struct MapState {
    keys: Option<Vec<(usize, Vec<String>)>>,
    values: Option<Vec<(usize, Vec<String>)>>,
}

pub fn for_each_map_field_value<ActType, SetMapFn>(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    state: &mut MapState,
    set_map_fn: SetMapFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetMapFn: Fn(&mut ActType, (Vec<String>, Vec<Option<String>>)),
{
    assert!(field[0] == "key_value");

    if let ParquetType::PrimitiveType {
        physical_type,
        converted_type,
        logical_type,
        ..
    } = descriptor.type_()
    {
        match (physical_type, converted_type, logical_type) {
            (PhysicalType::ByteArray, Some(PrimitiveConvertedType::Utf8), _) => {}
            _ => {
                return Err(ParseError::InvalidAction(format!(
                    "expect parquet utf8 type for map key/value, got physical type: {:?}, converted type: {:?}",
                    physical_type, converted_type
                )));
            }
        }
    }

    match field[1].as_str() {
        "key" => {
            let mut keys = vec![];
            for_each_repeated_string_field_value_with_idx(
                page,
                descriptor,
                |(row_idx, strings): (usize, Vec<String>)| -> Result<(), ParseError> {
                    keys.push((row_idx, strings));
                    Ok(())
                },
            )?;
            state.keys = Some(keys);
        }
        "value" => {
            let mut values = vec![];
            for_each_repeated_string_field_value_with_idx(
                page,
                descriptor,
                |(row_idx, strings): (usize, Vec<String>)| -> Result<(), ParseError> {
                    values.push((row_idx, strings));
                    Ok(())
                },
            )?;
            state.values = Some(values);
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "Unexpected map key: {:?}",
                field,
            )));
        }
    }

    if state.keys.is_some() && state.values.is_some() {
        let keys = state.keys.take().unwrap();
        let values = state.values.take().unwrap();

        let mut values_iter = values.into_iter().peekable();

        keys.into_iter()
            .try_for_each(|(key_row_idx, keys)| -> Result<(), ParseError> {
                let (row_idx, (keys, vals)) = match values_iter.peek() {
                    Some((val_row_idx, _)) if *val_row_idx == key_row_idx => {
                        let (_, vals) = values_iter.next().unwrap();
                        (
                            key_row_idx,
                            (
                                keys,
                                vals.into_iter()
                                    .map(|val| if val == "" { None } else { Some(val) })
                                    .collect(),
                            ),
                        )
                    }
                    _ => {
                        let vals = std::iter::repeat(None).take(keys.len()).collect();
                        (key_row_idx, (keys, vals))
                    }
                };

                let a = actions[row_idx].get_or_insert_with(ActType::default_action);
                set_map_fn(ActType::try_mut_from_action(a)?, (keys, vals));

                Ok(())
            })?;
    }

    Ok(())
}
