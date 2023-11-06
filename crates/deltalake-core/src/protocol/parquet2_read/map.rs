use parquet2::metadata::ColumnDescriptor;
use parquet2::page::{DataPage, DictPage};

use super::string::for_each_repeated_string_field_value_with_idx;
use super::{ActionVariant, ParseError};
use crate::kernel::Action;

#[derive(Default)]
pub struct MapState {
    keys: Option<Vec<(usize, Vec<String>)>>,
    values: Option<Vec<(usize, Vec<String>)>>,
}

pub fn for_each_map_field_value<ActType, SetMapFn>(
    field: &[String],
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    dict: &Option<DictPage>,
    descriptor: &ColumnDescriptor,
    state: &mut MapState,
    set_map_fn: SetMapFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetMapFn: Fn(&mut ActType, (Vec<String>, Vec<Option<String>>)),
{
    debug_assert!(field[0] == "key_value");
    #[cfg(debug_assertions)]
    {
        use parquet2::schema::types::PhysicalType;
        if page.descriptor.primitive_type.physical_type != PhysicalType::ByteArray {
            return Err(ParseError::InvalidAction(format!(
                "expect parquet utf8 type for map key/value, got primitive type: {:?}",
                page.descriptor.primitive_type,
            )));
        }
    }

    match field[1].as_str() {
        "key" => {
            let mut keys = vec![];
            for_each_repeated_string_field_value_with_idx(
                page,
                dict,
                descriptor,
                |result: Result<(usize, Vec<String>), ParseError>| -> Result<(), ParseError> {
                    let (row_idx, strings) = result?;
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
                dict,
                descriptor,
                |result: Result<(usize, Vec<String>), ParseError>| -> Result<(), ParseError> {
                    let (row_idx, strings) = result?;
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
