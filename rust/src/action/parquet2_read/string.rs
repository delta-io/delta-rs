//! Parquet string deserialization for Action enum

use parquet2::metadata::ColumnDescriptor;
use parquet2::page::DataPage;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveConvertedType;

use super::validity::ValidityRowIndexIter;
use super::{split_page, ActionVariant, ParseError};
use crate::action::Action;

/// Parquet string value reader
pub struct SomeStringValueIter<'a> {
    valid_row_idx_iter: ValidityRowIndexIter<'a>,
    values_buffer: &'a [u8],
}

impl<'a> SomeStringValueIter<'a> {
    /// Create parquet string value reader
    pub fn new(page: &'a DataPage, descriptor: &'a ColumnDescriptor) -> Self {
        let (max_def_level, validity_iter, values_buffer) = split_page(page, descriptor);
        let valid_row_idx_iter = ValidityRowIndexIter::new(max_def_level, validity_iter);
        Self {
            values_buffer,
            valid_row_idx_iter,
        }
    }
}

impl<'a> Iterator for SomeStringValueIter<'a> {
    type Item = (usize, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.valid_row_idx_iter.next().map(|idx| {
            let bytes_len = parquet2::encoding::get_length(self.values_buffer) as usize;
            let bytes_end = bytes_len + 4;
            // skip first 4 bytes (length)
            let bytes = &self.values_buffer[4..bytes_end];
            self.values_buffer = &self.values_buffer[bytes_end..];

            let value = std::str::from_utf8(bytes).unwrap().to_string();
            (idx, value)
        })
    }
}

#[inline]
pub fn for_each_string_field_value<ActType, SetFn>(
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    set_fn: SetFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetFn: Fn((&mut ActType, String)) -> (),
{
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
                    "expect parquet utf8 type, got physical type: {:?}, converted type: {:?}",
                    physical_type, converted_type
                )));
            }
        }
    }

    let some_value_iter = SomeStringValueIter::new(page, descriptor);
    for (idx, value) in some_value_iter {
        let a = actions[idx].get_or_insert_with(ActType::default_action);
        set_fn((ActType::try_mut_from_action(a)?, value));
    }
    Ok(())
}
