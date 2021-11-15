use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::DataPage;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;

use super::validity::ValidityRowIndexIter;
use super::{split_page, ActionVariant, ParseError};
use crate::action::Action;

/// Parquet dictionary primitive value reader
pub struct SomeBooleanValueIter<'a> {
    valid_row_idx_iter: ValidityRowIndexIter<'a>,
    value_iter: BitmapIter<'a>,
}

impl<'a> SomeBooleanValueIter<'a> {
    /// Create parquet primitive value reader
    pub fn new(page: &'a DataPage, descriptor: &'a ColumnDescriptor) -> Self {
        let (max_def_level, validity_iter, values_buffer) = split_page(page, descriptor);

        let valid_row_idx_iter = ValidityRowIndexIter::new(max_def_level, validity_iter);
        let value_len_upper_bound = values_buffer.len() * 8;
        let value_iter = BitmapIter::new(values_buffer, 0, value_len_upper_bound);
        Self {
            valid_row_idx_iter,
            value_iter,
        }
    }
}

impl<'a> Iterator for SomeBooleanValueIter<'a> {
    type Item = (usize, bool);

    fn next(&mut self) -> Option<Self::Item> {
        self.valid_row_idx_iter.next().map(|idx| {
            let value = self.value_iter.next().unwrap();
            (idx, value)
        })
    }
}

#[inline]
pub fn for_each_boolean_field_value<ActType, SetFn>(
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    set_fn: SetFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetFn: Fn(&mut ActType, bool),
{
    if let ParquetType::PrimitiveType { physical_type, .. } = descriptor.type_() {
        if physical_type != &PhysicalType::Boolean {
            return Err(ParseError::InvalidAction(format!(
                "expect physical parquet type boolean, got {:?}",
                physical_type
            )));
        }
    }

    let some_value_iter = SomeBooleanValueIter::new(page, descriptor);
    for (idx, value) in some_value_iter {
        let a = actions[idx].get_or_insert_with(ActType::default_action);
        set_fn(ActType::try_mut_from_action(a)?, value);
    }

    Ok(())
}
