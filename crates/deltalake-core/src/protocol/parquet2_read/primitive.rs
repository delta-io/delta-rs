//! Parquet primitive type deserialization for Action enum

use std::convert::TryInto;

use parquet2::encoding::hybrid_rle;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::DataPage;
use parquet2::page::DictPage;
use parquet2::types::NativeType;

use super::dictionary;
use super::validity::ValidityRowIndexIter;
use super::{split_page, ActionVariant, ParseError};
use crate::kernel::Action;

struct ExactChunksIter<'a, T: NativeType> {
    chunks: std::slice::ChunksExact<'a, u8>,
    phantom: std::marker::PhantomData<T>,
}

impl<'a, T: NativeType> ExactChunksIter<'a, T> {
    #[inline]
    pub fn new(slice: &'a [u8]) -> Self {
        assert_eq!(slice.len() % std::mem::size_of::<T>(), 0);
        let chunks = slice.chunks_exact(std::mem::size_of::<T>());
        Self {
            chunks,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T: NativeType> Iterator for ExactChunksIter<'a, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next().map(|chunk| {
            let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
                Ok(v) => v,
                Err(_) => unreachable!(),
            };
            T::from_le_bytes(chunk)
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }
}

/// Parquet primitive value reader
pub struct SomePrimitiveValueIter<'a, T: NativeType> {
    valid_row_idx_iter: ValidityRowIndexIter<'a>,
    value_iter: ExactChunksIter<'a, T>,
}

impl<'a, T: NativeType> SomePrimitiveValueIter<'a, T> {
    /// Create parquet primitive value reader
    pub fn try_new(
        page: &'a DataPage,
        descriptor: &'a ColumnDescriptor,
    ) -> Result<Self, ParseError> {
        let (max_def_level, validity_iter, values_buffer) = split_page(page, descriptor)?;
        let value_iter = ExactChunksIter::<T>::new(values_buffer);
        let valid_row_idx_iter = ValidityRowIndexIter::new(max_def_level, validity_iter);
        Ok(Self {
            value_iter,
            valid_row_idx_iter,
        })
    }
}

impl<'a, T: NativeType> Iterator for SomePrimitiveValueIter<'a, T> {
    type Item = Result<(usize, T), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.valid_row_idx_iter
            .next()
            .map(|idx_result| idx_result.map(|idx| (idx, self.value_iter.next().unwrap())))
    }
}

/// Parquet dictionary primitive value reader
pub struct SomeDictionaryPrimitiveValueIter<'a, T: NativeType> {
    valid_row_idx_iter: ValidityRowIndexIter<'a>,
    index_iter: hybrid_rle::HybridRleDecoder<'a>,
    dict_values: Vec<T>,
}

impl<'a, T: NativeType> SomeDictionaryPrimitiveValueIter<'a, T> {
    /// Create parquet primitive value reader
    pub fn try_new(
        page: &'a DataPage,
        dict: &DictPage,
        descriptor: &'a ColumnDescriptor,
    ) -> Result<Self, ParseError> {
        let (max_def_level, validity_iter, values_buffer) = split_page(page, descriptor)?;

        let valid_row_idx_iter = ValidityRowIndexIter::new(max_def_level, validity_iter);

        let dict_values = dictionary::primitive::read::<T>(&dict.buffer, dict.num_values)?;

        let indices_buffer = values_buffer;
        let bit_width = indices_buffer[0];
        let indices_buffer = &indices_buffer[1..];

        let additional = page.num_values();
        let index_iter =
            hybrid_rle::HybridRleDecoder::try_new(indices_buffer, bit_width as u32, additional)?;
        Ok(Self {
            index_iter,
            dict_values,
            valid_row_idx_iter,
        })
    }
}

impl<'a, T: NativeType> Iterator for SomeDictionaryPrimitiveValueIter<'a, T> {
    type Item = Result<(usize, T), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.valid_row_idx_iter.next().map(|idx_result| {
            let idx = idx_result?;
            let dict_idx = self.index_iter.next().ok_or_else(|| {
                ParseError::Generic(format!("No dict index matches row index: {}", idx))
            })??;
            let value = self.dict_values[dict_idx as usize];
            Ok((idx, value))
        })
    }
}

#[inline]
pub fn for_each_primitive_field_value<T, ActType, SetFn>(
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    dict: &Option<DictPage>,
    descriptor: &ColumnDescriptor,
    set_fn: SetFn,
) -> Result<(), ParseError>
where
    T: NativeType,
    ActType: ActionVariant,
    SetFn: Fn(&mut ActType, T),
{
    #[cfg(debug_assertions)]
    if page.descriptor.primitive_type.physical_type != T::TYPE {
        return Err(ParseError::InvalidAction(format!(
            "expect physical parquet type {:?}, got {:?}",
            T::TYPE,
            page.descriptor.primitive_type,
        )));
    }

    match (&page.encoding(), dict) {
        (Encoding::Plain, None) => {
            let some_value_iter = SomePrimitiveValueIter::<T>::try_new(page, descriptor)?;
            for entry in some_value_iter {
                let (idx, value) = entry?;
                let a = actions[idx].get_or_insert_with(ActType::default_action);
                set_fn(ActType::try_mut_from_action(a)?, value);
            }
        }
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict)) => {
            let some_value_iter =
                SomeDictionaryPrimitiveValueIter::try_new(page, &dict, descriptor)?;
            for entry in some_value_iter {
                let (idx, value) = entry?;
                let a = actions[idx].get_or_insert_with(ActType::default_action);
                set_fn(ActType::try_mut_from_action(a)?, value);
            }
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "unsupported page encoding type for primitive column: {:?}",
                page.encoding()
            )));
        }
    }

    Ok(())
}
