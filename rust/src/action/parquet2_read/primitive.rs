//! Parquet primitive type deserialization for Action enum

use std::convert::TryInto;
use std::sync::Arc;

use parquet2::encoding::hybrid_rle;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::DataPage;
use parquet2::page::DictPage;
use parquet2::page::PrimitivePageDict;
use parquet2::schema::types::ParquetType;
use parquet2::types::NativeType;

use super::validity::ValidityRowIndexIter;
use super::{split_page, ActionVariant, ParseError};
use crate::action::Action;

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
    pub fn new(page: &'a DataPage, descriptor: &'a ColumnDescriptor) -> Self {
        let (max_def_level, validity_iter, values_buffer) = split_page(page, descriptor);
        let value_iter = ExactChunksIter::<T>::new(values_buffer);
        let valid_row_idx_iter = ValidityRowIndexIter::new(max_def_level, validity_iter);
        Self {
            value_iter,
            valid_row_idx_iter,
        }
    }
}

impl<'a, T: NativeType> Iterator for SomePrimitiveValueIter<'a, T> {
    type Item = (usize, T);

    fn next(&mut self) -> Option<Self::Item> {
        self.valid_row_idx_iter
            .next()
            .map(|idx| (idx, self.value_iter.next().unwrap()))
    }
}

/// Parquet dictionary primitive value reader
pub struct SomeDictionaryPrimitiveValueIter<'a, T: NativeType> {
    valid_row_idx_iter: ValidityRowIndexIter<'a>,
    index_iter: hybrid_rle::HybridRleDecoder<'a>,
    dict_values: &'a [T],
}

impl<'a, T: NativeType> SomeDictionaryPrimitiveValueIter<'a, T> {
    /// Create parquet primitive value reader
    pub fn new(
        page: &'a DataPage,
        descriptor: &'a ColumnDescriptor,
        dict: &'a Arc<dyn DictPage>,
    ) -> Self {
        let (max_def_level, validity_iter, values_buffer) = split_page(page, descriptor);

        let valid_row_idx_iter = ValidityRowIndexIter::new(max_def_level, validity_iter);

        let dict_values = dict
            .as_any()
            .downcast_ref::<PrimitivePageDict<T>>()
            .unwrap()
            .values();
        let indices_buffer = values_buffer;
        let bit_width = indices_buffer[0];
        let indices_buffer = &indices_buffer[1..];

        let additional = page.num_values();
        let index_iter =
            hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
        Self {
            index_iter,
            dict_values,
            valid_row_idx_iter,
        }
    }
}

impl<'a, T: NativeType> Iterator for SomeDictionaryPrimitiveValueIter<'a, T> {
    type Item = (usize, T);

    fn next(&mut self) -> Option<Self::Item> {
        self.valid_row_idx_iter.next().map(|idx| {
            let index = self.index_iter.next().unwrap();
            let value = self.dict_values[index as usize];
            (idx, value)
        })
    }
}

#[inline]
pub fn for_each_primitive_field_value<T, ActType, SetFn>(
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    set_fn: SetFn,
) -> Result<(), ParseError>
where
    T: NativeType,
    ActType: ActionVariant,
    SetFn: Fn(&mut ActType, T),
{
    if let ParquetType::PrimitiveType { physical_type, .. } = descriptor.type_() {
        if physical_type != &T::TYPE {
            return Err(ParseError::InvalidAction(format!(
                "expect physical parquet type {:?}, got {:?}",
                T::TYPE,
                physical_type
            )));
        }
    }

    match (&page.encoding(), page.dictionary_page()) {
        (Encoding::Plain, None) => {
            let some_value_iter = SomePrimitiveValueIter::<T>::new(page, descriptor);
            for (idx, value) in some_value_iter {
                let a = actions[idx].get_or_insert_with(ActType::default_action);
                set_fn(ActType::try_mut_from_action(a)?, value);
            }
        }
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict)) => {
            let some_value_iter = SomeDictionaryPrimitiveValueIter::new(page, descriptor, dict);
            for (idx, value) in some_value_iter {
                let a = actions[idx].get_or_insert_with(ActType::default_action);
                set_fn(ActType::try_mut_from_action(a)?, value);
            }
        }
        _ => todo!(),
    }
    Ok(())
}
