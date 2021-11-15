//! Parquet string deserialization for Action enum

use std::sync::Arc;

use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::{BinaryPageDict, DataPage, DictPage};
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveConvertedType;

use super::validity::{ValidityRepeatedRowIndexIter, ValidityRowIndexIter};
use super::{split_page, split_page_nested, ActionVariant, ParseError};
use crate::action::Action;

pub trait StringValueIter<'a>: Iterator<Item = String> {
    fn from_encoded_values(
        buffer: &'a [u8],
        num_values: usize,
        dict_page: Option<&'a Arc<dyn DictPage>>,
    ) -> Self;
}

pub struct PlainStringValueIter<'a> {
    values_buffer: &'a [u8],
}

impl<'a> StringValueIter<'a> for PlainStringValueIter<'a> {
    fn from_encoded_values(
        values_buffer: &'a [u8],
        _num_values: usize,
        _dict_page: Option<&'a Arc<dyn DictPage>>,
    ) -> Self {
        Self { values_buffer }
    }
}

impl<'a> Iterator for PlainStringValueIter<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes_len = parquet2::encoding::get_length(self.values_buffer) as usize;
        let bytes_end = bytes_len + 4;
        // skip first 4 bytes (length)
        let bytes = &self.values_buffer[4..bytes_end];
        self.values_buffer = &self.values_buffer[bytes_end..];

        Some(std::str::from_utf8(bytes).unwrap().to_string())
    }
}

pub struct DictionaryStringValueIter<'a> {
    dict_idx_iter: HybridRleDecoder<'a>,
    dict: &'a BinaryPageDict,
}

impl<'a> StringValueIter<'a> for DictionaryStringValueIter<'a> {
    fn from_encoded_values(
        values_buf: &'a [u8],
        num_values: usize,
        dict_page: Option<&'a Arc<dyn DictPage>>,
    ) -> Self {
        let bit_width = values_buf[0];
        let indices_buf = &values_buf[1..];
        let dict = dict_page.unwrap().as_any().downcast_ref().unwrap();

        Self {
            dict_idx_iter: HybridRleDecoder::new(indices_buf, bit_width.into(), num_values),
            dict,
        }
    }
}

impl<'a> Iterator for DictionaryStringValueIter<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let dict_idx = self.dict_idx_iter.next().unwrap() as usize;
        let start = self.dict.offsets()[dict_idx] as usize;
        let end = self.dict.offsets()[dict_idx + 1] as usize;
        Some(
            std::str::from_utf8(&self.dict.values()[start..end])
                .unwrap()
                .to_string(),
        )
    }
}

/// Parquet string value reader
pub struct SomeStringValueIter<'a, ValIter>
where
    ValIter: StringValueIter<'a>,
{
    valid_row_idx_iter: ValidityRowIndexIter<'a>,
    values_iter: ValIter,
}

impl<'a, ValIter> SomeStringValueIter<'a, ValIter>
where
    ValIter: StringValueIter<'a>,
{
    /// Create parquet string value reader
    pub fn new(page: &'a DataPage, descriptor: &'a ColumnDescriptor) -> Self {
        let (max_def_level, validity_iter, values_buffer) = split_page(page, descriptor);
        let valid_row_idx_iter = ValidityRowIndexIter::new(max_def_level, validity_iter);
        Self {
            valid_row_idx_iter,
            // TODO: page.num_values is more than what's being packed in rle
            values_iter: ValIter::from_encoded_values(
                values_buffer,
                page.num_values(),
                page.dictionary_page(),
            ),
        }
    }
}

impl<'a, ValIter> Iterator for SomeStringValueIter<'a, ValIter>
where
    ValIter: StringValueIter<'a>,
{
    type Item = (usize, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.valid_row_idx_iter.next().map(|idx| {
            let value = self.values_iter.next().unwrap();
            (idx, value)
        })
    }
}

/// Parquet repeated string value reader
pub struct SomeRepeatedStringValueIter<'a, ValIter>
where
    ValIter: StringValueIter<'a>,
{
    repeated_row_idx_iter: ValidityRepeatedRowIndexIter<'a>,
    values_iter: ValIter,
}

impl<'a, ValIter> SomeRepeatedStringValueIter<'a, ValIter>
where
    ValIter: StringValueIter<'a>,
{
    /// Create parquet string value reader
    pub fn new(page: &'a DataPage, descriptor: &'a ColumnDescriptor) -> Self {
        let (max_rep_level, rep_iter, max_def_level, validity_iter, values_buffer) =
            split_page_nested(page, descriptor);
        let repeated_row_idx_iter = ValidityRepeatedRowIndexIter::new(
            max_rep_level,
            rep_iter,
            max_def_level,
            validity_iter,
        );
        Self {
            values_iter: ValIter::from_encoded_values(
                values_buffer,
                page.num_values(),
                page.dictionary_page(),
            ),
            repeated_row_idx_iter,
        }
    }
}

impl<'a, ValIter> Iterator for SomeRepeatedStringValueIter<'a, ValIter>
where
    ValIter: StringValueIter<'a>,
{
    type Item = (usize, Vec<String>);

    fn next(&mut self) -> Option<Self::Item> {
        self.repeated_row_idx_iter.next().map(|(idx, item_count)| {
            let strings = (0..item_count)
                .map(|_| self.values_iter.next().unwrap())
                .collect();

            (idx, strings)
        })
    }
}

pub fn for_each_repeated_string_field_value_with_idx<MapFn>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    map_fn: MapFn,
) -> Result<(), ParseError>
where
    MapFn: FnMut((usize, Vec<String>)) -> Result<(), ParseError>,
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

    match page.encoding() {
        Encoding::Plain => {
            SomeRepeatedStringValueIter::<PlainStringValueIter>::new(page, descriptor)
                .try_for_each(map_fn)?;
        }
        Encoding::RleDictionary => {
            SomeRepeatedStringValueIter::<DictionaryStringValueIter>::new(page, descriptor)
                .try_for_each(map_fn)?;
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "unsupported page encoding type: {:?}",
                page.encoding()
            )))
        }
    }

    Ok(())
}

pub fn for_each_repeated_string_field_value<ActType, SetFn>(
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    set_fn: SetFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetFn: Fn(&mut ActType, Vec<String>),
{
    for_each_repeated_string_field_value_with_idx(
        page,
        descriptor,
        |(idx, strings): (usize, Vec<String>)| -> Result<(), ParseError> {
            let a = actions[idx].get_or_insert_with(ActType::default_action);
            set_fn(ActType::try_mut_from_action(a)?, strings);
            Ok(())
        },
    )
}

pub fn for_each_string_field_value<ActType, SetFn>(
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    set_fn: SetFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetFn: Fn(&mut ActType, String),
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

    let map_fn = |(idx, value): (usize, String)| -> Result<(), ParseError> {
        let a = actions[idx].get_or_insert_with(ActType::default_action);
        set_fn(ActType::try_mut_from_action(a)?, value);

        Ok(())
    };

    match page.encoding() {
        Encoding::Plain => {
            SomeStringValueIter::<PlainStringValueIter>::new(page, descriptor)
                .try_for_each(map_fn)?;
        }
        Encoding::RleDictionary => {
            SomeStringValueIter::<DictionaryStringValueIter>::new(page, descriptor)
                .try_for_each(map_fn)?;
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "unsupported page encoding type: {:?}",
                page.encoding()
            )))
        }
    }

    Ok(())
}
