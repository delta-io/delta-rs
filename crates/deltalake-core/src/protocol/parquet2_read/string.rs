//! Parquet string deserialization for Action enum

use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::{DataPage, DictPage};

use super::dictionary;
use super::dictionary::binary::BinaryPageDict;
use super::validity::{ValidityRepeatedRowIndexIter, ValidityRowIndexIter};
use super::{split_page, split_page_nested, ActionVariant, ParseError};
use crate::kernel::Action;

pub trait StringValueIter<'a>: Iterator<Item = Result<String, ParseError>> {
    fn try_from_encoded_values(
        buffer: &'a [u8],
        num_values: usize,
        _dict: &'a Option<DictPage>,
    ) -> Result<Self, ParseError>
    where
        Self: Sized;
}

pub struct PlainStringValueIter<'a> {
    values_buffer: &'a [u8],
}

impl<'a> StringValueIter<'a> for PlainStringValueIter<'a> {
    fn try_from_encoded_values(
        values_buffer: &'a [u8],
        _num_values: usize,
        _dict: &Option<DictPage>,
    ) -> Result<Self, ParseError> {
        Ok(Self { values_buffer })
    }
}

impl<'a> Iterator for PlainStringValueIter<'a> {
    type Item = Result<String, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes_len = parquet2::encoding::get_length(self.values_buffer).unwrap() as usize;
        let bytes_end = bytes_len + 4;
        // skip first 4 bytes (length)
        let bytes = &self.values_buffer[4..bytes_end];
        self.values_buffer = &self.values_buffer[bytes_end..];

        Some(Ok(std::str::from_utf8(bytes).unwrap().to_string()))
    }
}

pub struct DictionaryStringValueIter<'a> {
    dict_idx_iter: HybridRleDecoder<'a>,
    dict: BinaryPageDict<'a>,
}

impl<'a> StringValueIter<'a> for DictionaryStringValueIter<'a> {
    fn try_from_encoded_values(
        values_buf: &'a [u8],
        num_values: usize,
        dict: &'a Option<DictPage>,
    ) -> Result<Self, ParseError> {
        let bit_width = values_buf[0];
        let indices_buf = &values_buf[1..];
        let dict = dict.as_ref().unwrap();
        let binary_dict = dictionary::binary::read(&dict.buffer, dict.num_values)?;

        Ok(Self {
            dict_idx_iter: HybridRleDecoder::try_new(indices_buf, bit_width.into(), num_values)?,
            dict: binary_dict,
        })
    }
}

impl<'a> Iterator for DictionaryStringValueIter<'a> {
    type Item = Result<String, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.dict_idx_iter.next().map(|result| {
            result
                .map(|dict_idx| {
                    let dict_idx = dict_idx as usize;
                    std::str::from_utf8(
                        &self.dict.value(dict_idx).expect("Invalid dictionary index"),
                    )
                    .unwrap()
                    .to_string()
                })
                .map_err(|e| e.into())
        })
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
    pub fn try_new(
        page: &'a DataPage,
        dict: &'a Option<DictPage>,
        descriptor: &'a ColumnDescriptor,
    ) -> Result<Self, ParseError> {
        let (max_def_level, validity_iter, values_buffer) = split_page(page, descriptor)?;
        let valid_row_idx_iter = ValidityRowIndexIter::new(max_def_level, validity_iter);
        Ok(Self {
            valid_row_idx_iter,
            // TODO: page.num_values is more than what's being packed in rle
            values_iter: ValIter::try_from_encoded_values(values_buffer, page.num_values(), dict)?,
        })
    }
}

impl<'a, ValIter> Iterator for SomeStringValueIter<'a, ValIter>
where
    ValIter: StringValueIter<'a>,
{
    type Item = Result<(usize, String), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.valid_row_idx_iter.next().map(|result| {
            let idx = result?;
            let value = self.values_iter.next().ok_or_else(|| {
                ParseError::Generic(format!("No string value matches row index: {}", idx))
            })??;
            Ok((idx, value))
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
    pub fn try_new(
        page: &'a DataPage,
        dict: &'a Option<DictPage>,
        descriptor: &'a ColumnDescriptor,
    ) -> Result<Self, ParseError> {
        let (max_rep_level, rep_iter, max_def_level, validity_iter, values_buffer) =
            split_page_nested(page, descriptor)?;
        let repeated_row_idx_iter = ValidityRepeatedRowIndexIter::new(
            max_rep_level,
            rep_iter,
            max_def_level,
            validity_iter,
        );

        Ok(Self {
            values_iter: ValIter::try_from_encoded_values(values_buffer, page.num_values(), dict)?,
            repeated_row_idx_iter,
        })
    }
}

impl<'a, ValIter> Iterator for SomeRepeatedStringValueIter<'a, ValIter>
where
    ValIter: StringValueIter<'a>,
{
    type Item = Result<(usize, Vec<String>), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.repeated_row_idx_iter.next().map(|result| {
            let (idx, item_count) = result?;

            let strings = (0..item_count)
                .map(|i| {
                    self.values_iter.next().ok_or_else(|| {
                        ParseError::Generic(format!("No string value found list index: {}", i))
                    })?
                })
                .collect::<Result<Vec<String>, _>>()?;

            Ok((idx, strings))
        })
    }
}

pub fn for_each_repeated_string_field_value_with_idx<MapFn>(
    page: &DataPage,
    dict: &Option<DictPage>,
    descriptor: &ColumnDescriptor,
    map_fn: MapFn,
) -> Result<(), ParseError>
where
    MapFn: FnMut(Result<(usize, Vec<String>), ParseError>) -> Result<(), ParseError>,
{
    #[cfg(debug_assertions)]
    {
        use parquet2::schema::types::PhysicalType;
        if page.descriptor.primitive_type.physical_type != PhysicalType::ByteArray {
            return Err(ParseError::InvalidAction(format!(
                "expect parquet utf8 type, got primitive type: {:?}",
                page.descriptor.primitive_type,
            )));
        }
    }

    match page.encoding() {
        Encoding::Plain => {
            SomeRepeatedStringValueIter::<PlainStringValueIter>::try_new(page, dict, descriptor)?
                .try_for_each(map_fn)?;
        }
        Encoding::RleDictionary | Encoding::PlainDictionary => {
            SomeRepeatedStringValueIter::<DictionaryStringValueIter>::try_new(
                page, dict, descriptor,
            )?
            .try_for_each(map_fn)?;
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "unsupported page encoding type for string list column: {:?}",
                page.encoding()
            )));
        }
    }

    Ok(())
}

pub fn for_each_repeated_string_field_value<ActType, SetFn>(
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    dict: &Option<DictPage>,
    descriptor: &ColumnDescriptor,
    set_fn: SetFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetFn: Fn(&mut ActType, Vec<String>),
{
    for_each_repeated_string_field_value_with_idx(
        page,
        dict,
        descriptor,
        |entry: Result<(usize, Vec<String>), ParseError>| -> Result<(), ParseError> {
            let (idx, strings) = entry?;
            let a = actions[idx].get_or_insert_with(ActType::default_action);
            set_fn(ActType::try_mut_from_action(a)?, strings);
            Ok(())
        },
    )
}

pub fn for_each_string_field_value<ActType, SetFn>(
    actions: &mut Vec<Option<Action>>,
    page: &DataPage,
    dict: &Option<DictPage>,
    descriptor: &ColumnDescriptor,
    set_fn: SetFn,
) -> Result<(), ParseError>
where
    ActType: ActionVariant,
    SetFn: Fn(&mut ActType, String),
{
    #[cfg(debug_assertions)]
    {
        use parquet2::schema::types::PhysicalType;
        if page.descriptor.primitive_type.physical_type != PhysicalType::ByteArray {
            return Err(ParseError::InvalidAction(format!(
                "expect parquet utf8 type, got primitive type: {:?}",
                page.descriptor.primitive_type,
            )));
        }
    }

    let map_fn = |entry: Result<(usize, String), ParseError>| -> Result<(), ParseError> {
        let (idx, value) = entry?;
        let a = actions[idx].get_or_insert_with(ActType::default_action);
        set_fn(ActType::try_mut_from_action(a)?, value);

        Ok(())
    };

    match page.encoding() {
        Encoding::Plain => {
            SomeStringValueIter::<PlainStringValueIter>::try_new(page, dict, descriptor)?
                .try_for_each(map_fn)?;
        }
        Encoding::RleDictionary | Encoding::PlainDictionary => {
            SomeStringValueIter::<DictionaryStringValueIter>::try_new(page, dict, descriptor)?
                .try_for_each(map_fn)?;
        }
        _ => {
            return Err(ParseError::InvalidAction(format!(
                "unsupported page encoding type for string column: {:?}",
                page.encoding()
            )));
        }
    }

    Ok(())
}
