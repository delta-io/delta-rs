//! Auxiliary methods for dealing with kernel scalars
use std::cmp::Ordering;

use arrow_array::Array;
use arrow_schema::TimeUnit;
use chrono::{DateTime, TimeZone, Utc};
use delta_kernel::{
    expressions::{Scalar, StructData},
    schema::StructField,
};
use object_store::path::Path;
#[cfg(test)]
use serde_json::Value;
use urlencoding::encode;

use crate::NULL_PARTITION_VALUE_DATA_PATH;

/// Auxiliary methods for dealing with kernel scalars
pub trait ScalarExt: Sized {
    /// Serialize to string
    fn serialize(&self) -> String;
    /// Serialize to string for use in hive partition file names
    fn serialize_encoded(&self) -> String;
    /// Create a [`Scalar`] from an arrow array row
    fn from_array(arr: &dyn Array, index: usize) -> Option<Self>;
    /// Serialize as serde_json::Value
    #[cfg(test)]
    fn to_json(&self) -> serde_json::Value;
}

impl ScalarExt for Scalar {
    /// Serializes this scalar as a string.
    fn serialize(&self) -> String {
        match self {
            Self::String(s) => s.to_owned(),
            Self::Byte(b) => b.to_string(),
            Self::Short(s) => s.to_string(),
            Self::Integer(i) => i.to_string(),
            Self::Long(l) => l.to_string(),
            Self::Float(f) => f.to_string(),
            Self::Double(d) => d.to_string(),
            Self::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
            Self::TimestampNtz(ts) | Self::Timestamp(ts) => {
                let ts = Utc.timestamp_micros(*ts).single().unwrap();
                ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
            Self::Date(days) => {
                let date = DateTime::from_timestamp(*days as i64 * 24 * 3600, 0).unwrap();
                date.format("%Y-%m-%d").to_string()
            }
            Self::Decimal(value, _, scale) => match scale.cmp(&0) {
                Ordering::Equal => value.to_string(),
                Ordering::Greater => {
                    let scalar_multiple = 10_i128.pow(*scale as u32);
                    let mut s = String::new();
                    s.push_str((value / scalar_multiple).to_string().as_str());
                    s.push('.');
                    s.push_str(&format!(
                        "{:0>scale$}",
                        value % scalar_multiple,
                        scale = *scale as usize
                    ));
                    s
                }
                Ordering::Less => {
                    let mut s = value.to_string();
                    for _ in 0..*scale {
                        s.push('0');
                    }
                    s
                }
            },
            Self::Binary(val) => create_escaped_binary_string(val.as_slice()),
            Self::Null(_) => "null".to_string(),
            Self::Struct(_) => unimplemented!(),
        }
    }

    /// Serializes this scalar as a string for use in hive partition file names.
    fn serialize_encoded(&self) -> String {
        if self.is_null() {
            return NULL_PARTITION_VALUE_DATA_PATH.to_string();
        }
        encode(Path::from(self.serialize()).as_ref()).to_string()
    }

    /// Create a [`Scalar`] form a row in an arrow array.
    fn from_array(arr: &dyn Array, index: usize) -> Option<Self> {
        use arrow_array::*;
        use arrow_schema::DataType::*;

        if arr.len() <= index {
            return None;
        }
        if arr.is_null(index) {
            return Some(Self::Null(arr.data_type().try_into().ok()?));
        }

        match arr.data_type() {
            Utf8 => arr
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|v| Self::String(v.value(index).to_string())),
            LargeUtf8 => arr
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|v| Self::String(v.value(index).to_string())),
            Boolean => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|v| Self::Boolean(v.value(index))),
            Binary => arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .map(|v| Self::Binary(v.value(index).to_vec())),
            LargeBinary => arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map(|v| Self::Binary(v.value(index).to_vec())),
            FixedSizeBinary(_) => arr
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .map(|v| Self::Binary(v.value(index).to_vec())),
            Int8 => arr
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(|v| Self::Byte(v.value(index))),
            Int16 => arr
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(|v| Self::Short(v.value(index))),
            Int32 => arr
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(|v| Self::Integer(v.value(index))),
            Int64 => arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|v| Self::Long(v.value(index))),
            UInt8 => arr
                .as_any()
                .downcast_ref::<UInt8Array>()
                .map(|v| Self::Byte(v.value(index) as i8)),
            UInt16 => arr
                .as_any()
                .downcast_ref::<UInt16Array>()
                .map(|v| Self::Short(v.value(index) as i16)),
            UInt32 => arr
                .as_any()
                .downcast_ref::<UInt32Array>()
                .map(|v| Self::Integer(v.value(index) as i32)),
            UInt64 => arr
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map(|v| Self::Long(v.value(index) as i64)),
            Float32 => arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(|v| Self::Float(v.value(index))),
            Float64 => arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|v| Self::Double(v.value(index))),
            Decimal128(precision, scale) => {
                arr.as_any().downcast_ref::<Decimal128Array>().map(|v| {
                    let value = v.value(index);
                    Self::Decimal(value, *precision, *scale as u8)
                })
            }
            Date32 => arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|v| Self::Date(v.value(index))),
            Timestamp(TimeUnit::Microsecond, None) => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|v| Self::TimestampNtz(v.value(index))),
            Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.eq_ignore_ascii_case("utc") => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|v| Self::Timestamp(v.clone().value(index))),
            Struct(fields) => {
                let struct_fields = fields
                    .iter()
                    .flat_map(|f| TryFrom::try_from(f.as_ref()))
                    .collect::<Vec<_>>();
                let values = arr
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .and_then(|struct_arr| {
                        struct_fields
                            .iter()
                            .map(|f: &StructField| {
                                struct_arr
                                    .column_by_name(f.name())
                                    .and_then(|c| Self::from_array(c.as_ref(), index))
                            })
                            .collect::<Option<Vec<_>>>()
                    })?;
                Some(Self::Struct(
                    StructData::try_new(struct_fields, values).ok()?,
                ))
            }
            Float16
            | Decimal256(_, _)
            | List(_)
            | LargeList(_)
            | FixedSizeList(_, _)
            | Map(_, _)
            | Date64
            | Timestamp(_, _)
            | Time32(_)
            | Time64(_)
            | Duration(_)
            | Interval(_)
            | Dictionary(_, _)
            | RunEndEncoded(_, _)
            | Union(_, _)
            | Utf8View
            | BinaryView
            | ListView(_)
            | LargeListView(_)
            | Null => None,
        }
    }

    /// Serializes this scalar as a serde_json::Value.
    #[cfg(test)]
    fn to_json(&self) -> serde_json::Value {
        match self {
            Self::String(s) => Value::String(s.to_owned()),
            Self::Byte(b) => Value::Number(serde_json::Number::from(*b)),
            Self::Short(s) => Value::Number(serde_json::Number::from(*s)),
            Self::Integer(i) => Value::Number(serde_json::Number::from(*i)),
            Self::Long(l) => Value::Number(serde_json::Number::from(*l)),
            Self::Float(f) => Value::Number(serde_json::Number::from_f64(*f as f64).unwrap()),
            Self::Double(d) => Value::Number(serde_json::Number::from_f64(*d).unwrap()),
            Self::Boolean(b) => Value::Bool(*b),
            Self::TimestampNtz(ts) | Self::Timestamp(ts) => {
                let ts = Utc.timestamp_micros(*ts).single().unwrap();
                Value::String(ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            }
            Self::Date(days) => {
                let date = DateTime::from_timestamp(*days as i64 * 24 * 3600, 0).unwrap();
                Value::String(date.format("%Y-%m-%d").to_string())
            }
            Self::Decimal(value, _, scale) => match scale.cmp(&0) {
                Ordering::Equal => Value::String(value.to_string()),
                Ordering::Greater => {
                    let scalar_multiple = 10_i128.pow(*scale as u32);
                    let mut s = String::new();
                    s.push_str((value / scalar_multiple).to_string().as_str());
                    s.push('.');
                    s.push_str(&format!(
                        "{:0>scale$}",
                        value % scalar_multiple,
                        scale = *scale as usize
                    ));
                    Value::String(s)
                }
                Ordering::Less => {
                    let mut s = value.to_string();
                    for _ in 0..*scale {
                        s.push('0');
                    }
                    Value::String(s)
                }
            },
            Self::Binary(val) => Value::String(create_escaped_binary_string(val.as_slice())),
            Self::Null(_) => Value::Null,
            Self::Struct(_) => unimplemented!(),
        }
    }
}

fn create_escaped_binary_string(data: &[u8]) -> String {
    let mut escaped_string = String::new();
    for &byte in data {
        // Convert each byte to its two-digit hexadecimal representation
        let hex_representation = format!("{:04X}", byte);
        // Append the hexadecimal representation with an escape sequence
        escaped_string.push_str("\\u");
        escaped_string.push_str(&hex_representation);
    }
    escaped_string
}
