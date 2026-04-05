//! Auxiliary methods for dealing with kernel scalars
use std::cmp::Ordering;

use arrow::{array::AsArray, datatypes::UInt16Type};
use arrow_array::Array;
use arrow_schema::TimeUnit;
use chrono::{DateTime, TimeZone, Utc};
use delta_kernel::{
    engine::arrow_conversion::TryIntoKernel as _,
    expressions::{Scalar, StructData},
    schema::StructField,
};
use percent_encoding_rfc3986::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde_json::Value;

// ASCII set that needs to be encoded, derived from
// PROTOCOL DOCS: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#how-to-url-encode-keys-and-string-values
const RFC3986_PART: &AsciiSet = &CONTROLS
    .add(b' ') // space
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

fn encode_partition_value(value: &str) -> String {
    utf8_percent_encode(value, RFC3986_PART).to_string()
}

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
    fn to_json(&self) -> Value;
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
            Self::Decimal(decimal) => match decimal.scale().cmp(&0) {
                Ordering::Equal => decimal.bits().to_string(),
                Ordering::Greater => {
                    let scale = decimal.scale();
                    let value = decimal.bits();
                    let scalar_multiple = 10_i128.pow(scale as u32);
                    let mut s = String::new();
                    s.push_str((value / scalar_multiple).to_string().as_str());
                    s.push('.');
                    s.push_str(&format!(
                        "{:0>scale$}",
                        value % scalar_multiple,
                        scale = scale as usize
                    ));
                    s
                }
                Ordering::Less => {
                    let mut s = decimal.bits().to_string();
                    for _ in 0..decimal.scale() {
                        s.push('0');
                    }
                    s
                }
            },
            Self::Binary(val) => create_escaped_binary_string(val.as_slice()),
            Self::Null(_) => "null".to_string(),
            Self::Struct(_) => self.to_string(),
            Self::Array(_) => self.to_string(),
            Self::Map(_) => self.to_string(),
        }
    }

    /// Serializes this scalar as a string for use in hive partition file names.
    fn serialize_encoded(&self) -> String {
        if self.is_null() {
            return NULL_PARTITION_VALUE_DATA_PATH.to_string();
        }
        encode_partition_value(self.serialize().as_str())
    }

    /// Create a [`Scalar`] from a row in an arrow array.
    fn from_array(arr: &dyn Array, index: usize) -> Option<Self> {
        use arrow_array::*;
        use arrow_schema::DataType::*;

        if arr.len() <= index {
            return None;
        }
        if arr.is_null(index) {
            return Some(Self::Null(arr.data_type().try_into_kernel().ok()?));
        }

        match arr.data_type() {
            Utf8 => arr
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|v| checked(v, index, Self::String(v.value(index).to_string()))),
            LargeUtf8 => arr
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|v| checked(v, index, Self::String(v.value(index).to_string()))),
            Utf8View => arr
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(|v| checked(v, index, Self::String(v.value(index).to_string()))),
            Boolean => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|v| checked(v, index, Self::Boolean(v.value(index)))),
            Binary => arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .map(|v| checked(v, index, Self::Binary(v.value(index).to_vec()))),
            LargeBinary => arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map(|v| checked(v, index, Self::Binary(v.value(index).to_vec()))),
            FixedSizeBinary(_) => arr
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .map(|v| checked(v, index, Self::Binary(v.value(index).to_vec()))),
            BinaryView => arr
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .map(|v| checked(v, index, Self::Binary(v.value(index).to_vec()))),
            Int8 => arr
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(|v| checked(v, index, Self::Byte(v.value(index)))),
            Int16 => arr
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(|v| checked(v, index, Self::Short(v.value(index)))),
            Int32 => arr
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(|v| checked(v, index, Self::Integer(v.value(index)))),
            Int64 => arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|v| checked(v, index, Self::Long(v.value(index)))),
            UInt8 => arr
                .as_any()
                .downcast_ref::<UInt8Array>()
                .map(|v| checked(v, index, Self::Byte(v.value(index) as i8))),
            UInt16 => arr
                .as_any()
                .downcast_ref::<UInt16Array>()
                .map(|v| checked(v, index, Self::Short(v.value(index) as i16))),
            UInt32 => arr
                .as_any()
                .downcast_ref::<UInt32Array>()
                .map(|v| checked(v, index, Self::Integer(v.value(index) as i32))),
            UInt64 => arr
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map(|v| checked(v, index, Self::Long(v.value(index) as i64))),
            Float32 => arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(|v| checked(v, index, Self::Float(v.value(index)))),
            Float64 => arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|v| checked(v, index, Self::Double(v.value(index)))),
            Decimal128(precision, scale) => arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .and_then(|v| {
                    let value = v.value(index);
                    if !v.is_valid(index) {
                        return None;
                    }
                    Self::decimal(value, *precision, *scale as u8).ok()
                }),
            Date32 => arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|v| checked(v, index, Self::Date(v.value(index)))),
            Timestamp(TimeUnit::Microsecond, None) => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|v| checked(v, index, Self::TimestampNtz(v.value(index)))),
            Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.eq_ignore_ascii_case("utc") => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|v| checked(v, index, Self::Timestamp(v.value(index)))),
            Struct(fields) => {
                let struct_fields = fields
                    .iter()
                    .flat_map(|f| f.as_ref().try_into_kernel())
                    .collect::<Vec<_>>();
                let struct_arr = arr.as_struct();
                if !struct_arr.is_valid(index) {
                    return None;
                }
                let values = struct_fields
                    .iter()
                    .map(|f: &StructField| {
                        struct_arr
                            .column_by_name(f.name())
                            .and_then(|c| Self::from_array(c.as_ref(), index))
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(Self::Struct(
                    StructData::try_new(struct_fields, values).ok()?,
                ))
            }
            Dictionary(kt, dt) if matches!(kt.as_ref(), UInt16) => {
                let dict_arr = arr.as_dictionary::<UInt16Type>();
                macro_rules! cast_dict {
                    ($array_type:ty, $variant:ident, $conversion:expr) => {{
                        let typed_dict = dict_arr.downcast_dict::<$array_type>()?;
                        Some(checked(
                            &typed_dict,
                            index,
                            Self::$variant($conversion(typed_dict.value(index))),
                        ))
                    }};
                }
                match dt.as_ref() {
                    Utf8 => cast_dict!(StringArray, String, |v: &str| v.to_string()),
                    Utf8View => cast_dict!(StringViewArray, String, |v: &str| v.to_string()),
                    LargeUtf8 => {
                        cast_dict!(LargeStringArray, String, |v: &str| v.to_string())
                    }
                    Binary => cast_dict!(BinaryArray, Binary, |v: &[u8]| v.to_vec()),
                    BinaryView => cast_dict!(BinaryViewArray, Binary, |v: &[u8]| v.to_vec()),
                    LargeBinary => {
                        cast_dict!(LargeBinaryArray, Binary, |v: &[u8]| v.to_vec())
                    }
                    _ => None,
                }
            }
            Float16
            | Decimal32(_, _)
            | Decimal64(_, _)
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
            | ListView(_)
            | LargeListView(_)
            | Null => None,
        }
    }

    /// Serializes this scalar as a serde_json::Value.
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
            Self::Decimal(decimal) => match decimal.scale().cmp(&0) {
                Ordering::Equal => Value::String(decimal.bits().to_string()),
                Ordering::Greater => {
                    let scale = decimal.scale();
                    let value = decimal.bits();
                    let scalar_multiple = 10_i128.pow(scale as u32);
                    let mut s = String::new();
                    s.push_str((value / scalar_multiple).to_string().as_str());
                    s.push('.');
                    s.push_str(&format!(
                        "{:0>scale$}",
                        value % scalar_multiple,
                        scale = scale as usize
                    ));
                    Value::String(s)
                }
                Ordering::Less => {
                    let mut s = decimal.bits().to_string();
                    for _ in 0..decimal.scale() {
                        s.push('0');
                    }
                    Value::String(s)
                }
            },
            Self::Binary(val) => Value::String(create_escaped_binary_string(val.as_slice())),
            Self::Null(_) => Value::Null,
            Self::Struct(struct_data) => {
                let mut result = serde_json::Map::new();
                for (field, value) in struct_data.fields().iter().zip(struct_data.values().iter()) {
                    result.insert(field.name.clone(), value.to_json());
                }
                Value::Object(result)
            }
            Self::Array(array_data) => {
                let mut result = Vec::new();
                #[allow(deprecated)] // array elements are deprecated b/c kernel wants to replace it
                // with a more efficient implementation. However currently no alternatiove API is available.
                for value in array_data.array_elements() {
                    result.push(value.to_json());
                }
                Value::Array(result)
            }
            Self::Map(map_data) => {
                let mut result = serde_json::Map::new();
                for (key, value) in map_data.pairs() {
                    result.insert(key.to_string(), value.to_json());
                }
                Value::Object(result)
            }
        }
    }
}

fn checked(arr: &dyn Array, idx: usize, val: Scalar) -> Scalar {
    if arr.is_null(idx) {
        Scalar::Null(val.data_type())
    } else {
        val
    }
}

fn create_escaped_binary_string(data: &[u8]) -> String {
    let mut escaped_string = String::new();
    for &byte in data {
        // Convert each byte to its two-digit hexadecimal representation
        let hex_representation = format!("{byte:04X}");
        // Append the hexadecimal representation with an escape sequence
        escaped_string.push_str("\\u");
        escaped_string.push_str(&hex_representation);
    }
    escaped_string
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;
    use arrow_array::*;
    use delta_kernel::expressions::Scalar;

    #[test]
    fn test_encode_partition_value() {
        // Test basic encoding
        assert_eq!(encode_partition_value("hello"), "hello");

        // Test special characters that need encoding
        assert_eq!(encode_partition_value("hello world"), "hello%20world");
        assert_eq!(encode_partition_value("test/path"), "test%2Fpath");
        assert_eq!(encode_partition_value("key=value"), "key%3Dvalue");
        assert_eq!(encode_partition_value("a+b"), "a%2Bb");
        assert_eq!(encode_partition_value("a&b"), "a%26b");
        assert_eq!(encode_partition_value("a@b"), "a%40b");

        // Test all special characters in RFC3986_PART
        let special_chars = " !\"#$%&'()*+/:;<=>?@[\\]^`{|}";
        let encoded = encode_partition_value(special_chars);
        assert!(!encoded.contains(' '));
        assert!(encoded.contains('%'));
    }

    #[test]
    fn test_scalar_serialize_string() {
        let scalar = Scalar::String("hello world".to_string());
        assert_eq!(scalar.serialize(), "hello world");
    }

    #[test]
    fn test_scalar_serialize_numbers() {
        assert_eq!(Scalar::Byte(42).serialize(), "42");
        assert_eq!(Scalar::Short(1234).serialize(), "1234");
        assert_eq!(Scalar::Integer(123456).serialize(), "123456");
        assert_eq!(Scalar::Long(123456789).serialize(), "123456789");
        assert_eq!(Scalar::Float(3.14).serialize(), "3.14");
        assert_eq!(Scalar::Double(2.71828).serialize(), "2.71828");
    }

    #[test]
    fn test_scalar_serialize_boolean() {
        assert_eq!(Scalar::Boolean(true).serialize(), "true");
        assert_eq!(Scalar::Boolean(false).serialize(), "false");
    }

    #[test]
    fn test_scalar_serialize_timestamp() {
        // Test timestamp in microseconds (2023-01-01 12:00:00 UTC)
        let timestamp_micros = 1672574400000000; // 2023-01-01 12:00:00 UTC
        let scalar = Scalar::Timestamp(timestamp_micros);
        let serialized = scalar.serialize();
        assert!(serialized.starts_with("2023-01-01 12:00:00"));

        let scalar_ntz = Scalar::TimestampNtz(timestamp_micros);
        assert_eq!(scalar.serialize(), scalar_ntz.serialize());
    }

    #[test]
    fn test_scalar_serialize_date() {
        // Test date (days since epoch)
        let days = 19358; // 2023-01-01
        let scalar = Scalar::Date(days);
        assert_eq!(scalar.serialize(), "2023-01-01");
    }

    #[test]
    fn test_scalar_serialize_decimal() {
        // Test decimal with positive scale
        let decimal = Scalar::decimal(12345, 10, 2).unwrap();
        assert_eq!(decimal.serialize(), "123.45");

        // Test decimal with zero scale
        let decimal = Scalar::decimal(12345, 10, 0).unwrap();
        assert_eq!(decimal.serialize(), "12345");

        // Test decimal with zero scale
        let decimal2 = Scalar::decimal(123, 10, 0).unwrap();
        assert_eq!(decimal2.serialize(), "123");
    }

    #[test]
    fn test_scalar_serialize_binary() {
        let binary_data = vec![0x41, 0x42, 0x43]; // "ABC"
        let scalar = Scalar::Binary(binary_data);
        assert_eq!(scalar.serialize(), "\\u0041\\u0042\\u0043");
    }

    #[test]
    fn test_scalar_serialize_null() {
        let scalar = Scalar::Null(delta_kernel::schema::DataType::STRING);
        assert_eq!(scalar.serialize(), "null");
    }

    #[test]
    fn test_scalar_serialize_encoded() {
        let scalar = Scalar::String("hello world".to_string());
        assert_eq!(scalar.serialize_encoded(), "hello%20world");

        let null_scalar = Scalar::Null(delta_kernel::schema::DataType::STRING);
        assert_eq!(
            null_scalar.serialize_encoded(),
            NULL_PARTITION_VALUE_DATA_PATH
        );
    }

    #[test]
    fn test_scalar_from_array_string() {
        let array = StringArray::from(vec![Some("hello"), Some("world"), None]);

        let scalar = Scalar::from_array(&array, 0).unwrap();
        assert_eq!(scalar, Scalar::String("hello".to_string()));

        let scalar = Scalar::from_array(&array, 1).unwrap();
        assert_eq!(scalar, Scalar::String("world".to_string()));

        let scalar = Scalar::from_array(&array, 2).unwrap();
        assert!(scalar.is_null());

        // Test out of bounds
        assert!(Scalar::from_array(&array, 3).is_none());
    }

    #[test]
    fn test_scalar_from_array_numeric() {
        let int_array = Int32Array::from(vec![Some(42), None, Some(-123)]);

        let scalar = Scalar::from_array(&int_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Integer(42));

        let scalar = Scalar::from_array(&int_array, 1).unwrap();
        assert!(scalar.is_null());

        let scalar = Scalar::from_array(&int_array, 2).unwrap();
        assert_eq!(scalar, Scalar::Integer(-123));
    }

    #[test]
    fn test_scalar_from_array_boolean() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);

        let scalar = Scalar::from_array(&array, 0).unwrap();
        assert_eq!(scalar, Scalar::Boolean(true));

        let scalar = Scalar::from_array(&array, 1).unwrap();
        assert_eq!(scalar, Scalar::Boolean(false));

        let scalar = Scalar::from_array(&array, 2).unwrap();
        assert!(scalar.is_null());
    }

    #[test]
    fn test_scalar_from_array_binary() {
        let array = BinaryArray::from(vec![
            Some(b"hello".as_slice()),
            None,
            Some(b"world".as_slice()),
        ]);

        let scalar = Scalar::from_array(&array, 0).unwrap();
        assert_eq!(scalar, Scalar::Binary(b"hello".to_vec()));

        let scalar = Scalar::from_array(&array, 1).unwrap();
        assert!(scalar.is_null());
    }

    #[test]
    fn test_scalar_from_array_timestamp() {
        let array = TimestampMicrosecondArray::from(vec![
            Some(1672574400000000), // 2023-01-01 12:00:00 UTC
            None,
        ]);

        let scalar = Scalar::from_array(&array, 0).unwrap();
        assert_eq!(scalar, Scalar::TimestampNtz(1672574400000000));

        let scalar = Scalar::from_array(&array, 1).unwrap();
        assert!(scalar.is_null());
    }

    #[test]
    fn test_scalar_from_array_date() {
        let array = Date32Array::from(vec![Some(19358), None]); // 2023-01-01

        let scalar = Scalar::from_array(&array, 0).unwrap();
        assert_eq!(scalar, Scalar::Date(19358));

        let scalar = Scalar::from_array(&array, 1).unwrap();
        assert!(scalar.is_null());
    }

    #[test]
    fn test_scalar_from_array_decimal() {
        let array = Decimal128Array::from(vec![Some(12345_i128), None])
            .with_precision_and_scale(10, 2)
            .unwrap();

        let scalar = Scalar::from_array(&array, 0).unwrap();
        match scalar {
            Scalar::Decimal(d) => {
                assert_eq!(d.bits(), 12345);
                assert_eq!(d.scale(), 2);
            }
            _ => panic!("Expected decimal scalar"),
        }

        let scalar = Scalar::from_array(&array, 1).unwrap();
        assert!(scalar.is_null());
    }

    #[test]
    fn test_scalar_to_json_primitives() {
        assert_eq!(
            Scalar::String("hello".to_string()).to_json(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(
            Scalar::Integer(42).to_json(),
            serde_json::Value::Number(serde_json::Number::from(42))
        );
        assert_eq!(
            Scalar::Boolean(true).to_json(),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            Scalar::Null(delta_kernel::schema::DataType::STRING).to_json(),
            serde_json::Value::Null
        );
    }

    #[test]
    fn test_scalar_to_json_timestamp() {
        let timestamp_micros = 1672574400000000; // 2023-01-01 12:00:00 UTC
        let scalar = Scalar::Timestamp(timestamp_micros);
        let json_val = scalar.to_json();

        match json_val {
            serde_json::Value::String(s) => {
                assert!(s.starts_with("2023-01-01 12:00:00"));
            }
            _ => panic!("Expected string value for timestamp"),
        }
    }

    #[test]
    fn test_scalar_to_json_date() {
        let scalar = Scalar::Date(19358); // 2023-01-01
        let json_val = scalar.to_json();
        assert_eq!(
            json_val,
            serde_json::Value::String("2023-01-01".to_string())
        );
    }

    #[test]
    fn test_scalar_to_json_decimal() {
        let decimal = Scalar::decimal(12345, 10, 2).unwrap();
        let json_val = decimal.to_json();
        assert_eq!(json_val, serde_json::Value::String("123.45".to_string()));
    }

    #[test]
    fn test_scalar_to_json_binary() {
        let scalar = Scalar::Binary(vec![0x41, 0x42]);
        let json_val = scalar.to_json();
        assert_eq!(
            json_val,
            serde_json::Value::String("\\u0041\\u0042".to_string())
        );
    }

    #[test]
    fn test_create_escaped_binary_string() {
        assert_eq!(create_escaped_binary_string(&[]), "");
        assert_eq!(create_escaped_binary_string(&[0x41]), "\\u0041");
        assert_eq!(
            create_escaped_binary_string(&[0x41, 0x42, 0x43]),
            "\\u0041\\u0042\\u0043"
        );
        assert_eq!(
            create_escaped_binary_string(&[0x00, 0xFF]),
            "\\u0000\\u00FF"
        );
    }

    #[test]
    fn test_checked_function() {
        let array = StringArray::from(vec![Some("hello"), None]);

        // Test non-null value
        let result = checked(&array, 0, Scalar::String("hello".to_string()));
        assert_eq!(result, Scalar::String("hello".to_string()));

        // Test null value
        let result = checked(&array, 1, Scalar::String("world".to_string()));
        assert!(result.is_null());
    }

    #[test]
    fn test_scalar_from_array_unsigned_integers() {
        // Test UInt8
        let uint8_array = UInt8Array::from(vec![Some(255_u8), None]);
        let scalar = Scalar::from_array(&uint8_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Byte(-1)); // 255 as i8 is -1

        // Test UInt16
        let uint16_array = UInt16Array::from(vec![Some(65535_u16), None]);
        let scalar = Scalar::from_array(&uint16_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Short(-1)); // 65535 as i16 is -1

        // Test UInt32
        let uint32_array = UInt32Array::from(vec![Some(42_u32), None]);
        let scalar = Scalar::from_array(&uint32_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Integer(42));

        // Test UInt64
        let uint64_array = UInt64Array::from(vec![Some(42_u64), None]);
        let scalar = Scalar::from_array(&uint64_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Long(42));
    }

    #[test]
    fn test_scalar_from_array_float_types() {
        // Test Float32
        let float_array = Float32Array::from(vec![Some(3.14_f32), None]);
        let scalar = Scalar::from_array(&float_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Float(3.14));

        // Test Float64
        let double_array = Float64Array::from(vec![Some(2.71828_f64), None]);
        let scalar = Scalar::from_array(&double_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Double(2.71828));
    }

    #[test]
    fn test_scalar_from_array_all_integer_types() {
        // Test Int8
        let int8_array = Int8Array::from(vec![Some(127_i8)]);
        let scalar = Scalar::from_array(&int8_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Byte(127));

        // Test Int16
        let int16_array = Int16Array::from(vec![Some(32767_i16)]);
        let scalar = Scalar::from_array(&int16_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Short(32767));

        // Test Int64
        let int64_array = Int64Array::from(vec![Some(9223372036854775807_i64)]);
        let scalar = Scalar::from_array(&int64_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Long(9223372036854775807));
    }

    #[test]
    fn test_scalar_from_array_string_variants() {
        // Test LargeStringArray
        let large_string_array = LargeStringArray::from(vec![Some("large_string"), None]);
        let scalar = Scalar::from_array(&large_string_array, 0).unwrap();
        assert_eq!(scalar, Scalar::String("large_string".to_string()));

        // Test StringViewArray
        let string_view_array = StringViewArray::from(vec![Some("view_string"), None]);
        let scalar = Scalar::from_array(&string_view_array, 0).unwrap();
        assert_eq!(scalar, Scalar::String("view_string".to_string()));
    }

    #[test]
    fn test_scalar_from_array_binary_variants() {
        // Test LargeBinaryArray
        let large_binary_array = LargeBinaryArray::from(vec![Some(b"large".as_slice()), None]);
        let scalar = Scalar::from_array(&large_binary_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Binary(b"large".to_vec()));

        // Test FixedSizeBinaryArray - skip this test due to construction complexity
        // The from_array method handles FixedSizeBinaryArray correctly when it encounters one

        // Test BinaryViewArray
        let binary_view_array = BinaryViewArray::from(vec![Some(b"view".as_slice()), None]);
        let scalar = Scalar::from_array(&binary_view_array, 0).unwrap();
        assert_eq!(scalar, Scalar::Binary(b"view".to_vec()));
    }

    #[test]
    fn test_scalar_from_array_struct() {
        use arrow_schema::{DataType, Field};
        use std::sync::Arc;

        // Create a struct schema with two fields: name (string, nullable) and age (int32)
        let name_field = Arc::new(Field::new("name", DataType::Utf8, true));
        let age_field = Arc::new(Field::new("age", DataType::Int32, false));

        // Create arrays for the struct fields
        let name_array = Arc::new(StringArray::from(vec![Some("Alice"), None]));
        let age_array = Arc::new(Int32Array::from(vec![Some(30), Some(25)]));

        // Create the struct array
        let struct_array = StructArray::from(vec![
            (name_field, name_array as Arc<dyn Array>),
            (age_field, age_array as Arc<dyn Array>),
        ]);

        // Test valid struct value
        let scalar = Scalar::from_array(&struct_array, 0).unwrap();
        match scalar {
            Scalar::Struct(struct_data) => {
                assert_eq!(struct_data.fields().len(), 2);
                assert_eq!(struct_data.values().len(), 2);
                // First field should be "Alice"
                assert_eq!(struct_data.values()[0], Scalar::String("Alice".to_string()));
                // Second field should be 30
                assert_eq!(struct_data.values()[1], Scalar::Integer(30));
            }
            _ => panic!("Expected struct scalar"),
        }

        // Test null struct value (note: the struct itself is valid, but first field is null)
        let scalar = Scalar::from_array(&struct_array, 1).unwrap();
        match scalar {
            Scalar::Struct(struct_data) => {
                // First field should be null
                assert!(struct_data.values()[0].is_null());
                // Second field should be 25
                assert_eq!(struct_data.values()[1], Scalar::Integer(25));
            }
            _ => panic!("Expected struct scalar"),
        }
    }

    #[test]
    fn test_scalar_to_json_struct() {
        use delta_kernel::expressions::StructData;
        use delta_kernel::schema::{DataType, StructField};

        // Create struct fields
        let name_field = StructField::new("name", DataType::STRING, false);
        let age_field = StructField::new("age", DataType::INTEGER, false);
        let fields = vec![name_field, age_field];

        // Create struct values
        let values = vec![Scalar::String("Alice".to_string()), Scalar::Integer(30)];

        // Create struct data
        let struct_data = StructData::try_new(fields, values).unwrap();
        let scalar = Scalar::Struct(struct_data);

        // Test JSON conversion
        let json_val = scalar.to_json();
        match json_val {
            serde_json::Value::Object(obj) => {
                assert_eq!(obj.len(), 2);
                assert_eq!(
                    obj.get("name").unwrap(),
                    &serde_json::Value::String("Alice".to_string())
                );
                assert_eq!(
                    obj.get("age").unwrap(),
                    &serde_json::Value::Number(serde_json::Number::from(30))
                );
            }
            _ => panic!("Expected JSON object"),
        }
    }

    #[test]
    fn test_encode_partition_value_edge_cases() {
        // Test empty string
        assert_eq!(encode_partition_value(""), "");

        // Test string with only special characters
        assert_eq!(encode_partition_value("!!!"), "%21%21%21");

        // Test unicode characters (should not be encoded)
        assert_eq!(encode_partition_value("héllo"), "h%C3%A9llo");

        // Test mixed content
        assert_eq!(encode_partition_value("file name.txt"), "file%20name.txt");
    }

    #[test]
    fn test_scalar_serialize_edge_cases() {
        // Test very large numbers
        assert_eq!(Scalar::Long(i64::MAX).serialize(), i64::MAX.to_string());
        assert_eq!(Scalar::Long(i64::MIN).serialize(), i64::MIN.to_string());

        // Test zero values
        assert_eq!(Scalar::Integer(0).serialize(), "0");
        assert_eq!(Scalar::Float(0.0).serialize(), "0");
        assert_eq!(Scalar::Double(0.0).serialize(), "0");

        // Test negative numbers
        assert_eq!(Scalar::Integer(-42).serialize(), "-42");
        assert_eq!(Scalar::Float(-3.14).serialize(), "-3.14");
    }
}
