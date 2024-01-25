//! Scalar values for use in expressions.

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use arrow_array::Array;
use arrow_schema::TimeUnit;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use object_store::path::Path;

use crate::kernel::{DataType, Error, PrimitiveType, StructField};
use crate::NULL_PARTITION_VALUE_DATA_PATH;

/// A single value, which can be null. Used for representing literal values
/// in [Expressions][crate::expressions::Expression].
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    /// 32bit integer
    Integer(i32),
    /// 64bit integer
    Long(i64),
    /// 16bit integer
    Short(i16),
    /// 8bit integer
    Byte(i8),
    /// 32bit floating point
    Float(f32),
    /// 64bit floating point
    Double(f64),
    /// utf-8 encoded string.
    String(String),
    /// true or false value
    Boolean(bool),
    /// Microsecond precision timestamp, adjusted to UTC.
    Timestamp(i64),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date(i32),
    /// Binary data
    Binary(Vec<u8>),
    /// Decimal value
    Decimal(i128, u8, i8),
    /// Null value with a given data type.
    Null(DataType),
    /// Struct value
    Struct(Vec<Scalar>, Vec<StructField>),
}

impl Scalar {
    /// Returns the data type of this scalar.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Integer(_) => DataType::Primitive(PrimitiveType::Integer),
            Self::Long(_) => DataType::Primitive(PrimitiveType::Long),
            Self::Short(_) => DataType::Primitive(PrimitiveType::Short),
            Self::Byte(_) => DataType::Primitive(PrimitiveType::Byte),
            Self::Float(_) => DataType::Primitive(PrimitiveType::Float),
            Self::Double(_) => DataType::Primitive(PrimitiveType::Double),
            Self::String(_) => DataType::Primitive(PrimitiveType::String),
            Self::Boolean(_) => DataType::Primitive(PrimitiveType::Boolean),
            Self::Timestamp(_) => DataType::Primitive(PrimitiveType::Timestamp),
            Self::Date(_) => DataType::Primitive(PrimitiveType::Date),
            Self::Binary(_) => DataType::Primitive(PrimitiveType::Binary),
            Self::Decimal(_, precision, scale) => DataType::decimal(*precision, *scale),
            Self::Null(data_type) => data_type.clone(),
            Self::Struct(_, fields) => DataType::struct_type(fields.clone()),
        }
    }

    /// Returns true if this scalar is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null(_))
    }

    /// Serializes this scalar as a string.
    pub fn serialize(&self) -> String {
        match self {
            Self::String(s) => s.to_owned(),
            Self::Byte(b) => b.to_string(),
            Self::Short(s) => s.to_string(),
            Self::Integer(i) => i.to_string(),
            Self::Long(l) => l.to_string(),
            Self::Float(f) => f.to_string(),
            Self::Double(d) => d.to_string(),
            Self::Boolean(b) => {
                if *b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            Self::Timestamp(ts) => {
                let ts = Utc.timestamp_micros(*ts).single().unwrap();
                ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
            Self::Date(days) => {
                let date = Utc.from_utc_datetime(
                    &NaiveDateTime::from_timestamp_opt(*days as i64 * 24 * 3600, 0).unwrap(),
                );
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
                    for _ in 0..(scale.abs()) {
                        s.push('0');
                    }
                    s
                }
            },
            Self::Binary(val) => create_escaped_binary_string(val.as_slice()),
            Self::Null(_) => "null".to_string(),
            Self::Struct(_, _) => todo!("serializing struct values is not yet supported"),
        }
    }

    /// Serializes this scalar as a string for use in hive partition file names.
    pub fn serialize_encoded(&self) -> String {
        if self.is_null() {
            return NULL_PARTITION_VALUE_DATA_PATH.to_string();
        }
        Path::from(self.serialize()).to_string()
    }

    /// Create a [`Scalar`] form a row in an arrow array.
    pub fn from_array(arr: &dyn Array, index: usize) -> Option<Self> {
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
                    Self::Decimal(value, *precision, *scale)
                })
            }
            Date32 => arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|v| Self::Date(v.value(index))),
            // TODO handle timezones when implementing timestamp ntz feature.
            Timestamp(TimeUnit::Microsecond, None) => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|v| Self::Timestamp(v.value(index))),
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
                if struct_fields.len() != values.len() {
                    return None;
                }
                Some(Self::Struct(values, struct_fields))
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
            | Null => None,
        }
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Scalar::*;
        match (self, other) {
            (Null(_), Null(_)) => Some(Ordering::Equal),
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (Long(a), Long(b)) => a.partial_cmp(b),
            (Short(a), Short(b)) => a.partial_cmp(b),
            (Byte(a), Byte(b)) => a.partial_cmp(b),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),
            (String(a), String(b)) => a.partial_cmp(b),
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),
            (Date(a), Date(b)) => a.partial_cmp(b),
            (Binary(a), Binary(b)) => a.partial_cmp(b),
            (Decimal(a, _, _), Decimal(b, _, _)) => a.partial_cmp(b),
            (Struct(a, _), Struct(b, _)) => a.partial_cmp(b),
            // TODO should we make an assumption about the ordering of nulls?
            // rigth now this is only used for internal purposes.
            (Null(_), _) => Some(Ordering::Less),
            (_, Null(_)) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(i) => write!(f, "{}", i),
            Self::Long(i) => write!(f, "{}", i),
            Self::Short(i) => write!(f, "{}", i),
            Self::Byte(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::Double(fl) => write!(f, "{}", fl),
            Self::String(s) => write!(f, "'{}'", s),
            Self::Boolean(b) => write!(f, "{}", b),
            Self::Timestamp(ts) => write!(f, "{}", ts),
            Self::Date(d) => write!(f, "{}", d),
            Self::Binary(b) => write!(f, "{:?}", b),
            Self::Decimal(value, _, scale) => match scale.cmp(&0) {
                Ordering::Equal => {
                    write!(f, "{}", value)
                }
                Ordering::Greater => {
                    let scalar_multiple = 10_i128.pow(*scale as u32);
                    write!(f, "{}", value / scalar_multiple)?;
                    write!(f, ".")?;
                    write!(
                        f,
                        "{:0>scale$}",
                        value % scalar_multiple,
                        scale = *scale as usize
                    )
                }
                Ordering::Less => {
                    write!(f, "{}", value)?;
                    for _ in 0..(scale.abs()) {
                        write!(f, "0")?;
                    }
                    Ok(())
                }
            },
            Self::Null(_) => write!(f, "null"),
            Self::Struct(values, fields) => {
                write!(f, "{{")?;
                for (i, (value, field)) in values.iter().zip(fields.iter()).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, value)?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl From<i32> for Scalar {
    fn from(i: i32) -> Self {
        Self::Integer(i)
    }
}

impl From<i64> for Scalar {
    fn from(i: i64) -> Self {
        Self::Long(i)
    }
}

impl From<bool> for Scalar {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

impl From<&str> for Scalar {
    fn from(s: &str) -> Self {
        Self::String(s.into())
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

// TODO: add more From impls

impl PrimitiveType {
    fn data_type(&self) -> DataType {
        DataType::Primitive(self.clone())
    }

    /// Parses a string into a scalar value.
    pub fn parse_scalar(&self, raw: &str) -> Result<Scalar, Error> {
        use PrimitiveType::*;

        lazy_static::lazy_static! {
            static ref UNIX_EPOCH: DateTime<Utc> = DateTime::from_timestamp(0, 0).unwrap();
        }

        if raw.is_empty() || raw == NULL_PARTITION_VALUE_DATA_PATH {
            return Ok(Scalar::Null(self.data_type()));
        }

        match self {
            String => Ok(Scalar::String(raw.to_string())),
            Byte => self.str_parse_scalar(raw, Scalar::Byte),
            Short => self.str_parse_scalar(raw, Scalar::Short),
            Integer => self.str_parse_scalar(raw, Scalar::Integer),
            Long => self.str_parse_scalar(raw, Scalar::Long),
            Float => self.str_parse_scalar(raw, Scalar::Float),
            Double => self.str_parse_scalar(raw, Scalar::Double),
            Boolean => {
                if raw.eq_ignore_ascii_case("true") {
                    Ok(Scalar::Boolean(true))
                } else if raw.eq_ignore_ascii_case("false") {
                    Ok(Scalar::Boolean(false))
                } else {
                    Err(self.parse_error(raw))
                }
            }
            Date => {
                let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                    .map_err(|_| self.parse_error(raw))?
                    .and_hms_opt(0, 0, 0)
                    .ok_or(self.parse_error(raw))?;
                let date = Utc.from_utc_datetime(&date);
                let days = date.signed_duration_since(*UNIX_EPOCH).num_days() as i32;
                Ok(Scalar::Date(days))
            }
            Timestamp => {
                let timestamp = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                    .map_err(|_| self.parse_error(raw))?;
                let timestamp = Utc.from_utc_datetime(&timestamp);
                let micros = timestamp
                    .signed_duration_since(*UNIX_EPOCH)
                    .num_microseconds()
                    .ok_or(self.parse_error(raw))?;
                Ok(Scalar::Timestamp(micros))
            }
            Binary => {
                let bytes = parse_escaped_binary_string(raw).map_err(|_| self.parse_error(raw))?;
                Ok(Scalar::Binary(bytes))
            }
            _ => todo!("parsing {:?} is not yet supported", self),
        }
    }

    fn parse_error(&self, raw: &str) -> Error {
        Error::Parse(raw.to_string(), self.data_type())
    }

    fn str_parse_scalar<T: std::str::FromStr>(
        &self,
        raw: &str,
        f: impl FnOnce(T) -> Scalar,
    ) -> Result<Scalar, Error> {
        match raw.parse() {
            Ok(val) => Ok(f(val)),
            Err(..) => Err(self.parse_error(raw)),
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

fn parse_escaped_binary_string(escaped_string: &str) -> Result<Vec<u8>, &'static str> {
    let mut parsed_bytes = Vec::new();
    let mut chars = escaped_string.chars();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            // Check for the escape sequence "\\u" indicating a hexadecimal value
            if chars.next() == Some('u') {
                // Read two hexadecimal digits and convert to u8
                if let (Some(digit1), Some(digit2), Some(digit3), Some(digit4)) =
                    (chars.next(), chars.next(), chars.next(), chars.next())
                {
                    if let Ok(byte) =
                        u8::from_str_radix(&format!("{}{}{}{}", digit1, digit2, digit3, digit4), 16)
                    {
                        parsed_bytes.push(byte);
                    } else {
                        return Err("Error parsing hexadecimal value");
                    }
                } else {
                    return Err("Incomplete escape sequence");
                }
            } else {
                // Unrecognized escape sequence
                return Err("Unrecognized escape sequence");
            }
        } else {
            // Regular character, convert to u8 and push into the result vector
            parsed_bytes.push(ch as u8);
        }
    }

    Ok(parsed_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_roundtrip() {
        let scalar = Scalar::Binary(vec![0, 1, 2, 3, 4, 5]);
        let parsed = PrimitiveType::Binary
            .parse_scalar(&scalar.serialize())
            .unwrap();
        assert_eq!(scalar, parsed);
    }

    #[test]
    fn test_decimal_display() {
        let s = Scalar::Decimal(123456789, 9, 2);
        assert_eq!(s.to_string(), "1234567.89");

        let s = Scalar::Decimal(123456789, 9, 0);
        assert_eq!(s.to_string(), "123456789");

        let s = Scalar::Decimal(123456789, 9, 9);
        assert_eq!(s.to_string(), "0.123456789");

        let s = Scalar::Decimal(123, 9, -3);
        assert_eq!(s.to_string(), "123000");
    }
}
