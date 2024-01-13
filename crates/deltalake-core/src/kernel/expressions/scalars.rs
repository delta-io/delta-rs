//! Scalar values for use in expressions.

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

use crate::kernel::schema::{DataType, PrimitiveType};
use crate::kernel::Error;

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
        }
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
                let ts = Utc.timestamp_millis_opt(*ts).single().unwrap();
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
            Self::Null(_) => "null".to_string(),
            _ => todo!(),
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

        if raw.is_empty() {
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
            _ => todo!(),
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

#[cfg(test)]
mod tests {
    use super::*;

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
