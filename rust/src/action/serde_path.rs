use std::str::Utf8Error;

use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};
use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    decode_path(&s).map_err(serde::de::Error::custom)
}

pub fn serialize<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = encode_path(value);
    String::serialize(&encoded, serializer)
}

pub const _DELIMITER: &str = "/";
/// The path delimiter as a single byte
pub const _DELIMITER_BYTE: u8 = _DELIMITER.as_bytes()[0];

/// Characters we want to encode.
const INVALID: &AsciiSet = &CONTROLS
    // The delimiter we are reserving for internal hierarchy
    // .add(DELIMITER_BYTE)
    // Characters AWS recommends avoiding for object keys
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
    .add(b'\\')
    .add(b'{')
    .add(b'^')
    .add(b'}')
    .add(b'%')
    .add(b'`')
    .add(b']')
    .add(b'"')
    .add(b'>')
    .add(b'[')
    // .add(b'~')
    .add(b'<')
    .add(b'#')
    .add(b'|')
    // Characters Google Cloud Storage recommends avoiding for object names
    // https://cloud.google.com/storage/docs/naming-objects
    .add(b'\r')
    .add(b'\n')
    .add(b'*')
    .add(b'?');

fn encode_path(path: &str) -> String {
    percent_encode(path.as_bytes(), INVALID).to_string()
}

fn decode_path(path: &str) -> Result<String, Utf8Error> {
    Ok(percent_decode_str(path).decode_utf8()?.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_path() {
        let cases = [
            (
                "string=$%25&%2F()%3D%5E%22%5B%5D%23%2A%3F.%3A/part-00023-4b06bc90-0678-4a63-94a2-f09af1adb945.c000.snappy.parquet", 
                "string=$%2525&%252F()%253D%255E%2522%255B%255D%2523%252A%253F.%253A/part-00023-4b06bc90-0678-4a63-94a2-f09af1adb945.c000.snappy.parquet",
            ),
            (
                "string=$%25&%2F()%3D%5E%22<>~%5B%5D%7B}`%23|%2A%3F%2F%5Cr%5Cn.%3A/part-00023-e0a68495-8098-40a6-be5f-b502b111b789.c000.snappy.parquet",
                "string=$%2525&%252F()%253D%255E%2522%3C%3E~%255B%255D%257B%7D%60%2523%7C%252A%253F%252F%255Cr%255Cn.%253A/part-00023-e0a68495-8098-40a6-be5f-b502b111b789.c000.snappy.parquet"
            ),
            (
                "string=$%25&%2F()%3D%5E%22<>~%5B%5D%7B}`%23|%2A%3F%2F%5Cr%5Cn.%3A_-/part-00023-346b6795-dafa-4948-bda5-ecdf4baa4445.c000.snappy.parquet",
                "string=$%2525&%252F()%253D%255E%2522%3C%3E~%255B%255D%257B%7D%60%2523%7C%252A%253F%252F%255Cr%255Cn.%253A_-/part-00023-346b6795-dafa-4948-bda5-ecdf4baa4445.c000.snappy.parquet"
            )
        ];

        for (raw, expected) in cases {
            let encoded = encode_path(raw);
            assert_eq!(encoded, expected);
            let decoded = decode_path(expected).unwrap();
            assert_eq!(decoded, raw);
        }
    }
}
