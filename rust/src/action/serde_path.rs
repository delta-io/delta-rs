use serde::{self, Deserialize, Deserializer, Serialize, Serializer};
use urlencoding::{decode, encode};

pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let decoded = decode(&s).map_err(serde::de::Error::custom)?.into_owned();
    Ok(decoded)
}

pub fn serialize<S>(value: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let decoded = encode(value).into_owned();
    String::serialize(&decoded, serializer)
}
