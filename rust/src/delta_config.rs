//! Delta Table configuration

use crate::{DeltaDataTypeInt, DeltaDataTypeLong, DeltaTableMetaData};
use lazy_static::lazy_static;
use std::time::Duration;

lazy_static! {
    /// How often to checkpoint the delta log.
    pub static ref CHECKPOINT_INTERVAL: DeltaConfig = DeltaConfig::new("checkpointInterval", "10");

    /// The shortest duration we have to keep logically deleted data files around before deleting
    /// them physically.
    /// Note: this value should be large enough:
    /// - It should be larger than the longest possible duration of a job if you decide to run "VACUUM"
    ///   when there are concurrent readers or writers accessing the table.
    ///- If you are running a streaming query reading from the table, you should make sure the query
    ///  doesn't stop longer than this value. Otherwise, the query may not be able to restart as it
    ///  still needs to read old files.
    pub static ref TOMBSTONE_RETENTION: DeltaConfig =
        DeltaConfig::new("deletedFileRetentionDuration", "interval 1 week");

    /// The shortest duration we have to keep delta files around before deleting them. We can only
    /// delete delta files that are before a compaction. We may keep files beyond this duration until
    /// the next calendar day.
    pub static ref LOG_RETENTION: DeltaConfig = DeltaConfig::new("logRetentionDuration", "interval 30 day");

    /// Whether to clean up expired checkpoints and delta logs.
    pub static ref ENABLE_EXPIRED_LOG_CLEANUP: DeltaConfig = DeltaConfig::new("enableExpiredLogCleanup", "true");
}

/// Delta configuration error
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum DeltaConfigError {
    /// Error returned when configuration validation failed.
    #[error("Validation failed - {0}")]
    Validation(String),
}

/// Delta table's `metadata.configuration` entry.
#[derive(Debug)]
pub struct DeltaConfig {
    /// The configuration name
    pub key: String,
    /// The default value if `key` is not set in `metadata.configuration`.
    pub default: String,
}

impl DeltaConfig {
    fn new(key: &str, default: &str) -> Self {
        Self {
            key: key.to_string(),
            default: default.to_string(),
        }
    }

    /// Returns the value from `metadata.configuration` for `self.key` as DeltaDataTypeInt.
    /// If it's missing in metadata then the `self.default` is used.
    #[allow(dead_code)]
    pub fn get_int_from_metadata(
        &self,
        metadata: &DeltaTableMetaData,
    ) -> Result<DeltaDataTypeInt, DeltaConfigError> {
        Ok(parse_int(&self.get_raw_from_metadata(metadata))? as i32)
    }

    /// Returns the value from `metadata.configuration` for `self.key` as DeltaDataTypeLong.
    /// If it's missing in metadata then the `self.default` is used.
    #[allow(dead_code)]
    pub fn get_long_from_metadata(
        &self,
        metadata: &DeltaTableMetaData,
    ) -> Result<DeltaDataTypeLong, DeltaConfigError> {
        parse_int(&self.get_raw_from_metadata(metadata))
    }

    /// Returns the value from `metadata.configuration` for `self.key` as Duration type for the interval.
    /// The string value of this config has to have the following format: interval <number> <unit>.
    /// Where <unit> is either week, day, hour, second, millisecond, microsecond or nanosecond.
    /// If it's missing in metadata then the `self.default` is used.
    pub fn get_interval_from_metadata(
        &self,
        metadata: &DeltaTableMetaData,
    ) -> Result<Duration, DeltaConfigError> {
        parse_interval(&self.get_raw_from_metadata(metadata))
    }

    /// Returns the value from `metadata.configuration` for `self.key` as bool.
    /// If it's missing in metadata then the `self.default` is used.
    pub fn get_boolean_from_metadata(
        &self,
        metadata: &DeltaTableMetaData,
    ) -> Result<bool, DeltaConfigError> {
        parse_bool(&self.get_raw_from_metadata(metadata))
    }

    fn get_raw_from_metadata(&self, metadata: &DeltaTableMetaData) -> String {
        metadata
            .configuration
            .get(&self.key)
            .and_then(|opt| opt.as_deref())
            .unwrap_or(self.default.as_str())
            .to_string()
    }
}

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

fn parse_interval(value: &str) -> Result<Duration, DeltaConfigError> {
    let not_an_interval =
        || DeltaConfigError::Validation(format!("'{}' is not an interval", value));

    if !value.starts_with("interval ") {
        return Err(not_an_interval());
    }
    let mut it = value.split_whitespace();
    let _ = it.next(); // skip "interval"
    let number = parse_int(it.next().ok_or_else(not_an_interval)?)?;
    if number < 0 {
        return Err(DeltaConfigError::Validation(format!(
            "interval '{}' cannot be negative",
            value
        )));
    }
    let number = number as u64;

    let duration = match it.next().ok_or_else(not_an_interval)? {
        "nanosecond" => Duration::from_nanos(number),
        "microsecond" => Duration::from_micros(number),
        "millisecond" => Duration::from_millis(number),
        "second" => Duration::from_secs(number),
        "minute" => Duration::from_secs(number * SECONDS_PER_MINUTE),
        "hour" => Duration::from_secs(number * SECONDS_PER_HOUR),
        "day" => Duration::from_secs(number * SECONDS_PER_DAY),
        "week" => Duration::from_secs(number * SECONDS_PER_WEEK),
        unit => {
            return Err(DeltaConfigError::Validation(format!(
                "Unknown unit '{}'",
                unit
            )));
        }
    };

    Ok(duration)
}

fn parse_int(value: &str) -> Result<i64, DeltaConfigError> {
    value.parse().map_err(|e| {
        DeltaConfigError::Validation(format!("Cannot parse '{}' as integer: {}", value, e))
    })
}

fn parse_bool(value: &str) -> Result<bool, DeltaConfigError> {
    value.parse().map_err(|e| {
        DeltaConfigError::Validation(format!("Cannot parse '{}' as bool: {}", value, e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schema;
    use std::collections::HashMap;

    fn dummy_metadata() -> DeltaTableMetaData {
        let schema = Schema::new(Vec::new());
        DeltaTableMetaData::new(None, None, None, schema, Vec::new(), HashMap::new())
    }

    #[test]
    fn get_interval_from_metadata_test() {
        let mut md = dummy_metadata();

        // default 1 week
        assert_eq!(
            TOMBSTONE_RETENTION
                .get_interval_from_metadata(&md)
                .unwrap()
                .as_secs(),
            1 * SECONDS_PER_WEEK,
        );

        // change to 2 day
        md.configuration.insert(
            TOMBSTONE_RETENTION.key.to_string(),
            Some("interval 2 day".to_string()),
        );
        assert_eq!(
            TOMBSTONE_RETENTION
                .get_interval_from_metadata(&md)
                .unwrap()
                .as_secs(),
            2 * SECONDS_PER_DAY,
        );
    }

    #[test]
    fn get_long_from_metadata_test() {
        assert_eq!(
            CHECKPOINT_INTERVAL
                .get_long_from_metadata(&dummy_metadata())
                .unwrap(),
            10,
        )
    }

    #[test]
    fn get_int_from_metadata_test() {
        assert_eq!(
            CHECKPOINT_INTERVAL
                .get_int_from_metadata(&dummy_metadata())
                .unwrap(),
            10,
        )
    }

    #[test]
    fn parse_interval_test() {
        assert_eq!(
            parse_interval("interval 123 nanosecond").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 microsecond").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 millisecond").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 second").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 minute").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 hour").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 day").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Duration::from_secs(123 * 604800)
        );
    }

    #[test]
    fn parse_interval_invalid_test() {
        assert_eq!(
            parse_interval("whatever").err().unwrap(),
            DeltaConfigError::Validation("'whatever' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval").err().unwrap(),
            DeltaConfigError::Validation("'interval' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval 2").err().unwrap(),
            DeltaConfigError::Validation("'interval 2' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval 2 years").err().unwrap(),
            DeltaConfigError::Validation("Unknown unit 'years'".to_string())
        );

        assert_eq!(
            parse_interval("interval two years").err().unwrap(),
            DeltaConfigError::Validation(
                "Cannot parse 'two' as integer: invalid digit found in string".to_string()
            )
        );

        assert_eq!(
            parse_interval("interval -25 hours").err().unwrap(),
            DeltaConfigError::Validation(
                "interval 'interval -25 hours' cannot be negative".to_string()
            )
        );
    }
}
