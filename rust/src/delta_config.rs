//! Delta Table configuration
use std::time::Duration;
use std::{collections::HashMap, str::FromStr};

use lazy_static::lazy_static;

use crate::operations::transaction::IsolationLevel;
use crate::{DeltaDataTypeInt, DeltaDataTypeLong, DeltaTableError, DeltaTableMetaData};

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

/// Typed property keys that can be defined on a delta table
/// <https://docs.delta.io/latest/table-properties.html#delta-table-properties-reference>
/// <https://learn.microsoft.com/en-us/azure/databricks/delta/table-properties>
pub enum DeltaConfigKey {
    /// true for this Delta table to be append-only. If append-only,
    /// existing records cannot be deleted, and existing values cannot be updated.
    AppendOnly,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
    AutoOptimizeAutoCompact,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table during writes.
    AutoOptimizeOptimizeWrite,

    /// true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
    CheckpointWriteStatsAsJson,

    /// true for Delta Lake to write file statistics to checkpoints in struct format for the
    /// stats_parsed column and to write partition values as a struct for partitionValues_parsed.
    CheckpointWriteStatsAsStruct,

    /// Whether column mapping is enabled for Delta table columns and the corresponding
    /// Parquet columns that use different names.
    ColumnMappingMode,

    /// The number of columns for Delta Lake to collect statistics about for data skipping.
    /// A value of -1 means to collect statistics for all columns. Updating this property does
    /// not automatically collect statistics again; instead, it redefines the statistics schema
    /// of the Delta table. Specifically, it changes the behavior of future statistics collection
    /// (such as during appends and optimizations) as well as data skipping (such as ignoring column
    /// statistics beyond this number, even when such statistics exist).
    DataSkippingNumIndexedCols,

    /// The shortest duration for Delta Lake to keep logically deleted data files before deleting
    /// them physically. This is to prevent failures in stale readers after compactions or partition overwrites.
    ///
    /// This value should be large enough to ensure that:
    ///
    /// * It is larger than the longest possible duration of a job if you run VACUUM when there are
    ///   concurrent readers or writers accessing the Delta table.
    /// * If you run a streaming query that reads from the table, that query does not stop for longer
    ///   than this value. Otherwise, the query may not be able to restart, as it must still read old files.
    DeletedFileRetentionDuration,

    /// true to enable change data feed.
    EnableChangeDataFeed,

    /// The degree to which a transaction must be isolated from modifications made by concurrent transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    IsolationLevel,

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log size increases.
    LogRetentionDuration,

    /// The minimum required protocol reader version for a reader that allows to read from this Delta table.
    MinReaderVersion,

    /// The minimum required protocol writer version for a writer that allows to write to this Delta table.
    MinWriterVersion,

    /// true for Delta Lake to generate a random prefix for a file path instead of partition information.
    ///
    /// For example, this ma
    /// y improve Amazon S3 performance when Delta Lake needs to send very high volumes
    /// of Amazon S3 calls to better partition across S3 servers.
    RandomizeFilePrefixes,

    /// When delta.randomizeFilePrefixes is set to true, the number of characters that Delta Lake generates for random prefixes.
    RandomPrefixLength,

    /// The shortest duration within which new snapshots will retain transaction identifiers (for example, SetTransactions).
    /// When a new snapshot sees a transaction identifier older than or equal to the duration specified by this property,
    /// the snapshot considers it expired and ignores it. The SetTransaction identifier is used when making the writes idempotent.
    SetTransactionRetentionDuration,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
    TargetFileSize,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
    TuneFileSizesForRewrites,
}

impl AsRef<str> for DeltaConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::AppendOnly => "delta.appendOnly",
            Self::AutoOptimizeAutoCompact => "delta.autoOptimize.autoCompact",
            Self::AutoOptimizeOptimizeWrite => "delta.autoOptimize.optimizeWrite",
            Self::CheckpointWriteStatsAsJson => "delta.checkpoint.writeStatsAsJson",
            Self::CheckpointWriteStatsAsStruct => "delta.checkpoint.writeStatsAsStruct",
            Self::ColumnMappingMode => "delta.columnMapping.mode",
            Self::DataSkippingNumIndexedCols => "delta.dataSkippingNumIndexedCols",
            Self::DeletedFileRetentionDuration => "delta.deletedFileRetentionDuration",
            Self::EnableChangeDataFeed => "delta.enableChangeDataFeed",
            Self::IsolationLevel => "delta.isolationLevel",
            Self::LogRetentionDuration => "delta.logRetentionDuration",
            Self::MinReaderVersion => "delta.minReaderVersion",
            Self::MinWriterVersion => "delta.minWriterVersion",
            Self::RandomizeFilePrefixes => "delta.randomizeFilePrefixes",
            Self::RandomPrefixLength => "delta.randomPrefixLength",
            Self::SetTransactionRetentionDuration => "delta.setTransactionRetentionDuration",
            Self::TargetFileSize => "delta.targetFileSize",
            Self::TuneFileSizesForRewrites => "delta.tuneFileSizesForRewrites",
        }
    }
}

impl FromStr for DeltaConfigKey {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "delta.appendOnly" => Ok(Self::AppendOnly),
            "delta.autoOptimize.autoCompact" => Ok(Self::AutoOptimizeAutoCompact),
            "delta.autoOptimize.optimizeWrite" => Ok(Self::AutoOptimizeOptimizeWrite),
            "delta.checkpoint.writeStatsAsJson" => Ok(Self::CheckpointWriteStatsAsJson),
            "delta.checkpoint.writeStatsAsStruct" => Ok(Self::CheckpointWriteStatsAsStruct),
            "delta.columnMapping.mode" => Ok(Self::ColumnMappingMode),
            "delta.dataSkippingNumIndexedCols" => Ok(Self::DataSkippingNumIndexedCols),
            "delta.deletedFileRetentionDuration" | "deletedFileRetentionDuration" => {
                Ok(Self::DeletedFileRetentionDuration)
            }
            "delta.enableChangeDataFeed" => Ok(Self::EnableChangeDataFeed),
            "delta.isolationLevel" => Ok(Self::IsolationLevel),
            "delta.logRetentionDuration" | "logRetentionDuration" => Ok(Self::LogRetentionDuration),
            "delta.minReaderVersion" => Ok(Self::MinReaderVersion),
            "delta.minWriterVersion" => Ok(Self::MinWriterVersion),
            "delta.randomizeFilePrefixes" => Ok(Self::RandomizeFilePrefixes),
            "delta.randomPrefixLength" => Ok(Self::RandomPrefixLength),
            "delta.setTransactionRetentionDuration" => Ok(Self::SetTransactionRetentionDuration),
            "delta.targetFileSize" => Ok(Self::TargetFileSize),
            "delta.tuneFileSizesForRewrites" => Ok(Self::TuneFileSizesForRewrites),
            _ => Err(DeltaTableError::Generic("unknown config key".into())),
        }
    }
}

/// Delta configuration error
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum DeltaConfigError {
    /// Error returned when configuration validation failed.
    #[error("Validation failed - {0}")]
    Validation(String),
}

macro_rules! table_config {
    ($(($key:expr, $name:ident, $ret:ty, $default:literal),)*) => {
        $(
            /// read property $key
            pub fn $name(&self) -> $ret {
                self.0
                    .get($key.as_ref())
                    .and_then(|opt| opt.as_ref().and_then(|value| value.parse().ok()))
                    .unwrap_or($default)
            }
        )*
    }
}

/// Well known delta table configuration
pub struct TableConfig<'a>(pub(crate) &'a HashMap<String, Option<String>>);

impl<'a> TableConfig<'a> {
    table_config!(
        (DeltaConfigKey::AppendOnly, append_only, bool, false),
        (
            DeltaConfigKey::CheckpointWriteStatsAsJson,
            write_stats_as_json,
            bool,
            true
        ),
        (
            DeltaConfigKey::CheckpointWriteStatsAsStruct,
            write_stats_as_struct,
            bool,
            true
        ),
        (
            DeltaConfigKey::TargetFileSize,
            target_file_size,
            i64,
            // Databricks / spark defaults to 104857600 (bytes) or 100mb
            104857600
        ),
        (
            DeltaConfigKey::EnableChangeDataFeed,
            enable_change_data_feed,
            bool,
            false
        ),
        (
            DeltaConfigKey::DataSkippingNumIndexedCols,
            num_indexed_cols,
            i32,
            32
        ),
    );

    /// Get the configured or default isolation level
    pub fn isolation_level(&self) -> IsolationLevel {
        self.0
            .get(DeltaConfigKey::IsolationLevel.as_ref())
            .and_then(|o| o.as_ref().and_then(|v| v.parse().ok()))
            .unwrap_or_default()
    }
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
    let not_an_interval = || DeltaConfigError::Validation(format!("'{value}' is not an interval"));

    if !value.starts_with("interval ") {
        return Err(not_an_interval());
    }
    let mut it = value.split_whitespace();
    let _ = it.next(); // skip "interval"
    let number = parse_int(it.next().ok_or_else(not_an_interval)?)?;
    if number < 0 {
        return Err(DeltaConfigError::Validation(format!(
            "interval '{value}' cannot be negative"
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
                "Unknown unit '{unit}'"
            )));
        }
    };

    Ok(duration)
}

fn parse_int(value: &str) -> Result<i64, DeltaConfigError> {
    value.parse().map_err(|e| {
        DeltaConfigError::Validation(format!("Cannot parse '{value}' as integer: {e}"))
    })
}

fn parse_bool(value: &str) -> Result<bool, DeltaConfigError> {
    value
        .parse()
        .map_err(|e| DeltaConfigError::Validation(format!("Cannot parse '{value}' as bool: {e}")))
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
            SECONDS_PER_WEEK,
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
    fn get_boolean_from_metadata_test() {
        let mut md = dummy_metadata();

        // default value is true
        assert!(ENABLE_EXPIRED_LOG_CLEANUP
            .get_boolean_from_metadata(&md)
            .unwrap(),);

        // change to false
        md.configuration.insert(
            ENABLE_EXPIRED_LOG_CLEANUP.key.to_string(),
            Some("false".to_string()),
        );
        assert!(!ENABLE_EXPIRED_LOG_CLEANUP
            .get_boolean_from_metadata(&md)
            .unwrap());
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
