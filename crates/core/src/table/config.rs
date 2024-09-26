//! Delta Table configuration
use std::time::Duration;
use std::{collections::HashMap, str::FromStr};

use delta_kernel::features::ColumnMappingMode;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use super::Constraint;
use crate::errors::DeltaTableError;

/// Typed property keys that can be defined on a delta table
/// <https://docs.delta.io/latest/table-properties.html#delta-table-properties-reference>
/// <https://learn.microsoft.com/en-us/azure/databricks/delta/table-properties>
#[derive(PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TableProperty {
    /// true for this Delta table to be append-only. If append-only,
    /// existing records cannot be deleted, and existing values cannot be updated.
    AppendOnly,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
    AutoOptimizeAutoCompact,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table during writes.
    AutoOptimizeOptimizeWrite,

    /// Interval (number of commits) after which a new checkpoint should be created
    CheckpointInterval,

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

    /// A comma-separated list of column names on which Delta Lake collects statistics to enhance
    /// data skipping functionality. This property takes precedence over
    /// [DataSkippingNumIndexedCols](Self::DataSkippingNumIndexedCols).
    DataSkippingStatsColumns,

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

    /// true to enable deletion vectors and predictive I/O for updates.
    EnableDeletionVectors,

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

    /// TODO I could not find this property in the documentation, but was defined here and makes sense..?
    EnableExpiredLogCleanup,

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

    /// 'classic' for classic Delta Lake checkpoints. 'v2' for v2 checkpoints.
    CheckpointPolicy,
}

impl AsRef<str> for TableProperty {
    fn as_ref(&self) -> &str {
        match self {
            Self::AppendOnly => "delta.appendOnly",
            Self::CheckpointInterval => "delta.checkpointInterval",
            Self::AutoOptimizeAutoCompact => "delta.autoOptimize.autoCompact",
            Self::AutoOptimizeOptimizeWrite => "delta.autoOptimize.optimizeWrite",
            Self::CheckpointWriteStatsAsJson => "delta.checkpoint.writeStatsAsJson",
            Self::CheckpointWriteStatsAsStruct => "delta.checkpoint.writeStatsAsStruct",
            Self::CheckpointPolicy => "delta.checkpointPolicy",
            Self::ColumnMappingMode => "delta.columnMapping.mode",
            Self::DataSkippingNumIndexedCols => "delta.dataSkippingNumIndexedCols",
            Self::DataSkippingStatsColumns => "delta.dataSkippingStatsColumns",
            Self::DeletedFileRetentionDuration => "delta.deletedFileRetentionDuration",
            Self::EnableChangeDataFeed => "delta.enableChangeDataFeed",
            Self::EnableDeletionVectors => "delta.enableDeletionVectors",
            Self::IsolationLevel => "delta.isolationLevel",
            Self::LogRetentionDuration => "delta.logRetentionDuration",
            Self::EnableExpiredLogCleanup => "delta.enableExpiredLogCleanup",
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

impl FromStr for TableProperty {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "delta.appendOnly" => Ok(Self::AppendOnly),
            "delta.checkpointInterval" => Ok(Self::CheckpointInterval),
            "delta.autoOptimize.autoCompact" => Ok(Self::AutoOptimizeAutoCompact),
            "delta.autoOptimize.optimizeWrite" => Ok(Self::AutoOptimizeOptimizeWrite),
            "delta.checkpoint.writeStatsAsJson" => Ok(Self::CheckpointWriteStatsAsJson),
            "delta.checkpoint.writeStatsAsStruct" => Ok(Self::CheckpointWriteStatsAsStruct),
            "delta.checkpointPolicy" => Ok(Self::CheckpointPolicy),
            "delta.columnMapping.mode" => Ok(Self::ColumnMappingMode),
            "delta.dataSkippingNumIndexedCols" => Ok(Self::DataSkippingNumIndexedCols),
            "delta.dataSkippingStatsColumns" => Ok(Self::DataSkippingStatsColumns),
            "delta.deletedFileRetentionDuration" | "deletedFileRetentionDuration" => {
                Ok(Self::DeletedFileRetentionDuration)
            }
            "delta.enableChangeDataFeed" => Ok(Self::EnableChangeDataFeed),
            "delta.enableDeletionVectors" => Ok(Self::EnableDeletionVectors),
            "delta.isolationLevel" => Ok(Self::IsolationLevel),
            "delta.logRetentionDuration" | "logRetentionDuration" => Ok(Self::LogRetentionDuration),
            "delta.enableExpiredLogCleanup" | "enableExpiredLogCleanup" => {
                Ok(Self::EnableExpiredLogCleanup)
            }
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
    ($(($docs:literal, $key:expr, $name:ident, $ret:ty, $default:literal),)*) => {
        $(
            #[doc = $docs]
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

/// Default num index cols
pub const DEFAULT_NUM_INDEX_COLS: i32 = 32;
/// Default target file size
pub const DEFAULT_TARGET_FILE_SIZE: i64 = 104857600;

impl<'a> TableConfig<'a> {
    table_config!(
        (
            "true for this Delta table to be append-only",
            TableProperty::AppendOnly,
            append_only,
            bool,
            false
        ),
        (
            "true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.",
            TableProperty::CheckpointWriteStatsAsJson,
            write_stats_as_json,
            bool,
            true
        ),
        (
            "true for Delta Lake to write file statistics to checkpoints in struct format",
            TableProperty::CheckpointWriteStatsAsStruct,
            write_stats_as_struct,
            bool,
            false
        ),
        (
            "The target file size in bytes or higher units for file tuning",
            TableProperty::TargetFileSize,
            target_file_size,
            i64,
            // Databricks / spark defaults to 104857600 (bytes) or 100mb
            104857600
        ),
        (
            "true to enable change data feed.",
            TableProperty::EnableChangeDataFeed,
            enable_change_data_feed,
            bool,
            false
        ),
        (
            "true to enable deletion vectors and predictive I/O for updates.",
            TableProperty::EnableDeletionVectors,
            enable_deletion_vectors,
            bool,
            // in databricks the default is dependent on the workspace settings and runtime version
            // https://learn.microsoft.com/en-us/azure/databricks/administration-guide/workspace-settings/deletion-vectors
            false
        ),
        (
            "The number of columns for Delta Lake to collect statistics about for data skipping.",
            TableProperty::DataSkippingNumIndexedCols,
            num_indexed_cols,
            i32,
            32
        ),
        (
            "whether to cleanup expired logs",
            TableProperty::EnableExpiredLogCleanup,
            enable_expired_log_cleanup,
            bool,
            true
        ),
        (
            "Interval (number of commits) after which a new checkpoint should be created",
            TableProperty::CheckpointInterval,
            checkpoint_interval,
            i32,
            100
        ),
    );

    /// The shortest duration for Delta Lake to keep logically deleted data files before deleting
    /// them physically. This is to prevent failures in stale readers after compactions or partition overwrites.
    ///
    /// This value should be large enough to ensure that:
    ///
    /// * It is larger than the longest possible duration of a job if you run VACUUM when there are
    ///   concurrent readers or writers accessing the Delta table.
    /// * If you run a streaming query that reads from the table, that query does not stop for longer
    ///   than this value. Otherwise, the query may not be able to restart, as it must still read old files.
    pub fn deleted_file_retention_duration(&self) -> Duration {
        lazy_static! {
            static ref DEFAULT_DURATION: Duration = parse_interval("interval 1 weeks").unwrap();
        }
        self.0
            .get(TableProperty::DeletedFileRetentionDuration.as_ref())
            .and_then(|o| o.as_ref().and_then(|v| parse_interval(v).ok()))
            .unwrap_or_else(|| DEFAULT_DURATION.to_owned())
    }

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log size increases.
    pub fn log_retention_duration(&self) -> Duration {
        lazy_static! {
            static ref DEFAULT_DURATION: Duration = parse_interval("interval 30 days").unwrap();
        }
        self.0
            .get(TableProperty::LogRetentionDuration.as_ref())
            .and_then(|o| o.as_ref().and_then(|v| parse_interval(v).ok()))
            .unwrap_or_else(|| DEFAULT_DURATION.to_owned())
    }

    /// The degree to which a transaction must be isolated from modifications made by concurrent transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.0
            .get(TableProperty::IsolationLevel.as_ref())
            .and_then(|o| o.as_ref().and_then(|v| v.parse().ok()))
            .unwrap_or_default()
    }

    /// Policy applied during chepoint creation
    pub fn checkpoint_policy(&self) -> CheckpointPolicy {
        self.0
            .get(TableProperty::CheckpointPolicy.as_ref())
            .and_then(|o| o.as_ref().and_then(|v| v.parse().ok()))
            .unwrap_or_default()
    }

    /// Return the column mapping mode according to delta.columnMapping.mode
    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.0
            .get(TableProperty::ColumnMappingMode.as_ref())
            .and_then(|o| o.as_ref().and_then(|v| v.parse().ok()))
            .unwrap_or_default()
    }

    /// Return the check constraints on the current table
    pub fn get_constraints(&self) -> Vec<Constraint> {
        self.0
            .iter()
            .filter_map(|(field, value)| {
                if field.starts_with("delta.constraints") {
                    value.as_ref().map(|f| Constraint::new("*", f))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Column names on which Delta Lake collects statistics to enhance data skipping functionality.
    /// This property takes precedence over [num_indexed_cols](Self::num_indexed_cols).
    pub fn stats_columns(&self) -> Option<Vec<&str>> {
        self.0
            .get(TableProperty::DataSkippingStatsColumns.as_ref())
            .and_then(|o| o.as_ref().map(|v| v.split(',').collect()))
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
/// The isolation level applied during transaction
pub enum IsolationLevel {
    /// The strongest isolation level. It ensures that committed write operations
    /// and all reads are Serializable. Operations are allowed as long as there
    /// exists a serial sequence of executing them one-at-a-time that generates
    /// the same outcome as that seen in the table. For the write operations,
    /// the serial sequence is exactly the same as that seen in the table’s history.
    Serializable,

    /// A weaker isolation level than Serializable. It ensures only that the write
    /// operations (that is, not reads) are serializable. However, this is still stronger
    /// than Snapshot isolation. WriteSerializable is the default isolation level because
    /// it provides great balance of data consistency and availability for most common operations.
    WriteSerializable,

    /// SnapshotIsolation is a guarantee that all reads made in a transaction will see a consistent
    /// snapshot of the database (in practice it reads the last committed values that existed at the
    /// time it started), and the transaction itself will successfully commit only if no updates
    /// it has made conflict with any concurrent updates made since that snapshot.
    SnapshotIsolation,
}

// Spark assumes Serializable as default isolation level
// https://github.com/delta-io/delta/blob/abb171c8401200e7772b27e3be6ea8682528ac72/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1023
impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Serializable
    }
}

impl AsRef<str> for IsolationLevel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Serializable => "Serializable",
            Self::WriteSerializable => "WriteSerializable",
            Self::SnapshotIsolation => "SnapshotIsolation",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "writeserializable" | "write_serializable" => Ok(Self::WriteSerializable),
            "snapshotisolation" | "snapshot_isolation" => Ok(Self::SnapshotIsolation),
            _ => Err(DeltaTableError::Generic(
                "Invalid string for IsolationLevel".into(),
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
/// The checkpoint policy applied when writing checkpoints
#[serde(rename_all = "camelCase")]
pub enum CheckpointPolicy {
    /// classic Delta Lake checkpoints
    Classic,
    /// v2 checkpoints
    V2,
    /// unknown checkpoint policy
    Other(String),
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self::Classic
    }
}

impl AsRef<str> for CheckpointPolicy {
    fn as_ref(&self) -> &str {
        match self {
            Self::Classic => "classic",
            Self::V2 => "v2",
            Self::Other(s) => s,
        }
    }
}

impl FromStr for CheckpointPolicy {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "classic" => Ok(Self::Classic),
            "v2" => Ok(Self::V2),
            _ => Err(DeltaTableError::Generic(
                "Invalid string for CheckpointPolicy".into(),
            )),
        }
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
        "nanosecond" | "nanoseconds" => Duration::from_nanos(number),
        "microsecond" | "microseconds" => Duration::from_micros(number),
        "millisecond" | "milliseconds" => Duration::from_millis(number),
        "second" | "seconds" => Duration::from_secs(number),
        "minute" | "minutes" => Duration::from_secs(number * SECONDS_PER_MINUTE),
        "hour" | "hours" => Duration::from_secs(number * SECONDS_PER_HOUR),
        "day" | "days" => Duration::from_secs(number * SECONDS_PER_DAY),
        "week" | "weeks" => Duration::from_secs(number * SECONDS_PER_WEEK),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{Metadata, StructType};
    use std::collections::HashMap;

    fn dummy_metadata() -> Metadata {
        let schema = StructType::new(Vec::new());
        Metadata::try_new(schema, Vec::<String>::new(), HashMap::new()).unwrap()
    }

    #[test]
    fn get_interval_from_metadata_test() {
        let md = dummy_metadata();
        let config = TableConfig(&md.configuration);

        // default 1 week
        assert_eq!(
            config.deleted_file_retention_duration().as_secs(),
            SECONDS_PER_WEEK,
        );

        // change to 2 day
        let mut md = dummy_metadata();
        md.configuration.insert(
            TableProperty::DeletedFileRetentionDuration
                .as_ref()
                .to_string(),
            Some("interval 2 day".to_string()),
        );
        let config = TableConfig(&md.configuration);

        assert_eq!(
            config.deleted_file_retention_duration().as_secs(),
            2 * SECONDS_PER_DAY,
        );
    }

    #[test]
    fn get_long_from_metadata_test() {
        let md = dummy_metadata();
        let config = TableConfig(&md.configuration);
        assert_eq!(config.checkpoint_interval(), 100,)
    }

    #[test]
    fn get_boolean_from_metadata_test() {
        let md = dummy_metadata();
        let config = TableConfig(&md.configuration);

        // default value is true
        assert!(config.enable_expired_log_cleanup());

        // change to false
        let mut md = dummy_metadata();
        md.configuration.insert(
            TableProperty::EnableExpiredLogCleanup.as_ref().into(),
            Some("false".to_string()),
        );
        let config = TableConfig(&md.configuration);

        assert!(!config.enable_expired_log_cleanup());
    }

    #[test]
    fn parse_interval_test() {
        assert_eq!(
            parse_interval("interval 123 nanosecond").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 nanoseconds").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 microsecond").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 microseconds").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 millisecond").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 milliseconds").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 second").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 seconds").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 minute").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 minutes").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 hour").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 hours").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 day").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 days").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Duration::from_secs(123 * 604800)
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
