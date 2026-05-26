//! Delta Table configuration
use std::collections::HashMap;
use std::num::{NonZero, NonZeroU64};
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

#[cfg(feature = "datafusion")]
use datafusion::config::{EncryptionFactoryOptions, TableParquetOptions};
use delta_kernel::table_properties::{DataSkippingNumIndexedCols, IsolationLevel, TableProperties};

use super::Constraint;
use crate::errors::DeltaTableError;

/// Typed property keys that can be defined on a delta table
///
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

    /// true for Delta Lake to write checkpoint files using run length encoding (RLE).
    /// Some readers don't support run length encoding (i.e. Fabric) so this can be disabled.
    CheckpointUseRunLengthEncoding,

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
            Self::CheckpointUseRunLengthEncoding => "delta-rs.checkpoint.useRunLengthEncoding",
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
            "delta-rs.checkpoint.useRunLengthEncoding" => Ok(Self::CheckpointUseRunLengthEncoding),
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

/// Default num index cols
pub const DEFAULT_NUM_INDEX_COLS: u64 = 32;
/// Default target file size
pub const DEFAULT_TARGET_FILE_SIZE: NonZeroU64 = NonZeroU64::new(100 * 1024 * 1024).unwrap();

pub trait TablePropertiesExt {
    /// true for this Delta table to be append-only. If append-only, existing records cannot be
    /// deleted, and existing values cannot be updated. See [append-only tables] in the protocol.
    ///
    /// [append-only tables]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#append-only-tables
    fn append_only(&self) -> bool;

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log
    /// size increases.
    fn log_retention_duration(&self) -> Duration;

    /// Whether to clean up expired checkpoints/commits in the delta log.
    fn enable_expired_log_cleanup(&self) -> bool;

    /// Interval (expressed as number of commits) after which a new checkpoint should be created.
    /// E.g. if checkpoint interval = 10, then a checkpoint should be written every 10 commits.
    fn checkpoint_interval(&self) -> NonZero<u64>;

    /// Number of columns to be indexed.
    fn num_indexed_cols(&self) -> DataSkippingNumIndexedCols;

    fn target_file_size(&self) -> NonZero<u64>;

    fn enable_change_data_feed(&self) -> bool;

    fn deleted_file_retention_duration(&self) -> Duration;

    fn isolation_level(&self) -> IsolationLevel;

    fn get_constraints(&self) -> Vec<Constraint>;
}

impl TablePropertiesExt for TableProperties {
    fn append_only(&self) -> bool {
        self.append_only.unwrap_or(false)
    }

    fn log_retention_duration(&self) -> Duration {
        static DEFAULT_DURATION: LazyLock<Duration> =
            LazyLock::new(|| parse_interval("interval 30 days").unwrap());
        self.log_retention_duration
            .unwrap_or(DEFAULT_DURATION.to_owned())
    }

    fn enable_expired_log_cleanup(&self) -> bool {
        self.enable_expired_log_cleanup.unwrap_or(true)
    }

    fn checkpoint_interval(&self) -> NonZero<u64> {
        static DEFAULT_INTERVAL: LazyLock<NonZero<u64>> =
            LazyLock::new(|| NonZero::new(100).unwrap());
        self.checkpoint_interval
            .unwrap_or(DEFAULT_INTERVAL.to_owned())
    }

    fn num_indexed_cols(&self) -> DataSkippingNumIndexedCols {
        self.data_skipping_num_indexed_cols
            .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32))
    }

    fn target_file_size(&self) -> NonZeroU64 {
        self.target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE)
    }

    fn enable_change_data_feed(&self) -> bool {
        self.enable_change_data_feed.unwrap_or(false)
    }

    fn deleted_file_retention_duration(&self) -> Duration {
        static DEFAULT_DURATION: LazyLock<Duration> =
            LazyLock::new(|| parse_interval("interval 1 weeks").unwrap());
        self.deleted_file_retention_duration
            .unwrap_or(DEFAULT_DURATION.to_owned())
    }

    fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap_or_default()
    }

    /// Return the check constraints on the current table
    fn get_constraints(&self) -> Vec<Constraint> {
        // TODO: upstream parsing of constraints to delta-kernel
        self.unknown_properties
            .iter()
            .filter_map(|(field, value)| {
                if field.starts_with("delta.constraints") {
                    let constraint_name = field.replace("delta.constraints.", "");
                    Some(Constraint::new(&constraint_name, value))
                } else {
                    None
                }
            })
            .collect()
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

// ---------------------------------------------------------------------------
// EncryptionConfig — parsed from delta.encryption.* table properties
// ---------------------------------------------------------------------------

/// Delta table property keys for encryption configuration.
pub const ENCRYPTION_KMS_ID_PROP: &str = "delta.encryption.kms.id";
pub const ENCRYPTION_KMS_CONFIGURATION_PROP: &str = "delta.encryption.kms.configuration";
pub const ENCRYPTION_FOOTER_KEY_PROP: &str = "delta.encryption.footer.key";
pub const ENCRYPTION_PLAINTEXT_FOOTER_PROP: &str = "delta.encryption.plaintext.footer";
pub const ENCRYPTION_COLUMN_KEYS_PROP: &str = "delta.encryption.column.keys";

/// Key names forwarded to [`EncryptionFactoryOptions`] (suffix after `delta.encryption.` stripped).
#[cfg(feature = "datafusion")]
pub(crate) const FACTORY_OPT_KMS_CONFIGURATION: &str = "kms.configuration";
#[cfg(feature = "datafusion")]
pub(crate) const FACTORY_OPT_FOOTER_KEY: &str = "footer.key";
#[cfg(feature = "datafusion")]
pub(crate) const FACTORY_OPT_PLAINTEXT_FOOTER: &str = "plaintext.footer";
#[cfg(feature = "datafusion")]
pub(crate) const FACTORY_OPT_COLUMN_KEYS: &str = "column.keys";

/// Parquet encryption configuration derived from `delta.encryption.*` table properties.
///
/// These properties are stored in the Delta log metadata and are automatically applied to
/// all read and write operations — no per-operation configuration is needed.
///
/// # Protocol
/// Tables using encryption require Reader Version 3, Writer Version 7, and the
/// `parquetEncryption` writer feature.
///
/// # Registering a KMS client
/// Before operating on an encrypted table, register an [`EncryptionFactory`] whose ID
/// matches `delta.encryption.kms.id` with DataFusion's `RuntimeEnv`:
///
/// ```rust,ignore
/// session.runtime_env().register_parquet_encryption_factory("my-kms", factory);
/// ```
///
/// [`EncryptionFactory`]: datafusion::execution::parquet_encryption::EncryptionFactory
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    /// Identifies the `EncryptionFactory` registered in DataFusion's `RuntimeEnv`.
    /// Corresponds to `delta.encryption.kms.id`.
    pub kms_id: String,
    /// Opaque KMS-specific configuration string (e.g. JSON) forwarded to the factory.
    /// Corresponds to `delta.encryption.kms.configuration`.
    pub kms_configuration: Option<String>,
    /// Master key identifier for footer encryption.
    /// Corresponds to `delta.encryption.footer.key`.
    pub footer_key: String,
    /// If `true` the parquet footer is left unencrypted (plaintext footer mode).
    /// Defaults to `false`. Corresponds to `delta.encryption.plaintext.footer`.
    pub plaintext_footer: bool,
    /// Per-column encryption: map from master key identifier → list of column names.
    /// Stored in the natural wire format (`keyId → [col1, col2]`) so serialisation is
    /// a direct forward pass with no inversion.
    /// Corresponds to `delta.encryption.column.keys` with format `keyId:col1,col2;keyId2:col3`.
    pub column_keys: HashMap<String, Vec<String>>,
}

impl EncryptionConfig {
    /// Parse encryption configuration from a table's `unknown_properties`.
    ///
    /// Returns `None` if `delta.encryption.kms.id` is not set (unencrypted table).
    /// Returns `None` if `delta.encryption.footer.key` is absent — both are required.
    ///
    /// For write paths that must fail fast on partial configuration, use
    /// [`EncryptionConfig::try_from_properties`] instead.
    pub fn from_properties(props: &TableProperties) -> Option<Self> {
        let kms_id = props
            .unknown_properties
            .get(ENCRYPTION_KMS_ID_PROP)?
            .clone();
        // footer.key is required when kms.id is set.
        let footer_key = props
            .unknown_properties
            .get(ENCRYPTION_FOOTER_KEY_PROP)
            .filter(|v| !v.is_empty())
            .cloned()?;

        let kms_configuration = props
            .unknown_properties
            .get(ENCRYPTION_KMS_CONFIGURATION_PROP)
            .cloned();

        let plaintext_footer = props
            .unknown_properties
            .get(ENCRYPTION_PLAINTEXT_FOOTER_PROP)
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);

        let column_keys = Self::parse_column_keys(
            props
                .unknown_properties
                .get(ENCRYPTION_COLUMN_KEYS_PROP)
                .map(|s| s.as_str()),
        );

        Some(Self {
            kms_id,
            kms_configuration,
            footer_key,
            plaintext_footer,
            column_keys,
        })
    }

    /// Like [`from_properties`](Self::from_properties) but returns an error when
    /// `delta.encryption.kms.id` is set without a corresponding `delta.encryption.footer.key`.
    /// Use this in write paths to detect partially-configured tables before writing.
    #[cfg(feature = "datafusion")]
    pub(crate) fn try_from_properties(
        props: &TableProperties,
    ) -> crate::errors::DeltaResult<Option<Self>> {
        if props
            .unknown_properties
            .get(ENCRYPTION_KMS_ID_PROP)
            .is_some()
            && props
                .unknown_properties
                .get(ENCRYPTION_FOOTER_KEY_PROP)
                .filter(|v| !v.is_empty())
                .is_none()
        {
            return Err(crate::errors::DeltaTableError::Generic(format!(
                "Table has '{}' configured but '{}' is missing or empty. \
                 Both are required for an encrypted table.",
                ENCRYPTION_KMS_ID_PROP, ENCRYPTION_FOOTER_KEY_PROP,
            )));
        }
        Ok(Self::from_properties(props))
    }

    /// Parse `"keyId:col1,col2;keyId2:col3"` into `{keyId: [col1, col2], keyId2: [col3]}`.
    fn parse_column_keys(value: Option<&str>) -> HashMap<String, Vec<String>> {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        let Some(value) = value else { return map };
        for segment in value.split(';') {
            let segment = segment.trim();
            if let Some((key_id, cols)) = segment.split_once(':') {
                let key_id = key_id.trim().to_string();
                let cols: Vec<String> = cols
                    .split(',')
                    .map(|c| c.trim().to_string())
                    .filter(|c| !c.is_empty())
                    .collect();
                if !cols.is_empty() {
                    map.entry(key_id).or_default().extend(cols);
                }
            }
        }
        map
    }

    /// Build a [`TableParquetOptions`] that tells DataFusion's parquet scan to look up the
    /// decryption factory by [`kms_id`](EncryptionConfig::kms_id) in the `RuntimeEnv`.
    #[cfg(feature = "datafusion")]
    pub fn to_table_parquet_options(&self) -> TableParquetOptions {
        let mut opts = TableParquetOptions::default();
        opts.crypto.factory_id = Some(self.kms_id.clone());
        opts.crypto.factory_options = self.factory_options();
        opts
    }

    /// Build [`EncryptionFactoryOptions`] forwarded to the registered factory.
    #[cfg(feature = "datafusion")]
    pub fn factory_options(&self) -> EncryptionFactoryOptions {
        let mut opts = EncryptionFactoryOptions::default();
        if let Some(cfg) = &self.kms_configuration {
            opts.options
                .insert(FACTORY_OPT_KMS_CONFIGURATION.to_string(), cfg.clone());
        }
        opts.options
            .insert(FACTORY_OPT_FOOTER_KEY.to_string(), self.footer_key.clone());
        opts.options.insert(
            FACTORY_OPT_PLAINTEXT_FOOTER.to_string(),
            self.plaintext_footer.to_string(),
        );
        if !self.column_keys.is_empty() {
            // Serialise key_id→[cols] in sorted order so the string is deterministic
            // across runs (factories may use it as a cache key).
            let mut sorted_keys: Vec<(&str, &Vec<String>)> = self
                .column_keys
                .iter()
                .map(|(k, v)| (k.as_str(), v))
                .collect();
            sorted_keys.sort_unstable_by_key(|(k, _)| *k);
            let encoded = sorted_keys
                .iter()
                .map(|(key_id, cols)| {
                    let mut sorted_cols: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
                    sorted_cols.sort_unstable();
                    format!("{}:{}", key_id, sorted_cols.join(","))
                })
                .collect::<Vec<_>>()
                .join(";");
            opts.options
                .insert(FACTORY_OPT_COLUMN_KEYS.to_string(), encoded);
        }
        opts
    }
}

/// Extension method for conveniently reading encryption config from any `TableProperties`.
pub trait EncryptionExt {
    fn encryption_config(&self) -> Option<EncryptionConfig>;
}

impl EncryptionExt for TableProperties {
    fn encryption_config(&self) -> Option<EncryptionConfig> {
        EncryptionConfig::from_properties(self)
    }
}
