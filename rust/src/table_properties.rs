//! properties defined on Delta Tables
//! <https://docs.databricks.com/delta/table-properties.html>

/// true for this Delta table to be append-only. If append-only,
/// existing records cannot be deleted, and existing values cannot be updated.
pub const APPEND_ONLY: &str = "delta.appendOnly";
/// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
pub const AUTO_OPTIMIZE_AUTO_COMPACT: &str = "delta.autoOptimize.autoCompact";
/// true for Delta Lake to automatically optimize the layout of the files for this Delta table during writes.
pub const AUTO_OPTIMIZE_OPTIMIZE_WRITE: &str = "delta.autoOptimize.optimizeWrite";
/// true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
pub const CHECKPOINT_WRITE_STATS_AS_JSON: &str = "delta.checkpoint.writeStatsAsJson";
/// true for Delta Lake to write file statistics to checkpoints in struct format for the
/// stats_parsed column and to write partition values as a struct for partitionValues_parsed.
pub const CHECKPOINT_WRITE_STATS_AS_STRUCT: &str = "delta.checkpoint.writeStatsAsStruct";
/// Whether column mapping is enabled for Delta table columns and the corresponding Parquet columns that use different names.
pub const COLUMN_MAPPING_MODE: &str = "delta.columnMapping.mode";
/// Whether column mapping is enabled for Delta table columns and the corresponding Parquet columns that use different names.
pub const COMPATIBILITY_SYMLINK_FORMAT_MANIFEST_ENABLED: &str =
    "delta.compatibility.symlinkFormatManifest.enabled";
/// The number of columns for Delta Lake to collect statistics about for data skipping.
/// A value of -1 means to collect statistics for all columns. Updating this property does
/// not automatically collect statistics again; instead, it redefines the statistics schema
/// of the Delta table. Specifically, it changes the behavior of future statistics collection
/// (such as during appends and optimizations) as well as data skipping (such as ignoring column
/// statistics beyond this number, even when such statistics exist).
pub const DATA_SKIPPING_NUM_INDEXED_COLS: &str = "delta.dataSkippingNumIndexedCols";
/// The shortest duration for Delta Lake to keep logically deleted data files before deleting
/// them physically. This is to prevent failures in stale readers after compactions or partition overwrites.
///
/// This value should be large enough to ensure that:
///
/// * It is larger than the longest possible duration of a job if you run VACUUM when there are
///   concurrent readers or writers accessing the Delta table.
/// * If you run a streaming query that reads from the table, that query does not stop for longer
///   than this value. Otherwise, the query may not be able to restart, as it must still read old files.
pub const DELETED_FILE_RETENTION_DURATION: &str = "delta.deletedFileRetentionDuration";
/// true to enable change data feed.
pub const ENABLE_CHANGE_DATA_FEED: &str = "delta.enableChangeDataFeed";
/// The degree to which a transaction must be isolated from modifications made by concurrent transactions.
///
/// Valid values are `Serializable` and `WriteSerializable`.
pub const ISOLATION_LEVEL: &str = "delta.isolationLevel";
/// How long the history for a Delta table is kept.
///
/// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
/// than the retention interval. If you set this property to a large enough value, many log
/// entries are retained. This should not impact performance as operations against the log are
/// constant time. Operations on history are parallel but will become more expensive as the log size increases.
pub const LOG_RETENTION_DURATION: &str = "delta.logRetentionDuration";
/// The minimum required protocol reader version for a reader that allows to read from this Delta table.
pub const MIN_READER_VERSION: &str = "delta.minReaderVersion";
/// The minimum required protocol writer version for a writer that allows to write to this Delta table.
pub const MIN_WRITER_VERSION: &str = "delta.minWriterVersion";
/// true for Delta Lake to generate a random prefix for a file path instead of partition information.
///
/// For example, this may improve Amazon S3 performance when Delta Lake needs to send very high volumes
/// of Amazon S3 calls to better partition across S3 servers.
pub const RANDOMIZE_FILE_PREFIXES: &str = "delta.randomizeFilePrefixes";
/// When delta.randomizeFilePrefixes is set to true, the number of characters that Delta Lake generates for random prefixes.
pub const RANDOM_PREFIX_LENGTH: &str = "delta.randomPrefixLength";
/// The shortest duration within which new snapshots will retain transaction identifiers (for example, SetTransactions).
/// When a new snapshot sees a transaction identifier older than or equal to the duration specified by this property,
/// the snapshot considers it expired and ignores it. The SetTransaction identifier is used when making the writes idempotent.
pub const SET_TRANSACTION_RETENTION_DURATION: &str = "delta.setTransactionRetentionDuration";
/// The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
pub const TARGET_FILE_SIZE: &str = "delta.targetFileSize";
/// The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
pub const TUNE_FILE_SIZES_FOR_REWRITES: &str = "delta.tuneFileSizesForRewrites";
