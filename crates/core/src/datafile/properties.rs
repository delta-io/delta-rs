//! Engine-agnostic Delta read configuration.
//!
//! [`ReaderProperties`] centralizes construction of DataFusion's
//! [`TableParquetOptions`](datafusion::config::TableParquetOptions) for Delta
//! scans, so read/parquet-IO config (future: per-file decryption) lives in one
//! place. Read-side counterpart to `WriterProperties`.

/// Engine-agnostic parquet read configuration for a Delta scan.
// Future fields (e.g. per-file decryption) attach here.
#[derive(Clone, Debug, Default)]
pub struct ReaderProperties {}

#[cfg(feature = "datafusion")]
impl ReaderProperties {
    /// Build DataFusion's `TableParquetOptions` for a `ParquetSource`, inheriting
    /// the session's parquet execution settings.
    ///
    /// INT96 timestamps are read as microseconds, the precision of Delta timestamps,
    /// unless the session sets `coerce_int96`.
    pub fn to_table_parquet_options(
        &self,
        session: &dyn datafusion::catalog::Session,
    ) -> datafusion::config::TableParquetOptions {
        let mut global = session.config().options().execution.parquet.clone();
        global.coerce_int96.get_or_insert_with(|| "us".to_string());
        datafusion::config::TableParquetOptions {
            global,
            ..Default::default()
        }
    }
}
