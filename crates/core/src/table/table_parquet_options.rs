#[cfg(feature = "datafusion")]
pub use datafusion::config::{TableParquetOptions, ConfigFileType, TableOptions};
#[cfg(feature = "datafusion")]
use datafusion::execution::SessionState;

use parquet::file::properties::WriterProperties;

#[cfg(not(feature = "datafusion"))]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TableParquetOptions {}


#[cfg(feature = "datafusion")]
pub fn build_writer_properties(table_parquet_options: &Option<TableParquetOptions>) -> Option<WriterProperties> {
    use datafusion::common::file_options::parquet_writer::ParquetWriterOptions;
    table_parquet_options.as_ref().map(|tpo| {
        let mut tpo = tpo.clone();
        tpo.global.skip_arrow_metadata = true;
        ParquetWriterOptions::try_from(&tpo)
            .expect("Failed to convert TableParquetOptions to ParquetWriterOptions")
            .writer_options()
            .clone()
    })
}


// Propagate any options set in table_parquet_options to the session state
// This allows the table to use these options when reading Parquet files.
#[cfg(feature = "datafusion")]
pub fn apply_table_options_to_state(mut state: SessionState, table_parquet_options: Option<TableParquetOptions>) -> SessionState{
    if let Some(table_parquet_options) = table_parquet_options {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config.parquet = table_parquet_options.clone();
        let tom = state.table_options_mut();
        *tom = table_config;
    }
    state
}
