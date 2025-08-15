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