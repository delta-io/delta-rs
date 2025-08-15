use datafusion::common::file_options::parquet_writer::ParquetWriterOptions;
#[cfg(feature = "datafusion")]
pub use datafusion::config::TableParquetOptions;
use parquet::file::properties::WriterProperties;

#[cfg(not(feature = "datafusion"))]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TableParquetOptions {}


#[cfg(feature = "datafusion")]
pub fn build_writer_properties(table_parquet_options: &Option<TableParquetOptions>) -> Option<WriterProperties> {
    table_parquet_options.as_ref().map(|tpo| {
        let mut tpo = tpo.clone();
        tpo.global.skip_arrow_metadata = true;
        ParquetWriterOptions::try_from(&tpo)
            .expect("Failed to convert TableParquetOptions to ParquetWriterOptions")
            .writer_options()
            .clone()
    })
}

/*
let mut table_parquet_options: TableParquetOptions = self.clone().try_into()
            .expect("Failed to convert ParquetConfig to TableParquetOptions");

        table_parquet_options.global.skip_arrow_metadata = true;

        // Convert TableParquetOptions to ParquetWriterOptions
        let writer_options : ParquetWriterOptions =
            ParquetWriterOptions::try_from(&table_parquet_options)
                .expect("Failed to convert TableParquetOptions to ParquetWriterOptions");

        writer_options.writer_options().clone()
 */