#[cfg(feature = "datafusion")]
pub use datafusion::config::{ConfigFileType, TableOptions, TableParquetOptions};
#[cfg(feature = "datafusion")]
use datafusion::execution::{SessionState, SessionStateBuilder};
use parquet::file::properties::WriterProperties;

#[cfg(not(feature = "datafusion"))]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TableParquetOptions {}

#[cfg(feature = "datafusion")]
pub fn build_writer_properties(
    table_parquet_options: &Option<TableParquetOptions>,
) -> Option<WriterProperties> {
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

#[cfg(feature = "datafusion")]
pub fn state_with_parquet_options(
    state: SessionState,
    parquet_options: Option<&TableParquetOptions>,
) -> SessionState {
    if parquet_options.is_some() {
        let mut sb = SessionStateBuilder::new_from_existing(state.clone());
        let mut tbl_opts = TableOptions::new();
        tbl_opts.parquet = parquet_options.unwrap().clone();
        tbl_opts.set_config_format(ConfigFileType::PARQUET);
        sb = sb.with_table_options(tbl_opts);
        let state = sb.build();
        return state;
    }
    state
}
