#[cfg(feature = "datafusion")]
pub use datafusion::config::TableParquetOptions;

#[cfg(not(feature = "datafusion"))]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TableParquetOptions {}