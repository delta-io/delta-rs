use delta_kernel::{
    table_configuration::TableConfiguration, table_properties::DataSkippingNumIndexedCols,
};

use crate::table::config::TablePropertiesExt as _;

/// Configuration for the writer on how to collect stats
#[derive(Clone)]
pub struct WriterStatsConfig {
    /// Number of columns to collect stats for, idx based
    pub num_indexed_cols: DataSkippingNumIndexedCols,
    /// Optional list of columns which to collect stats for, takes precedende over num_index_cols
    pub stats_columns: Option<Vec<String>>,
}

impl WriterStatsConfig {
    /// Create new writer stats config
    pub fn new(
        num_indexed_cols: DataSkippingNumIndexedCols,
        stats_columns: Option<Vec<String>>,
    ) -> Self {
        Self {
            num_indexed_cols,
            stats_columns,
        }
    }

    pub fn from_config(config: &TableConfiguration) -> Self {
        Self {
            num_indexed_cols: config.table_properties().num_indexed_cols(),
            stats_columns: config
                .table_properties()
                .data_skipping_stats_columns
                .as_ref()
                .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()),
        }
    }
}
