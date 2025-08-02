use delta_kernel::table_properties::DataSkippingNumIndexedCols;

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
}
