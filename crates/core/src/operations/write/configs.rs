use delta_kernel::{
    table_configuration::TableConfiguration, table_properties::DataSkippingNumIndexedCols,
};

use crate::kernel::arrow::engine_ext::stats_table_properties;
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

    /// Derive writer statistics configuration from a table's [`TableConfiguration`].
    pub fn from_config(config: &TableConfiguration) -> Self {
        let properties = stats_table_properties(
            config.logical_schema().as_ref(),
            config.table_properties(),
            config.column_mapping_mode(),
        );
        Self {
            num_indexed_cols: properties.num_indexed_cols(),
            stats_columns: properties
                .data_skipping_stats_columns
                .as_ref()
                .map(|columns| columns.iter().map(|c| c.to_string()).collect()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use delta_kernel::schema::{DataType, StructField, StructType};

    use super::*;
    use crate::test_utils::{build_test_table_configuration, column_mapping_test_field};

    #[test]
    fn from_config_translates_stats_columns_to_physical_names() {
        // `physical_name` resolves to the `physicalName` annotation under both name and id modes.
        for mode in ["name", "id"] {
            let logical_schema = StructType::try_new([
                column_mapping_test_field("p", "col_p", 1),
                column_mapping_test_field("a", "col_a", 2),
            ])
            .unwrap();
            let table_config = build_test_table_configuration(
                logical_schema,
                vec!["p".to_string()],
                HashMap::from([
                    ("delta.columnMapping.mode".to_string(), mode.to_string()),
                    (
                        "delta.dataSkippingStatsColumns".to_string(),
                        "a".to_string(),
                    ),
                ]),
            );

            let config = WriterStatsConfig::from_config(&table_config);
            assert_eq!(
                config.stats_columns,
                Some(vec!["col_a".to_string()]),
                "stats columns should be physical names in {mode} mode"
            );
        }
    }

    #[test]
    fn from_config_keeps_logical_names_without_column_mapping() {
        let logical_schema = StructType::try_new([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::STRING),
        ])
        .unwrap();
        let table_config = build_test_table_configuration(
            logical_schema,
            vec![],
            HashMap::from([(
                "delta.dataSkippingStatsColumns".to_string(),
                "a".to_string(),
            )]),
        );

        let config = WriterStatsConfig::from_config(&table_config);
        assert_eq!(config.stats_columns, Some(vec!["a".to_string()]));
    }
}
