//! Engine-agnostic Delta read and write configuration.
//!
//! [`DeltaWriterProperties`] holds everything that shapes a data file: the parquet
//! [`WriterProperties`], the [`ArrowWriterOptions`], file and batch sizes, and the
//! data-skipping stats to collect. Every write path carries one value end to end.
//!
//! Concerns that adjust the parquet properties are [`WriterPropertiesLayer`]s, run
//! in order per file by [`DeltaWriterProperties::resolve`]. A table-level setting
//! (content-defined chunking from `format.options`) and a per-file one (encryption
//! keys) compose without knowing about each other.
//!
//! [`ReaderProperties`] is the read-side counterpart: it builds DataFusion's
//! [`TableParquetOptions`](datafusion::config::TableParquetOptions) for Delta scans.

use std::fmt::Debug;
use std::num::NonZeroU64;
use std::sync::{Arc, LazyLock};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

use crate::datafile::writer::ArrowWriterOptions;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::arrow::engine_ext::stats_table_properties;
use crate::parquet_utils::default_writer_properties;
use crate::table::config::{DEFAULT_NUM_INDEX_COLS, TablePropertiesExt as _};

/// Rows per slice handed to the parquet writer when none is set.
pub(crate) const DEFAULT_WRITE_BATCH_SIZE: usize = 8192;

/// Parquet writer properties when none are set: SNAPPY, delta-rs `created_by`.
static DEFAULT_PARQUET_PROPERTIES: LazyLock<WriterProperties> =
    LazyLock::new(|| default_writer_properties(Compression::SNAPPY));

/// Engine-agnostic parquet read configuration for a Delta scan.
// Future fields (e.g. per-file decryption) attach here.
#[derive(Clone, Debug, Default)]
pub struct ReaderProperties {}

#[cfg(feature = "datafusion")]
impl ReaderProperties {
    /// Build DataFusion's `TableParquetOptions` for a `ParquetSource`, inheriting
    /// the session's parquet execution settings.
    pub fn to_table_parquet_options(
        &self,
        session: &dyn datafusion::catalog::Session,
    ) -> datafusion::config::TableParquetOptions {
        datafusion::config::TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        }
    }
}

/// Which columns get Delta data-skipping stats.
#[derive(Clone, Debug)]
pub struct WriterStatsConfig {
    /// Number of leading columns to collect stats for.
    pub num_indexed_cols: DataSkippingNumIndexedCols,
    /// Columns to collect stats for; takes precedence over `num_indexed_cols`.
    pub stats_columns: Option<Vec<String>>,
}

/// Stats config when none is set and no table fills one in.
static DEFAULT_STATS: WriterStatsConfig = WriterStatsConfig {
    num_indexed_cols: DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
    stats_columns: None,
};

impl Default for WriterStatsConfig {
    fn default() -> Self {
        DEFAULT_STATS.clone()
    }
}

impl WriterStatsConfig {
    /// Stats for the first `num_indexed_cols` columns, or for `stats_columns` when set.
    pub fn new(
        num_indexed_cols: DataSkippingNumIndexedCols,
        stats_columns: Option<Vec<String>>,
    ) -> Self {
        Self {
            num_indexed_cols,
            stats_columns,
        }
    }

    /// The table's stats configuration, with column names made physical.
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

/// The data file a [`WriterPropertiesLayer`] produces properties for.
#[derive(Clone, Copy, Debug)]
pub struct FileContext<'a> {
    /// Path relative to the table root, as the Delta log records it
    /// (`_change_data/...` for change data).
    pub path: &'a Path,
    /// Schema of the file, partition columns removed.
    pub schema: &'a ArrowSchemaRef,
}

/// One concern's adjustment to a data file's parquet properties.
///
/// Layers run in insertion order, each over the previous one's builder, so a
/// later layer wins on the settings both touch. A layer may set per-column
/// compression but not the default compression or the row-group bounds: the
/// file extension and the row-group aligned roll are decided from the
/// configured properties before the file opens, and
/// [`DeltaWriterProperties::resolve`] rejects a layer that changes them.
#[async_trait::async_trait]
pub trait WriterPropertiesLayer: Send + Sync + Debug {
    /// Adjust the properties `file` is written with.
    async fn apply(
        &self,
        builder: WriterPropertiesBuilder,
        file: FileContext<'_>,
    ) -> DeltaResult<WriterPropertiesBuilder>;
}

/// Everything that shapes a Delta data file.
///
/// Unset fields fall back to delta-rs defaults, or to the table's where an
/// operation knows them, so `Default::default()` is a complete configuration.
#[derive(Clone, Debug, Default)]
pub struct DeltaWriterProperties {
    /// Parquet writer properties; `None` is the delta-rs default, or the operation's.
    pub(crate) parquet: Option<WriterProperties>,
    /// Arrow writer options.
    pub(crate) arrow: ArrowWriterOptions,
    /// Size at which a data file rolls. Unset, operations use the table's
    /// ([`Self::with_table_defaults`]); a writer handed `None` never rolls.
    pub(crate) target_file_size: Option<NonZeroU64>,
    /// Rows per slice handed to the parquet writer.
    pub(crate) write_batch_size: Option<usize>,
    /// Which columns get data-skipping stats. Unset, operations use the table's.
    pub(crate) stats: Option<WriterStatsConfig>,
    /// Per-file adjustments to the parquet properties, applied in order.
    pub(crate) layers: Vec<Arc<dyn WriterPropertiesLayer>>,
}

impl DeltaWriterProperties {
    /// Parquet writer properties, instead of the delta-rs default.
    pub fn with_parquet_properties(mut self, properties: WriterProperties) -> Self {
        self.parquet = Some(properties);
        self
    }

    /// Arrow writer options.
    pub fn with_arrow_options(mut self, options: ArrowWriterOptions) -> Self {
        self.arrow = options;
        self
    }

    /// Roll data files at `size`. `None` leaves it to the operation (the table's
    /// size); only `WriteBuilder::with_target_file_size(None)` disables rolling.
    pub fn with_target_file_size(mut self, size: Option<NonZeroU64>) -> Self {
        self.target_file_size = size;
        self
    }

    /// Rows per slice handed to the parquet writer; zero is rejected when a
    /// writer is built.
    pub fn with_write_batch_size(mut self, rows: usize) -> Self {
        self.write_batch_size = Some(rows);
        self
    }

    /// Which columns get data-skipping stats, instead of the table's configuration.
    pub fn with_stats_config(mut self, stats: WriterStatsConfig) -> Self {
        self.stats = Some(stats);
        self
    }

    /// The table's stats config, if none is set.
    pub(crate) fn with_table_stats(mut self, table_config: &TableConfiguration) -> Self {
        if self.stats.is_none() {
            self.stats = Some(WriterStatsConfig::from_config(table_config));
        }
        self
    }

    /// The table's target file size and stats config, where none are set.
    pub(crate) fn with_table_defaults(mut self, table_config: &TableConfiguration) -> Self {
        if self.target_file_size.is_none() {
            self.target_file_size = Some(table_config.table_properties().target_file_size());
        }
        self.with_table_stats(table_config)
    }

    /// Run `layer` on every file's parquet properties, after the layers added before it.
    pub fn with_layer(mut self, layer: impl WriterPropertiesLayer + 'static) -> Self {
        self.layers.push(Arc::new(layer));
        self
    }

    /// The parquet writer properties set, if any.
    pub fn parquet_properties(&self) -> Option<&WriterProperties> {
        self.parquet.as_ref()
    }

    /// The arrow writer options.
    pub fn arrow_options(&self) -> &ArrowWriterOptions {
        &self.arrow
    }

    /// The size at which data files roll, if set.
    pub fn target_file_size(&self) -> Option<NonZeroU64> {
        self.target_file_size
    }

    /// Rows per slice handed to the parquet writer, if set.
    pub fn write_batch_size(&self) -> Option<usize> {
        self.write_batch_size
    }

    /// Which columns get data-skipping stats: the set value, or the delta-rs default.
    pub fn stats(&self) -> &WriterStatsConfig {
        self.stats.as_ref().unwrap_or(&DEFAULT_STATS)
    }

    /// The parquet writer properties set, or the delta-rs default (SNAPPY, delta-rs
    /// `created_by`). Every file starts from these; [`Self::resolve`] runs the layers over them.
    pub fn parquet_properties_or_default(&self) -> &WriterProperties {
        self.parquet.as_ref().unwrap_or(&DEFAULT_PARQUET_PROPERTIES)
    }

    /// One file's parquet writer properties: the configured ones run through
    /// every layer. `path` is relative to the table root.
    pub async fn resolve(
        &self,
        path: &Path,
        schema: &ArrowSchemaRef,
    ) -> DeltaResult<WriterProperties> {
        let configured = self.parquet_properties_or_default();
        if self.layers.is_empty() {
            return Ok(configured.clone());
        }
        let file = FileContext { path, schema };
        let mut builder = configured.clone().into_builder();
        for layer in &self.layers {
            builder = layer.apply(builder, file).await?;
        }
        let resolved = builder.build();
        // Already decided from the configured properties: file name, row-group roll.
        let default_column = ColumnPath::new(vec![]);
        if resolved.compression(&default_column) != configured.compression(&default_column)
            || resolved.max_row_group_row_count() != configured.max_row_group_row_count()
            || resolved.max_row_group_bytes() != configured.max_row_group_bytes()
        {
            return Err(DeltaTableError::generic(
                "a writer properties layer must not change the default compression or the row-group bounds",
            ));
        }
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use delta_kernel::schema::{DataType, StructField, StructType};

    use super::*;
    use crate::test_utils::{build_test_table_configuration, column_mapping_test_field};

    /// Stamps `created_by` with its tag and, when asked, the file path.
    #[derive(Debug)]
    struct Tag(&'static str, bool);

    /// Changes the compression, which `resolve` must reject.
    #[derive(Debug)]
    struct Recompress;

    #[async_trait::async_trait]
    impl WriterPropertiesLayer for Recompress {
        async fn apply(
            &self,
            builder: WriterPropertiesBuilder,
            _file: FileContext<'_>,
        ) -> DeltaResult<WriterPropertiesBuilder> {
            Ok(builder.set_compression(Compression::UNCOMPRESSED))
        }
    }

    #[async_trait::async_trait]
    impl WriterPropertiesLayer for Tag {
        async fn apply(
            &self,
            builder: WriterPropertiesBuilder,
            file: FileContext<'_>,
        ) -> DeltaResult<WriterPropertiesBuilder> {
            let created_by = if self.1 {
                format!("{} {}", self.0, file.path)
            } else {
                self.0.to_string()
            };
            Ok(builder.set_created_by(created_by))
        }
    }

    fn file_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]))
    }

    #[tokio::test]
    async fn resolve_without_layers_is_the_configured_properties() {
        let props = DeltaWriterProperties::default();
        let resolved = props
            .resolve(&Path::from("part-0.parquet"), &file_schema())
            .await
            .unwrap();
        assert_eq!(
            resolved.created_by(),
            props.parquet_properties_or_default().created_by()
        );
    }

    #[tokio::test]
    async fn layers_apply_in_order_and_see_the_file() {
        let props = DeltaWriterProperties::default()
            .with_layer(Tag("first", false))
            .with_layer(Tag("second", true));
        let resolved = props
            .resolve(&Path::from("p=1/part-0.parquet"), &file_schema())
            .await
            .unwrap();
        assert_eq!(resolved.created_by(), "second p=1/part-0.parquet");
        // The configured settings a layer does not touch survive.
        assert_eq!(
            resolved.compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
    }

    #[test]
    fn table_defaults_fill_only_what_is_unset() {
        let logical_schema = StructType::try_new([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::STRING),
        ])
        .unwrap();
        let table_config = build_test_table_configuration(
            logical_schema,
            vec![],
            HashMap::from([
                ("delta.targetFileSize".to_string(), "1024".to_string()),
                (
                    "delta.dataSkippingStatsColumns".to_string(),
                    "a".to_string(),
                ),
            ]),
        );

        let filled = DeltaWriterProperties::default().with_table_defaults(&table_config);
        assert_eq!(filled.target_file_size(), NonZeroU64::new(1024));
        assert_eq!(filled.stats().stats_columns, Some(vec!["a".to_string()]));

        let kept = DeltaWriterProperties::default()
            .with_target_file_size(NonZeroU64::new(7))
            .with_stats_config(WriterStatsConfig::new(
                DataSkippingNumIndexedCols::AllColumns,
                None,
            ))
            .with_table_defaults(&table_config);
        assert_eq!(kept.target_file_size(), NonZeroU64::new(7));
        assert_eq!(
            kept.stats().num_indexed_cols,
            DataSkippingNumIndexedCols::AllColumns
        );
        assert_eq!(kept.stats().stats_columns, None);
    }

    #[tokio::test]
    async fn resolve_rejects_a_layer_that_changes_compression() {
        let props = DeltaWriterProperties::default().with_layer(Recompress);
        let err = props
            .resolve(&Path::from("part-0.parquet"), &file_schema())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("compression"), "{err}");
    }

    #[test]
    fn defaults_fall_back_to_delta_rs_parquet_properties() {
        let props = DeltaWriterProperties::default();
        assert!(props.parquet_properties().is_none());
        let default = props.parquet_properties_or_default();
        assert_eq!(
            default.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
        assert_eq!(
            default.compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
        assert!(props.target_file_size().is_none());
        assert!(props.write_batch_size().is_none());
        assert_eq!(
            props.stats().num_indexed_cols,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS)
        );
    }

    #[test]
    fn setters_round_trip() {
        let parquet = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        let props = DeltaWriterProperties::default()
            .with_parquet_properties(parquet)
            .with_arrow_options(ArrowWriterOptions::new().with_enable_parallel_encoding(false))
            .with_target_file_size(NonZeroU64::new(10))
            .with_write_batch_size(7)
            .with_stats_config(WriterStatsConfig::new(
                DataSkippingNumIndexedCols::AllColumns,
                Some(vec!["a".to_string()]),
            ));
        assert_eq!(
            props
                .parquet_properties_or_default()
                .compression(&ColumnPath::from("id")),
            Compression::UNCOMPRESSED
        );
        assert!(!props.arrow_options().enable_parallel_encoding());
        assert_eq!(props.target_file_size(), NonZeroU64::new(10));
        assert_eq!(props.write_batch_size(), Some(7));
        assert_eq!(props.stats().stats_columns, Some(vec!["a".to_string()]));
    }

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
