use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use delta_kernel::PredicateRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::ColumnName;
#[cfg(feature = "datafusion")]
use delta_kernel::scan::Scan as KernelScan;
use delta_kernel::schema::{DataType, SchemaRef as KernelSchemaRef, StructField, StructType};
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::table_features::ColumnMappingMode;
use tracing::debug;

#[cfg(test)]
use super::Snapshot;
use crate::DeltaResult;
use crate::kernel::SCAN_ROW_ARROW_SCHEMA;
use crate::kernel::arrow::engine_ext::SnapshotExt;

pub(crate) const FIELD_MAX_VALUES: &str = "maxValues";
pub(crate) const FIELD_MIN_VALUES: &str = "minValues";
pub(crate) const FIELD_NULL_COUNT: &str = "nullCount";
pub(crate) const FIELD_NUM_RECORDS: &str = "numRecords";
pub(crate) const FIELD_PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
pub(crate) const FIELD_STATS: &str = "stats";
pub(crate) const FIELD_STATS_PARSED: &str = "stats_parsed";
const STATS_VALUE_FIELDS: [&str; 3] = [FIELD_MIN_VALUES, FIELD_MAX_VALUES, FIELD_NULL_COUNT];
const ORDERED_STATS_VALUE_FIELDS: [&str; 3] =
    [FIELD_NULL_COUNT, FIELD_MIN_VALUES, FIELD_MAX_VALUES];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatsProjection {
    /// Do not materialize parsed file statistics.
    None,
    /// Materialize the full stats schema supported by the table configuration.
    Full,
    /// Materialize only the `numRecords` field.
    NumRecordsOnly,
    /// Materialize `numRecords` and stats for selected physical data columns.
    PredicateColumns(BTreeSet<ColumnName>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatsSourcePolicy {
    /// Prefer `stats_parsed`, falling back to raw JSON `stats` when needed.
    ParsedWithJsonFallback,
    /// Do not read or emit parsed stats.
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawStatsPolicy {
    /// Preserve the raw JSON `stats` field in emitted scan rows.
    Preserve,
    /// Omit the raw JSON `stats` field from emitted scan rows.
    Omit,
}

/// Controls file statistics materialization in scan metadata output.
///
/// The policy stores the parsed stats fields to emit, the source for parsed stats,
/// and whether raw JSON stats remain in the output.
///
/// Query paths can emit narrow parsed stats and omit raw JSON. Compatibility paths can keep
/// raw JSON stats for callers that convert file views back to Add actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileStatsMaterialization {
    stats_projection: StatsProjection,
    stats_source_policy: StatsSourcePolicy,
    raw_stats_policy: RawStatsPolicy,
}

impl FileStatsMaterialization {
    /// Materialization for query paths.
    ///
    /// Emits only the parsed stats needed by the scan and omits raw JSON stats.
    pub(crate) fn query(stats_projection: StatsProjection) -> Self {
        Self {
            stats_projection,
            stats_source_policy: StatsSourcePolicy::ParsedWithJsonFallback,
            raw_stats_policy: RawStatsPolicy::Omit,
        }
    }

    /// Materialization for compatibility paths.
    ///
    /// Emits parsed stats and preserves raw JSON stats for callers that convert
    /// [`LogicalFileView`](crate::kernel::LogicalFileView) back to Add actions.
    pub(crate) fn compatibility(stats_projection: StatsProjection) -> Self {
        Self {
            stats_projection,
            stats_source_policy: StatsSourcePolicy::ParsedWithJsonFallback,
            raw_stats_policy: RawStatsPolicy::Preserve,
        }
    }

    pub(crate) fn without_stats() -> Self {
        Self {
            stats_projection: StatsProjection::none(),
            stats_source_policy: StatsSourcePolicy::None,
            raw_stats_policy: RawStatsPolicy::Omit,
        }
    }

    pub(crate) fn stats_projection(&self) -> &StatsProjection {
        &self.stats_projection
    }

    pub(crate) fn stats_source_policy(&self) -> StatsSourcePolicy {
        self.stats_source_policy
    }

    pub(crate) fn preserves_raw_stats(&self) -> bool {
        self.raw_stats_policy == RawStatsPolicy::Preserve
    }
}

impl StatsProjection {
    pub(crate) fn none() -> Self {
        Self::None
    }

    pub(crate) fn full() -> Self {
        Self::Full
    }

    /// Build the stats projection from scan builder inputs before constructing the kernel scan.
    /// This matches enough of kernel scan planning to find physical data column stats without a
    /// temporary scan.
    pub(crate) fn for_scan_inputs(
        snapshot: &KernelSnapshot,
        schema: Option<&KernelSchemaRef>,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<Self> {
        let Some(predicate) = predicate else {
            debug!(
                projection = "num_records_only",
                reason = "no physical predicate",
                "stats projection selected"
            );
            return Ok(Self::NumRecordsOnly);
        };

        let snapshot_schema;
        let logical_schema = match schema {
            Some(schema) => schema.as_ref(),
            None => {
                snapshot_schema = snapshot.schema();
                snapshot_schema.as_ref()
            }
        };
        let stats_schema = snapshot.table_configuration().stats_schema()?;
        let column_mapping_mode = snapshot.table_configuration().column_mapping_mode();
        let requested_columns = predicate
            .references()
            .into_iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        let columns = requested_columns
            .iter()
            .filter_map(|column| {
                physicalize_column_path(logical_schema, column, column_mapping_mode)
            })
            .filter(|column| stats_schema_contains_data_column(stats_schema.as_ref(), column))
            .collect::<BTreeSet<_>>();

        if columns.is_empty() {
            debug!(
                projection = "num_records_only",
                reason = "no physical data columns referenced",
                requested_columns = %display_columns(&requested_columns),
                "stats projection selected"
            );
            Ok(Self::NumRecordsOnly)
        } else {
            let filtered_columns = requested_columns
                .iter()
                .filter_map(|column| {
                    physicalize_column_path(logical_schema, column, column_mapping_mode)
                })
                .filter(|column| !columns.contains(column))
                .collect::<BTreeSet<_>>();
            debug!(
                projection = "predicate_columns",
                requested_columns = %display_columns(&requested_columns),
                selected_columns = %display_columns(&columns),
                filtered_columns = %display_columns(&filtered_columns),
                "stats projection selected"
            );
            Ok(Self::PredicateColumns(columns))
        }
    }

    /// Builds the stats projection for a scan using physical predicate references.
    ///
    /// Use `NumRecordsOnly` when a predicate only touches partition columns,
    /// skips all files, or references no data columns.
    #[cfg(feature = "datafusion")]
    pub(crate) fn for_scan(scan: &KernelScan) -> DeltaResult<Self> {
        let Some(predicate) = scan.physical_predicate() else {
            debug!(
                projection = "num_records_only",
                reason = "no physical predicate",
                "stats projection selected"
            );
            return Ok(Self::NumRecordsOnly);
        };

        let physical_schema = scan.physical_schema();
        let requested_columns = predicate
            .references()
            .into_iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        let columns = requested_columns
            .iter()
            .filter(|column| schema_contains_path(physical_schema.as_ref(), column))
            .cloned()
            .collect::<BTreeSet<_>>();

        if columns.is_empty() {
            debug!(
                projection = "num_records_only",
                reason = "no physical data columns referenced",
                requested_columns = %display_columns(&requested_columns),
                "stats projection selected"
            );
            Ok(Self::NumRecordsOnly)
        } else {
            let filtered_columns = requested_columns
                .difference(&columns)
                .cloned()
                .collect::<BTreeSet<_>>();
            debug!(
                projection = "predicate_columns",
                requested_columns = %display_columns(&requested_columns),
                selected_columns = %display_columns(&columns),
                filtered_columns = %display_columns(&filtered_columns),
                "stats projection selected"
            );
            Ok(Self::PredicateColumns(columns))
        }
    }

    pub(crate) fn stats_schema(&self, snapshot: &KernelSnapshot) -> DeltaResult<KernelSchemaRef> {
        match self {
            Self::None => Ok(Arc::new(StructType::try_new([])?)),
            Self::Full => Ok(snapshot.table_configuration().stats_schema()?),
            Self::NumRecordsOnly => num_records_only_stats_schema(),
            Self::PredicateColumns(columns) => {
                let full_stats_schema = snapshot.table_configuration().stats_schema()?;
                Ok(Arc::new(filter_stats_schema(
                    full_stats_schema.as_ref(),
                    columns,
                )?))
            }
        }
    }

    pub(crate) fn parsed_scan_row_schema_arrow(
        &self,
        snapshot: &KernelSnapshot,
    ) -> DeltaResult<ArrowSchemaRef> {
        let mut fields = SCAN_ROW_ARROW_SCHEMA.fields().to_vec();
        let stats_schema = self.stats_schema(snapshot)?;
        let stats_schema: ArrowSchema = stats_schema.as_ref().try_into_arrow()?;
        fields.push(Arc::new(ArrowField::new(
            FIELD_STATS_PARSED,
            ArrowDataType::Struct(stats_schema.fields().to_owned()),
            true,
        )));

        if let Some(partition_schema) = snapshot.table_configuration().partitions_schema()? {
            let partition_schema: ArrowSchema = partition_schema.as_ref().try_into_arrow()?;
            fields.push(Arc::new(ArrowField::new(
                FIELD_PARTITION_VALUES_PARSED,
                ArrowDataType::Struct(partition_schema.fields().to_owned()),
                false,
            )));
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    /// Returns whether this projection emits stats for a root physical column.
    ///
    /// A nested reference such as `nested.leaf` still emits the enclosing field
    /// `nested` stores the root field name used by DataFusion file statistics.
    #[cfg(feature = "datafusion")]
    pub(crate) fn emits_top_level_column_stats(&self, physical_name: &str) -> bool {
        match self {
            Self::None | Self::NumRecordsOnly => false,
            Self::Full => true,
            Self::PredicateColumns(columns) => columns.iter().any(|column| {
                column
                    .path()
                    .first()
                    .is_some_and(|name| name.as_str() == physical_name)
            }),
        }
    }
}

fn num_records_only_stats_schema() -> DeltaResult<KernelSchemaRef> {
    Ok(Arc::new(StructType::try_new([StructField::nullable(
        FIELD_NUM_RECORDS,
        DataType::LONG,
    )])?))
}

/// Returns whether a physical schema contains the path referenced by `column`.
fn schema_contains_path(schema: &StructType, column: &ColumnName) -> bool {
    let Some((last, parents)) = column.path().split_last() else {
        return false;
    };

    let mut current = schema;
    for segment in parents {
        let Some(field) = current.field(segment) else {
            return false;
        };
        let DataType::Struct(inner) = field.data_type() else {
            return false;
        };
        current = inner;
    }

    current.field(last).is_some()
}

fn physicalize_column_path(
    schema: &StructType,
    column: &ColumnName,
    column_mapping_mode: ColumnMappingMode,
) -> Option<ColumnName> {
    let mut current = schema;
    let mut physical_path = Vec::with_capacity(column.path().len());
    let mut path = column.path().iter().peekable();

    while let Some(segment) = path.next() {
        let field = current
            .fields()
            .find(|field| field.name().eq_ignore_ascii_case(segment.as_str()))?;
        physical_path.push(field.physical_name(column_mapping_mode).to_string());

        if path.peek().is_some() {
            let DataType::Struct(inner) = field.data_type() else {
                return None;
            };
            current = inner;
        }
    }

    Some(ColumnName::new(physical_path))
}

fn stats_schema_contains_data_column(stats_schema: &StructType, column: &ColumnName) -> bool {
    STATS_VALUE_FIELDS
        .into_iter()
        .filter_map(|field_name| stats_schema.field(field_name))
        .filter_map(|field| match field.data_type() {
            DataType::Struct(inner) => Some(inner.as_ref()),
            _ => None,
        })
        .any(|schema| schema_contains_path(schema, column))
}

fn display_columns(columns: &BTreeSet<ColumnName>) -> String {
    let values = columns.iter().map(ToString::to_string).collect::<Vec<_>>();
    format!("[{}]", values.join(", "))
}

/// Filters a full stats schema down to the subset needed for a scan projection.
///
/// `numRecords` is always kept. The min, max, and null count structs are pruned
/// to the referenced paths and dropped when nothing remains.
fn filter_stats_schema(
    stats_schema: &StructType,
    paths: &BTreeSet<ColumnName>,
) -> DeltaResult<StructType> {
    let mut fields = Vec::with_capacity(4);
    fields.push(
        stats_schema
            .field(FIELD_NUM_RECORDS)
            .cloned()
            .unwrap_or_else(|| StructField::nullable(FIELD_NUM_RECORDS, DataType::LONG)),
    );

    for stats_field_name in ORDERED_STATS_VALUE_FIELDS {
        let Some(field) = stats_schema.field(stats_field_name) else {
            continue;
        };
        let DataType::Struct(inner) = field.data_type() else {
            continue;
        };
        let Some(filtered) = filter_schema_by_paths(inner, paths)? else {
            continue;
        };

        fields.push(
            StructField::new(
                field.name().clone(),
                DataType::Struct(Box::new(filtered)),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
        );
    }

    StructType::try_new(fields).map_err(Into::into)
}

/// Recursively filters a struct schema to the referenced paths.
///
/// If a parent path matches, keep the full subtree. Otherwise rebuild only the
/// matching children so the result keeps the original shape for the requested leaves.
fn filter_schema_by_paths(
    schema: &StructType,
    paths: &BTreeSet<ColumnName>,
) -> DeltaResult<Option<StructType>> {
    let mut fields = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        let matches = paths
            .iter()
            .filter(|path| path.path().first().is_some_and(|name| name == field_name))
            .collect::<BTreeSet<_>>();

        if matches.is_empty() {
            continue;
        }

        if matches.iter().any(|path| path.path().len() == 1) {
            fields.push(field.clone());
            continue;
        }

        match field.data_type() {
            DataType::Struct(inner) => {
                let child_paths = matches
                    .into_iter()
                    .filter_map(|path| {
                        let (_, remainder) = path.path().split_first()?;
                        (!remainder.is_empty()).then(|| ColumnName::new(remainder.iter().cloned()))
                    })
                    .collect::<BTreeSet<_>>();
                if let Some(filtered) = filter_schema_by_paths(inner, &child_paths)? {
                    fields.push(
                        StructField::new(
                            field.name().clone(),
                            DataType::Struct(Box::new(filtered)),
                            field.is_nullable(),
                        )
                        .with_metadata(field.metadata().clone()),
                    );
                }
            }
            _ => fields.push(field.clone()),
        }
    }

    if fields.is_empty() {
        Ok(None)
    } else {
        Ok(Some(StructType::try_new(fields)?))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use delta_kernel::Expression;
    use delta_kernel::expressions::Scalar;

    use super::*;
    use crate::test_utils::TestResult;
    use crate::{DeltaTable, DeltaTableBuilder, DeltaTableError, TableProperty};

    fn column_mapping_builder() -> DeltaResult<DeltaTableBuilder> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../test/tests/data/table_with_column_mapping");
        let path = path.canonicalize()?;
        let url = url::Url::from_directory_path(&path).map_err(|_| {
            DeltaTableError::InvalidTableLocation(format!(
                "failed to convert {} to a directory URL",
                path.display()
            ))
        })?;
        DeltaTableBuilder::from_url(url).map(|builder| builder.with_allow_http(true))
    }

    async fn synthetic_snapshot() -> DeltaResult<Snapshot> {
        let nested = StructType::try_new([
            StructField::nullable("leaf", DataType::INTEGER),
            StructField::nullable("other_leaf", DataType::STRING),
        ])?;
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                StructField::nullable("value", DataType::INTEGER),
                StructField::nullable("unreferenced_col", DataType::STRING),
                StructField::nullable("part", DataType::STRING),
                StructField::nullable("nested", DataType::Struct(Box::new(nested))),
            ])
            .with_partition_columns(["part"])
            .await?;
        Snapshot::try_new(table.log_store().as_ref(), Default::default(), None).await
    }

    fn nested_data_type(path: &[&str]) -> DeltaResult<DataType> {
        let Some((field_name, child_path)) = path.split_first() else {
            return Ok(DataType::INTEGER);
        };
        Ok(DataType::Struct(Box::new(StructType::try_new([
            StructField::nullable(*field_name, nested_data_type(child_path)?),
        ])?)))
    }

    async fn deep_nested_snapshot() -> DeltaResult<Snapshot> {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([StructField::nullable(
                "level1",
                nested_data_type(&["level2", "level3", "level4", "level5", "level6", "leaf"])?,
            )])
            .await?;
        Snapshot::try_new(table.log_store().as_ref(), Default::default(), None).await
    }

    fn assert_struct_path(schema: &StructType, path: &[&str]) {
        let Some((field_name, child_path)) = path.split_first() else {
            return;
        };
        let field = schema
            .field(field_name)
            .unwrap_or_else(|| panic!("{field_name} should be projected"));
        if child_path.is_empty() {
            return;
        }
        let DataType::Struct(inner) = field.data_type() else {
            panic!("{field_name} should be a struct");
        };
        assert_struct_path(inner, child_path);
    }

    async fn synthetic_snapshot_with_num_indexed_cols(
        num_indexed_cols: &str,
    ) -> DeltaResult<Snapshot> {
        let nested = StructType::try_new([
            StructField::nullable("leaf", DataType::INTEGER),
            StructField::nullable("other_leaf", DataType::STRING),
        ])?;
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                StructField::nullable("value", DataType::INTEGER),
                StructField::nullable("unreferenced_col", DataType::STRING),
                StructField::nullable("part", DataType::STRING),
                StructField::nullable("nested", DataType::Struct(Box::new(nested))),
            ])
            .with_partition_columns(["part"])
            .with_configuration_property(
                TableProperty::DataSkippingNumIndexedCols,
                Some(num_indexed_cols),
            )
            .await?;
        Snapshot::try_new(table.log_store().as_ref(), Default::default(), None).await
    }

    async fn column_mapping_snapshot() -> DeltaResult<Snapshot> {
        let log_store = column_mapping_builder()?.build_storage()?;
        Snapshot::try_new(log_store.as_ref(), Default::default(), None).await
    }

    fn projection_for_predicate(
        snapshot: &Snapshot,
        predicate: Option<&PredicateRef>,
    ) -> DeltaResult<StatsProjection> {
        StatsProjection::for_scan_inputs(snapshot.inner.as_ref(), None, predicate)
    }

    #[test]
    fn file_stats_materialization_query_defaults_to_omit_raw() {
        let projection = StatsProjection::NumRecordsOnly;
        let materialization = FileStatsMaterialization::query(projection.clone());

        assert_eq!(
            materialization.stats_source_policy(),
            StatsSourcePolicy::ParsedWithJsonFallback
        );
        assert!(!materialization.preserves_raw_stats());
        assert_eq!(materialization.stats_projection(), &projection);
    }

    #[test]
    fn file_stats_materialization_compatibility_preserves_raw() {
        let projection = StatsProjection::full();
        let materialization = FileStatsMaterialization::compatibility(projection.clone());

        assert_eq!(
            materialization.stats_source_policy(),
            StatsSourcePolicy::ParsedWithJsonFallback
        );
        assert!(materialization.preserves_raw_stats());
        assert_eq!(materialization.stats_projection(), &projection);
    }

    #[test]
    fn file_stats_materialization_without_stats_disables_sources() {
        let materialization = FileStatsMaterialization::without_stats();

        assert_eq!(
            materialization.stats_source_policy(),
            StatsSourcePolicy::None
        );
        assert!(!materialization.preserves_raw_stats());
        assert_eq!(materialization.stats_projection(), &StatsProjection::none());
    }

    #[tokio::test]
    async fn stats_projection_full_matches_snapshot_stats_schema() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let expected = snapshot.inner.stats_schema()?;
        let actual = StatsProjection::Full.stats_schema(snapshot.inner.as_ref())?;

        assert_eq!(actual, expected);

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_data_predicate_includes_only_referenced_columns() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef =
            Arc::new(Expression::column(["value"]).gt(Scalar::Integer(10)));

        let projection = projection_for_predicate(&snapshot, Some(&predicate))?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;

        assert!(stats_schema.field("numRecords").is_some());
        for field_name in ["minValues", "maxValues", "nullCount"] {
            let field = stats_schema
                .field(field_name)
                .unwrap_or_else(|| panic!("{field_name} should be projected"));
            let DataType::Struct(inner) = field.data_type() else {
                panic!("{field_name} should be a struct");
            };
            assert!(inner.field("value").is_some());
            assert!(inner.field("unreferenced_col").is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_wrapped_expression_keeps_referenced_columns() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef = Arc::new(
            Expression::coalesce([
                Expression::column(["value"]),
                Expression::literal(Scalar::Integer(0)),
            ])
            .gt(Scalar::Integer(10)),
        );

        let projection = projection_for_predicate(&snapshot, Some(&predicate))?;
        assert_eq!(
            projection,
            StatsProjection::PredicateColumns(BTreeSet::from([ColumnName::new(["value"])]))
        );

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_arithmetic_expression_keeps_referenced_columns() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef = Arc::new(
            Expression::binary(
                delta_kernel::expressions::BinaryExpressionOp::Plus,
                Expression::column(["value"]),
                Expression::literal(Scalar::Integer(1)),
            )
            .gt(Scalar::Integer(10)),
        );

        let projection = projection_for_predicate(&snapshot, Some(&predicate))?;

        assert_eq!(
            projection,
            StatsProjection::PredicateColumns(BTreeSet::from([ColumnName::new(["value"])]))
        );

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_multi_column_and_keeps_all_referenced_columns() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef = Arc::new(delta_kernel::Predicate::and(
            Expression::column(["value"]).gt(Scalar::Integer(10)),
            Expression::column(["unreferenced_col"]).eq(Scalar::String("match".to_string())),
        ));

        let projection = projection_for_predicate(&snapshot, Some(&predicate))?;

        assert_eq!(
            projection,
            StatsProjection::PredicateColumns(BTreeSet::from([
                ColumnName::new(["unreferenced_col"]),
                ColumnName::new(["value"]),
            ]))
        );

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_column_mapping_uses_physical_field_names() -> TestResult {
        let snapshot = column_mapping_snapshot().await?;
        let predicate: PredicateRef = Arc::new(
            Expression::column(["Super Name"]).eq(Scalar::String("Timothy Lamb".to_string())),
        );

        let projection = projection_for_predicate(&snapshot, Some(&predicate))?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;

        let logical_schema = snapshot.inner.table_configuration().logical_schema();
        let logical = logical_schema
            .field("Super Name")
            .expect("missing logical column");
        let physical =
            logical.physical_name(snapshot.inner.table_configuration().column_mapping_mode());

        assert_eq!(
            projection,
            StatsProjection::PredicateColumns(BTreeSet::from([ColumnName::new([
                physical.to_string()
            ])]))
        );
        for field_name in ["minValues", "maxValues", "nullCount"] {
            let field = stats_schema
                .field(field_name)
                .unwrap_or_else(|| panic!("{field_name} should be projected"));
            let DataType::Struct(inner) = field.data_type() else {
                panic!("{field_name} should be a struct");
            };
            assert!(inner.field(physical).is_some());
            assert!(inner.field("Super Name").is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_nested_struct_predicate_keeps_only_referenced_subtree() -> TestResult
    {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef =
            Arc::new(Expression::column(["nested", "leaf"]).gt(Scalar::Integer(1)));

        let projection = projection_for_predicate(&snapshot, Some(&predicate))?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;

        for field_name in ["minValues", "maxValues", "nullCount"] {
            let field = stats_schema
                .field(field_name)
                .unwrap_or_else(|| panic!("{field_name} should be projected"));
            let DataType::Struct(inner) = field.data_type() else {
                panic!("{field_name} should be a struct");
            };
            let nested = inner.field("nested").expect("nested should be projected");
            let DataType::Struct(nested_inner) = nested.data_type() else {
                panic!("nested stats should be a struct");
            };
            assert!(nested_inner.field("leaf").is_some());
            assert!(nested_inner.field("other_leaf").is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_deep_nested_struct_predicate_keeps_referenced_path() -> TestResult {
        let snapshot = deep_nested_snapshot().await?;
        let path = [
            "level1", "level2", "level3", "level4", "level5", "level6", "leaf",
        ];
        let predicate: PredicateRef = Arc::new(Expression::column(path).gt(Scalar::Integer(1)));

        let projection = projection_for_predicate(&snapshot, Some(&predicate))?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;

        for field_name in STATS_VALUE_FIELDS {
            let field = stats_schema
                .field(field_name)
                .unwrap_or_else(|| panic!("{field_name} should be projected"));
            let DataType::Struct(inner) = field.data_type() else {
                panic!("{field_name} should be a struct");
            };
            assert_struct_path(inner, &path);
        }

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_respects_num_indexed_cols_limit() -> TestResult {
        let snapshot = synthetic_snapshot_with_num_indexed_cols("1").await?;
        let predicate: PredicateRef = Arc::new(
            Expression::column(["unreferenced_col"]).eq(Scalar::String("match".to_string())),
        );

        let projection = projection_for_predicate(&snapshot, Some(&predicate))?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;

        assert_eq!(projection, StatsProjection::NumRecordsOnly);
        assert!(stats_schema.field("numRecords").is_some());
        assert!(stats_schema.field("minValues").is_none());
        assert!(stats_schema.field("maxValues").is_none());
        assert!(stats_schema.field("nullCount").is_none());

        Ok(())
    }
}
