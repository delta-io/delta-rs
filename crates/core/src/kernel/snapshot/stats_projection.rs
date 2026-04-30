#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
#[cfg(test)]
use delta_kernel::PredicateRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::ColumnName;
use delta_kernel::scan::Scan as KernelScan;
use delta_kernel::schema::{DataType, SchemaRef as KernelSchemaRef, StructField, StructType};
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use tracing::debug;

#[cfg(test)]
use super::Snapshot;
use crate::DeltaResult;
use crate::kernel::SCAN_ROW_ARROW_SCHEMA;
use crate::kernel::arrow::engine_ext::SnapshotExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatsProjection {
    None,
    Full,
    NumRecordsOnly,
    PredicateColumns(BTreeSet<ColumnName>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatsSourcePolicy {
    ParsedWithJsonFallback,
    JsonOnly,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawStatsPolicy {
    Preserve,
    Omit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileStatsMaterialization {
    stats_projection: StatsProjection,
    stats_source_policy: StatsSourcePolicy,
    raw_stats_policy: RawStatsPolicy,
}

impl FileStatsMaterialization {
    pub(crate) fn query(stats_projection: StatsProjection) -> Self {
        Self {
            stats_projection,
            stats_source_policy: StatsSourcePolicy::ParsedWithJsonFallback,
            raw_stats_policy: RawStatsPolicy::Omit,
        }
    }

    pub(crate) fn compatibility(stats_projection: StatsProjection) -> Self {
        Self {
            stats_projection,
            stats_source_policy: StatsSourcePolicy::ParsedWithJsonFallback,
            raw_stats_policy: RawStatsPolicy::Preserve,
        }
    }

    pub(crate) fn json_only(
        stats_projection: StatsProjection,
        raw_stats_policy: RawStatsPolicy,
    ) -> Self {
        Self {
            stats_projection,
            stats_source_policy: StatsSourcePolicy::JsonOnly,
            raw_stats_policy,
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

    pub(crate) fn raw_stats_policy(&self) -> RawStatsPolicy {
        self.raw_stats_policy
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

    pub(crate) fn num_records_only() -> Self {
        Self::NumRecordsOnly
    }

    pub(crate) fn predicate_columns(columns: impl IntoIterator<Item = ColumnName>) -> Self {
        Self::PredicateColumns(columns.into_iter().collect())
    }

    /// Builds the stats projection for a scan using physical predicate references.
    ///
    /// Use `NumRecordsOnly` when a predicate only touches partition columns,
    /// skips all files, or references no data columns.
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
                requested_columns = ?requested_columns,
                "stats projection selected"
            );
            Ok(Self::NumRecordsOnly)
        } else {
            let filtered_columns = requested_columns
                .difference(&columns)
                .cloned()
                .collect::<Vec<_>>();
            debug!(
                projection = "predicate_columns",
                requested_columns = ?requested_columns,
                selected_columns = ?columns,
                filtered_columns = ?filtered_columns,
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
            "stats_parsed",
            ArrowDataType::Struct(stats_schema.fields().to_owned()),
            true,
        )));

        if let Some(partition_schema) = snapshot.table_configuration().partitions_schema()? {
            let partition_schema: ArrowSchema = partition_schema.as_ref().try_into_arrow()?;
            fields.push(Arc::new(ArrowField::new(
                "partitionValues_parsed",
                ArrowDataType::Struct(partition_schema.fields().to_owned()),
                false,
            )));
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    /// Returns whether this projection emits stats for a root physical column.
    ///
    /// A nested reference such as `nested.leaf` still emits the enclosing field
    /// `nested` because replay looks up stats by root column name.
    #[cfg(feature = "datafusion")]
    pub(crate) fn emits_top_level_column_stats(&self, physical_name: &str) -> bool {
        match self {
            Self::None => false,
            Self::Full => true,
            Self::NumRecordsOnly => false,
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
        "numRecords",
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
            .field("numRecords")
            .cloned()
            .unwrap_or_else(|| StructField::nullable("numRecords", DataType::LONG)),
    );

    for stats_field_name in ["nullCount", "minValues", "maxValues"] {
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
    use crate::{DeltaTable, DeltaTableBuilder, TableProperty};

    fn column_mapping_builder() -> DeltaResult<DeltaTableBuilder> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../test/tests/data/table_with_column_mapping");
        let url = url::Url::from_directory_path(path.canonicalize().unwrap()).unwrap();
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

    fn scan_with_predicate(
        snapshot: &Snapshot,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<crate::kernel::snapshot::Scan> {
        snapshot.scan_builder().with_predicate(predicate).build()
    }

    #[test]
    fn file_stats_materialization_query_defaults_to_omit_raw() {
        let projection = StatsProjection::num_records_only();
        let materialization = FileStatsMaterialization::query(projection.clone());

        assert_eq!(
            materialization.stats_source_policy(),
            StatsSourcePolicy::ParsedWithJsonFallback
        );
        assert_eq!(materialization.raw_stats_policy(), RawStatsPolicy::Omit);
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
        assert_eq!(materialization.raw_stats_policy(), RawStatsPolicy::Preserve);
        assert_eq!(materialization.stats_projection(), &projection);
    }

    #[test]
    fn file_stats_materialization_without_stats_disables_sources() {
        let materialization = FileStatsMaterialization::without_stats();

        assert_eq!(
            materialization.stats_source_policy(),
            StatsSourcePolicy::None
        );
        assert_eq!(materialization.raw_stats_policy(), RawStatsPolicy::Omit);
        assert_eq!(materialization.stats_projection(), &StatsProjection::none());
    }

    #[tokio::test]
    async fn stats_projection_no_predicate_uses_num_records_only() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let scan = scan_with_predicate(&snapshot, None)?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;

        assert_eq!(projection, StatsProjection::NumRecordsOnly);
        assert!(stats_schema.field("numRecords").is_some());
        assert!(stats_schema.field("minValues").is_none());
        assert!(stats_schema.field("maxValues").is_none());
        assert!(stats_schema.field("nullCount").is_none());

        Ok(())
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
    async fn stats_projection_partition_only_predicate_uses_num_records_only() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef =
            Arc::new(Expression::column(["part"]).eq(Scalar::String("A".to_string())));
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;

        assert_eq!(projection, StatsProjection::NumRecordsOnly);
        assert!(stats_schema.field("numRecords").is_some());
        assert!(stats_schema.field("minValues").is_none());
        assert!(stats_schema.field("maxValues").is_none());
        assert!(stats_schema.field("nullCount").is_none());

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_static_skip_all_uses_num_records_only() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef = Arc::new(delta_kernel::Predicate::literal(false));
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;

        assert_eq!(projection, StatsProjection::NumRecordsOnly);
        assert!(stats_schema.field("numRecords").is_some());
        assert!(stats_schema.field("minValues").is_none());
        assert!(stats_schema.field("maxValues").is_none());
        assert!(stats_schema.field("nullCount").is_none());

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_data_predicate_includes_only_referenced_columns() -> TestResult {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef =
            Arc::new(Expression::column(["value"]).gt(Scalar::Integer(10)));
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;
        let stats_schema = stats_schema.to_string();

        assert!(stats_schema.contains("numRecords"));
        assert!(stats_schema.contains("minValues"));
        assert!(stats_schema.contains("maxValues"));
        assert!(stats_schema.contains("nullCount"));
        assert!(stats_schema.contains("value"));
        assert!(!stats_schema.contains("unreferenced_col"));

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
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;
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
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;

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
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;

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
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;
        let stats_schema = stats_schema.to_string();

        let logical_schema = snapshot.inner.table_configuration().logical_schema();
        let logical = logical_schema
            .field("Super Name")
            .expect("missing logical column");
        let physical =
            logical.physical_name(snapshot.inner.table_configuration().column_mapping_mode());

        assert!(stats_schema.contains(physical));
        assert!(!stats_schema.contains("Super Name"));

        Ok(())
    }

    #[tokio::test]
    async fn stats_projection_nested_struct_predicate_keeps_only_referenced_subtree() -> TestResult
    {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef =
            Arc::new(Expression::column(["nested", "leaf"]).gt(Scalar::Integer(1)));
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;
        let stats_schema = stats_schema.to_string();

        assert!(stats_schema.contains("nested"));
        assert!(stats_schema.contains("leaf"));
        assert!(!stats_schema.contains("other_leaf"));

        Ok(())
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn stats_projection_emits_only_top_level_stats_fields() {
        let projection = StatsProjection::PredicateColumns(BTreeSet::from([ColumnName::new([
            "nested", "leaf",
        ])]));

        assert!(projection.emits_top_level_column_stats("nested"));
        assert!(!projection.emits_top_level_column_stats("leaf"));
        assert!(!projection.emits_top_level_column_stats("value"));
    }

    #[tokio::test]
    async fn stats_projection_respects_num_indexed_cols_limit() -> TestResult {
        let snapshot = synthetic_snapshot_with_num_indexed_cols("1").await?;
        let predicate: PredicateRef = Arc::new(
            Expression::column(["unreferenced_col"]).eq(Scalar::String("match".to_string())),
        );
        let scan = scan_with_predicate(&snapshot, Some(predicate))?;

        let projection = StatsProjection::for_scan(scan.inner().as_ref())?;
        let stats_schema = projection.stats_schema(snapshot.inner.as_ref())?;
        let stats_schema = stats_schema.to_string();

        assert!(stats_schema.contains("numRecords"));
        assert!(!stats_schema.contains("unreferenced_col"));
        assert!(!stats_schema.contains("minValues"));
        assert!(!stats_schema.contains("maxValues"));
        assert!(!stats_schema.contains("nullCount"));

        Ok(())
    }
}
