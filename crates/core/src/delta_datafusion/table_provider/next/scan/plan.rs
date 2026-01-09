//! Logical planning for scans against Delta tables using Delta Kernel
//!
//! This module encapsulates the logic to process the inputs passed by
//! the DataFusion planner (projections, filters) and produce a
//! kernel based scan plan that can be used to create execution plans.
//!
//! The main complexity arises when handling predicates as we want to
//! leverage predicates as best as possible both when integrating with
//! Delta Kernel and DataFusion's Parquet scan capabilities. Specifically
//! - file level skipping in Delta Kernel
//! - predicate pushdown in DataFusion's Parquet scan
//!
//! Since the TableProvider (DeltaScan) exposes the logical table schema,
//! we need to handle translation between the predicates expressed
//! against the logical schema,
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow_schema::{DataType, Field, FieldRef, SchemaBuilder};
use datafusion::common::error::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{HashMap, HashSet, plan_err};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::utils::conjunction;
use datafusion::prelude::Expr;
use datafusion_datasource::file_scan_config::wrap_partition_type_in_dict;
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel as _};
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_features::TableFeature;
use delta_kernel::{Expression, Predicate, PredicateRef};
use itertools::Itertools;
use tracing::warn;

use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::engine::{to_datafusion_expr, to_delta_expression, to_predicate};
use crate::delta_datafusion::table_provider::next::FILE_ID_COLUMN_DEFAULT;
use crate::kernel::{Scan, Snapshot};

/// Logical scan plan for Delta tables using Delta Kernel.
///
/// This structure bridges DataFusion's query planning with Delta Kernel's scan capabilities.
/// It handles schema projection, predicate translation, and determines which predicates can
/// be pushed to kernel file skipping vs. Parquet readers.
///
/// # Schema Handling
///
/// Manages three schemas:
/// - **result_schema**: Logical schema exposed to query after all transformations
/// - **output_schema**: Final schema including metadata columns (e.g., file_id)
/// - **parquet_read_schema**: Physical schema for reading Parquet files
///
/// # Predicate Pushdown
///
/// Predicates are assigned to two levels:
/// - Kernel scan: File-level skipping using table statistics (pushed to [`scan`])
/// - Parquet scan: File/Row-level filtering within files ([`parquet_predicate`])
#[derive(Clone, Debug)]
pub(crate) struct KernelScanPlan {
    /// Wrapped kernel scan to produce logical file stream
    pub(crate) scan: Arc<Scan>,
    /// The resulting schema exposed to the caller (used for expression evaluation)
    pub(crate) result_schema: SchemaRef,
    /// The final output schema (includes file_id column if configured)
    pub(crate) output_schema: SchemaRef,
    /// If set, indicates a projection to apply to the
    /// scan output to obtain the result schema
    pub(crate) result_projection: Option<Vec<usize>>,
    /// The schema the inner Parquet scan should read from data files.
    pub(crate) parquet_read_schema: SchemaRef,
    /// If set, indicates a predicate to apply at the Parquet scan level
    pub(crate) parquet_predicate: Option<Expr>,
    pub(crate) parquet_predicate_covers_all_filters: bool,
    pub(crate) filter_predicate: Option<Expr>,
}

impl KernelScanPlan {
    pub(crate) fn try_new(
        snapshot: &Snapshot,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        config: &DeltaScanConfig,
    ) -> Result<Self> {
        let table_config = snapshot.table_configuration();
        let table_schema = config.table_schema(table_config)?;

        // At this point we should only have supported predicates, but we decide where
        // when can handle them (kernel scan and/or parquet scan)
        let (kernel_predicate, parquet_predicate, parquet_predicate_covers_all_filters) =
            process_filters(filters, table_config)?;
        let filter_predicate = if config.schema_force_view_types {
            conjunction(filters.iter().cloned())
        } else {
            None
        };
        let scan_builder = snapshot.scan_builder().with_predicate(kernel_predicate);

        let Some(projection) = projection else {
            let scan = Arc::new(scan_builder.build()?);
            return Self::try_new_with_scan(
                scan,
                config,
                table_schema,
                None,
                parquet_predicate,
                parquet_predicate_covers_all_filters,
                filter_predicate,
            );
        };

        // The table projection may not include all columns referenced in filters,
        // Specifically, if a filter references a partition column that is not
        // part of the projection, we need to add it to the scan projection.
        // This is because may not include columns that are handled Exact by a provider.
        let result_schema = Arc::new(table_schema.project(projection)?);
        let columns_in_filters: HashSet<_> = filters
            .iter()
            .flat_map(|f| f.column_refs().iter().map(|c| c.name()).collect_vec())
            .collect();
        let columns_in_scan: HashSet<_> = result_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let missing_columns: Vec<_> = columns_in_filters
            .difference(&columns_in_scan)
            .cloned()
            .collect();

        let mut projection = projection.clone();
        for col in missing_columns {
            projection.push(table_schema.index_of(col)?);
        }

        // With the updated projection, build the scan
        let kernel_scan_schema = Arc::new((&table_schema.project(&projection)?).try_into_kernel()?);
        let scan = Arc::new(scan_builder.with_schema(kernel_scan_schema).build()?);

        // We may have read columns in the scan that are purely for predicate processing.
        // We need to project them out of the final result schema
        let logical_columns: HashSet<_> = scan
            .logical_schema()
            .fields()
            .map(|f| f.name().as_str())
            .collect();
        let excess_columns = logical_columns.difference(&columns_in_scan).collect_vec();
        let result_projection = if !excess_columns.is_empty() {
            let mut result_projection = Vec::with_capacity(result_schema.fields().len());
            for (i, field) in scan.logical_schema().fields().enumerate() {
                if columns_in_scan.contains(field.name().as_str()) {
                    result_projection.push(i);
                }
            }
            Some(result_projection)
        } else {
            None
        };

        drop(columns_in_scan);
        drop(logical_columns);

        Self::try_new_with_scan(
            scan,
            config,
            result_schema,
            result_projection,
            parquet_predicate,
            parquet_predicate_covers_all_filters,
            filter_predicate,
        )
    }

    fn try_new_with_scan(
        scan: Arc<Scan>,
        config: &DeltaScanConfig,
        result_schema: SchemaRef,
        result_projection: Option<Vec<usize>>,
        parquet_predicate: Option<Expr>,
        parquet_predicate_covers_all_filters: bool,
        filter_predicate: Option<Expr>,
    ) -> Result<Self> {
        let output_schema = if config.retain_file_id() {
            let mut schema_builder = SchemaBuilder::from(result_schema.as_ref());
            schema_builder.push(config.file_id_field());
            Arc::new(schema_builder.finish())
        } else {
            result_schema.clone()
        };
        let parquet_read_schema = config.parquet_file_schema(
            scan.snapshot().table_configuration(),
            &scan.physical_schema().as_ref().try_into_arrow()?,
        )?;
        Ok(Self {
            scan,
            result_schema,
            output_schema,
            result_projection,
            parquet_read_schema,
            parquet_predicate,
            parquet_predicate_covers_all_filters,
            filter_predicate,
        })
    }

    /// Denotes if the scan can be resolved using only file metadata
    ///
    /// It may still be impossible to perform a metadata-only scan if the
    /// file statistics are not sufficient to satisfy the query.
    pub(crate) fn is_metadata_only(&self) -> bool {
        self.scan.physical_schema().fields().len() == 0
    }

    pub(crate) fn table_configuration(&self) -> &TableConfiguration {
        self.scan.snapshot().table_configuration()
    }
}

impl DeltaScanConfig {
    pub(crate) fn file_id_field(&self) -> FieldRef {
        Arc::new(Field::new(
            self.file_column_name
                .as_deref()
                .unwrap_or(FILE_ID_COLUMN_DEFAULT),
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ))
    }

    pub(crate) fn retain_file_id(&self) -> bool {
        self.file_column_name.is_some()
    }

    /// The physical arrow schema exposed by the table provider
    ///
    /// This includes any adjustments to the logical schema
    /// such as dictionary encoding of partition columns or
    /// view types.
    pub(crate) fn table_schema(&self, table_config: &TableConfiguration) -> Result<SchemaRef> {
        let table_schema: Schema = table_config.schema().as_ref().try_into_arrow()?;
        self.physical_arrow_schema(table_config, &table_schema)
    }

    fn physical_arrow_schema(
        &self,
        table_config: &TableConfiguration,
        base: &Schema,
    ) -> Result<SchemaRef> {
        // delta exposes a logical schema which gets converted to default physical types,
        // but we may want specific physical types
        // (e.g. dictionary encoded partition columns, view types, etc) so we transform
        // the schema accordingly here
        let cols = table_config.metadata().partition_columns();
        let table_schema = Arc::new(Schema::new(
            base.fields()
                .iter()
                .map(|f| self.map_field(f.clone(), cols))
                .collect_vec(),
        ));
        Ok(table_schema)
    }

    fn parquet_file_schema(
        &self,
        table_config: &TableConfiguration,
        base: &Schema,
    ) -> Result<SchemaRef> {
        let cols = table_config.metadata().partition_columns();
        let table_schema = Arc::new(Schema::new(
            base.fields()
                .iter()
                .map(|f| self.map_field_for_parquet(f.clone(), cols))
                .collect_vec(),
        ));
        Ok(table_schema)
    }

    fn map_view_field(&self, field: FieldRef) -> FieldRef {
        if !self.schema_force_view_types {
            return field;
        }
        let mapped = self.map_view_type(field.data_type());
        if &mapped == field.data_type() {
            field
        } else {
            field.as_ref().clone().with_data_type(mapped).into()
        }
    }

    fn map_view_type(&self, data_type: &DataType) -> DataType {
        if !self.schema_force_view_types {
            return data_type.clone();
        }
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
            DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
            DataType::Struct(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|field| {
                        let mapped = self.map_view_type(field.data_type());
                        Arc::new(field.as_ref().clone().with_data_type(mapped))
                    })
                    .collect(),
            ),
            DataType::List(field) => DataType::List(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(self.map_view_type(field.data_type())),
            )),
            DataType::LargeList(field) => DataType::LargeList(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(self.map_view_type(field.data_type())),
            )),
            DataType::ListView(field) => DataType::ListView(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(self.map_view_type(field.data_type())),
            )),
            DataType::LargeListView(field) => DataType::LargeListView(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(self.map_view_type(field.data_type())),
            )),
            DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(self.map_view_type(field.data_type())),
                ),
                *size,
            ),
            DataType::Map(field, sorted) => DataType::Map(
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(self.map_view_type(field.data_type())),
                ),
                *sorted,
            ),
            DataType::Dictionary(key, value) => {
                DataType::Dictionary(key.clone(), Box::new(self.map_view_type(value)))
            }
            _ => data_type.clone(),
        }
    }

    fn map_field_for_parquet(&self, field: FieldRef, partition_cols: &[String]) -> FieldRef {
        let field = self.map_view_field(field);
        if partition_cols.contains(field.name()) && self.wrap_partition_values {
            return match field.data_type() {
                DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary => field
                    .as_ref()
                    .clone()
                    .with_data_type(wrap_partition_type_in_dict(field.data_type().clone()))
                    .into(),
                DataType::Utf8View => field
                    .as_ref()
                    .clone()
                    .with_data_type(wrap_partition_type_in_dict(DataType::Utf8))
                    .into(),
                DataType::BinaryView => field
                    .as_ref()
                    .clone()
                    .with_data_type(wrap_partition_type_in_dict(DataType::Binary))
                    .into(),
                _ => field,
            };
        }
        field
    }

    fn map_field(&self, field: FieldRef, partition_cols: &[String]) -> FieldRef {
        let field = self.map_view_field(field);
        if partition_cols.contains(field.name()) && self.wrap_partition_values {
            return match field.data_type() {
                DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary => field
                    .as_ref()
                    .clone()
                    .with_data_type(wrap_partition_type_in_dict(field.data_type().clone()))
                    .into(),
                DataType::Utf8View => field
                    .as_ref()
                    .clone()
                    .with_data_type(wrap_partition_type_in_dict(DataType::Utf8))
                    .into(),
                DataType::BinaryView => field
                    .as_ref()
                    .clone()
                    .with_data_type(wrap_partition_type_in_dict(DataType::Binary))
                    .into(),
                _ => field,
            };
        }
        field
    }
}

pub(crate) fn supports_filters_pushdown(
    filter: &[&Expr],
    config: &TableConfiguration,
) -> Vec<TableProviderFilterPushDown> {
    filter
        .iter()
        .map(|f| process_predicate(f, config).pushdown)
        .collect()
}

/// Process a list of filter expressions and determine which
/// predicates can be pushed down to the parquet scan and which
/// can be handled at the kernel scan level.
///
/// The returned kernel predicate can be used when crating
/// the kernel scan object, while the parquet predicate
/// can be converted into a PhysicalExpr and passed
/// to the parquet scan. The returned predcate is apready translated
/// to use physical column names if column mapping is enabled.
fn process_filters(
    filters: &[Expr],
    config: &TableConfiguration,
) -> Result<(Option<PredicateRef>, Option<Expr>, bool)> {
    let (parquet, kernel): (Vec<_>, Vec<_>) = filters
        .iter()
        .map(|f| process_predicate(f, config))
        .map(|p| (p.parquet_predicate, p.kernel_predicate))
        .unzip();

    let parquet_all = parquet.iter().all(|predicate| predicate.is_some());
    let parquet_predicates: Vec<Expr> = if config.is_feature_enabled(&TableFeature::ColumnMapping) {
        parquet
            .iter()
            .flatten()
            .filter_map(|ex| match rewrite_expression((*ex).clone(), config) {
                Ok(expr) => Some(expr),
                Err(err) => {
                    warn!(
                        "Failed to rewrite predicate for column mapping; skipping parquet pushdown: {}",
                        err
                    );
                    None
                }
            })
            .collect()
    } else {
        parquet.iter().flatten().map(|ex| (*ex).clone()).collect()
    };

    let parquet_predicate_covers_all_filters =
        parquet_all && parquet_predicates.len() == filters.len();
    let parquet = conjunction(parquet_predicates.into_iter());

    let kernel_predicates: Vec<_> = kernel.into_iter().flatten().collect();
    let kernel = (!kernel_predicates.is_empty()).then(|| Predicate::and_from(kernel_predicates));
    Ok((
        kernel.map(Arc::new),
        parquet,
        parquet_predicate_covers_all_filters,
    ))
}

struct ProcessedPredicate<'a> {
    pub pushdown: TableProviderFilterPushDown,
    pub kernel_predicate: Option<Predicate>,
    pub parquet_predicate: Option<&'a Expr>,
}

fn process_predicate<'a>(expr: &'a Expr, config: &TableConfiguration) -> ProcessedPredicate<'a> {
    let cols = config.metadata().partition_columns();
    let only_partition_refs = expr.column_refs().iter().all(|c| cols.contains(&c.name));
    let any_partition_refs =
        only_partition_refs || expr.column_refs().iter().any(|c| cols.contains(&c.name));

    // TODO(roeap): we may allow pusing predicates referencing partition columns
    // into the parquet scan, if the table has materialized partition columns
    let _has_partition_data = config.is_feature_enabled(&TableFeature::MaterializePartitionColumns);

    // Try to convert the expression into a kernel predicate
    if let Ok(kernel_predicate) = to_predicate(expr) {
        let (pushdown, parquet_predicate) = if only_partition_refs {
            // All references are to partition columns so the kernel
            // scan can fully handle the predicate and return exact results
            (TableProviderFilterPushDown::Exact, None)
        } else if any_partition_refs {
            // Some references are to partition columns, so the kernel
            // scan can only handle the predicate on best effort. Since the
            // parquet scan cannot reference partition columns, we do not
            // push down any predicate to parquet
            (TableProviderFilterPushDown::Inexact, None)
        } else {
            (TableProviderFilterPushDown::Inexact, Some(expr))
        };
        return ProcessedPredicate {
            pushdown,
            kernel_predicate: Some(kernel_predicate),
            parquet_predicate,
        };
    }

    // If there are any partition column references, we cannot
    // push down the predicate to parquet scan
    if any_partition_refs {
        return ProcessedPredicate {
            pushdown: TableProviderFilterPushDown::Unsupported,
            kernel_predicate: None,
            parquet_predicate: None,
        };
    }

    ProcessedPredicate {
        pushdown: TableProviderFilterPushDown::Inexact,
        kernel_predicate: None,
        parquet_predicate: Some(expr),
    }
}

fn rewrite_expression(expr: Expr, config: &TableConfiguration) -> Result<Expr> {
    let logical_fields = config.schema().leaves(None);
    let (logical_names, _) = logical_fields.as_ref();
    let physical_schema = config
        .schema()
        .make_physical(config.column_mapping_mode())
        .leaves(None);
    let (physical_names, _) = physical_schema.as_ref();
    let name_mapping: HashMap<_, _> = logical_names.iter().zip(physical_names).collect();
    let transformed = expr.transform(|node| match &node {
        // Scalar functions might be field a field access for a nested column
        // (e.g. `a.b.c`), so we might be able to handle them here as well
        Expr::Column(_) | Expr::ScalarFunction(_) => {
            let col_name = to_delta_expression(&node)?;
            if let Expression::Column(name) = &col_name {
                if let Some(physical_name) = name_mapping.get(name) {
                    return Ok(Transformed::yes(to_datafusion_expr(
                        &Expression::Column((*physical_name).clone()),
                        // This is just a dummy datatype, since column re-writes
                        // do not require datatype information
                        &KernelDataType::BOOLEAN,
                    )?));
                } else {
                    return plan_err!("Column '{name}' not found in physical schema");
                }
            }
            Ok(Transformed::no(node))
        }
        _ => Ok(Transformed::no(node)),
    })?;

    Ok(transformed.data)
}

#[cfg(test)]
mod tests {
    use datafusion::{
        assert_batches_sorted_eq,
        physical_plan::collect,
        prelude::{col, lit},
        scalar::ScalarValue,
    };

    use crate::{
        delta_datafusion::create_session,
        test_utils::{TestResult, open_fs_path},
    };

    use super::*;

    #[tokio::test]
    async fn test_rewrite_expression() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let config = table.snapshot()?.snapshot().table_configuration();

        let expr = col(r#""Super Name""#).eq(lit("Anthony Johnson"));
        let rewritten = rewrite_expression(expr.clone(), config)?;
        let expected = col("col-3877fd94-0973-4941-ac6b-646849a1ff65").eq(lit("Anthony Johnson"));
        assert_eq!(rewritten, expected);

        let expr = col(r#""Company Very Short""#).eq(lit("BME"));
        let rewritten = rewrite_expression(expr.clone(), config)?;
        let expected = col("col-173b4db9-b5ad-427f-9e75-516aae37fbbb").eq(lit("BME"));
        assert_eq!(rewritten, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_plan() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let expr = col(r#""Super Name""#).eq(lit("Anthony Johnson"));
        let scan_plan = KernelScanPlan::try_new(
            table.snapshot()?.snapshot().snapshot(),
            None,
            &[expr.clone()],
            &DeltaScanConfig::default(),
        )?;
        let expected_pq =
            col("col-3877fd94-0973-4941-ac6b-646849a1ff65").eq(lit("Anthony Johnson"));
        assert_eq!(scan_plan.parquet_predicate, Some(expected_pq));
        assert!(scan_plan.parquet_predicate_covers_all_filters);

        let expr = col(r#""Company Very Short""#).eq(lit("BME"));
        let scan_plan = KernelScanPlan::try_new(
            table.snapshot()?.snapshot().snapshot(),
            None,
            &[expr.clone()],
            &DeltaScanConfig::default(),
        )?;
        assert!(scan_plan.parquet_predicate.is_none());
        assert!(!scan_plan.parquet_predicate_covers_all_filters);

        let expr = col(r#""Super Name""#).eq(lit("Timothy Lamb"));
        let scan_plan = KernelScanPlan::try_new(
            table.snapshot()?.snapshot().snapshot(),
            None,
            &[expr.clone()],
            &DeltaScanConfig::default(),
        )?;
        println!("Scan plan: {:?}", scan_plan.parquet_predicate);

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_kernel_predicate_is_none() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let expr = col(r#""Company Very Short""#).like(lit("B%"));
        let config = table.snapshot()?.snapshot().table_configuration();
        let (kernel, _parquet, _covers_all) = process_filters(&[expr], config)?;
        assert!(kernel.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_kernel_scan_plan_basic() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let provider = table.table_provider().await?;
        let ctx = create_session().into_inner();

        let batches = ctx.read_table(provider.clone())?.collect().await?;
        let expected = vec![
            "+--------------------+------------------------+",
            "| Company Very Short | Super Name             |",
            "+--------------------+------------------------+",
            "| BME                | Timothy Lamb           |",
            "| BMS                | Anthony Johnson        |",
            "| BMS                | Mr. Daniel Ferguson MD |",
            "| BMS                | Nathan Bennett         |",
            "| BMS                | Stephanie Mcgrath      |",
            "+--------------------+------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);
        let scan = provider.scan(&ctx.state(), None, &[], None).await?;
        let batches = collect(scan, ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(&expected, &batches);

        let filter =
            col(r#""Company Very Short""#).eq(lit(ScalarValue::Utf8(Some("BME".to_string()))));
        let batches = ctx
            .read_table(provider.clone())?
            .filter(filter.clone())?
            .collect()
            .await?;
        let expected = vec![
            "+--------------------+--------------+",
            "| Company Very Short | Super Name   |",
            "+--------------------+--------------+",
            "| BME                | Timothy Lamb |",
            "+--------------------+--------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);
        let scan = provider.scan(&ctx.state(), None, &[filter], None).await?;
        let batches = collect(scan, ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(&expected, &batches);

        let filter =
            col(r#""Super Name""#).eq(lit(ScalarValue::Utf8(Some("Timothy Lamb".to_string()))));
        let batches = ctx
            .read_table(provider.clone())?
            .filter(filter.clone())?
            .collect()
            .await?;
        assert_batches_sorted_eq!(&expected, &batches);
        let scan = provider
            .scan(&ctx.state(), None, &[filter.clone()], None)
            .await?;
        let batches = collect(scan, ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_view_types_honored_in_next_scan_path() -> TestResult {
        use arrow_schema::DataType;

        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let snapshot = table.snapshot()?.snapshot().snapshot();

        let config = DeltaScanConfig {
            file_column_name: None,
            wrap_partition_values: true,
            enable_parquet_pushdown: true,
            schema_force_view_types: true,
            schema: None,
        };

        let scan_plan = KernelScanPlan::try_new(snapshot, None, &[], &config)?;

        let has_view_type = scan_plan.parquet_read_schema.fields().iter().any(|f| {
            matches!(f.data_type(), DataType::Utf8View | DataType::BinaryView)
        });
        assert!(
            has_view_type,
            "schema_force_view_types should apply view types in parquet_read_schema"
        );

        let has_view_type_exposed = scan_plan.result_schema.fields().iter().any(|f| {
            matches!(f.data_type(), DataType::Utf8View | DataType::BinaryView)
        });
        assert!(
            has_view_type_exposed,
            "schema_force_view_types should apply view types in exposed schema"
        );

        Ok(())
    }

    #[test]
    fn test_view_type_mapping_nested_fields() {
        let config = DeltaScanConfig {
            schema_force_view_types: true,
            ..DeltaScanConfig::new()
        };

        let nested = Arc::new(Field::new(
            "nested",
            DataType::Struct(
                vec![Field::new(
                    "inner",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                )]
                .into(),
            ),
            true,
        ));
        let mapped = config.map_field(nested, &[]);
        let DataType::Struct(fields) = mapped.data_type() else {
            panic!("expected struct field");
        };
        let inner = fields.iter().find(|f| f.name() == "inner").unwrap();
        let DataType::List(list_field) = inner.data_type() else {
            panic!("expected list field");
        };
        assert!(matches!(list_field.data_type(), DataType::Utf8View));
    }

    #[tokio::test]
    async fn test_filter_with_utf8_literal_works_on_string_column() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let provider = table.table_provider().await?;
        let ctx = create_session().into_inner();

        let filter = col(r#""Super Name""#).eq(lit(ScalarValue::Utf8(Some("Timothy Lamb".to_string()))));
        let batches = ctx
            .read_table(provider.clone())?
            .filter(filter)?
            .collect()
            .await?;

        let expected = vec![
            "+--------------------+--------------+",
            "| Company Very Short | Super Name   |",
            "+--------------------+--------------+",
            "| BME                | Timothy Lamb |",
            "+--------------------+--------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }
}
