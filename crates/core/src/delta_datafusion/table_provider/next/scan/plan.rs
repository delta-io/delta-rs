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
use datafusion::scalar::ScalarValue;
use datafusion_datasource::file_scan_config::wrap_partition_type_in_dict;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_features::TableFeature;
use delta_kernel::{Expression, Predicate, PredicateRef};
use itertools::Itertools;
use tracing::debug;

use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::engine::{
    to_datafusion_expr, to_delta_expression, to_delta_predicate,
};
use crate::delta_datafusion::table_provider::next::FILE_ID_COLUMN_DEFAULT;
use crate::kernel::{Scan, Snapshot};

/// Query scoped contract between the provider, logical planner, and scan execs.
///
/// This centralizes all schema and file id visibility decisions for a single
/// `TableProvider::scan` request so planning and execution do not rederive them independently.
#[derive(Clone, Debug)]
pub(crate) struct ProjectedScanContract {
    /// Logical data schema after removing temporary predicate only columns and internal metadata.
    pub(crate) result_schema: SchemaRef,
    /// Logical schema produced by the kernel scan before dropping filter-only columns.
    pub(crate) scan_schema: SchemaRef,
    /// Actual schema the scan must return to its parent for this query.
    pub(crate) output_schema: SchemaRef,
    /// Projection against the logical table schema used to build the kernel scan.
    pub(crate) kernel_projection: Option<Vec<usize>>,
    /// Projection from the kernel scan result into [`result_schema`](Self::result_schema) when
    /// filter only columns were added to the kernel projection.
    pub(crate) result_projection: Option<Vec<usize>>,
    /// Synthetic file id field used internally to correlate batches with their source files.
    pub(crate) file_id_field: FieldRef,
    /// Whether the scan must preserve file-id in its output for projection or filter semantics.
    pub(crate) retain_file_id: bool,
    /// Row index field produced by the scan.
    pub(crate) row_index_field: Option<FieldRef>,
    /// Whether scan output includes row index.
    pub(crate) retain_row_index: bool,
}

impl ProjectedScanContract {
    /// Returns the row index field when retained in scan output.
    pub(crate) fn retained_row_index_field(&self) -> Option<FieldRef> {
        match (self.retain_row_index, self.row_index_field.as_ref()) {
            (true, Some(field)) => Some(Arc::clone(field)),
            _ => None,
        }
    }

    pub(crate) fn try_new(
        table_schema: SchemaRef,
        provider_schema: SchemaRef,
        config: &DeltaScanConfig,
        row_index_column: Option<&str>,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<Self> {
        let file_id_field = config.file_id_field();
        let provider_exposes_file_id = config.has_file_id();
        let file_id_idx = provider_exposes_file_id
            .then(|| provider_schema.index_of(file_id_field.name()).ok())
            .flatten();

        let query_projects_file_id = if let Some(file_id_idx) = file_id_idx {
            match projection {
                None => true,
                Some(projection) => projection.contains(&file_id_idx),
            }
        } else {
            false
        };

        let row_index_field = row_index_column
            .map(|name| Arc::new(Field::new(name, DataType::UInt64, false)) as FieldRef);
        let row_index_idx = row_index_column.and_then(|name| provider_schema.index_of(name).ok());

        let query_projects_row_index = if let Some(row_index_idx) = row_index_idx {
            match projection {
                None => true,
                Some(projection) => projection.contains(&row_index_idx),
            }
        } else {
            false
        };

        let filters_reference_file_id = provider_exposes_file_id
            && filters.iter().any(|filter| {
                filter
                    .column_refs()
                    .iter()
                    .any(|column| column.name == file_id_field.name().as_str())
            });
        let retain_file_id =
            provider_exposes_file_id && (query_projects_file_id || filters_reference_file_id);
        let filters_reference_row_index = row_index_column.is_some_and(|row_index_column| {
            filters.iter().any(|filter| {
                filter
                    .column_refs()
                    .iter()
                    .any(|column| column.name == row_index_column)
            })
        });
        let retain_row_index =
            row_index_field.is_some() && (query_projects_row_index || filters_reference_row_index);

        let requested_data_projection = projection.map(|projection| {
            projection
                .iter()
                .filter(|&&idx| Some(idx) != file_id_idx && Some(idx) != row_index_idx)
                .copied()
                .collect_vec()
        });

        let result_schema = match requested_data_projection.as_ref() {
            Some(projection) => Arc::new(table_schema.project(projection)?),
            None => table_schema.clone(),
        };

        let columns_in_filters: HashSet<String> = filters
            .iter()
            .flat_map(|f| f.column_refs().iter().map(|c| c.name.clone()).collect_vec())
            .collect();
        let columns_in_result: HashSet<String> = result_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let missing_columns: Vec<_> = columns_in_filters
            .difference(&columns_in_result)
            .filter(|column| {
                column.as_str() != file_id_field.name().as_str()
                    && row_index_column != Some(column.as_str())
            })
            .cloned()
            .sorted()
            .collect();

        let kernel_projection = requested_data_projection
            .as_ref()
            .map(|projection| {
                let mut projection = projection.clone();
                for column in &missing_columns {
                    projection.push(table_schema.index_of(column)?);
                }
                Ok::<_, datafusion::common::DataFusionError>(projection)
            })
            .transpose()?;

        let scan_schema = match kernel_projection.as_ref() {
            Some(projection) => Arc::new(table_schema.project(projection)?),
            None => table_schema.clone(),
        };

        let result_projection = kernel_projection.as_ref().and_then(|projection| {
            let projected_len = requested_data_projection.as_ref()?.len();
            (projection.len() > projected_len).then(|| (0..projected_len).collect())
        });

        let output_schema = if retain_file_id {
            let mut schema_builder = SchemaBuilder::from(result_schema.as_ref());
            schema_builder.push(file_id_field.clone());
            if retain_row_index {
                schema_builder.push(row_index_field.as_ref().expect("row index field").clone());
            }
            Arc::new(schema_builder.finish())
        } else if retain_row_index {
            let mut schema_builder = SchemaBuilder::from(result_schema.as_ref());
            schema_builder.push(row_index_field.as_ref().expect("row index field").clone());
            Arc::new(schema_builder.finish())
        } else {
            result_schema.clone()
        };

        Ok(Self {
            result_schema,
            scan_schema,
            output_schema,
            kernel_projection,
            result_projection,
            file_id_field,
            retain_file_id,
            row_index_field,
            retain_row_index,
        })
    }
}

/// Logical scan plan for Delta tables using Delta Kernel.
///
/// This structure bridges DataFusion's query planning with Delta Kernel's scan capabilities.
/// It finalizes the query scoped scan contract and determines which predicates can
/// be pushed to kernel file skipping vs. Parquet readers.
#[derive(Clone, Debug)]
pub(crate) struct KernelScanPlan {
    /// Wrapped kernel scan to produce logical file stream
    pub(crate) scan: Arc<Scan>,
    /// Query scoped contract shared across planning and execution.
    pub(crate) contract: ProjectedScanContract,
    /// Physical schema used for Parquet reads and predicate evaluation.
    pub(crate) parquet_read_schema: SchemaRef,
    /// Predicate binding schema used to plan Parquet pushdown, including the synthetic file id
    /// column.
    pub(crate) parquet_predicate_schema: SchemaRef,
    /// If set, indicates a predicate to apply at the Parquet scan level
    pub(crate) parquet_predicate: Option<Expr>,
}

impl KernelScanPlan {
    pub(crate) fn try_new(
        snapshot: &Snapshot,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        config: &DeltaScanConfig,
        skipping_predicate: Option<Vec<Expr>>,
    ) -> Result<Self> {
        let table_schema = config.table_schema(snapshot.table_configuration())?;
        let provider_schema = if config.has_file_id() {
            let mut schema_builder = SchemaBuilder::from(table_schema.as_ref());
            schema_builder.push(config.file_id_field());
            Arc::new(schema_builder.finish())
        } else {
            table_schema.clone()
        };
        let contract = ProjectedScanContract::try_new(
            table_schema,
            provider_schema,
            config,
            None,
            projection,
            filters,
        )?;
        Self::try_new_with_contract(snapshot, contract, filters, config, skipping_predicate)
    }

    pub(crate) fn try_new_with_contract(
        snapshot: &Snapshot,
        contract: ProjectedScanContract,
        filters: &[Expr],
        config: &DeltaScanConfig,
        skipping_predicate: Option<Vec<Expr>>,
    ) -> Result<Self> {
        let table_config = snapshot.table_configuration();
        let kernel_logical_schema = table_config.logical_schema();

        // At this point we should only have supported predicates, but we decide where
        // when can handle them (kernel scan and/or parquet scan)
        let (kernel_predicate, parquet_predicate) = process_filters(filters, table_config, config)?;

        // if some dedicated file skipping predicate is supplied,
        // we do not push the scan filters into the kernel scan.
        let scan_predicate = if let Some(sp) = skipping_predicate {
            let (scan_predicate, _) = process_filters(&sp, table_config, config)?;
            if scan_predicate.is_none() {
                debug!(
                    predicate = ?sp,
                    "Dropping dedicated file-skipping predicate because no kernel terms survived conversion"
                );
            }
            scan_predicate
        } else {
            kernel_predicate
        };

        let scan_builder = snapshot
            .scan_builder()
            .with_predicate(scan_predicate.clone());

        let scan = if contract.kernel_projection.is_some() {
            let kernel_projection_names = contract
                .scan_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect_vec();
            let kernel_scan_schema = kernel_logical_schema
                .project(&kernel_projection_names)
                .map_err(crate::DeltaTableError::from)?;
            Arc::new(scan_builder.with_schema(kernel_scan_schema).build()?)
        } else {
            Arc::new(scan_builder.build()?)
        };
        let parquet_read_schema = config.physical_arrow_schema(
            scan.snapshot().table_configuration(),
            &scan.physical_schema().as_ref().try_into_arrow()?,
        )?;
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&parquet_read_schema, &contract.file_id_field);
        Ok(Self {
            scan,
            contract,
            parquet_read_schema,
            parquet_predicate_schema,
            parquet_predicate,
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

pub(crate) fn build_parquet_predicate_schema(
    predicate_schema: &SchemaRef,
    file_id_field: &FieldRef,
) -> SchemaRef {
    let mut schema_builder = SchemaBuilder::from(predicate_schema.as_ref().clone());
    schema_builder.push(file_id_field.as_ref().clone().with_nullable(true));
    Arc::new(schema_builder.finish())
}

impl DeltaScanConfig {
    pub(crate) fn file_id_field(&self) -> FieldRef {
        crate::delta_datafusion::file_id::file_id_field(self.file_column_name.as_deref())
    }

    pub(crate) fn has_file_id(&self) -> bool {
        self.file_column_name.is_some()
    }

    pub(crate) fn provider_file_id_column<'a>(
        &'a self,
        projection: Option<&Vec<usize>>,
        result_schema: &Schema,
    ) -> Option<&'a str> {
        let name = self.file_column_name.as_deref()?;
        let Some(projection) = projection else {
            return Some(name);
        };
        let file_id_idx = result_schema.fields().len();
        projection.contains(&file_id_idx).then_some(name)
    }

    /// The physical arrow schema exposed by the table provider
    ///
    /// This includes any adjustments to the logical schema
    /// such as dictionary encoding of partition columns or
    /// view types.
    pub(crate) fn table_schema(&self, table_config: &TableConfiguration) -> Result<SchemaRef> {
        if let Some(schema) = &self.schema {
            return Ok(schema.clone());
        }
        let table_schema: Schema = table_config.logical_schema().as_ref().try_into_arrow()?;
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

    fn map_field(&self, field: FieldRef, partition_cols: &[String]) -> FieldRef {
        if partition_cols.contains(field.name()) && self.wrap_partition_values {
            return match field.data_type() {
                // Only dictionary-encode types that may be large
                // https://github.com/apache/arrow-datafusion/pull/5545
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(wrap_partition_type_in_dict(field.data_type().clone()))
                        .into()
                }
                _ => field,
            };
        }
        if !self.schema_force_view_types {
            return field;
        }
        match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 if self.schema_force_view_types => field
                .as_ref()
                .clone()
                .with_data_type(DataType::Utf8View)
                .into(),
            DataType::Binary | DataType::LargeBinary if self.schema_force_view_types => field
                .as_ref()
                .clone()
                .with_data_type(DataType::BinaryView)
                .into(),
            DataType::Struct(fields) => {
                let new_fields = fields
                    .iter()
                    .map(|f| self.map_field(f.clone(), partition_cols))
                    .collect();
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(new_fields))
                    .into()
            }
            DataType::List(inner) => field
                .as_ref()
                .clone()
                .with_data_type(DataType::List(
                    self.map_field(inner.clone(), partition_cols),
                ))
                .into(),
            DataType::LargeList(inner) => field
                .as_ref()
                .clone()
                .with_data_type(DataType::LargeList(
                    self.map_field(inner.clone(), partition_cols),
                ))
                .into(),
            DataType::ListView(inner) => field
                .as_ref()
                .clone()
                .with_data_type(DataType::ListView(
                    self.map_field(inner.clone(), partition_cols),
                ))
                .into(),
            _ => field,
        }
    }

    // internal helper function to map scalar values
    //
    // This is specifically meant to align file stats values with the parquet
    // scan. We track it here to have one place where view type mapping is handled.
    pub(super) fn map_scalar_value(&self, value: ScalarValue) -> ScalarValue {
        match value {
            ScalarValue::Utf8(Some(v)) if self.schema_force_view_types => {
                ScalarValue::Utf8View(Some(v))
            }
            ScalarValue::Binary(Some(v)) if self.schema_force_view_types => {
                ScalarValue::BinaryView(Some(v))
            }
            other => other,
        }
    }
}

pub(crate) fn supports_filters_pushdown(
    filter: &[&Expr],
    config: &TableConfiguration,
    scan_config: &DeltaScanConfig,
) -> Vec<TableProviderFilterPushDown> {
    let file_id_field = scan_config
        .file_column_name
        .as_deref()
        .unwrap_or(FILE_ID_COLUMN_DEFAULT);

    // Parquet predicate pushdown is enabled only when we can safely apply it at read time.
    // Deletion vectors require preserving row order for selection masks, and row tracking
    // disables predicate pushdown in the read plan.
    let parquet_pushdown_enabled = scan_config.enable_parquet_pushdown
        && !config.is_feature_enabled(&TableFeature::RowTracking)
        && !config.is_feature_enabled(&TableFeature::DeletionVectors);
    filter
        .iter()
        .map(|f| {
            process_predicate(
                f,
                config,
                scan_config,
                file_id_field,
                parquet_pushdown_enabled,
            )
            .pushdown
        })
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
    scan_config: &DeltaScanConfig,
) -> Result<(Option<PredicateRef>, Option<Expr>)> {
    let file_id_field = scan_config
        .file_column_name
        .as_deref()
        .unwrap_or(FILE_ID_COLUMN_DEFAULT);

    let parquet_pushdown_enabled = scan_config.enable_parquet_pushdown
        && !config.is_feature_enabled(&TableFeature::RowTracking)
        && !config.is_feature_enabled(&TableFeature::DeletionVectors);
    let (parquet, kernel): (Vec<_>, Vec<_>) = filters
        .iter()
        .map(|f| {
            process_predicate(
                f,
                config,
                scan_config,
                file_id_field,
                parquet_pushdown_enabled,
            )
        })
        .map(|p| (p.parquet_predicate, p.kernel_predicate))
        .unzip();
    let parquet = if config.is_feature_enabled(&TableFeature::ColumnMapping) {
        conjunction(
            parquet
                .iter()
                .flatten()
                .filter_map(|ex| rewrite_expression((*ex).clone(), config).ok()),
        )
    } else {
        conjunction(parquet.iter().flatten().map(|ex| (*ex).clone()))
    };
    let kernel_terms = kernel.into_iter().flatten().collect_vec();
    let kernel = (!kernel_terms.is_empty()).then(|| Predicate::and_from(kernel_terms));
    Ok((kernel.map(Arc::new), parquet))
}

struct ProcessedPredicate<'a> {
    pub pushdown: TableProviderFilterPushDown,
    pub kernel_predicate: Option<Predicate>,
    pub parquet_predicate: Option<&'a Expr>,
}

fn process_predicate<'a>(
    expr: &'a Expr,
    config: &TableConfiguration,
    scan_config: &DeltaScanConfig,
    file_id_column: &str,
    parquet_pushdown_enabled: bool,
) -> ProcessedPredicate<'a> {
    let cols = config.metadata().partition_columns();
    let only_partition_refs = expr.column_refs().iter().all(|c| cols.contains(&c.name));
    let any_partition_refs =
        only_partition_refs || expr.column_refs().iter().any(|c| cols.contains(&c.name));
    let has_file_id = expr.column_refs().iter().any(|c| file_id_column == c.name);

    if has_file_id {
        // file-id filters cannot be evaluated in kernel and must not be pushed to parquet.
        // Mark as Unsupported so DataFusion keeps a post-scan filter for correctness.
        return ProcessedPredicate {
            pushdown: TableProviderFilterPushDown::Unsupported,
            kernel_predicate: None,
            parquet_predicate: None,
        };
    }

    // TODO(roeap): we may allow pusing predicates referencing partition columns
    // into the parquet scan, if the table has materialized partition columns
    let _has_partition_data = config.is_feature_enabled(&TableFeature::MaterializePartitionColumns);

    // Try to convert the expression into a kernel predicate
    if let Ok(kernel_predicate) = to_delta_predicate(expr) {
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
            // For non-partition predicates we can *attempt* Parquet pushdown, but it is not a
            // correctness boundary (it may be partially applied or skipped). Keep this Inexact so
            // DataFusion retains a post-scan Filter.
            (
                TableProviderFilterPushDown::Inexact,
                parquet_pushdown_enabled.then_some(expr),
            )
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

    // Reject filters that would be pushed down with wrong data type
    // from an overridden schema
    if let (Some(override_schema), Ok(table_schema)) = (
        scan_config.schema.as_ref(),
        delta_kernel::engine::arrow_conversion::TryIntoArrow::<Schema>::try_into_arrow(
            config.physical_schema().as_ref(),
        ),
    ) {
        let has_override_type_mismatch = expr.column_refs().iter().any(|c| {
            if let (Ok(outer_field), Ok(table_field)) = (
                table_schema.field_with_name(c.name.as_str()),
                override_schema.field_with_name(&c.name),
            ) {
                outer_field.data_type() != table_field.data_type()
            } else {
                false
            }
        });
        if has_override_type_mismatch {
            return ProcessedPredicate {
                pushdown: TableProviderFilterPushDown::Unsupported,
                kernel_predicate: None,
                parquet_predicate: None,
            };
        }
    };

    ProcessedPredicate {
        pushdown: TableProviderFilterPushDown::Inexact,
        kernel_predicate: None,
        parquet_predicate: parquet_pushdown_enabled.then_some(expr),
    }
}

fn rewrite_expression(expr: Expr, config: &TableConfiguration) -> Result<Expr> {
    let logical_fields = config.logical_schema().leaves(None);
    let (logical_names, _) = logical_fields.as_ref();
    let physical_schema = config.physical_schema().leaves(None);
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
    use super::*;
    use crate::{
        delta_datafusion::create_session,
        test_utils::{TestResult, open_fs_path},
    };
    use datafusion::logical_expr::and;
    use datafusion::{
        assert_batches_sorted_eq,
        common::ToDFSchema,
        physical_plan::collect,
        prelude::{col, lit},
        scalar::ScalarValue,
    };

    fn schema_has_view_types(schema: &Schema) -> bool {
        schema
            .fields()
            .iter()
            .any(|f| data_type_has_view_types(f.data_type()))
    }

    fn data_type_has_view_types(dt: &DataType) -> bool {
        match dt {
            DataType::Utf8View | DataType::BinaryView => true,
            DataType::Dictionary(_, value) => data_type_has_view_types(value.as_ref()),
            DataType::Map(entry, _) => data_type_has_view_types(entry.data_type()),
            DataType::Struct(fields) => fields
                .iter()
                .any(|f| data_type_has_view_types(f.data_type())),
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::ListView(inner)
            | DataType::FixedSizeList(inner, _) => data_type_has_view_types(inner.data_type()),
            _ => false,
        }
    }

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
            std::slice::from_ref(&expr),
            &DeltaScanConfig::default(),
            None,
        )?;
        let expected_pq =
            col("col-3877fd94-0973-4941-ac6b-646849a1ff65").eq(lit("Anthony Johnson"));
        assert_eq!(scan_plan.parquet_predicate, Some(expected_pq));

        let expr = col(r#""Company Very Short""#).eq(lit("BME"));
        let scan_plan = KernelScanPlan::try_new(
            table.snapshot()?.snapshot().snapshot(),
            None,
            std::slice::from_ref(&expr),
            &DeltaScanConfig::default(),
            None,
        )?;
        assert!(scan_plan.parquet_predicate.is_none());

        let expr = col(r#""Super Name""#).eq(lit("Timothy Lamb"));
        let scan_plan = KernelScanPlan::try_new(
            table.snapshot()?.snapshot().snapshot(),
            None,
            std::slice::from_ref(&expr),
            &DeltaScanConfig::default(),
            None,
        )?;
        let expected_pq = col("col-3877fd94-0973-4941-ac6b-646849a1ff65").eq(lit("Timothy Lamb"));
        assert_eq!(scan_plan.parquet_predicate, Some(expected_pq));

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

        let filter = col(r#""Company Very Short""#).eq(lit("BME"));
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

        // we need to pass a more specific type here since we are not going
        // through datafusions predicate handling.
        let filter =
            col(r#""Company Very Short""#).eq(lit(ScalarValue::Utf8View(Some("BME".to_string()))));
        let scan = provider.scan(&ctx.state(), None, &[filter], None).await?;
        let batches = collect(scan, ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(&expected, &batches);

        let filter = col(r#""Super Name""#).eq(lit("Timothy Lamb"));
        let batches = ctx
            .read_table(provider.clone())?
            .filter(filter.clone())?
            .collect()
            .await?;
        assert_batches_sorted_eq!(&expected, &batches);

        let filter =
            col(r#""Super Name""#).eq(lit(ScalarValue::Utf8View(Some("Timothy Lamb".to_string()))));
        let scan = provider
            .scan(&ctx.state(), None, std::slice::from_ref(&filter), None)
            .await?;
        let batches = collect(scan, ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_column_mapping_direct_provider_scan_for_data_column_filter() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let provider = table.table_provider().await?;
        let ctx = create_session().into_inner();

        let filter =
            col(r#""Super Name""#).eq(lit(ScalarValue::Utf8View(Some("Timothy Lamb".to_string()))));
        let scan = provider.scan(&ctx.state(), None, &[filter], None).await?;
        let batches = collect(scan, ctx.task_ctx()).await?;

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

    #[tokio::test]
    async fn test_scan_schema_contract() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let snapshot = table.snapshot()?.snapshot().snapshot();

        let override_schema = Arc::new(Schema::new(vec![
            Arc::new(arrow_schema::Field::new(
                "Company Very Short",
                DataType::Utf8,
                true,
            )),
            Arc::new(arrow_schema::Field::new("Super Name", DataType::Utf8, true)),
        ]));
        let config = DeltaScanConfig::default().with_schema(override_schema.clone());
        let scan_plan = KernelScanPlan::try_new(snapshot, None, &[], &config, None)?;
        assert_eq!(
            scan_plan.contract.result_schema.as_ref(),
            override_schema.as_ref()
        );
        assert!(!schema_has_view_types(
            scan_plan.contract.result_schema.as_ref()
        ));

        let config = DeltaScanConfig {
            schema_force_view_types: true,
            ..Default::default()
        };
        let scan_plan = KernelScanPlan::try_new(snapshot, None, &[], &config, None)?;
        assert!(schema_has_view_types(
            scan_plan.contract.result_schema.as_ref()
        ));
        assert!(schema_has_view_types(
            scan_plan.parquet_read_schema.as_ref()
        ));

        let expected_parquet_schema = config.physical_arrow_schema(
            scan_plan.scan.snapshot().table_configuration(),
            &scan_plan.scan.physical_schema().as_ref().try_into_arrow()?,
        )?;
        assert_eq!(
            scan_plan.parquet_read_schema.as_ref(),
            expected_parquet_schema.as_ref()
        );

        // `parquet_read_schema` contains only physical file columns (no Delta partitions, no file-id).
        assert!(
            scan_plan
                .parquet_read_schema
                .field_with_name("Company Very Short")
                .is_err()
        );
        assert!(
            scan_plan
                .parquet_read_schema
                .field_with_name(config.file_id_field().name())
                .is_err()
        );

        // Column-mapped tables use logical names in the result schema, but physical names for Parquet reads.
        assert!(
            scan_plan
                .contract
                .result_schema
                .field_with_name("Super Name")
                .is_ok()
        );
        assert!(
            scan_plan
                .parquet_read_schema
                .field_with_name("Super Name")
                .is_err()
        );
        assert!(
            scan_plan
                .parquet_read_schema
                .field_with_name("col-3877fd94-0973-4941-ac6b-646849a1ff65")
                .is_ok()
        );
        assert!(matches!(
            scan_plan
                .parquet_read_schema
                .field_with_name("col-3877fd94-0973-4941-ac6b-646849a1ff65")?
                .data_type(),
            DataType::Utf8View | DataType::BinaryView
        ));

        let config = DeltaScanConfig {
            schema_force_view_types: false,
            ..Default::default()
        };
        let scan_plan = KernelScanPlan::try_new(snapshot, None, &[], &config, None)?;
        assert!(!schema_has_view_types(
            scan_plan.contract.result_schema.as_ref()
        ));
        assert!(!schema_has_view_types(
            scan_plan.parquet_read_schema.as_ref()
        ));

        let mut partitioned_table = open_fs_path("../test/tests/data/delta-0.8.0-partitioned");
        partitioned_table.load().await?;
        let partitioned_snapshot = partitioned_table.snapshot()?.snapshot().snapshot();
        let scan_plan = KernelScanPlan::try_new(
            partitioned_snapshot,
            None,
            &[],
            &DeltaScanConfig::default(),
            None,
        )?;
        assert!(
            scan_plan
                .parquet_read_schema
                .field_with_name("year")
                .is_err()
        );
        assert!(
            scan_plan
                .parquet_read_schema
                .field_with_name("month")
                .is_err()
        );
        assert!(
            scan_plan
                .parquet_read_schema
                .field_with_name("day")
                .is_err()
        );

        Ok(())
    }

    #[test]
    fn test_projected_scan_contract_separates_provider_capability_from_scan_output_requirement()
    -> TestResult {
        let table_schema = Arc::new(Schema::new(vec![
            Arc::new(arrow_schema::Field::new("data", DataType::Utf8, true)),
            Arc::new(arrow_schema::Field::new("letter", DataType::Utf8, true)),
        ]));
        let config = DeltaScanConfig::default().with_file_column_name("file_id");
        let projection = vec![0];
        let provider_schema = Arc::new(Schema::new(vec![
            Arc::new(arrow_schema::Field::new("data", DataType::Utf8, true)),
            Arc::new(arrow_schema::Field::new("letter", DataType::Utf8, true)),
            config.file_id_field(),
        ]));

        let contract = ProjectedScanContract::try_new(
            table_schema,
            provider_schema,
            &config,
            None,
            Some(&projection),
            &[],
        )?;

        assert!(!contract.retain_file_id);
        assert_eq!(
            contract
                .scan_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect_vec(),
            vec!["data"]
        );
        assert_eq!(contract.kernel_projection, Some(vec![0]));
        assert_eq!(contract.result_projection, None);
        assert_eq!(
            contract
                .output_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect_vec(),
            vec!["data"]
        );
        Ok(())
    }

    #[test]
    fn test_projected_scan_contract_keeps_file_id_for_filter_only_queries() -> TestResult {
        let table_schema = Arc::new(Schema::new(vec![
            Arc::new(arrow_schema::Field::new("data", DataType::Utf8, true)),
            Arc::new(arrow_schema::Field::new("letter", DataType::Utf8, true)),
        ]));
        let config = DeltaScanConfig::default().with_file_column_name("file_id");
        let projection = vec![0];
        let filters = vec![col("file_id").eq(lit("file:///tmp/part-0000.parquet"))];
        let provider_schema = Arc::new(Schema::new(vec![
            Arc::new(arrow_schema::Field::new("data", DataType::Utf8, true)),
            Arc::new(arrow_schema::Field::new("letter", DataType::Utf8, true)),
            config.file_id_field(),
        ]));

        let contract = ProjectedScanContract::try_new(
            table_schema,
            provider_schema,
            &config,
            None,
            Some(&projection),
            &filters,
        )?;

        assert!(contract.retain_file_id);
        assert_eq!(contract.kernel_projection, Some(vec![0]));
        assert_eq!(contract.result_projection, None);
        assert_eq!(
            contract
                .scan_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect_vec(),
            vec!["data"]
        );
        assert_eq!(
            contract
                .output_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect_vec(),
            vec!["data", "file_id"]
        );
        Ok(())
    }

    #[test]
    fn test_projected_scan_contract_tracks_internal_and_output_file_id_requirements() -> TestResult
    {
        let config = DeltaScanConfig::default().with_file_column_name("file_id");
        let table_schema = Arc::new(Schema::new(vec![
            Arc::new(arrow_schema::Field::new("data", DataType::Utf8, true)),
            Arc::new(arrow_schema::Field::new("letter", DataType::Utf8, true)),
        ]));
        let projection = vec![0];
        let filters = vec![
            col("letter").eq(lit("b")),
            col("file_id").eq(lit("file:///tmp/part-0000.parquet")),
        ];
        let provider_schema = Arc::new(Schema::new(vec![
            Arc::new(arrow_schema::Field::new("data", DataType::Utf8, true)),
            Arc::new(arrow_schema::Field::new("letter", DataType::Utf8, true)),
            config.file_id_field(),
        ]));
        let contract = ProjectedScanContract::try_new(
            table_schema,
            provider_schema,
            &config,
            None,
            Some(&projection),
            &filters,
        )?;
        let expected_result_schema = Schema::new(vec![Arc::new(arrow_schema::Field::new(
            "data",
            DataType::Utf8,
            true,
        ))]);

        assert_eq!(contract.result_schema.as_ref(), &expected_result_schema);
        assert_eq!(
            contract
                .output_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect_vec(),
            vec!["data", "file_id"]
        );
        assert_eq!(contract.kernel_projection, Some(vec![0, 1]));
        assert_eq!(contract.result_projection, Some(vec![0]));
        assert_eq!(
            contract
                .scan_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect_vec(),
            vec!["data", "letter"]
        );
        assert!(contract.retain_file_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_kernel_scan_plan_carries_explicit_parquet_predicate_schema() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let snapshot = table.snapshot()?.snapshot().snapshot();
        let config = DeltaScanConfig::default().with_file_column_name("file_id");
        let filter = col(r#""Super Name""#).eq(lit("Anthony Johnson"));
        let scan_plan = KernelScanPlan::try_new(snapshot, None, &[filter], &config, None)?;

        assert!(
            scan_plan
                .parquet_predicate_schema
                .field_with_name("col-3877fd94-0973-4941-ac6b-646849a1ff65")
                .is_ok()
        );
        assert!(
            scan_plan
                .parquet_predicate_schema
                .field_with_name("file_id")
                .is_ok()
        );
        assert!(
            scan_plan
                .parquet_predicate_schema
                .field_with_name("Super Name")
                .is_err()
        );
        let session = create_session().into_inner();
        session.state().create_physical_expr(
            scan_plan
                .parquet_predicate
                .clone()
                .expect("expected parquet predicate for column-mapped filter"),
            &scan_plan.parquet_predicate_schema.clone().to_dfschema()?,
        )?;

        Ok(())
    }

    #[tokio::test]
    async fn test_projected_scan_plan_preserves_column_mapping_annotations() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let snapshot = table.snapshot()?.snapshot().snapshot();
        let config = DeltaScanConfig::default();
        let table_schema = config.table_schema(snapshot.table_configuration())?;
        let projection = vec![table_schema.index_of("Super Name")?];
        let filter = col(r#""Company Very Short""#).eq(lit("BME"));

        let scan_plan =
            KernelScanPlan::try_new(snapshot, Some(&projection), &[filter], &config, None)?;

        let field = scan_plan
            .scan
            .logical_schema()
            .field("Super Name")
            .expect("projected scan should keep projected column");
        assert!(matches!(
            field.metadata().get("delta.columnMapping.id"),
            Some(delta_kernel::schema::MetadataValue::Number(_))
        ));
        assert!(matches!(
            field.metadata().get("delta.columnMapping.physicalName"),
            Some(delta_kernel::schema::MetadataValue::String(_))
        ));
        assert_eq!(scan_plan.contract.result_projection, Some(vec![0]));

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_exactness_policy() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/table_with_column_mapping");
        table.load().await?;

        let table_config = table.snapshot()?.snapshot().table_configuration();
        let scan_config = DeltaScanConfig::default();

        // Partition-only filters are enforced by the kernel scan (exact).
        let partition_only = col(r#""Company Very Short""#).eq(lit("BME"));
        assert_eq!(
            supports_filters_pushdown(&[&partition_only], table_config, &scan_config),
            vec![TableProviderFilterPushDown::Exact]
        );

        // Non-partition filters are best-effort (Parquet pruning/pushdown); keep inexact so DF keeps
        // a correctness filter above the scan.
        let data_only = col(r#""Super Name""#).eq(lit("Timothy Lamb"));
        assert_eq!(
            supports_filters_pushdown(&[&data_only], table_config, &scan_config),
            vec![TableProviderFilterPushDown::Inexact]
        );

        // Mixed partition + data filters are also inexact.
        assert_eq!(
            supports_filters_pushdown(&[&partition_only, &data_only], table_config, &scan_config),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Inexact,
            ]
        );

        let file_id_only = col(FILE_ID_COLUMN_DEFAULT).eq(lit("part-00000"));
        assert_eq!(
            supports_filters_pushdown(&[&file_id_only], table_config, &scan_config),
            vec![TableProviderFilterPushDown::Unsupported]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dedicated_skipping_predicate_with_no_survivors_falls_back_to_full_scan()
    -> TestResult {
        let mut table = open_fs_path("../test/tests/data/delta-0.8.0-partitioned");
        table.load().await?;

        let snapshot = table.snapshot()?.snapshot().snapshot();
        let scan_plan = KernelScanPlan::try_new(
            snapshot,
            None,
            &[],
            &DeltaScanConfig::default(),
            Some(vec![
                col("year").in_list(vec![lit(ScalarValue::Utf8(None))], false),
            ]),
        )?;

        assert!(scan_plan.scan.physical_predicate().is_none());
        assert!(scan_plan.parquet_predicate.is_none());

        Ok(())
    }

    #[test]
    fn test_file_id_field_uses_canonical_file_id_type() {
        let config = DeltaScanConfig::default();
        let field = config.file_id_field();
        assert_eq!(
            field.data_type(),
            &crate::delta_datafusion::file_id::file_id_data_type()
        );
    }

    #[test]
    fn test_retained_row_index_field_ignores_incomplete_internal_contract() {
        let schema = Arc::new(Schema::empty());
        let contract = ProjectedScanContract {
            result_schema: Arc::clone(&schema),
            scan_schema: Arc::clone(&schema),
            output_schema: schema,
            kernel_projection: None,
            result_projection: None,
            file_id_field: DeltaScanConfig::default().file_id_field(),
            retain_file_id: false,
            row_index_field: None,
            retain_row_index: true,
        };

        assert!(contract.retained_row_index_field().is_none());
    }

    /// The scan in this test only projects one column. This requires the scan plan to add the
    /// columns required for the filter to the physical schema. This test should assert that
    /// these additional columns in the physical schema are created deterministically.
    #[tokio::test]
    async fn test_scan_with_projection_has_stable_schema_for_filters() {
        let mut table = open_fs_path("../test/tests/data/COVID-19_NYT");
        table.load().await.unwrap();

        let snapshot = table.snapshot().unwrap().snapshot().snapshot();
        let filter = and(
            col("state").eq(lit("Louisiana")),
            col("county").eq(lit("Cameron")),
        );
        let scan_plan = KernelScanPlan::try_new(
            snapshot,
            Some(&vec![4]),
            &[filter],
            &DeltaScanConfig::default(),
            None,
        )
        .unwrap();

        let expected_schema = snapshot
            .schema()
            .project(&["cases", "county", "state"])
            .unwrap();
        // Assert string representation as the equality check is order-insensitive.
        assert_eq!(
            scan_plan.scan.physical_schema().to_string(),
            expected_schema.to_string()
        )
    }
}
