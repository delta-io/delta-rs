//! Parquet files as DataFusion's own `ParquetSource`.
//!
//! The expression adapter supplies the kernel's column resolution: parquet columns are matched by
//! field id, then by name ([`expr_adapter`](super::expr_adapter)), a missing nullable column is
//! filled with NULLs, and a missing non-nullable column is an error.
//!
//! Nested nullability is relaxed on the read side only ([`expr_adapter`](super::expr_adapter)):
//! Spark writes every
//! nested leaf as `optional`, which DataFusion refuses at planning time to read into a
//! non-nullable field. [`with_declared_nullability`] casts the rows back to the declared types
//! above the source; that cast is where a NULL under a non-nullable nested field becomes an error,
//! and where a NULL struct's NULLs reach its children.
//!
//! The source reads with the session's parquet options ([`table_parquet_options`]) and its file
//! metadata cache, so a footer the kernel already fetched is not parsed again.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_datasource::TableSchema;

use super::ScanLayout;
use super::declared_type::DeclaredTypeCast;
use super::expr_adapter::{DeltaPhysicalExprAdapterFactory, relax_schema_nested_nullability};

/// DataFusion's `TableParquetOptions` for a `ParquetSource`, inheriting the session's parquet
/// execution settings.
///
/// INT96 timestamps are read as microseconds, the precision of Delta timestamps, unless the
/// session sets `coerce_int96`.
pub fn table_parquet_options(session: &dyn Session) -> TableParquetOptions {
    let mut global = session.config().options().execution.parquet.clone();
    global.coerce_int96.get_or_insert_with(|| "us".to_string());
    TableParquetOptions {
        global,
        ..Default::default()
    }
}

/// The file schema with nested nullability relaxed, plus the per-file value columns.
pub(super) fn table_schema(layout: &ScanLayout) -> TableSchema {
    let read_schema = Arc::new(relax_schema_nested_nullability(&layout.file_schema));
    TableSchema::builder(read_schema)
        .with_table_partition_cols(layout.constant_fields.clone())
        .build()
}

/// A scan of `store_url`'s files through `ParquetSource`, with nested nullability relaxed.
pub(super) fn scan_config(
    session: &dyn Session,
    table_schema: &TableSchema,
    store_url: &ObjectStoreUrl,
) -> DFResult<FileScanConfigBuilder> {
    let runtime = session.runtime_env();
    let reader_factory = CachedParquetFileReaderFactory::new(
        runtime.object_store(store_url)?,
        runtime.cache_manager.get_file_metadata_cache(),
    );
    let options = table_parquet_options(session);
    let metadata_size_hint = options.global.metadata_size_hint;

    let mut source = ParquetSource::new(table_schema.clone())
        .with_table_parquet_options(options)
        .with_parquet_file_reader_factory(Arc::new(reader_factory));
    if let Some(hint) = metadata_size_hint {
        source = source.with_metadata_size_hint(hint);
    }

    Ok(
        FileScanConfigBuilder::new(store_url.clone(), Arc::new(source))
            .with_expr_adapter(Some(Arc::new(DeltaPhysicalExprAdapterFactory))),
    )
}

/// Casts `plan`'s rows to `schema`: the declared types, nested nullability included.
///
/// Arrow's struct, list and map casts refuse NULLs under a non-nullable field, so a non-nullable
/// nested field that is missing from a file, or that holds NULLs, is an error here, as it is in
/// the kernel's default engine ([`DeclaredTypeCast`]). Every struct column goes through the cast,
/// also when its type is already the declared one, to get its children's NULLs.
pub(super) fn with_declared_nullability(
    plan: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let read_schema = plan.schema();
    let columns = schema
        .fields()
        .iter()
        .zip(read_schema.fields())
        .enumerate()
        .map(|(index, (field, read_field))| {
            let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), index));
            let expr: Arc<dyn PhysicalExpr> = if read_field.data_type() == field.data_type()
                && !matches!(field.data_type(), DataType::Struct(_))
            {
                column
            } else {
                Arc::new(DeclaredTypeCast::new(column, Arc::clone(field)))
            };
            (expr, field.name().clone())
        });
    Ok(Arc::new(ProjectionExec::try_new(columns, plan)?))
}
