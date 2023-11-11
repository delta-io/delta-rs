//! This module contains the implementation of the DeltaTable scan operator for datafusion.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use arrow::error::ArrowError;
use chrono::{NaiveDateTime, TimeZone, Utc};
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileScanConfig,
};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::Expr;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};

use super::{get_null_of_arrow_type, to_correct_scalar_value};
use super::{logical_expr_to_physical_expr, logical_schema, PATH_COLUMN};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;
use crate::logstore::LogStoreRef;
use crate::table::builder::ensure_table_uri;
use crate::table::state::DeltaTableState;

#[derive(Debug, Clone, Default)]
/// Used to specify if additional metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Include the source path for each record. The name of this column is determine by `file_column_name`
    pub(super) include_file_column: bool,
    /// Column name that contains the source path.
    ///
    /// If include_file_column is true and the name is None then it will be auto-generated
    /// Otherwise the user provided name will be used
    pub(super) file_column_name: Option<String>,
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Indicate that a column containing a records file path is included and column name is user defined.
    pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
        self.file_column_name = Some(name.to_string());
        self.include_file_column = true;
        self
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &DeltaTableState) -> DeltaResult<DeltaScanConfig> {
        let input_schema = snapshot.arrow_schema(false)?;
        let mut file_column_name = None;
        let mut column_names: HashSet<&String> = HashSet::new();
        for field in input_schema.fields.iter() {
            column_names.insert(field.name());
        }

        if self.include_file_column {
            match &self.file_column_name {
                Some(name) => {
                    if column_names.contains(name) {
                        return Err(DeltaTableError::Generic(format!(
                            "Unable to add file path column since column with name {} exits",
                            name
                        )));
                    }

                    file_column_name = Some(name.to_owned())
                }
                None => {
                    let prefix = PATH_COLUMN;
                    let mut idx = 0;
                    let mut name = prefix.to_owned();

                    while column_names.contains(&name) {
                        idx += 1;
                        name = format!("{}_{}", prefix, idx);
                    }

                    file_column_name = Some(name);
                }
            }
        }

        Ok(DeltaScanConfig { file_column_name })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Include additional metadata columns during a [`DeltaScan`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
}

#[derive(Debug)]
pub(crate) struct DeltaScanBuilder<'a> {
    snapshot: &'a DeltaTableState,
    log_store: LogStoreRef,
    filter: Option<Expr>,
    state: &'a SessionState,
    projection: Option<&'a Vec<usize>>,
    limit: Option<usize>,
    files: Option<&'a [Add]>,
    config: DeltaScanConfig,
    schema: Option<SchemaRef>,
}

impl<'a> DeltaScanBuilder<'a> {
    pub fn new(
        snapshot: &'a DeltaTableState,
        log_store: LogStoreRef,
        state: &'a SessionState,
    ) -> Self {
        DeltaScanBuilder {
            snapshot,
            log_store,
            filter: None,
            state,
            files: None,
            projection: None,
            limit: None,
            config: DeltaScanConfig::default(),
            schema: None,
        }
    }

    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_files(mut self, files: &'a [Add]) -> Self {
        self.files = Some(files);
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub async fn build(self) -> DeltaResult<DeltaScan> {
        let config = self.config;
        let schema = match self.schema {
            Some(schema) => schema,
            None => {
                self.snapshot
                    .physical_arrow_schema(self.log_store.object_store())
                    .await?
            }
        };
        let logical_schema = logical_schema(self.snapshot, &config)?;

        let logical_schema = if let Some(used_columns) = self.projection {
            let mut fields = vec![];
            for idx in used_columns {
                fields.push(logical_schema.field(*idx).to_owned());
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            logical_schema
        };

        let logical_filter = self
            .filter
            .map(|expr| logical_expr_to_physical_expr(&expr, &logical_schema));

        // Perform Pruning of files to scan
        let files = match self.files {
            Some(files) => files.to_owned(),
            None => {
                if let Some(predicate) = &logical_filter {
                    let pruning_predicate =
                        PruningPredicate::try_new(predicate.clone(), logical_schema.clone())?;
                    let files_to_prune = pruning_predicate.prune(self.snapshot)?;
                    self.snapshot
                        .files()
                        .iter()
                        .zip(files_to_prune.into_iter())
                        .filter_map(
                            |(action, keep)| {
                                if keep {
                                    Some(action.to_owned())
                                } else {
                                    None
                                }
                            },
                        )
                        .collect()
                } else {
                    self.snapshot.files().to_owned()
                }
            }
        };

        // TODO we group files together by their partition values. If the table is partitioned
        // and partitions are somewhat evenly distributed, probably not the worst choice ...
        // However we may want to do some additional balancing in case we are far off from the above.
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

        let table_partition_cols = &self
            .snapshot
            .current_metadata()
            .ok_or(DeltaTableError::NoMetadata)?
            .partition_columns;

        for action in files.iter() {
            let mut part = partitioned_file_from_action(action, table_partition_cols, &schema);

            if config.file_column_name.is_some() {
                part.partition_values
                    .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        action.path.clone(),
                    ))));
            }

            file_groups
                .entry(part.partition_values.clone())
                .or_default()
                .push(part);
        }

        let file_schema = Arc::new(ArrowSchema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<arrow::datatypes::FieldRef>>(),
        ));

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|c| Ok((c.to_owned(), schema.field_with_name(c)?.data_type().clone())))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        if let Some(file_column_name) = &config.file_column_name {
            table_partition_cols.push((
                file_column_name.clone(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ));
        }

        let scan = ParquetFormat::new()
            .create_physical_plan(
                self.state,
                FileScanConfig {
                    object_store_url: self.log_store.object_store_url(),
                    file_schema,
                    file_groups: file_groups.into_values().collect(),
                    statistics: self.snapshot.datafusion_table_statistics(),
                    projection: self.projection.cloned(),
                    limit: self.limit,
                    table_partition_cols,
                    output_ordering: vec![],
                    infinite_source: false,
                },
                logical_filter.as_ref(),
            )
            .await?;

        Ok(DeltaScan {
            table_uri: ensure_table_uri(self.log_store.root_uri())?.as_str().into(),
            parquet_scan: scan,
            config,
            logical_schema,
        })
    }
}

// TODO: this will likely also need to perform column mapping later when we support reader protocol v2
/// A wrapper for parquet scans
#[derive(Debug)]
pub struct DeltaScan {
    /// The URL of the ObjectStore root
    pub table_uri: String,
    /// Column that contains an index that maps to the original metadata Add
    pub config: DeltaScanConfig,
    /// The parquet scan to wrap
    pub parquet_scan: Arc<dyn ExecutionPlan>,
    /// The schema of the table to be used when evaluating expressions
    pub logical_schema: Arc<ArrowSchema>,
}

impl DisplayAs for DeltaScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaScan")
    }
}

impl ExecutionPlan for DeltaScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parquet_scan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.parquet_scan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.parquet_scan.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.parquet_scan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        ExecutionPlan::with_new_children(self.parquet_scan.clone(), children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.parquet_scan.execute(partition, context)
    }

    fn statistics(&self) -> Statistics {
        self.parquet_scan.statistics()
    }
}

fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &ArrowSchema,
) -> PartitionedFile {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            action
                .partition_values
                .get(part)
                .map(|val| {
                    schema
                        .field_with_name(part)
                        .map(|field| match val {
                            Some(value) => to_correct_scalar_value(
                                &serde_json::Value::String(value.to_string()),
                                field.data_type(),
                            )
                            .unwrap_or(ScalarValue::Null),
                            None => get_null_of_arrow_type(field.data_type())
                                .unwrap_or(ScalarValue::Null),
                        })
                        .unwrap_or(ScalarValue::Null)
                })
                .unwrap_or(ScalarValue::Null)
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    let last_modified =
        Utc.from_utc_datetime(&NaiveDateTime::from_timestamp_opt(ts_secs, ts_ns as u32).unwrap());
    PartitionedFile {
        object_meta: ObjectMeta {
            last_modified,
            ..action.try_into().unwrap()
        },
        partition_values,
        range: None,
        extensions: None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType as ArrowDataType, Field};
    use chrono::{TimeZone, Utc};
    use object_store::path::Path;

    use super::*;

    #[test]
    fn test_partitioned_file_from_action() {
        let mut partition_values = std::collections::HashMap::new();
        partition_values.insert("month".to_string(), Some("1".to_string()));
        partition_values.insert("year".to_string(), Some("2015".to_string()));
        let action = Add {
            path: "year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string(),
            size: 10644,
            partition_values,
            modification_time: 1660497727833,
            partition_values_parsed: None,
            data_change: true,
            stats: None,
            deletion_vector: None,
            stats_parsed: None,
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
        };
        let schema = ArrowSchema::new(vec![
            Field::new("year", ArrowDataType::Int64, true),
            Field::new("month", ArrowDataType::Int64, true),
        ]);

        let part_columns = vec!["year".to_string(), "month".to_string()];
        let file = partitioned_file_from_action(&action, &part_columns, &schema);
        let ref_file = PartitionedFile {
            object_meta: object_store::ObjectMeta {
                location: Path::from("year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string()), 
                last_modified: Utc.timestamp_millis_opt(1660497727833).unwrap(),
                size: 10644,
                e_tag: None
            },
            partition_values: [ScalarValue::Int64(Some(2015)), ScalarValue::Int64(Some(1))].to_vec(),
            range: None,
            extensions: None,
        };
        assert_eq!(file.partition_values, ref_file.partition_values)
    }
}
